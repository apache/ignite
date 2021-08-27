/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.client.service.impl;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.exception.RaftException;
import org.apache.ignite.raft.client.message.ActionRequest;
import org.apache.ignite.raft.client.message.ActionResponse;
import org.apache.ignite.raft.client.message.AddLearnersRequest;
import org.apache.ignite.raft.client.message.AddPeersRequest;
import org.apache.ignite.raft.client.message.ChangePeersResponse;
import org.apache.ignite.raft.client.message.GetLeaderRequest;
import org.apache.ignite.raft.client.message.GetLeaderResponse;
import org.apache.ignite.raft.client.message.GetPeersRequest;
import org.apache.ignite.raft.client.message.GetPeersResponse;
import org.apache.ignite.raft.client.message.RaftClientMessagesFactory;
import org.apache.ignite.raft.client.message.RaftErrorResponse;
import org.apache.ignite.raft.client.message.RemoveLearnersRequest;
import org.apache.ignite.raft.client.message.RemovePeersRequest;
import org.apache.ignite.raft.client.message.SnapshotRequest;
import org.apache.ignite.raft.client.message.TransferLeadershipRequest;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;

import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.ThreadLocalRandom.current;
import static org.apache.ignite.raft.client.RaftErrorCode.LEADER_CHANGED;
import static org.apache.ignite.raft.client.RaftErrorCode.NO_LEADER;

/**
 * The implementation of {@link RaftGroupService}
 */
public class RaftGroupServiceImpl implements RaftGroupService {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(RaftGroupServiceImpl.class);

    /** */
    private volatile long timeout;

    /** */
    private final String groupId;

    /** */
    private final RaftClientMessagesFactory factory;

    /** */
    private volatile Peer leader;

    /** */
    private volatile List<Peer> peers;

    /** */
    private volatile List<Peer> learners;

    /** */
    private final ClusterService cluster;

    /** */
    private final long retryDelay;

    /** */
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    /**
     * Constructor.
     *
     * @param groupId Group id.
     * @param cluster A cluster.
     * @param factory A message factory.
     * @param timeout Request timeout.
     * @param peers Initial group configuration.
     * @param leader Group leader.
     * @param retryDelay Retry delay.
     */
    private RaftGroupServiceImpl(
        String groupId,
        ClusterService cluster,
        RaftClientMessagesFactory factory,
        int timeout,
        List<Peer> peers,
        Peer leader,
        long retryDelay
    ) {
        this.cluster = requireNonNull(cluster);
        this.peers = requireNonNull(peers);
        this.factory = factory;
        this.timeout = timeout;
        this.groupId = groupId;
        this.retryDelay = retryDelay;
        this.leader = leader;
    }

    /**
     * Starts raft group service.
     *
     * @param groupId Raft group id.
     * @param cluster Cluster service.
     * @param factory Message factory.
     * @param timeout Timeout.
     * @param peers List of all peers.
     * @param getLeader {@code True} to get the group's leader upon service creation.
     * @param retryDelay Retry delay.
     * @return Future representing pending completion of the operation.
     */
    public static CompletableFuture<RaftGroupService> start(
        String groupId,
        ClusterService cluster,
        RaftClientMessagesFactory factory,
        int timeout,
        List<Peer> peers,
        boolean getLeader,
        long retryDelay
    ) {
        var service = new RaftGroupServiceImpl(groupId, cluster, factory, timeout, peers, null, retryDelay);

        if (!getLeader) {
            return CompletableFuture.completedFuture(service);
        }

        return service.refreshLeader().handle((unused, throwable) -> {
            if (throwable != null)
                LOG.error("Failed to refresh a leader", throwable);

            return service;
        });
    }

    /** {@inheritDoc} */
    @Override public @NotNull String groupId() {
        return groupId;
    }

    /** {@inheritDoc} */
    @Override public long timeout() {
        return timeout;
    }

    /** {@inheritDoc} */
    @Override public void timeout(long newTimeout) {
        this.timeout = newTimeout;
    }

    /** {@inheritDoc} */
    @Override public Peer leader() {
        return leader;
    }

    /** {@inheritDoc} */
    @Override public List<Peer> peers() {
        return peers;
    }

    /** {@inheritDoc} */
    @Override public List<Peer> learners() {
        return learners;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> refreshLeader() {
        GetLeaderRequest req = factory.getLeaderRequest().groupId(groupId).build();

        CompletableFuture<GetLeaderResponse> fut = new CompletableFuture<>();

        sendWithRetry(randomNode(), req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            leader = resp.leader();

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> refreshMembers(boolean onlyAlive) {
        GetPeersRequest req = factory.getPeersRequest().onlyAlive(onlyAlive).groupId(groupId).build();

        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> refreshMembers(onlyAlive));

        CompletableFuture<GetPeersResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            peers = resp.peers();
            learners = resp.learners();

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> addPeers(List<Peer> peers) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> addPeers(peers));

        AddPeersRequest req = factory.addPeersRequest().groupId(groupId).peers(peers).build();

        CompletableFuture<ChangePeersResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            this.peers = resp.newPeers();

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> removePeers(List<Peer> peers) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> removePeers(peers));

        RemovePeersRequest req = factory.removePeersRequest().groupId(groupId).peers(peers).build();

        CompletableFuture<ChangePeersResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            this.peers = resp.newPeers();

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> addLearners(List<Peer> learners) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> addLearners(learners));

        AddLearnersRequest req = factory.addLearnersRequest().groupId(groupId).learners(learners).build();

        CompletableFuture<ChangePeersResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            this.learners = resp.newPeers();

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> removeLearners(List<Peer> learners) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> removeLearners(learners));

        RemoveLearnersRequest req = factory.removeLearnersRequest().groupId(groupId).learners(learners).build();

        CompletableFuture<ChangePeersResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            this.learners = resp.newPeers();

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> snapshot(Peer peer) {
        SnapshotRequest req = factory.snapshotRequest().groupId(groupId).build();

        // Disable the timeout for a snapshot request.
        CompletableFuture<NetworkMessage> fut = cluster.messagingService().invoke(peer.address(), req, Integer.MAX_VALUE);

        return fut.thenApply(resp -> {
            if (resp != null) {
                RaftErrorResponse resp0 = (RaftErrorResponse) resp;

                if (resp0.errorCode() != null)
                    throw new RaftException(resp0.errorCode(), resp0.errorMessage());
            }

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> transferLeadership(Peer newLeader) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> transferLeadership(newLeader));

        TransferLeadershipRequest req = factory.transferLeadershipRequest().groupId(groupId).newLeader(newLeader).build();

        CompletableFuture<?> fut = cluster.messagingService().invoke(newLeader.address(), req, timeout);

        return fut.thenApply(resp -> null);
    }

    /** {@inheritDoc} */
    @Override public <R> CompletableFuture<R> run(Command cmd) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> run(cmd));

        ActionRequest req = factory.actionRequest().command(cmd).groupId(groupId).readOnlySafe(true).build();

        CompletableFuture<ActionResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> (R) resp.result());
    }

    /**
     * {@inheritDoc}
     */
    @Override public <R> CompletableFuture<R> run(Peer peer, ReadCommand cmd) {
        ActionRequest req = factory.actionRequest().command(cmd).groupId(groupId).readOnlySafe(false).build();

        CompletableFuture<?> fut = cluster.messagingService().invoke(peer.address(), req, timeout);

        return fut.thenApply(resp -> (R) ((ActionResponse) resp).result());
    }

    /** {@inheritDoc} */
    @Override public void shutdown() {
        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
    }

    /**
     * Retries a request until success or timeout.
     *
     * @param peer Target peer.
     * @param req The request.
     * @param stopTime Stop time.
     * @param fut The future.
     * @param <R> Response type.
     */
    private <R> void sendWithRetry(Peer peer, Object req, long stopTime, CompletableFuture<R> fut) {
        if (currentTimeMillis() >= stopTime) {
            fut.completeExceptionally(new TimeoutException());

            return;
        }

        CompletableFuture<?> fut0 = cluster.messagingService().invoke(peer.address(), (NetworkMessage) req, timeout);

        fut0.whenComplete(new BiConsumer<Object, Throwable>() {
            @Override public void accept(Object resp, Throwable err) {
                if (err != null) {
                    if (recoverable(err)) {
                        executor.schedule(() -> {
                            sendWithRetry(randomNode(), req, stopTime, fut);

                            return null;
                        }, retryDelay, TimeUnit.MILLISECONDS);
                    }
                    else
                        fut.completeExceptionally(err);
                }
                else if (resp instanceof RaftErrorResponse) {
                    RaftErrorResponse resp0 = (RaftErrorResponse) resp;

                    if (resp0.errorCode() == null) { // Handle OK response.
                        leader = peer; // The OK response was received from a leader.

                        fut.complete(null); // Void response.
                    }
                    else if (resp0.errorCode().equals(NO_LEADER)) {
                        executor.schedule(() -> {
                            sendWithRetry(randomNode(), req, stopTime, fut);

                            return null;
                        }, retryDelay, TimeUnit.MILLISECONDS);
                    }
                    else if (resp0.errorCode().equals(LEADER_CHANGED)) {
                        leader = resp0.newLeader(); // Update a leader.

                        executor.schedule(() -> {
                            sendWithRetry(resp0.newLeader(), req, stopTime, fut);

                            return null;
                        }, retryDelay, TimeUnit.MILLISECONDS);
                    }
                    else
                        fut.completeExceptionally(new RaftException(resp0.errorCode(), resp0.errorMessage()));
                }
                else {
                    leader = peer; // The OK response was received from a leader.

                    fut.complete((R) resp);
                }
            }
        });
    }

    /**
     * Checks if an error is recoverable, for example, {@link java.net.ConnectException}.
     * @param t The throwable.
     * @return {@code True} if this is a recoverable exception.
     */
    private boolean recoverable(Throwable t) {
        return t.getCause() instanceof IOException;
    }

    /**
     * @return Random node.
     */
    private Peer randomNode() {
        List<Peer> peers0 = peers;

        if (peers0 == null || peers0.isEmpty())
            return null;

        return peers0.get(current().nextInt(peers0.size()));
    }
}

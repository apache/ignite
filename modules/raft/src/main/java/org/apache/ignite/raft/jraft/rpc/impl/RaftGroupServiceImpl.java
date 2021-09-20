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

package org.apache.ignite.raft.jraft.rpc.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.ActionResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.jetbrains.annotations.NotNull;

import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.ThreadLocalRandom.current;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.AddLearnersRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.AddPeerRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.AddPeerResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.GetLeaderRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.GetLeaderResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.GetPeersRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.GetPeersResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.LearnersOpResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.RemoveLearnersRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.RemovePeerRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.RemovePeerResponse;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.ResetLearnersRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.SnapshotRequest;
import static org.apache.ignite.raft.jraft.rpc.CliRequests.TransferLeaderRequest;

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
    private final RaftMessagesFactory factory;

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

    /** TODO: Use shared executors instead https://issues.apache.org/jira/browse/IGNITE-15136 */
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("Raft-Group-Service-Pool"));

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
        RaftMessagesFactory factory,
        int timeout,
        List<Peer> peers,
        Peer leader,
        long retryDelay
    ) {
        this.cluster = requireNonNull(cluster);
        this.peers = requireNonNull(peers);
        this.learners = Collections.emptyList();
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
        RaftMessagesFactory factory,
        int timeout,
        List<Peer> peers,
        boolean getLeader,
        long retryDelay
    ) {
        var service = new RaftGroupServiceImpl(groupId, cluster, factory, timeout, peers, null, retryDelay);

        if (!getLeader)
            return CompletableFuture.completedFuture(service);

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
            leader = parsePeer(resp.leaderId());

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
            peers = parsePeerList(resp.peersList());
            learners = parsePeerList(resp.learnersList());

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> addPeer(Peer peer) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> addPeer(peer));

        AddPeerRequest req = factory.addPeerRequest().groupId(groupId).peerId(PeerId.fromPeer(peer).toString()).build();

        CompletableFuture<AddPeerResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            this.peers = parsePeerList(resp.newPeersList());

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> removePeer(Peer peer) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> removePeer(peer));

        RemovePeerRequest req = factory.removePeerRequest().groupId(groupId).peerId(PeerId.fromPeer(peer).toString()).build();

        CompletableFuture<RemovePeerResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            this.peers = parsePeerList(resp.newPeersList());

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> changePeers(List<Peer> peers) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> changePeers(peers));

        List<String> peersToChange = peers.stream().map(p -> PeerId.fromPeer(p).toString())
            .collect(Collectors.toList());

        ChangePeersRequest req = factory.changePeersRequest().groupId(groupId)
            .newPeersList(peersToChange).build();

        CompletableFuture<ChangePeersResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            this.peers = parsePeerList(resp.newPeersList());

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> addLearners(List<Peer> learners) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> addLearners(learners));

        List<String> lrns = learners.stream().map(p -> PeerId.fromPeer(p).toString()).collect(Collectors.toList());
        AddLearnersRequest req = factory.addLearnersRequest().groupId(groupId).learnersList(lrns).build();

        CompletableFuture<LearnersOpResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            this.learners = parsePeerList(resp.newLearnersList());

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> removeLearners(List<Peer> learners) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> addLearners(learners));

        List<String> lrns = learners.stream().map(p -> PeerId.fromPeer(p).toString()).collect(Collectors.toList());
        RemoveLearnersRequest req = factory.removeLearnersRequest().groupId(groupId).learnersList(lrns).build();

        CompletableFuture<LearnersOpResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            this.learners = parsePeerList(resp.newLearnersList());

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> resetLearners(List<Peer> learners) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> resetLearners(learners));

        List<String> lrns = learners.stream().map(p -> PeerId.fromPeer(p).toString()).collect(Collectors.toList());
        ResetLearnersRequest req = factory.resetLearnersRequest().groupId(groupId).learnersList(lrns).build();

        CompletableFuture<LearnersOpResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            this.learners = parsePeerList(resp.newLearnersList());

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> snapshot(Peer peer) {
        SnapshotRequest req = factory.snapshotRequest().groupId(groupId).build();

        // Disable the timeout for a snapshot request.
        CompletableFuture<NetworkMessage> fut = cluster.messagingService().invoke(peer.address(), req, Integer.MAX_VALUE);

        return fut.thenCompose(resp -> {
            if (resp != null) {
                RpcRequests.ErrorResponse resp0 = (RpcRequests.ErrorResponse) resp;

                if (resp0.errorCode() != RaftError.SUCCESS.getNumber())
                    return CompletableFuture.failedFuture(new RaftException(RaftError.forNumber(resp0.errorCode()), resp0.errorMsg()));
            }

            return CompletableFuture.completedFuture(null);
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> transferLeadership(Peer newLeader) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> transferLeadership(newLeader));

        TransferLeaderRequest req = factory.transferLeaderRequest()
            .groupId(groupId).leaderId(PeerId.fromPeer(newLeader).toString()).build();

        CompletableFuture<NetworkMessage> fut = cluster.messagingService().invoke(newLeader.address(), req, timeout);

        return fut.thenCompose(resp -> {
            if (resp != null) {
                RpcRequests.ErrorResponse resp0 = (RpcRequests.ErrorResponse) resp;

                if (resp0.errorCode() != RaftError.SUCCESS.getNumber())
                    CompletableFuture.failedFuture(
                        new RaftException(
                            RaftError.forNumber(resp0.errorCode()), resp0.errorMsg()
                        )
                    );
                else
                    this.leader = newLeader;
            }

            return CompletableFuture.completedFuture(null);
        });
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
                else if (resp instanceof RpcRequests.ErrorResponse) {
                    RpcRequests.ErrorResponse resp0 = (RpcRequests.ErrorResponse) resp;

                    if (resp0.errorCode() == RaftError.SUCCESS.getNumber()) { // Handle OK response.
                        leader = peer; // The OK response was received from a leader.

                        fut.complete(null); // Void response.
                    }
                    else if (resp0.errorCode() == RaftError.EBUSY.getNumber() ||
                        resp0.errorCode() == (RaftError.EAGAIN.getNumber())) {
                        executor.schedule(() -> {
                            sendWithRetry(peer, req, stopTime, fut);

                            return null;
                        }, retryDelay, TimeUnit.MILLISECONDS);
                    }
                    else if (resp0.errorCode() == RaftError.EPERM.getNumber()) {
                        if (resp0.leaderId() == null) {
                            executor.schedule(() -> {
                                sendWithRetry(randomNode(), req, stopTime, fut);

                                return null;
                            }, retryDelay, TimeUnit.MILLISECONDS);
                        }
                        else {
                            leader = parsePeer(resp0.leaderId()); // Update a leader.

                            executor.schedule(() -> {
                                sendWithRetry(leader, req, stopTime, fut);

                                return null;
                            }, retryDelay, TimeUnit.MILLISECONDS);

                        }
                    }
                    else
                        fut.completeExceptionally(
                            new RaftException(RaftError.forNumber(resp0.errorCode()), resp0.errorMsg()));
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

    /**
     * Parse {@link Peer} from string representation of {@link PeerId}.
     *
     * @param peerId String representation of {@link PeerId}
     * @return Peer
     */
    // TODO: Remove after IGNITE-15506
    private static Peer parsePeer(String peerId) {
        return peerFromPeerId(PeerId.parsePeer(peerId));
    }

    /**
     * Creates new {@link Peer} from {@link PeerId}.
     *
     * @param peer PeerId
     * @return {@link Peer}
     */
    private static Peer peerFromPeerId(PeerId peer) {
        if (peer == null)
            return null;
        else
            return new Peer(NetworkAddress.from(peer.getEndpoint().getIp() + ":" + peer.getEndpoint().getPort()));
    }

    /**
     * Parse list of {@link PeerId} from list with string representations.
     *
     * @param peers List of {@link PeerId} string representations.
     * @return List of {@link PeerId}
     */
    private List<Peer> parsePeerList(List<String> peers) {
        if (peers == null)
            return null;

        List<Peer> res = new ArrayList<>(peers.size());

        for (String peer: peers)
            res.add(parsePeer(peer));

        return res;
    }

    /**
     * Convert list of {@link PeerId} to list of {@link Peer}.
     *
     * @param peers List of {@link PeerId}
     * @return List of {@link Peer}
     */
    private List<Peer> convertPeerIdList(List<PeerId> peers) {
        if (peers == null)
            return Collections.emptyList();

        List<Peer> res = new ArrayList<>(peers.size());

        for (PeerId peerId: peers)
            res.add(peerFromPeerId(peerId));

        return res;
    }
}

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

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.exception.RaftException;
import org.apache.ignite.raft.client.message.AddLearnersRequest;
import org.apache.ignite.raft.client.message.AddPeersRequest;
import org.apache.ignite.raft.client.message.ChangePeersResponse;
import org.apache.ignite.raft.client.message.GetLeaderResponse;
import org.apache.ignite.raft.client.message.RaftErrorResponse;
import org.apache.ignite.raft.client.message.GetLeaderRequest;
import org.apache.ignite.raft.client.message.GetPeersRequest;
import org.apache.ignite.raft.client.message.GetPeersResponse;
import org.apache.ignite.raft.client.message.ActionRequest;
import org.apache.ignite.raft.client.message.ActionResponse;
import org.apache.ignite.raft.client.message.RemoveLearnersRequest;
import org.apache.ignite.raft.client.message.RemovePeersRequest;
import org.apache.ignite.raft.client.message.SnapshotRequest;
import org.apache.ignite.raft.client.message.TransferLeadershipRequest;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactory;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;

import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.ThreadLocalRandom.current;
import static org.apache.ignite.raft.client.RaftErrorCode.LEADER_CHANGED;
import static org.apache.ignite.raft.client.RaftErrorCode.NO_LEADER;
import static org.apache.ignite.raft.client.RaftErrorCode.SUCCESS;

/**
 * The implementation of {@link RaftGroupService}
 */
public class RaftGroupServiceImpl implements RaftGroupService {
    /** */
    private static LogWrapper LOG = new LogWrapper(RaftGroupServiceImpl.class);

    /** */
    private volatile int timeout;

    /** */
    private final String groupId;

    /** */
    private final RaftClientMessageFactory factory;

    /** */
    private volatile Peer leader;

    /** */
    private volatile List<Peer> peers;

    /** */
    private volatile List<Peer> learners;

    /** */
    private final NetworkCluster cluster;

    /** */
    private final long retryDelay;

    /** */
    private final Timer timer;

    /**
     * @param groupId Group id.
     * @param cluster A cluster.
     * @param factory A message factory.
     * @param timeout Request timeout.
     * @param peers Initial group configuration.
     * @param refreshLeader {@code True} to synchronously refresh leader on service creation.
     * @param retryDelay Retry delay.
     * @param timer Timer for scheduled execution.
     */
    public RaftGroupServiceImpl(
        String groupId,
        NetworkCluster cluster,
        RaftClientMessageFactory factory,
        int timeout,
        List<Peer> peers,
        boolean refreshLeader,
        long retryDelay,
        Timer timer
    ) {
        this.cluster = requireNonNull(cluster);
        this.peers = requireNonNull(peers);
        this.factory = factory;
        this.timeout = timeout;
        this.groupId = groupId;
        this.retryDelay = retryDelay;
        this.timer = requireNonNull(timer);

        if (refreshLeader) {
            try {
                refreshLeader().get();
            }
            catch (Exception e) {
                LOG.error("Failed to refresh a leader", e);
            }
        }
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
        this.timeout = timeout;
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

        sendWithRetry(leader.getNode(), req, currentTimeMillis() + timeout, fut);

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

        sendWithRetry(leader.getNode(), req, currentTimeMillis() + timeout, fut);

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

        RemovePeersRequest req = factory.removePeerRequest().groupId(groupId).peers(peers).build();

        CompletableFuture<ChangePeersResponse> fut = new CompletableFuture<>();

        sendWithRetry(leader.getNode(), req, currentTimeMillis() + timeout, fut);

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

        sendWithRetry(leader.getNode(), req, currentTimeMillis() + timeout, fut);

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

        sendWithRetry(leader.getNode(), req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> {
            this.learners = resp.newPeers();

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> snapshot(Peer peer) {
        SnapshotRequest req = factory.snapshotRequest().groupId(groupId).build();

        CompletableFuture<?> fut = cluster.sendWithResponse(peer.getNode(), req, timeout);

        return fut.thenApply(resp -> null);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> transferLeadership(Peer newLeader) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> transferLeadership(newLeader));

        TransferLeadershipRequest req = factory.transferLeaderRequest().groupId(groupId).peer(newLeader).build();

        CompletableFuture<?> fut = cluster.sendWithResponse(newLeader.getNode(), req, timeout);

        return fut.thenApply(resp -> null);
    }

    /** {@inheritDoc} */
    @Override public <R> CompletableFuture<R> run(Command cmd) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> run(cmd));

        ActionRequest req = factory.actionRequest().command(cmd).groupId(groupId).build();

        CompletableFuture<ActionResponse<R>> fut = new CompletableFuture<>();

        sendWithRetry(leader.getNode(), req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> resp.result());
    }

    /** {@inheritDoc} */
    @Override public <R> CompletableFuture<R> run(Peer peer, ReadCommand cmd) {
        ActionRequest req = factory.actionRequest().command(cmd).groupId(groupId).build();

        CompletableFuture fut = cluster.sendWithResponse(peer.getNode(), req, timeout);

        return fut.thenApply(resp -> ((ActionResponse) resp).result());
    }

    /**
     * Retries request until success or time is run out.
     *
     * @param req Request.
     * @param stopTime Stop time.
     * @param <R> Return value.
     * @return A future.
     */
    private <R> void sendWithRetry(NetworkMember node, Object req, long stopTime, CompletableFuture<R> fut) {
        if (currentTimeMillis() >= stopTime) {
            fut.completeExceptionally(new TimeoutException());

            return;
        }

        CompletableFuture fut0 = cluster.sendWithResponse(node, req, timeout);

        fut0.whenComplete(new BiConsumer<Object, Throwable>() {
            @Override public void accept(Object resp, Throwable err) {
                if (err != null)
                    fut.completeExceptionally(err);
                else {
                    if (resp instanceof RaftErrorResponse) {
                        RaftErrorResponse resp0 = (RaftErrorResponse) resp;

                        if (resp0.errorCode().equals(NO_LEADER)) {
                            timer.schedule(new TimerTask() {
                                @Override public void run() {
                                sendWithRetry(randomNode(), req, stopTime, fut);
                                }
                            }, retryDelay);
                        }
                        else if (resp0.errorCode().equals(LEADER_CHANGED)) {
                            leader = resp0.newLeader(); // Update a leader.

                            timer.schedule(new TimerTask() {
                                @Override public void run() {
                                sendWithRetry(resp0.newLeader().getNode(), req, stopTime, fut);
                                }
                            }, retryDelay);
                        }
                        else if (resp0.errorCode().equals(SUCCESS)) { // Handle default response.
                            fut.complete(null);
                        }
                        else
                            fut.completeExceptionally(new RaftException(resp0.errorCode()));
                    }
                    else
                        fut.complete((R) resp);
                }
            }
        });
    }

    /**
     * @return Random node.
     */
    private NetworkMember randomNode() {
        List<Peer> peers0 = peers;

        if (peers0 == null || peers0.isEmpty())
            return null;

        return peers0.get(current().nextInt(peers0.size())).getNode();
    }
}

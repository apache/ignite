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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.message.NetworkMessage;
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
import org.apache.ignite.raft.client.message.RaftClientMessageFactory;
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

/**
 * The implementation of {@link RaftGroupService}
 */
public class RaftGroupServiceImpl implements RaftGroupService {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(RaftGroupServiceImpl.class);

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
    private final ClusterService cluster;

    /** */
    private final long retryDelay;

    /** */
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    /**
     * @param groupId Group id.
     * @param cluster A cluster.
     * @param factory A message factory.
     * @param timeout Request timeout.
     * @param peers Initial group configuration.
     * @param refreshLeader {@code True} to synchronously refresh leader on service creation.
     * @param retryDelay Retry delay.
     */
    public RaftGroupServiceImpl(
        String groupId,
        ClusterService cluster,
        RaftClientMessageFactory factory,
        int timeout,
        List<Peer> peers,
        boolean refreshLeader,
        long retryDelay
    ) {
        this.cluster = requireNonNull(cluster);
        this.peers = requireNonNull(peers);
        this.factory = factory;
        this.timeout = timeout;
        this.groupId = groupId;
        this.retryDelay = retryDelay;

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

        CompletableFuture<GetLeaderResponse> fut = sendWithRetry(randomNode(), req, currentTimeMillis() + timeout);

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

        CompletableFuture<GetPeersResponse> fut = sendWithRetry(leader.getNode(), req, currentTimeMillis() + timeout);

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

        CompletableFuture<ChangePeersResponse> fut = sendWithRetry(leader.getNode(), req, currentTimeMillis() + timeout);

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

        CompletableFuture<ChangePeersResponse> fut = sendWithRetry(leader.getNode(), req, currentTimeMillis() + timeout);

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

        CompletableFuture<ChangePeersResponse> fut = sendWithRetry(leader.getNode(), req, currentTimeMillis() + timeout);

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

        CompletableFuture<ChangePeersResponse> fut = sendWithRetry(leader.getNode(), req, currentTimeMillis() + timeout);

        return fut.thenApply(resp -> {
            this.learners = resp.newPeers();

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> snapshot(Peer peer) {
        SnapshotRequest req = factory.snapshotRequest().groupId(groupId).build();

        CompletableFuture<?> fut = cluster.messagingService().invoke(peer.getNode(), req, timeout);

        return fut.thenApply(resp -> null);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> transferLeadership(Peer newLeader) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> transferLeadership(newLeader));

        TransferLeadershipRequest req = factory.transferLeaderRequest().groupId(groupId).peer(newLeader).build();

        CompletableFuture<?> fut = cluster.messagingService().invoke(newLeader.getNode(), req, timeout);

        return fut.thenApply(resp -> null);
    }

    /** {@inheritDoc} */
    @Override public <R> CompletableFuture<R> run(Command cmd) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> run(cmd));

        ActionRequest<R> req = factory.actionRequest().command(cmd).groupId(groupId).build();

        CompletableFuture<ActionResponse<R>> fut = sendWithRetry(leader.getNode(), req, currentTimeMillis() + timeout);

        return fut.thenApply(resp -> resp.result());
    }

    /** {@inheritDoc} */
    @Override public <R> CompletableFuture<R> run(Peer peer, ReadCommand cmd) {
        ActionRequest req = factory.actionRequest().command(cmd).groupId(groupId).build();

        CompletableFuture<?> fut = cluster.messagingService().invoke(peer.getNode(), req, timeout);

        return fut.thenApply(resp -> ((ActionResponse<R>) resp).result());
    }

    private <R> CompletableFuture<R> sendWithRetry(ClusterNode node, NetworkMessage req, long stopTime) {
        if (currentTimeMillis() >= stopTime)
            return CompletableFuture.failedFuture(new TimeoutException());
        return cluster.messagingService().invoke(node, req, timeout)
            .thenCompose(resp -> {
                if (resp instanceof RaftErrorResponse) {
                    RaftErrorResponse resp0 = (RaftErrorResponse)resp;
                    switch (resp0.errorCode()) {
                        case NO_LEADER:
                            return composeWithDelay(() -> sendWithRetry(randomNode(), req, stopTime));
                        case LEADER_CHANGED:
                            leader = resp0.newLeader();
                            return composeWithDelay(() -> sendWithRetry(resp0.newLeader().getNode(), req, stopTime));
                        case SUCCESS:
                            return CompletableFuture.completedFuture(null);
                        default:
                            return CompletableFuture.failedFuture(new RaftException(resp0.errorCode()));
                    }
                } else
                    return CompletableFuture.completedFuture((R) resp);
            });
    }

    private <T> CompletableFuture<T> composeWithDelay(Supplier<CompletableFuture<T>> supplier) {
        var result = new CompletableFuture<T>();
        executor.schedule(() -> {
            supplier.get().whenComplete((res, err) -> {
                if (err == null)
                    result.complete(res);
                else
                    result.completeExceptionally(err);
            });
        }, retryDelay, TimeUnit.MILLISECONDS);
        return result;
    }

    /**
     * @return Random node.
     */
    private ClusterNode randomNode() {
        List<Peer> peers0 = peers;

        if (peers0 == null || peers0.isEmpty())
            return null;

        return peers0.get(current().nextInt(peers0.size())).getNode();
    }
}

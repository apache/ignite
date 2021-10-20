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

package org.apache.ignite.internal.raft;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JRaftServerImpl;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupServiceImpl;
import org.apache.ignite.raft.jraft.util.Utils;
import org.jetbrains.annotations.ApiStatus.Experimental;

/**
 * Best raft manager ever since 1982.
 */
public class Loza implements IgniteComponent {
    /** Ignite logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(IgniteLogger.class);

    /** Factory. */
    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    /** Raft client pool name. */
    public static final String CLIENT_POOL_NAME = "Raft-Group-Client";

    /**
     * Raft client pool size. Size was taken from jraft's TimeManager.
     */
    private static final int CLIENT_POOL_SIZE = Math.min(Utils.cpus() * 3, 20);

    /** Timeout. */
    // TODO: IGNITE-15705 Correct value should be investigated
    private static final int TIMEOUT = 10000;

    /** Network timeout. */
    // TODO: IGNITE-15705 Correct value should be investigated
    private static final int NETWORK_TIMEOUT = 3000;

    /** Retry delay. */
    private static final int DELAY = 200;

    /** Cluster network service. */
    private final ClusterService clusterNetSvc;

    /** Raft server. */
    private final RaftServer raftServer;

    /** Executor for raft group services. */
    private final ScheduledExecutorService executor;

    /**
     * Constructor.
     *
     * @param clusterNetSvc Cluster network service.
     */
    public Loza(ClusterService clusterNetSvc, Path dataPath) {
        this.clusterNetSvc = clusterNetSvc;

        this.raftServer = new JRaftServerImpl(clusterNetSvc, dataPath);

        this.executor = new ScheduledThreadPoolExecutor(CLIENT_POOL_SIZE,
            new NamedThreadFactory(NamedThreadFactory.threadPrefix(clusterNetSvc.localConfiguration().getName(),
                CLIENT_POOL_NAME)
            )
        );
    }

    /** {@inheritDoc} */
    @Override public void start() {
        raftServer.start();
    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {
        // TODO: IGNITE-15161 Implement component's stop.
        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        raftServer.stop();
    }

    /**
     * Creates a raft group service providing operations on a raft group.
     * If {@code nodes} contains the current node, then raft group starts on the current node.
     *
     * IMPORTANT: DON'T USE. This method should be used only for long running changePeers requests - until
     * IGNITE-14209 will be fixed with stable solution.
     *
     * @param groupId Raft group id.
     * @param nodes Raft group nodes.
     * @param lsnrSupplier Raft group listener supplier.
     * @param clientTimeout Client retry timeout.
     * @param networkTimeout Client network timeout.
     * @return Future representing pending completion of the operation.
     */
    @Experimental
    public CompletableFuture<RaftGroupService> prepareRaftGroup(
        String groupId,
        List<ClusterNode> nodes,
        Supplier<RaftGroupListener> lsnrSupplier,
        int clientTimeout,
        int networkTimeout) {
        assert !nodes.isEmpty();

        List<Peer> peers = nodes.stream().map(n -> new Peer(n.address())).collect(Collectors.toList());

        String locNodeName = clusterNetSvc.topologyService().localMember().name();

        if (nodes.stream().anyMatch(n -> locNodeName.equals(n.name())))
            raftServer.startRaftGroup(groupId, lsnrSupplier.get(), peers);

        return RaftGroupServiceImpl.start(
            groupId,
            clusterNetSvc,
            FACTORY,
            clientTimeout,
            networkTimeout,
            peers,
            true,
            DELAY,
            executor
        );
    }

    /**
     * Creates a raft group service providing operations on a raft group.
     * If {@code nodes} contains the current node, then raft group starts on the current node.
     *
     * @param groupId Raft group id.
     * @param nodes Raft group nodes.
     * @param lsnrSupplier Raft group listener supplier.
     * @return Future representing pending completion of the operation.
     */
    public CompletableFuture<RaftGroupService> prepareRaftGroup(
        String groupId,
        List<ClusterNode> nodes,
        Supplier<RaftGroupListener> lsnrSupplier) {

        return prepareRaftGroup(groupId, nodes, lsnrSupplier, TIMEOUT, NETWORK_TIMEOUT);
    }

    /**
     * Stops a raft group on the current node if {@code nodes} contains the current node.
     *
     * @param groupId Raft group id.
     * @param nodes Raft group nodes.
     * @return {@code true} if group has been stopped.
     */
    public boolean stopRaftGroup(String groupId, List<ClusterNode> nodes) {
        String locNodeName = clusterNetSvc.topologyService().localMember().name();

        if (nodes.stream().anyMatch(n -> locNodeName.equals(n.name()))) {
            raftServer.stopRaftGroup(groupId);

            return true;
        }

        return false;
    }

    /**
     * Creates a raft group service providing operations on a raft group.
     * If {@code deltaNodes} contains the current node, then raft group starts on the current node.
     * @param groupId Raft group id.
     * @param nodes Full set of raft group nodes.
     * @param deltaNodes New raft group nodes.
     * @param lsnrSupplier Raft group listener supplier.
     * @return Future representing pending completion of the operation.
     * @return
     */
    public CompletableFuture<RaftGroupService> updateRaftGroup(
        String groupId,
        Collection<ClusterNode> nodes,
        Collection<ClusterNode> deltaNodes,
        Supplier<RaftGroupListener> lsnrSupplier
    ) {
        assert !nodes.isEmpty();

        List<Peer> peers = nodes.stream().map(n -> new Peer(n.address())).collect(Collectors.toList());

        String locNodeName = clusterNetSvc.topologyService().localMember().name();

        if (deltaNodes.stream().anyMatch(n -> locNodeName.equals(n.name()))) {
            if (!raftServer.startRaftGroup(groupId, lsnrSupplier.get(), peers))
                LOG.error("Failed to start raft group on node " + locNodeName);
        }

        return RaftGroupServiceImpl.start(
            groupId,
            clusterNetSvc,
            FACTORY,
            TIMEOUT,
            peers,
            true,
            DELAY,
            executor
        );
    }
}

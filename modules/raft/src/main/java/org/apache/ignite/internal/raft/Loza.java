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
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupServiceImpl;
import org.apache.ignite.raft.jraft.util.Utils;

/**
 * Best raft manager ever since 1982.
 */
public class Loza implements IgniteComponent {
    /** Factory. */
    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    /** Raft client pool name. */
    public static final String CLIENT_POOL_NAME = "Raft-Group-Client";

    /**
     * Raft client pool size. Size was taken from jraft's TimeManager.
     */
    private static final int CLIENT_POOL_SIZE = Math.min(Utils.cpus() * 3, 20);

    /** Timeout. */
    private static final int TIMEOUT = 1000;

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
     * @param groupId Raft group id.
     * @param nodes Raft group nodes.
     * @param lsnrSupplier Raft group listener supplier.
     * @return Future representing pending completion of the operation.
     */
    public CompletableFuture<RaftGroupService> prepareRaftGroup(
        String groupId,
        List<ClusterNode> nodes,
        Supplier<RaftGroupListener> lsnrSupplier) {
        assert !nodes.isEmpty();

        List<Peer> peers = nodes.stream().map(n -> new Peer(n.address())).collect(Collectors.toList());

        String locNodeName = clusterNetSvc.topologyService().localMember().name();

        if (nodes.stream().anyMatch(n -> locNodeName.equals(n.name())))
            raftServer.startRaftGroup(groupId, lsnrSupplier.get(), peers);

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

    /**
     * Stops a raft group on the current node if {@code nodes} contains the current node.
     *
     * @param groupId Raft group id.
     * @param nodes Raft group nodes.
     */
    public void stopRaftGroup(String groupId, List<ClusterNode> nodes) {
        assert !nodes.isEmpty();

        String locNodeName = clusterNetSvc.topologyService().localMember().name();

        if (nodes.stream().anyMatch(n -> locNodeName.equals(n.name())))
            raftServer.stopRaftGroup(groupId);
    }
}

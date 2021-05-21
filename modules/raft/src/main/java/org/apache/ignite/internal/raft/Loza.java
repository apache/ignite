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

import java.util.List;
import java.util.Map;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.message.RaftClientMessageFactory;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactoryImpl;
import org.apache.ignite.raft.client.service.RaftGroupCommandListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.apache.ignite.raft.server.RaftServer;
import org.apache.ignite.raft.server.impl.RaftServerImpl;

/**
 * Best raft manager ever since 1982.
 */
public class Loza {
    /** Factory. */
    private static RaftClientMessageFactory FACTORY = new RaftClientMessageFactoryImpl();

    /** Timeout. */
    private static final int TIMEOUT = 1000;

    /** Retry delay. */
    private static final int DELAY = 200;

    /** Cluster network service. */
    private final ClusterService clusterNetSvc;

    /** Raft server. */
    private RaftServer raftServer;

    /**
     * Constructor.
     *
     * @param clusterNetSvc Cluster network service.
     */
    public Loza(ClusterService clusterNetSvc) {
        this.clusterNetSvc = clusterNetSvc;

        this.raftServer = new RaftServerImpl(clusterNetSvc, FACTORY, 1000, Map.of());
    }

    /**
     * Creates a RAFT group.
     *
     * @param groupId RAFT group id.
     * @param peers Group peers.
     * @param lsnr Group listener.
     * @return A RAFT group client.
     */
    public RaftGroupService startRaftGroup(String groupId, List<ClusterNode> peers, RaftGroupCommandListener lsnr) {
        assert !peers.isEmpty();

        //Now we are using only one node in a raft group.
        //TODO: IGNITE-13885 Investigate jraft implementation for replication framework based on RAFT protocol.
        if (peers.get(0).name().equals(clusterNetSvc.topologyService().localMember().name()))
            raftServer.setListener(groupId, lsnr);

        return new RaftGroupServiceImpl(
            groupId,
            clusterNetSvc,
            FACTORY,
            TIMEOUT,
            List.of(new Peer(peers.get(0))),
            true,
            DELAY
        );
    }

    /**
     * Stops a RAFT group.
     *
     * @param groupId RAFT group id.
     * @param peers Group peers.
     */
    public void stopRaftGroup(String groupId, List<ClusterNode> peers) {
        assert !peers.isEmpty();

        //Now we are using only one node in a raft group.
        //TODO: IGNITE-13885 Investigate jraft implementation for replication framework based on RAFT protocol.
        if (peers.get(0).name().equals(clusterNetSvc.topologyService().localMember().name()))
            raftServer.clearListener(groupId);
    }
}

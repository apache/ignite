/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.raft;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * There are tests for RAFT manager.
 * It is mocking all components except Loza and checks API methods of the component in various conditions.
 */
@ExtendWith(MockitoExtension.class)
public class LozaTest extends IgniteAbstractTest {
    /** Mock for network service. */
    @Mock
    private ClusterService clusterNetSvc;

    /**
     * Checks that the all API methods throw the exception ({@link org.apache.ignite.lang.NodeStoppingException})
     * when Loza is closed.
     *
     * @throws Exception If fail.
     */
    @Test
    public void testLozaStop() throws Exception {
        Mockito.doReturn(new ClusterLocalConfiguration("test_node", null)).when(clusterNetSvc).localConfiguration();
        Mockito.doReturn(Mockito.mock(MessagingService.class)).when(clusterNetSvc).messagingService();
        Mockito.doReturn(Mockito.mock(TopologyService.class)).when(clusterNetSvc).topologyService();

        Loza loza = new Loza(clusterNetSvc, workDir);

        loza.start();

        loza.beforeNodeStop();
        loza.stop();

        String raftGroupId = "test_raft_group";

        List<ClusterNode> nodes = List.of(
                new ClusterNode(UUID.randomUUID().toString(), UUID.randomUUID().toString(), NetworkAddress.from("127.0.0.1:123")));

        List<ClusterNode> newNodes = List.of(
                new ClusterNode(UUID.randomUUID().toString(), UUID.randomUUID().toString(), NetworkAddress.from("127.0.0.1:124")),
                new ClusterNode(UUID.randomUUID().toString(), UUID.randomUUID().toString(), NetworkAddress.from("127.0.0.1:125")));

        Supplier<RaftGroupListener> lsnrSupplier = () -> null;

        assertThrows(NodeStoppingException.class, () -> loza.updateRaftGroup(raftGroupId, nodes, newNodes, lsnrSupplier));
        assertThrows(NodeStoppingException.class, () -> loza.stopRaftGroup(raftGroupId));
        assertThrows(NodeStoppingException.class, () -> loza.prepareRaftGroup(raftGroupId, nodes, lsnrSupplier));
        assertThrows(NodeStoppingException.class, () -> loza.changePeers(raftGroupId, nodes, newNodes));
    }
}

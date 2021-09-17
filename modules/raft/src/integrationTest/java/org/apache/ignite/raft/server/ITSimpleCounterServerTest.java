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

package org.apache.ignite.raft.server;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JRaftServerImpl;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupServiceImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.apache.ignite.raft.jraft.test.TestUtils.waitForTopology;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Single node raft server.
 */
@ExtendWith(WorkDirectoryExtension.class)
class ITSimpleCounterServerTest extends RaftServerAbstractTest {
    /**
     * The server implementation.
     */
    private RaftServer server;

    /**
     * Counter raft group 0.
     */
    private static final String COUNTER_GROUP_ID_0 = "counter0";

    /**
     * Counter raft group 1.
     */
    private static final String COUNTER_GROUP_ID_1 = "counter1";

    /**
     * The client 1.
     */
    private RaftGroupService client1;

    /**
     * The client 2.
     */
    private RaftGroupService client2;

    /** */
    @WorkDirectory
    private Path dataPath;

    /**
     * @param testInfo Test info.
     * @throws Exception If failed.
     */
    @BeforeEach
    void before(TestInfo testInfo) throws Exception {
        LOG.info(">>>> Starting test {}", testInfo.getTestMethod().orElseThrow().getName());

        var addr = new NetworkAddress("localhost", PORT);

        ClusterService service = clusterService(addr.toString(), PORT, List.of(), true);

        server = new JRaftServerImpl(service, dataPath) {
            @Override public synchronized void stop() {
                super.stop();

                service.stop();
            }
        };

        server.start();

        ClusterNode serverNode = server.clusterService().topologyService().localMember();

        server.startRaftGroup(COUNTER_GROUP_ID_0, new CounterListener(), List.of(new Peer(serverNode.address())));
        server.startRaftGroup(COUNTER_GROUP_ID_1, new CounterListener(), List.of(new Peer(serverNode.address())));

        ClusterService clientNode1 = clusterService("localhost:" + (PORT + 1), PORT + 1, List.of(addr), true);

        client1 = RaftGroupServiceImpl.start(COUNTER_GROUP_ID_0, clientNode1, FACTORY, 1000,
            List.of(new Peer(serverNode.address())), false, 200).get(3, TimeUnit.SECONDS);

        ClusterService clientNode2 = clusterService("localhost:" + (PORT + 2), PORT + 2, List.of(addr), true);

        client2 = RaftGroupServiceImpl.start(COUNTER_GROUP_ID_1, clientNode2, FACTORY, 1000,
            List.of(new Peer(serverNode.address())), false, 200).get(3, TimeUnit.SECONDS);

        assertTrue(waitForTopology(service, 2, 1000));
        assertTrue(waitForTopology(clientNode1, 2, 1000));
        assertTrue(waitForTopology(clientNode2, 2, 1000));
    }

    /**
     * @throws Exception If failed.
     */
    @AfterEach
    @Override public void after(TestInfo testInfo) throws Exception {
        server.stop();
        client1.shutdown();
        client2.shutdown();

        super.after(testInfo);
    }

    /**
     *
     */
    @Test
    public void testRefreshLeader() throws Exception {
        Peer leader = client1.leader();

        assertNull(leader);

        client1.refreshLeader().get();

        assertNotNull(client1.leader());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCounterCommandListener() throws Exception {
        client1.refreshLeader().get();
        client2.refreshLeader().get();

        assertNotNull(client1.leader());
        assertNotNull(client2.leader());

        assertEquals(2, client1.<Long>run(new IncrementAndGetCommand(2)).get());
        assertEquals(2, client1.<Long>run(new GetValueCommand()).get());
        assertEquals(3, client1.<Long>run(new IncrementAndGetCommand(1)).get());
        assertEquals(3, client1.<Long>run(new GetValueCommand()).get());

        assertEquals(4, client2.<Long>run(new IncrementAndGetCommand(4)).get());
        assertEquals(4, client2.<Long>run(new GetValueCommand()).get());
        assertEquals(7, client2.<Long>run(new IncrementAndGetCommand(3)).get());
        assertEquals(7, client2.<Long>run(new GetValueCommand()).get());
    }
}

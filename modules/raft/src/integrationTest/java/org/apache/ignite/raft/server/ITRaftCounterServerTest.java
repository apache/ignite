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

import java.util.List;
import java.util.Map;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.scalecube.ScaleCubeClusterServiceFactory;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.message.RaftClientMessageFactory;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactoryImpl;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.apache.ignite.raft.server.impl.RaftServerImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** */
class ITRaftCounterServerTest {
    /** */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ITRaftCounterServerTest.class);

    /** */
    private static final RaftClientMessageFactory FACTORY = new RaftClientMessageFactoryImpl();

    /** Network factory. */
    private static final ClusterServiceFactory NETWORK_FACTORY = new ScaleCubeClusterServiceFactory();

    /** */
    // TODO: IGNITE-14088: Uncomment and use real serializer provider
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistry();
//            .registerFactory((short)1000, ???)
//            .registerFactory((short)1001, ???)
//            .registerFactory((short)1005, ???)
//            .registerFactory((short)1006, ???)
//            .registerFactory((short)1009, ???);

    /** */
    private RaftServer server;

    /** */
    private ClusterService client;

    /** */
    private static final String SERVER_ID = "testServer";

    /** */
    private static final String CLIENT_ID = "testClient";

    /** */
    private static final String COUNTER_GROUP_ID_0 = "counter0";

    /** */
    private static final String COUNTER_GROUP_ID_1 = "counter1";

    /**
     * @param testInfo Test info.
     */
    @BeforeEach
    void before(TestInfo testInfo) {
        LOG.info(">>>> Starting test " + testInfo.getTestMethod().orElseThrow().getName());

        server = new RaftServerImpl(SERVER_ID,
            20100,
            FACTORY,
            1000,
            Map.of(COUNTER_GROUP_ID_0, new CounterCommandListener(), COUNTER_GROUP_ID_1, new CounterCommandListener()));

        client = startClient(CLIENT_ID, 20101, List.of("localhost:20100"));

        assertTrue(waitForTopology(client, 2, 1000));
    }

    /**
     * @throws Exception
     */
    @AfterEach
    void after() throws Exception {
        server.shutdown();
        client.shutdown();
    }

    /**
     */
    @Test
    public void testRefreshLeader() {
        Peer server = new Peer(client.topologyService().allMembers().stream().filter(m -> SERVER_ID.equals(m.name())).findFirst().orElseThrow());

        RaftGroupService service = new RaftGroupServiceImpl(COUNTER_GROUP_ID_0, client, FACTORY, 1000,
            List.of(server), true, 200);

        Peer leader = service.leader();

        assertNotNull(leader);
        assertEquals(server.getNode().name(), leader.getNode().name());
    }

    /**
     * @throws Exception
     */
    @Test
    public void testCounterCommandListener() throws Exception {
        Peer server = new Peer(client.topologyService().allMembers().stream().filter(m -> SERVER_ID.equals(m.name())).findFirst().orElseThrow());

        RaftGroupService service0 = new RaftGroupServiceImpl(COUNTER_GROUP_ID_0, client, FACTORY, 1000,
            List.of(server), true, 200);

        RaftGroupService service1 = new RaftGroupServiceImpl(COUNTER_GROUP_ID_1, client, FACTORY, 1000,
            List.of(server), true, 200);

        assertNotNull(service0.leader());
        assertNotNull(service1.leader());

        assertEquals(2, service0.<Integer>run(new IncrementAndGetCommand(2)).get());
        assertEquals(2, service0.<Integer>run(new GetValueCommand()).get());
        assertEquals(3, service0.<Integer>run(new IncrementAndGetCommand(1)).get());
        assertEquals(3, service0.<Integer>run(new GetValueCommand()).get());

        assertEquals(4, service1.<Integer>run(new IncrementAndGetCommand(4)).get());
        assertEquals(4, service1.<Integer>run(new GetValueCommand()).get());
        assertEquals(7, service1.<Integer>run(new IncrementAndGetCommand(3)).get());
        assertEquals(7, service1.<Integer>run(new GetValueCommand()).get());
    }

    /**
     * @param name Node name.
     * @param port Local port.
     * @param servers Server nodes of the cluster.
     * @return The client cluster view.
     */
    private ClusterService startClient(String name, int port, List<String> servers) {
        var context = new ClusterLocalConfiguration(name, port, servers, SERIALIZATION_REGISTRY);
        var network = NETWORK_FACTORY.createClusterService(context);
        network.start();
        return network;
    }

    /**
     * @param cluster The cluster.
     * @param expected Expected count.
     * @param timeout The timeout in millis.
     * @return {@code True} if topology size is equal to expected.
     */
    private boolean waitForTopology(ClusterService cluster, int expected, int timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while(System.currentTimeMillis() < stop) {
            if (cluster.topologyService().allMembers().size() >= expected)
                return true;

            try {
                Thread.sleep(50);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }
}

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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JRaftServerImpl;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.exception.RaftException;
import org.apache.ignite.raft.client.message.RaftClientMessagesFactory;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.apache.ignite.raft.jraft.core.State.STATE_ERROR;
import static org.apache.ignite.raft.jraft.core.State.STATE_LEADER;
import static org.apache.ignite.raft.jraft.test.TestUtils.getLocalAddress;
import static org.apache.ignite.raft.jraft.test.TestUtils.waitForCondition;
import static org.apache.ignite.raft.jraft.test.TestUtils.waitForTopology;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Jraft server.
 */
@ExtendWith(WorkDirectoryExtension.class)
class ITJRaftCounterServerTest extends RaftServerAbstractTest {
    /**
     * The logger.
     */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ITJRaftCounterServerTest.class);

    /**
     * Counter group name 0.
     */
    private static final String COUNTER_GROUP_0 = "counter0";

    /**
     * Counter group name 1.
     */
    private static final String COUNTER_GROUP_1 = "counter1";

    /**
     * The server port offset.
     */
    private static final int PORT = 5003;

    /**
     * The client port offset.
     */
    private static final int CLIENT_PORT = 6003;

    /**
     * Initial configuration.
     */
    private static final List<Peer> INITIAL_CONF = IntStream.rangeClosed(0, 2)
        .mapToObj(i -> new NetworkAddress(getLocalAddress(), PORT + i))
        .map(Peer::new)
        .collect(Collectors.toUnmodifiableList());

    /**
     * Listener factory.
     */
    private Supplier<CounterListener> listenerFactory = CounterListener::new;

    /**
     * Servers list.
     */
    protected final List<JRaftServerImpl> servers = new ArrayList<>();

    /**
     * Clients list.
     */
    private final List<RaftGroupService> clients = new ArrayList<>();

    /**
     * Data path.
     */
    @WorkDirectory
    private Path dataPath;

    /** */
    @BeforeEach
    void before(TestInfo testInfo) {
        LOG.info(">>>>>>>>>>>>>>> Start test method: {}", testInfo.getTestMethod().orElseThrow().getName());
    }

    /** */
    @AfterEach
    void after(TestInfo testInfo) throws Exception {
        LOG.info("Start client shutdown");

        for (RaftGroupService client : clients)
            client.shutdown();

        LOG.info("Start server shutdown servers={}", servers.size());

        for (RaftServer server : servers)
            server.shutdown();

        LOG.info(">>>>>>>>>>>>>>> End test method: {}", testInfo.getTestMethod().orElseThrow().getName());
    }

    /**
     * @param idx The index.
     * @return Raft server instance.
     */
    private JRaftServerImpl startServer(int idx, Consumer<RaftServer> clo) {
        var addr = new NetworkAddress(getLocalAddress(), PORT);

        ClusterService service = clusterService("server" + idx, PORT + idx, List.of(addr), true);

        JRaftServerImpl server = new JRaftServerImpl(service, dataPath.toString()) {
            @Override public void shutdown() throws Exception {
                super.shutdown();

                service.shutdown();
            }
        };

        clo.accept(server);

        servers.add(server);

        assertTrue(waitForTopology(service, servers.size(), 15_000));

        return server;
    }

    /**
     * @param groupId Group id.
     * @return The client.
     */
    private RaftGroupService startClient(String groupId) {
        var addr = new NetworkAddress(getLocalAddress(), PORT);

        ClusterService clientNode = clusterService(
            "client_" + groupId + "_", CLIENT_PORT + clients.size(), List.of(addr), true);

        RaftGroupServiceImpl client = new RaftGroupServiceImpl(groupId, clientNode, FACTORY, 10_000,
            List.of(new Peer(addr)), false, 200) {
            @Override public void shutdown() {
                super.shutdown();

                clientNode.shutdown();
            }
        };

        clients.add(client);

        return client;
    }

    /**
     * Starts a cluster for the test.
     */
    private void startCluster() {
        for (int i = 0; i < 3; i++) {
            startServer(i, raftServer -> {
                raftServer.startRaftGroup(COUNTER_GROUP_0, listenerFactory.get(), INITIAL_CONF);
                raftServer.startRaftGroup(COUNTER_GROUP_1, listenerFactory.get(), INITIAL_CONF);
            });
        }

        startClient(COUNTER_GROUP_0);
        startClient(COUNTER_GROUP_1);
    }

    /**
     *
     */
    @Test
    public void testRefreshLeader() throws Exception {
        startCluster();

        Peer leader = clients.get(0).leader();

        assertNull(leader);

        clients.get(0).refreshLeader().get();

        assertNotNull(clients.get(0).leader());

        leader = clients.get(1).leader();

        assertNull(leader);

        clients.get(1).refreshLeader().get();

        assertNotNull(clients.get(1).leader());
    }

    /**
     * @throws Exception
     */
    @Test
    public void testCounterCommandListener() throws Exception {
        startCluster();

        RaftGroupService client1 = clients.get(0);
        RaftGroupService client2 = clients.get(1);

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

    @Test
    public void testCreateSnapshot() throws Exception {
        startCluster();

        RaftGroupService client1 = clients.get(0);
        RaftGroupService client2 = clients.get(1);

        client1.refreshLeader().get();
        client2.refreshLeader().get();

        JRaftServerImpl server = servers.get(0);

        long val = applyIncrements(client1, 1, 10);

        assertEquals(sum(10), val);

        client1.snapshot(server.localPeer(COUNTER_GROUP_0)).get();

        long val2 = applyIncrements(client2, 1, 20);

        assertEquals(sum(20), val2);

        client2.snapshot(server.localPeer(COUNTER_GROUP_1)).get();

        String snapshotDir0 = server.getServerDataPath(COUNTER_GROUP_0) + File.separator + "snapshot";
        assertEquals(1, new File(snapshotDir0).list().length);

        String snapshotDir1 = server.getServerDataPath(COUNTER_GROUP_1) + File.separator + "snapshot";
        assertEquals(1, new File(snapshotDir1).list().length);
    }

    @Test
    public void testCreateSnapshotGracefulFailure() throws Exception {
        listenerFactory = () -> new CounterListener() {
            @Override public void onSnapshotSave(String path, Consumer<Throwable> doneClo) {
                doneClo.accept(new IgniteInternalException("Very bad"));
            }
        };

        startCluster();

        RaftGroupService client1 = clients.get(0);
        RaftGroupService client2 = clients.get(1);

        client1.refreshLeader().get();
        client2.refreshLeader().get();

        RaftServer server = servers.get(0);

        Peer peer = server.localPeer(COUNTER_GROUP_0);

        long val = applyIncrements(client1, 1, 10);

        assertEquals(sum(10), val);

        try {
            client1.snapshot(peer).get();

            fail();
        }
        catch (Exception e) {
            assertTrue(e.getCause() instanceof RaftException);
        }
    }

    @Test
    public void testCreateSnapshotAbnormalFailure() throws Exception {
        listenerFactory = () -> new CounterListener() {
            @Override public void onSnapshotSave(String path, Consumer<Throwable> doneClo) {
                doneClo.accept(new IgniteInternalException("Very bad"));
            }
        };

        startCluster();

        RaftGroupService client1 = clients.get(0);
        RaftGroupService client2 = clients.get(1);

        client1.refreshLeader().get();
        client2.refreshLeader().get();

        long val = applyIncrements(client1, 1, 10);

        assertEquals(sum(10), val);

        Peer peer = servers.get(0).localPeer(COUNTER_GROUP_0);

        try {
            client1.snapshot(peer).get();

            fail();
        }
        catch (Exception e) {
            assertTrue(e.getCause() instanceof RaftException);
        }
    }

    /** Tests if a raft group become unavaiable in case of a critical error */
    @Test
    public void testApplyWithFailure() throws Exception {
        listenerFactory = () -> new CounterListener() {
            @Override public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
                Iterator<CommandClosure<WriteCommand>> wrapper = new Iterator<>() {
                    @Override public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override public CommandClosure<WriteCommand> next() {
                        CommandClosure<WriteCommand> cmd = iterator.next();

                        IncrementAndGetCommand command = (IncrementAndGetCommand)cmd.command();

                        if (command.delta() == 10)
                            throw new IgniteInternalException("Very bad");

                        return cmd;
                    }
                };

                super.onWrite(wrapper);
            }
        };

        startCluster();

        RaftGroupService client1 = clients.get(0);
        RaftGroupService client2 = clients.get(1);

        client1.refreshLeader().get();
        client2.refreshLeader().get();

        NodeImpl leader = servers.stream().map(s -> ((NodeImpl) s.raftGroupService(COUNTER_GROUP_0).getRaftNode())).
            filter(n -> n.getState() == STATE_LEADER).findFirst().orElse(null);

        assertNotNull(leader);

        long val1 = applyIncrements(client1, 1, 5);
        long val2 = applyIncrements(client2, 1, 7);

        assertEquals(sum(5), val1);
        assertEquals(sum(7), val2);

        long val3 = applyIncrements(client1, 6, 9);
        assertEquals(sum(9), val3);

        try {
            client1.<Long>run(new IncrementAndGetCommand(10)).get();

            fail();
        }
        catch (Exception e) {
            // Expected.
            Throwable cause = e.getCause();

            assertTrue(cause instanceof RaftException);
        }

        NodeImpl finalLeader = leader;
        waitForCondition(() -> finalLeader.getState() == STATE_ERROR, 5_000);

        // Client can't switch to new leader, because only one peer in the list.
        try {
            client1.<Long>run(new IncrementAndGetCommand(11)).get();
        }
        catch (Exception e) {
            boolean isValid = e.getCause() instanceof TimeoutException;

            if (!isValid)
                LOG.error("Got unexpected exception", e);

            assertTrue(isValid, "Expecting the timeout");
        }
    }

    /** Tests if a follower is catching up the leader after restarting. */
    @Test
    public void testFollowerCatchUpFromLog() throws Exception {
        doTestFollowerCatchUp(false, true);
    }

    /**
     *
     */
    @Test
    public void testFollowerCatchUpFromSnapshot() throws Exception {
        doTestFollowerCatchUp(true, true);
    }

    /**
     *
     */
    @Test
    public void testFollowerCatchUpFromLog2() throws Exception {
        doTestFollowerCatchUp(false, false);
    }

    /**
     *
     */
    @Test
    public void testFollowerCatchUpFromSnapshot2() throws Exception {
        doTestFollowerCatchUp(true, false);
    }

    /**
     * @param snapshot {@code True} to create snapshot on leader and truncate log.
     * @param cleanDir {@code True} to clean persistent state on follower before restart.
     * @throws Exception If failed.
     */
    private void doTestFollowerCatchUp(boolean snapshot, boolean cleanDir) throws Exception {
        startCluster();

        RaftGroupService client1 = clients.get(0);
        RaftGroupService client2 = clients.get(1);

        client1.refreshLeader().get();
        client2.refreshLeader().get();

        Peer leader1 = client1.leader();
        assertNotNull(leader1);

        Peer leader2 = client2.leader();
        assertNotNull(leader2);

        applyIncrements(client1, 0, 10);
        applyIncrements(client2, 0, 20);

        // First snapshot will not truncate logs.
        client1.snapshot(leader1).get();
        client2.snapshot(leader2).get();

        JRaftServerImpl toStop = null;

        // Find the follower for both groups.
        for (JRaftServerImpl server : servers) {
            Peer peer = server.localPeer(COUNTER_GROUP_0);

            if (!peer.equals(leader1) && !peer.equals(leader2)) {
                toStop = server;
                break;
            }
        }

        String serverDataPath0 = toStop.getServerDataPath(COUNTER_GROUP_0);
        String serverDataPath1 = toStop.getServerDataPath(COUNTER_GROUP_1);

        int stopIdx = servers.indexOf(toStop);

        servers.remove(stopIdx);

        toStop.shutdown();

        applyIncrements(client1, 11, 20);
        applyIncrements(client2, 21, 30);

        if (snapshot) {
            client1.snapshot(leader1).get();
            client2.snapshot(leader2).get();
        }

        if (cleanDir) {
            IgniteUtils.deleteIfExists(Paths.get(serverDataPath0));
            IgniteUtils.deleteIfExists(Paths.get(serverDataPath1));
        }

        var svc2 = startServer(stopIdx, r -> {
            r.startRaftGroup(COUNTER_GROUP_0, listenerFactory.get(), INITIAL_CONF);
            r.startRaftGroup(COUNTER_GROUP_1, listenerFactory.get(), INITIAL_CONF);
        });

        waitForCondition(() -> validateStateMachine(sum(20), svc2, COUNTER_GROUP_0), 5_000);
        waitForCondition(() -> validateStateMachine(sum(30), svc2, COUNTER_GROUP_1), 5_000);

        svc2.shutdown();

        var svc3 = startServer(stopIdx, r -> {
            r.startRaftGroup(COUNTER_GROUP_0, listenerFactory.get(), INITIAL_CONF);
            r.startRaftGroup(COUNTER_GROUP_1, listenerFactory.get(), INITIAL_CONF);
        });

        waitForCondition(() -> validateStateMachine(sum(20), svc3, COUNTER_GROUP_0), 5_000);
        waitForCondition(() -> validateStateMachine(sum(30), svc3, COUNTER_GROUP_1), 5_000);
    }

    /**
     * @param client The client
     * @param start Start element.
     * @param stop Stop element.
     * @return The counter value.
     * @throws Exception If failed.
     */
    private static long applyIncrements(RaftGroupService client, int start, int stop) throws Exception {
        long val = 0;

        for (int i = start; i <= stop; i++) {
            val = client.<Long>run(new IncrementAndGetCommand(i)).get();

            LOG.info("Val={}, i={}", val, i);
        }

        return val;
    }

    /**
     * Calculates a progression sum.
     *
     * @param until Until value.
     * @return The sum.
     */
    private static long sum(long until) {
        return (1 + until) * until / 2;
    }

    /**
     * @param expected Expected value.
     * @param server The server.
     * @param groupId Group id.
     * @return Validation result.
     */
    private static boolean validateStateMachine(long expected, JRaftServerImpl server, String groupId) {
        org.apache.ignite.raft.jraft.RaftGroupService svc = server.raftGroupService(groupId);

        JRaftServerImpl.DelegatingStateMachine fsm0 =
            (JRaftServerImpl.DelegatingStateMachine) svc.getRaftNode().getOptions().getFsm();

        return expected == ((CounterListener) fsm0.getListener()).value();
    }
}

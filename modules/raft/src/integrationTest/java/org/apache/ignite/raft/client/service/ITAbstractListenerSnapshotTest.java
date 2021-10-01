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

package org.apache.ignite.raft.client.service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.server.impl.JRaftServerImpl;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupServiceImpl;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base class for persistent raft group's snapshots tests.
 *
 * @param <T> Type of the raft group listener.
 */
@ExtendWith(WorkDirectoryExtension.class)
public abstract class ITAbstractListenerSnapshotTest<T extends RaftGroupListener> {
    /** Starting server port. */
    private static final int PORT = 5003;

    /** Starting client port. */
    private static final int CLIENT_PORT = 6003;

    /**
     * Peers list.
     */
    private static final List<Peer> INITIAL_CONF = IntStream.rangeClosed(0, 2)
        .mapToObj(i -> new NetworkAddress(getLocalAddress(), PORT + i))
        .map(Peer::new)
        .collect(Collectors.toUnmodifiableList());

    /** Factory. */
    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    /** Network factory. */
    private static final ClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();

    /** */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistryImpl();

    /** */
    @WorkDirectory
    private Path workDir;

    /** Cluster. */
    private final List<ClusterService> cluster = new ArrayList<>();

    /** Servers. */
    private final List<JRaftServerImpl> servers = new ArrayList<>();

    /** Clients. */
    private final List<RaftGroupService> clients = new ArrayList<>();

    /** Executor for raft group services. */
    private ScheduledExecutorService executor;

    /**
     * Create executor for raft group services.
     */
    @BeforeEach
    public void beforeTest() {
        executor = new ScheduledThreadPoolExecutor(20, new NamedThreadFactory(Loza.CLIENT_POOL_NAME));
    }

    /**
     * Shutdown raft server, executor for raft group services and stop all cluster nodes.
     *
     * @throws Exception If failed to shutdown raft server,
     */
    @AfterEach
    public void afterTest() throws Exception {
        for (RaftGroupService client : clients)
            client.shutdown();

        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        for (JRaftServerImpl server : servers)
            server.stop();

        for (ClusterService service : cluster)
            service.stop();
    }

    /**
     * Test parameters for {@link #testSnapshot}.
     */
    private static class TestData {
        /**
         * {@code true} if the raft group's persistence must be cleared before the follower's restart,
         * {@code false} otherwise.
         */
        private final boolean deleteFolder;

        /**
         * {@code true} if test should interact with the raft group after a snapshot has been captured.
         * In this case, the follower node should catch up with the leader using raft log.
         */
        private final boolean interactAfterSnapshot;

        /** */
        private TestData(boolean deleteFolder, boolean interactAfterSnapshot) {
            this.deleteFolder = deleteFolder;
            this.interactAfterSnapshot = interactAfterSnapshot;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return String.format("deleteFolder=%s, interactAfterSnapshot=%s", deleteFolder, interactAfterSnapshot);
        }
    }

    /**
     * @return {@link #testSnapshot} parameters.
     */
    private static List<TestData> testSnapshotData() {
        return List.of(
            new TestData(false, false),
            new TestData(true, true),
            new TestData(false, true),
            new TestData(true, false)
        );
    }

    /**
     * Tests that a joining raft node successfully restores a snapshot.
     *
     * @param testData Test parameters.
     * @param testInfo Test info.
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @MethodSource("testSnapshotData")
    public void testSnapshot(TestData testData, TestInfo testInfo) throws Exception {
        // Set up a raft group service
        RaftGroupService service = prepareRaftGroup(testInfo);

        beforeFollowerStop(service);

        // Select any node that is not the leader of the group
        JRaftServerImpl toStop = servers.stream()
            .filter(server -> !server.localPeer(raftGroupId()).equals(service.leader()))
            .findAny()
            .orElseThrow();

        // Get the path to that node's raft directory
        Path serverDataPath = toStop.getServerDataPath(raftGroupId());

        // Get the path to that node's RocksDB key-value storage
        Path dbPath = getListenerPersistencePath(getListener(toStop, raftGroupId()));

        int stopIdx = servers.indexOf(toStop);

        // Remove that node from the list of servers
        servers.remove(stopIdx);

        // Shutdown that node
        toStop.stop();

        // Create a snapshot of the raft group
        service.snapshot(service.leader()).get();

        afterFollowerStop(service);

        // Create another raft snapshot
        service.snapshot(service.leader()).get();

        if (testData.deleteFolder) {
            // Delete a stopped node's raft directory and key-value storage directory
            // to check if snapshot could be restored by the restarted node
            IgniteUtils.deleteIfExists(dbPath);
            IgniteUtils.deleteIfExists(serverDataPath);
        }

        if (testData.interactAfterSnapshot) {
            // Interact with the raft group after the second snapshot to check if the restarted node would see these
            // interactions after restoring a snapshot and raft logs
            afterSnapshot(service);
        }

        // Restart the node
        JRaftServerImpl restarted = startServer(testInfo, stopIdx);

        assertTrue(waitForTopology(cluster.get(0), servers.size(), 3_000));

        BooleanSupplier closure = snapshotCheckClosure(restarted, testData.interactAfterSnapshot);

        boolean success = waitForCondition(closure, 10_000);

        assertTrue(success);
    }

    /**
     * Interacts with the raft group before a follower is stopped.
     *
     * @param service Raft group service.
     * @throws Exception If failed.
     */
    public abstract void beforeFollowerStop(RaftGroupService service) throws Exception;

    /**
     * Interacts with the raft group after a follower is stopped.
     *
     * @param service Raft group service.
     * @throws Exception If failed.
     */
    public abstract void afterFollowerStop(RaftGroupService service) throws Exception;

    /**
     * Interacts with a raft group after the leader has captured a snapshot.
     *
     * @param service Raft group service.
     * @throws Exception If failed.
     */
    public abstract void afterSnapshot(RaftGroupService service) throws Exception;

    /**
     * Creates a closure that will be executed periodically to check if the snapshot and (conditionally on the
     * {@link TestData#interactAfterSnapshot}) the raft log was successfully restored by the follower node.
     *
     * @param restarted Restarted follower node.
     * @param interactedAfterSnapshot {@code true} whether raft group was interacted with after the snapshot operation.
     * @return Closure.
     */
    public abstract BooleanSupplier snapshotCheckClosure(JRaftServerImpl restarted, boolean interactedAfterSnapshot);

    /**
     * @param listener Raft group listener.
     * @return Path to the group's persistence.
     */
    public abstract Path getListenerPersistencePath(T listener);

    /**
     * Creates raft group listener.
     *
     * @param listenerPersistencePath Path to storage persistent data.
     * @return Raft group listener.
     */
    public abstract RaftGroupListener createListener(Path listenerPersistencePath);

    /**
     * @return Raft group id for tests.
     */
    public abstract String raftGroupId();

    /**
     * Get the raft group listener from the jraft server.
     *
     * @param server Server.
     * @param grpId Raft group id.
     * @return Raft group listener.
     */
    protected T getListener(JRaftServerImpl server, String grpId) {
        org.apache.ignite.raft.jraft.RaftGroupService svc = server.raftGroupService(grpId);

        JRaftServerImpl.DelegatingStateMachine fsm =
            (JRaftServerImpl.DelegatingStateMachine) svc.getRaftNode().getOptions().getFsm();

        return (T) fsm.getListener();
    }

    /**
     * @param cluster The cluster.
     * @param exp Expected count.
     * @param timeout The timeout in millis.
     * @return {@code True} if topology size is equal to expected.
     */
    private boolean waitForTopology(ClusterService cluster, int exp, int timeout) throws InterruptedException {
        return waitForCondition(() -> cluster.topologyService().allMembers().size() >= exp, timeout);
    }

    /**
     * @return Local address.
     */
    private static String getLocalAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a cluster service.
     */
    private ClusterService clusterService(TestInfo testInfo, int port, NetworkAddress otherPeer) {
        var nodeFinder = new StaticNodeFinder(List.of(otherPeer));

        var network = ClusterServiceTestUtils.clusterService(
            testInfo,
            port,
            nodeFinder,
            SERIALIZATION_REGISTRY,
            NETWORK_FACTORY
        );

        network.start();

        cluster.add(network);

        return network;
    }

    /**
     * Starts a raft server.
     *
     * @param testInfo Test info.
     * @param idx Server index (affects port of the server).
     * @return Server.
     */
    private JRaftServerImpl startServer(TestInfo testInfo, int idx) {
        var addr = new NetworkAddress(getLocalAddress(), PORT);

        ClusterService service = clusterService(testInfo, PORT + idx, addr);

        Path jraft = workDir.resolve("jraft" + idx);

        JRaftServerImpl server = new JRaftServerImpl(service, jraft) {
            @Override public void stop() {
                super.stop();

                service.stop();
            }
        };

        server.start();

        Path listenerPersistencePath = workDir.resolve("db" + idx);

        server.startRaftGroup(
            raftGroupId(),
            createListener(listenerPersistencePath),
            INITIAL_CONF
        );

        servers.add(server);

        return server;
    }

    /**
     * Prepares raft group service by instantiating raft servers and a client.
     *
     * @return Raft group service instance.
     */
    private RaftGroupService prepareRaftGroup(TestInfo testInfo) throws Exception {
        for (int i = 0; i < INITIAL_CONF.size(); i++)
            startServer(testInfo, i);

        assertTrue(waitForTopology(cluster.get(0), servers.size(), 3_000));

        return startClient(testInfo, raftGroupId(), new NetworkAddress(getLocalAddress(), PORT));
    }

    /**
     * Starts a client with a specific address.
     */
    private RaftGroupService startClient(TestInfo testInfo, String groupId, NetworkAddress addr) throws Exception {
        ClusterService clientNode = clusterService(testInfo, CLIENT_PORT + clients.size(), addr);

        RaftGroupService client = RaftGroupServiceImpl.start(groupId, clientNode, FACTORY, 10_000,
            List.of(new Peer(addr)), false, 200, executor).get(3, TimeUnit.SECONDS);

        clients.add(client);

        return client;
    }
}

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

package org.apache.ignite.internal.metastorage.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDBKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.raft.server.impl.JRaftServerImpl;
import org.apache.ignite.internal.raft.server.impl.JRaftServerImpl.DelegatingStateMachine;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.message.RaftClientMessagesFactory;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Persistent (rocksdb-based) meta storage client tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ITMetaStorageServicePersistenceTest {
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

    /** */
    private static final String METASTORAGE_RAFT_GROUP_NAME = "METASTORAGE_RAFT_GROUP";

    /** Factory. */
    private static final RaftClientMessagesFactory FACTORY = new RaftClientMessagesFactory();

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

    /**
     * Shutdown raft server and stop all cluster nodes.
     *
     * @throws Exception If failed to shutdown raft server,
     */
    @AfterEach
    public void afterTest() throws Exception {
        for (RaftGroupService client : clients)
            client.shutdown();

        for (JRaftServerImpl server : servers)
            server.stop();
    }

    /**
     * Test parameters for {@link #testSnapshot}.
     */
    private static class TestData {
        /** Delete raft group folder. */
        private final boolean deleteFolder;

        /** Write to meta storage after a snapshot. */
        private final boolean writeAfterSnapshot;

        /** */
        private TestData(boolean deleteFolder, boolean writeAfterSnapshot) {
            this.deleteFolder = deleteFolder;
            this.writeAfterSnapshot = writeAfterSnapshot;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return String.format("deleteFolder=%s, writeAfterSnapshot=%s", deleteFolder, writeAfterSnapshot);
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
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @MethodSource("testSnapshotData")
    public void testSnapshot(TestData testData) throws Exception {
        ByteArray firstKey = ByteArray.fromString("first");
        byte[] firstValue = "firstValue".getBytes(StandardCharsets.UTF_8);

        // Setup a metastorage raft service
        RaftGroupService metaStorageSvc = prepareMetaStorage();

        MetaStorageServiceImpl metaStorage = new MetaStorageServiceImpl(metaStorageSvc, null);

        // Put some data in the metastorage
        metaStorage.put(firstKey, firstValue).get();

        // Check that data has been written successfully
        check(metaStorage, new EntryImpl(firstKey, firstValue, 1, 1));;

        // Select any node that is not the leader of the group
        JRaftServerImpl toStop = servers.stream()
            .filter(server -> !server.localPeer(METASTORAGE_RAFT_GROUP_NAME).equals(metaStorageSvc.leader()))
            .findAny()
            .orElseThrow();

        // Get the path to that node's raft directory
        String serverDataPath = toStop.getServerDataPath(METASTORAGE_RAFT_GROUP_NAME);

        // Get the path to that node's RocksDB key-value storage
        Path dbPath = getStorage(toStop).getDbPath();

        int stopIdx = servers.indexOf(toStop);

        // Remove that node from the list of servers
        servers.remove(stopIdx);

        // Shutdown that node
        toStop.stop();

        // Create a raft snapshot of the metastorage service
        metaStorageSvc.snapshot(metaStorageSvc.leader()).get();

        // Remove the first key from the metastorage
        metaStorage.remove(firstKey).get();

        // Check that data has been removed
        check(metaStorage, new EntryImpl(firstKey, null, 2, 2));

        // Put same data again
        metaStorage.put(firstKey, firstValue).get();

        // Check that it has been written
        check(metaStorage, new EntryImpl(firstKey, firstValue, 3, 3));

        // Create another raft snapshot
        metaStorageSvc.snapshot(metaStorageSvc.leader()).get();

        byte[] lastKey = firstKey.bytes();
        byte[] lastValue = firstValue;

        if (testData.deleteFolder) {
            // Delete stopped node's raft directory and key-value storage directory
            // to check if snapshot could be restored by the restarted node
            IgniteUtils.deleteIfExists(dbPath);
            IgniteUtils.deleteIfExists(Paths.get(serverDataPath));
        }

        if (testData.writeAfterSnapshot) {
            // Put new data after the second snapshot to check if after
            // the snapshot restore operation restarted node will receive it
            ByteArray secondKey = ByteArray.fromString("second");
            byte[] secondValue = "secondValue".getBytes(StandardCharsets.UTF_8);

            metaStorage.put(secondKey, secondValue).get();

            lastKey = secondKey.bytes();
            lastValue = secondValue;
        }

        // Restart the node
        JRaftServerImpl restarted = startServer(stopIdx, new RocksDBKeyValueStorage(dbPath));

        assertTrue(waitForTopology(cluster.get(0), servers.size(), 3_000));

        KeyValueStorage storage = getStorage(restarted);

        byte[] finalLastKey = lastKey;

        int expectedRevision = testData.writeAfterSnapshot ? 4 : 3;
        int expectedUpdateCounter = testData.writeAfterSnapshot ? 4 : 3;

        EntryImpl expectedLastEntry = new EntryImpl(new ByteArray(lastKey), lastValue, expectedRevision, expectedUpdateCounter);

        // Wait until the snapshot is restored
        boolean success = waitForCondition(() -> {
            org.apache.ignite.internal.metastorage.server.Entry e = storage.get(finalLastKey);
            return e.empty() == expectedLastEntry.empty()
                && e.tombstone() == expectedLastEntry.tombstone()
                && e.revision() == expectedLastEntry.revision()
                && e.updateCounter() == expectedLastEntry.revision()
                && Arrays.equals(e.key(), expectedLastEntry.key().bytes())
                && Arrays.equals(e.value(), expectedLastEntry.value());
        }, 3_000);

        assertTrue(success);

        // Check that the last value has been written successfully
        check(metaStorage, expectedLastEntry);
    }

    /**
     * Get the meta store's key-value storage of the jraft server.
     *
     * @param server Server.
     * @return Meta store's key value storage.
     */
    private static RocksDBKeyValueStorage getStorage(JRaftServerImpl server) {
        org.apache.ignite.raft.jraft.RaftGroupService svc = server.raftGroupService(METASTORAGE_RAFT_GROUP_NAME);

        DelegatingStateMachine fsm = (DelegatingStateMachine) svc.getRaftNode().getOptions().getFsm();

        MetaStorageListener listener = (MetaStorageListener) fsm.getListener();

        KeyValueStorage storage = listener.getStorage();

        return (RocksDBKeyValueStorage) storage;
    }

    /**
     * Check meta storage entry.
     *
     * @param metaStorage Meta storage service.
     * @param expected Expected entry.
     * @throws ExecutionException If failed.
     * @throws InterruptedException If failed.
     */
    private void check(MetaStorageServiceImpl metaStorage, EntryImpl expected)
        throws ExecutionException, InterruptedException {
        Entry entry = metaStorage.get(expected.key()).get();

        assertEquals(expected, entry);
    }

    /** */
    @SuppressWarnings("BusyWait") private static boolean waitForCondition(BooleanSupplier cond, long timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < stop) {
            if (cond.getAsBoolean())
                return true;

            try {
                sleep(50);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }

    /**
     * @param cluster The cluster.
     * @param exp Expected count.
     * @param timeout The timeout in millis.
     * @return {@code True} if topology size is equal to expected.
     */
    private boolean waitForTopology(ClusterService cluster, int exp, int timeout) {
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
    private ClusterService clusterService(String name, int port, NetworkAddress otherPeer) {
        var nodeFinder = new StaticNodeFinder(List.of(otherPeer));

        var context = new ClusterLocalConfiguration(name, port, nodeFinder, SERIALIZATION_REGISTRY);

        var network = NETWORK_FACTORY.createClusterService(context);

        network.start();

        cluster.add(network);

        return network;
    }

    /**
     * Starts a raft server.
     *
     * @param idx Server index (affects port of the server).
     * @param storage KeyValueStorage for the MetaStorage.
     * @return Server.
     */
    private JRaftServerImpl startServer(int idx, KeyValueStorage storage) {
        var addr = new NetworkAddress(getLocalAddress(), PORT);

        ClusterService service = clusterService("server" + idx, PORT + idx, addr);

        Path jraft = workDir.resolve("jraft" + idx);

        JRaftServerImpl server = new JRaftServerImpl(service, jraft.toString()) {
            @Override public void stop() {
                super.stop();

                service.stop();
            }
        };

        server.start();

        server.startRaftGroup(
            METASTORAGE_RAFT_GROUP_NAME,
            new MetaStorageListener(storage),
            INITIAL_CONF
        );

        servers.add(server);

        return server;
    }

    /**
     * Prepares meta storage by instantiating corresponding raft server with {@link MetaStorageListener} and
     * a client.
     *
     * @return Meta storage raft group service instance.
     */
    private RaftGroupService prepareMetaStorage() throws IOException {
        for (int i = 0; i < INITIAL_CONF.size(); i++)
            startServer(i, new RocksDBKeyValueStorage(workDir.resolve(UUID.randomUUID().toString())));

        assertTrue(waitForTopology(cluster.get(0), servers.size(), 3_000));

        return startClient(METASTORAGE_RAFT_GROUP_NAME, new NetworkAddress(getLocalAddress(), PORT));
    }

    /**
     * Starts a client with a specific address.
     */
    private RaftGroupService startClient(String groupId, NetworkAddress addr) {
        ClusterService clientNode = clusterService("client_" + groupId + "_", CLIENT_PORT + clients.size(), addr);

        RaftGroupServiceImpl client = new RaftGroupServiceImpl(groupId, clientNode, FACTORY, 10_000,
            List.of(new Peer(addr)), false, 200) {
            @Override public void shutdown() {
                super.shutdown();

                clientNode.stop();
            }
        };

        clients.add(client);

        return client;
    }
}

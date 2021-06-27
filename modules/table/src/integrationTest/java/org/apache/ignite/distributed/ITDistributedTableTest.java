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

package org.apache.ignite.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.affinity.RendezvousAffinityFunction;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.Row;
import org.apache.ignite.internal.schema.RowAssembler;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.command.GetCommand;
import org.apache.ignite.internal.table.distributed.command.InsertCommand;
import org.apache.ignite.internal.table.distributed.command.response.SingleRowResponse;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.message.RaftClientMessagesFactory;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.RaftServerImpl;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Distributed internal table tests.
 */
public class ITDistributedTableTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ITDistributedTableTest.class);

    /** Base network port. */
    public static final int NODE_PORT_BASE = 20_000;

    /** Nodes. */
    public static final int NODES = 5;

    /** Partitions. */
    public static final int PARTS = 10;

    /** Factory. */
    private static final RaftClientMessagesFactory FACTORY = new RaftClientMessagesFactory();

    /** Network factory. */
    private static final ClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();

    /** */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistryImpl();

    /** Client. */
    private ClusterService client;

    /** Schema. */
    public static SchemaDescriptor SCHEMA = new SchemaDescriptor(UUID.randomUUID(),
        1,
        new Column[] {new Column("key", NativeTypes.LONG, false)},
        new Column[] {new Column("value", NativeTypes.LONG, false)}
    );

    /** Cluster. */
    private ArrayList<ClusterService> cluster = new ArrayList<>();

    /**
     * Start all cluster nodes before each test.
     */
    @BeforeEach
    public void beforeTest() {
        List<NetworkAddress> allNodes = IntStream.range(NODE_PORT_BASE, NODE_PORT_BASE + NODES)
            .mapToObj(port -> new NetworkAddress("localhost", port))
            .collect(Collectors.toList());

        for (int i = 0; i < NODES; i++)
            cluster.add(startClient("node_" + i, NODE_PORT_BASE + i, allNodes));

        for (ClusterService node : cluster)
            assertTrue(waitForTopology(node, NODES, 1000));

        LOG.info("Cluster started.");

        client = startClient("client", NODE_PORT_BASE + NODES, allNodes);

        assertTrue(waitForTopology(client, NODES + 1, 1000));

        LOG.info("Client started.");
    }

    /**
     * Shutdowns all cluster nodes after each test.
     *
     * @throws Exception If failed.
     */
    @AfterEach
    public void afterTest() throws Exception {
        for (ClusterService node : cluster) {
            node.shutdown();
        }

        client.shutdown();
    }

    /**
     * Tests partition listener.
     *
     * @throws Exception If failed.
     */
    @Test
    public void partitionListener() throws Exception {
        String grpId = "part";

        RaftServer partSrv = new RaftServerImpl(cluster.get(0), FACTORY);

        List<Peer> conf = List.of(new Peer(cluster.get(0).topologyService().localMember().address()));

        partSrv.startRaftGroup(grpId, new PartitionListener(), conf);

        RaftGroupService partRaftGrp = new RaftGroupServiceImpl(grpId, client, FACTORY, 10_000, conf, true, 200);

        Row testRow = getTestRow();

        CompletableFuture<Boolean> insertFur = partRaftGrp.run(new InsertCommand(testRow));

        assertTrue(insertFur.get());

//        Row keyChunk = new Row(SCHEMA, new ByteBufferRow(testRow.keySlice()));
        Row keyChunk = getTestKey();

        CompletableFuture<SingleRowResponse> getFut = partRaftGrp.run(new GetCommand(keyChunk));

        assertNotNull(getFut.get().getValue());

        assertEquals(testRow.longValue(1), new Row(SCHEMA, getFut.get().getValue()).longValue(1));

        partSrv.shutdown();
    }

    /**
     * Prepares a test row which contains one field.
     *
     * @return Row.
     */
    @NotNull private Row getTestKey() {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 4096, 0, 0);

        rowBuilder.appendLong(1L);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.build()));
    }

    /**
     * Prepares a test row which contains two fields.
     *
     * @return Row.
     */
    @NotNull private Row getTestRow() {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 4096, 0, 0);

        rowBuilder.appendLong(1L);
        rowBuilder.appendLong(10L);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.build()));
    }

    /**
     * The test prepares a distributed table and checks operation over various views.
     */
    @Test
    public void partitionedTable() {
        HashMap<ClusterNode, RaftServer> raftServers = new HashMap<>(NODES);

        for (int i = 0; i < NODES; i++)
            raftServers.put(cluster.get(i).topologyService().localMember(), new RaftServerImpl(cluster.get(i), FACTORY));

        List<List<ClusterNode>> assignment = RendezvousAffinityFunction.assignPartitions(
            cluster.stream().map(node -> node.topologyService().localMember()).collect(Collectors.toList()),
            PARTS,
            1,
            false,
            null
        );

        int p = 0;

        Map<Integer, RaftGroupService> partMap = new HashMap<>();

        for (List<ClusterNode> partNodes : assignment) {
            RaftServer rs = raftServers.get(partNodes.get(0));

            String grpId = "part-" + p;

            List<Peer> conf = List.of(new Peer(partNodes.get(0).address()));

            rs.startRaftGroup(grpId, new PartitionListener(), conf);

            partMap.put(p, new RaftGroupServiceImpl(grpId, client, FACTORY, 10_000, conf, true, 200));

            p++;
        }

        Table tbl = new TableImpl(new InternalTableImpl(
            "tbl",
            UUID.randomUUID(),
            partMap,
            PARTS
        ), new SchemaRegistry() {
            @Override public SchemaDescriptor schema() {
                return SCHEMA;
            }

            @Override public SchemaDescriptor schema(int ver) {
                return SCHEMA;
            }
        });

        partitionedTableView(tbl, PARTS * 10);

        partitionedTableKVBinaryView(tbl.kvView(), PARTS * 10);
    }

    /**
     * Checks operation over row table view.
     *
     * @param view Table view.
     * @param keysCnt Count of keys.
     */
    public void partitionedTableView(Table view, int keysCnt) {
        LOG.info("Test for Table view [keys={}]", keysCnt);

        for (int i = 0; i < keysCnt; i++) {
            view.insert(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .set("value", Long.valueOf(i + 2))
                .build()
            );
        }

        for (int i = 0; i < keysCnt; i++) {
            Tuple entry = view.get(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .build());

            assertEquals(Long.valueOf(i + 2), entry.longValue("value"));
        }

        for (int i = 0; i < keysCnt; i++) {
            view.upsert(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .set("value", Long.valueOf(i + 5))
                .build()
            );

            Tuple entry = view.get(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .build());

            assertEquals(Long.valueOf(i + 5), entry.longValue("value"));
        }

        HashSet<Tuple> keys = new HashSet<>();

        for (int i = 0; i < keysCnt; i++) {
            keys.add(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .build());
        }

        Collection<Tuple> entries = view.getAll(keys);

        assertEquals(keysCnt, entries.size());

        for (int i = 0; i < keysCnt; i++) {
            boolean res = view.replace(
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .set("value", Long.valueOf(i + 5))
                    .build(),
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .set("value", Long.valueOf(i + 2))
                    .build());

            assertTrue(res);
        }

        for (int i = 0; i < keysCnt; i++) {
            boolean res = view.delete(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .build());

            assertTrue(res);

            Tuple entry = view.get(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .build());

            assertNull(entry);
        }

        ArrayList<Tuple> batch = new ArrayList<>(keysCnt);

        for (int i = 0; i < keysCnt; i++) {
            batch.add(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .set("value", Long.valueOf(i + 2))
                .build());
        }

        view.upsertAll(batch);

        for (int i = 0; i < keysCnt; i++) {
            Tuple entry = view.get(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .build());

            assertEquals(Long.valueOf(i + 2), entry.longValue("value"));
        }

        view.deleteAll(keys);

        for (Tuple key : keys) {
            Tuple entry = view.get(key);

            assertNull(entry);
        }
    }

    /**
     * Checks operation over key-value binary table view.
     *
     * @param view Table view.
     * @param keysCnt Count of keys.
     */
    public void partitionedTableKVBinaryView(KeyValueBinaryView view, int keysCnt) {
        LOG.info("Tes for Key-Value binary view [keys={}]", keysCnt);

        for (int i = 0; i < keysCnt; i++) {
            view.putIfAbsent(
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .build(),
                view.tupleBuilder()
                    .set("value", Long.valueOf(i + 2))
                    .build());
        }

        for (int i = 0; i < keysCnt; i++) {
            Tuple entry = view.get(
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .build());

            assertEquals(Long.valueOf(i + 2), entry.longValue("value"));
        }

        for (int i = 0; i < keysCnt; i++) {
            view.put(
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .build(),
                view.tupleBuilder()
                    .set("value", Long.valueOf(i + 5))
                    .build());

            Tuple entry = view.get(
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .build());

            assertEquals(Long.valueOf(i + 5), entry.longValue("value"));
        }

        HashSet<Tuple> keys = new HashSet<>();

        for (int i = 0; i < keysCnt; i++) {
            keys.add(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .build());
        }

        Map<Tuple, Tuple> entries = view.getAll(keys);

        assertEquals(keysCnt, entries.size());

        for (int i = 0; i < keysCnt; i++) {
            boolean res = view.replace(
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .build(),
                view.tupleBuilder()
                    .set("value", Long.valueOf(i + 5))
                    .build(),
                view.tupleBuilder()
                    .set("value", Long.valueOf(i + 2))
                    .build());

            assertTrue(res);
        }

        for (int i = 0; i < keysCnt; i++) {
            boolean res = view.remove(
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .build());

            assertTrue(res);

            Tuple entry = view.get(
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .build());

            assertNull(entry);
        }

        HashMap<Tuple, Tuple> batch = new HashMap<>(keysCnt);

        for (int i = 0; i < keysCnt; i++) {
            batch.put(
                view.tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .build(),
                view.tupleBuilder()
                    .set("value", Long.valueOf(i + 2))
                    .build());
        }

        view.putAll(batch);

        for (int i = 0; i < keysCnt; i++) {
            Tuple entry = view.get(view.tupleBuilder()
                .set("key", Long.valueOf(i))
                .build());

            assertEquals(Long.valueOf(i + 2), entry.longValue("value"));
        }

        view.removeAll(keys);

        for (Tuple key : keys) {
            Tuple entry = view.get(key);

            assertNull(entry);
        }
    }

    /**
     * @param name Node name.
     * @param port Local port.
     * @param servers Server nodes of the cluster.
     * @return The client cluster view.
     */
    private ClusterService startClient(String name, int port, List<NetworkAddress> servers) {
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

        while (System.currentTimeMillis() < stop) {
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

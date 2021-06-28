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

package org.apache.ignite.internal.runner.app;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.IgnitionManager;
import org.apache.ignite.internal.app.IgnitionCleaner;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Ignition interface tests.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-14581")
class DynamicTableCreationTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(SchemaManager.class);

    /** Nodes bootstrap configuration. */
    private final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>() {{
            put("node0", "{\n" +
                "  \"node\": {\n" +
                "    \"metastorageNodes\":[ \"node0\" ]\n" +
                "  },\n" +
                "  \"network\": {\n" +
                "    \"port\":3344,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
                "  }\n" +
                "}");

            put("node1", "{\n" +
                "  \"node\": {\n" +
                "    \"metastorageNodes\":[ \"node0\" ]\n" +
                "  },\n" +
                "  \"network\": {\n" +
                "    \"port\":3345,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
                "  }\n" +
                "}");

            put("node2", "{\n" +
                "  \"node\": {\n" +
                "    \"metastorageNodes\":[ \"node0\"]\n" +
                "  },\n" +
                "  \"network\": {\n" +
                "    \"port\":3346,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
                "  }\n" +
                "}");
        }};

    /** */
    private final List<Ignite> clusterNodes = new ArrayList<>();

    /** */
    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(clusterNodes);

        IgnitionCleaner.removeAllData();
    }

    /**
     * Check dynamic table creation.
     */
    @Test
    void testDynamicSimpleTableCreation() {
        for (Map.Entry<String, String> nodeBootstrapCfg : nodesBootstrapCfg.entrySet())
            clusterNodes.add(IgnitionManager.start(nodeBootstrapCfg.getKey(), nodeBootstrapCfg.getValue()));

        assertEquals(3, clusterNodes.size());

        // Create table on node 0.
        SchemaTable schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
            SchemaBuilders.column("key", ColumnType.INT64).asNonNull().build(),
            SchemaBuilders.column("val", ColumnType.INT32).asNullable().build()
        ).withPrimaryKey("key").build();

        clusterNodes.get(0).tables().createTable(schTbl1.canonicalName(), tblCh ->
            SchemaConfigurationConverter.convert(schTbl1, tblCh)
                .changeReplicas(1)
                .changePartitions(10)
        );

        // Put data on node 1.
        Table tbl1 = clusterNodes.get(1).tables().table(schTbl1.canonicalName());
        KeyValueBinaryView kvView1 = tbl1.kvView();

        tbl1.insert(tbl1.tupleBuilder().set("key", 1L).set("val", 111).build());
        kvView1.put(tbl1.tupleBuilder().set("key", 2L).build(), tbl1.tupleBuilder().set("val", 222).build());

        // Get data on node 2.
        Table tbl2 = clusterNodes.get(2).tables().table(schTbl1.canonicalName());
        KeyValueBinaryView kvView2 = tbl2.kvView();

        final Tuple keyTuple1 = tbl2.tupleBuilder().set("key", 1L).build();
        final Tuple keyTuple2 = kvView2.tupleBuilder().set("key", 2L).build();

        assertThrows(IllegalArgumentException.class, () -> kvView2.get(keyTuple1).value("key"));
        assertThrows(IllegalArgumentException.class, () -> kvView2.get(keyTuple1).value("key"));
        assertEquals(1, (Long)tbl2.get(keyTuple1).value("key"));
        assertEquals(2, (Long)tbl2.get(keyTuple2).value("key"));

        assertEquals(111, (Integer)tbl2.get(keyTuple1).value("val"));
        assertEquals(111, (Integer)kvView2.get(keyTuple1).value("val"));
        assertEquals(222, (Integer)tbl2.get(keyTuple2).value("val"));
        assertEquals(222, (Integer)kvView2.get(keyTuple2).value("val"));
    }

    /**
     * Check dynamic table creation.
     */
    @Test
    void testDynamicTableCreation() {
        for (Map.Entry<String, String> nodeBootstrapCfg : nodesBootstrapCfg.entrySet())
            clusterNodes.add(IgnitionManager.start(nodeBootstrapCfg.getKey(), nodeBootstrapCfg.getValue()));

        assertEquals(3, clusterNodes.size());

        // Create table on node 0.
        SchemaTable scmTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
            SchemaBuilders.column("key", ColumnType.UUID).asNonNull().build(),
            SchemaBuilders.column("affKey", ColumnType.INT64).asNonNull().build(),
            SchemaBuilders.column("valStr", ColumnType.string()).asNullable().build(),
            SchemaBuilders.column("valInt", ColumnType.INT32).asNullable().build(),
            SchemaBuilders.column("valNull", ColumnType.INT16).asNullable().build()
        ).withIndex(
            SchemaBuilders.pkIndex()
                .addIndexColumn("key").done()
                .addIndexColumn("affKey").done()
                .withAffinityColumns("affKey")
                .build()
        ).build();

        clusterNodes.get(0).tables().createTable(scmTbl1.canonicalName(), tblCh ->
            SchemaConfigurationConverter.convert(scmTbl1, tblCh)
                .changeReplicas(1)
                .changePartitions(10));

        final UUID uuid = UUID.randomUUID();
        final UUID uuid2 = UUID.randomUUID();

        // Put data on node 1.
        Table tbl1 = clusterNodes.get(1).tables().table(scmTbl1.canonicalName());
        KeyValueBinaryView kvView1 = tbl1.kvView();

        tbl1.insert(tbl1.tupleBuilder().set("key", uuid).set("affKey", 42L)
            .set("valStr", "String value").set("valInt", 73).set("valNull", null).build());

        kvView1.put(kvView1.tupleBuilder().set("key", uuid2).set("affKey", 4242L).build(),
            kvView1.tupleBuilder().set("valStr", "String value 2").set("valInt", 7373).set("valNull", null).build());

        // Get data on node 2.
        Table tbl2 = clusterNodes.get(2).tables().table(scmTbl1.canonicalName());
        KeyValueBinaryView kvView2 = tbl2.kvView();

        final Tuple keyTuple1 = tbl2.tupleBuilder().set("key", uuid).set("affKey", 42L).build();
        final Tuple keyTuple2 = tbl2.tupleBuilder().set("key", uuid2).set("affKey", 4242L).build();

        // KV view must NOT return key columns in value.
        assertThrows(IllegalArgumentException.class, () -> kvView2.get(keyTuple1).value("key"));
        assertThrows(IllegalArgumentException.class, () -> kvView2.get(keyTuple1).value("affKey"));
        assertThrows(IllegalArgumentException.class, () -> kvView2.get(keyTuple2).value("key"));
        assertThrows(IllegalArgumentException.class, () -> kvView2.get(keyTuple2).value("affKey"));

        // Record binary view MUST return key columns in value.
        assertEquals(uuid, tbl2.get(keyTuple1).value("key"));
        assertEquals(42L, (Long)tbl2.get(keyTuple1).value("affKey"));
        assertEquals(uuid2, tbl2.get(keyTuple2).value("key"));
        assertEquals(4242L, (Long)tbl2.get(keyTuple2).value("affKey"));

        assertEquals("String value", tbl2.get(keyTuple1).value("valStr"));
        assertEquals(73, (Integer)tbl2.get(keyTuple1).value("valInt"));
        assertNull(tbl2.get(keyTuple1).value("valNull"));

        assertEquals("String value 2", tbl2.get(keyTuple2).value("valStr"));
        assertEquals(7373, (Integer)tbl2.get(keyTuple2).value("valInt"));
        assertNull(tbl2.get(keyTuple2).value("valNull"));

        assertEquals("String value", kvView2.get(keyTuple1).value("valStr"));
        assertEquals(73, (Integer)kvView2.get(keyTuple1).value("valInt"));
        assertNull(kvView2.get(keyTuple1).value("valNull"));

        assertEquals("String value 2", kvView2.get(keyTuple2).value("valStr"));
        assertEquals(7373, (Integer)kvView2.get(keyTuple2).value("valInt"));
        assertNull(kvView2.get(keyTuple2).value("valNull"));
    }
}

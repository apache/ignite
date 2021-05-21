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
import java.util.List;
import java.util.UUID;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.IgnitionManager;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Ignition interface tests.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-14581")
class DynamicTableCreationTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(SchemaManager.class);

    /** Nodes bootstrap configuration. */
    private final String[] nodesBootstrapCfg =
        {
            "{\n" +
                "  \"node\": {\n" +
                "    \"name\":node0,\n" +
                "    \"metastorageNodes\":[ \"node0\" ]\n" +
                "  },\n" +
                "  \"network\": {\n" +
                "    \"port\":3344,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
                "  }\n" +
                "}",

            "{\n" +
                "  \"node\": {\n" +
                "    \"name\":node1,\n" +
                "    \"metastorageNodes\":[ \"node0\" ]\n" +
                "  },\n" +
                "  \"network\": {\n" +
                "    \"port\":3345,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
                "  }\n" +
                "}",

            "{\n" +
                "  \"node\": {\n" +
                "    \"name\":node2,\n" +
                "    \"metastorageNodes\":[ \"node0\"]\n" +
                "  },\n" +
                "  \"network\": {\n" +
                "    \"port\":3346,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
                "  }\n" +
                "}",
        };

    /**
     * Check dynamic table creation.
     */
    @Test
    void testDynamicSimpleTableCreation() {
        List<Ignite> clusterNodex = new ArrayList<>();

        for (String nodeBootstrapCfg : nodesBootstrapCfg)
            clusterNodex.add(IgnitionManager.start(nodeBootstrapCfg));

        assertEquals(3, clusterNodex.size());

        // Create table on node 0.
        clusterNodex.get(0).tables().createTable("tbl1", tbl -> tbl
            .changeName("tbl1")
            .changeReplicas(1)
            .changePartitions(10)
            .changeColumns(cols -> cols
                .create("key", c -> c.changeName("key").changeNullable(false).changeType(t -> t.changeType("int")))
                .create("val", c -> c.changeName("val").changeNullable(true).changeType(t -> t.changeType("int")))
            )
            .changeIndices(idxs -> idxs
                .create("PK", idx -> idx
                    .changeName("PK")
                    .changeType("PRIMARY")
                    .changeColNames(new String[] {"key"})
                    .changeColumns(c -> c
                        .create("key", t -> t.changeName("key").changeAsc(true)))
                    .changeAffinityColumns(new String[] {"key"}))
            ));

        // Put data on node 1.
        Table tbl1 = waitForTable(clusterNodex.get(1));
        tbl1.insert(tbl1.tupleBuilder().set("key", 1).set("val", 111).build());

        // Get data on node 2.
        Table tbl2 = waitForTable(clusterNodex.get(2));
        assertEquals(111, (int)tbl2.get(tbl2.tupleBuilder().set("key", 1).build()).value("val"));
    }

    /**
     * Waits for table, until it is initialized.
     *
     * @param ign Ignite.
     * @return Table.
     */
    private Table waitForTable(Ignite ign) {
        while (ign.tables().table("tbl1") == null) {
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                LOG.warn("Waiting for table is interrupted.");
            }
        }

        return ign.tables().table("tbl1");
    }

    /**
     * Check dynamic table creation.
     */
    @Test
    void testDynamicTableCreation() {
        List<Ignite> clusterNodes = new ArrayList<>();

        for (String nodeBootstrapCfg : nodesBootstrapCfg)
            clusterNodes.add(IgnitionManager.start(nodeBootstrapCfg));

        assertEquals(3, clusterNodes.size());

        // Create table on node 0.
        clusterNodes.get(0).tables().createTable("tbl1", tbl -> tbl
            .changeName("tbl1")
            .changeReplicas(1)
            .changePartitions(10)
            .changeColumns(cols -> cols
                .create("key", c -> c.changeName("key").changeNullable(false).changeType(t -> t.changeType("uuid")))
                .create("affKey", c -> c.changeName("affKey").changeNullable(false).changeType(t -> t.changeType("int")))
                .create("valStr", c -> c.changeName("valStr").changeNullable(true).changeType(t -> t.changeType("string")))
                .create("valInt", c -> c.changeName("valInt").changeNullable(true).changeType(t -> t.changeType("int")))
                .create("valNullable", c -> c.changeName("valNullable").changeNullable(true).changeType(t -> t.changeType("short")).changeNullable(true))
            )
            .changeIndices(idxs -> idxs
                .create("PK", idx -> idx
                    .changeName("PK")
                    .changeType("PRIMARY")
                    .changeColNames(new String[] {"key", "affKey"})
                    .changeColumns(c -> c
                        .create("key", t -> t.changeName("key").changeAsc(true))
                        .create("affKey", t -> t.changeName("affKey").changeAsc(true)))
                    .changeAffinityColumns(new String[] {"affKey"}))
            ));

        final UUID uuid = UUID.randomUUID();

        // Put data on node 1.
        Table tbl1 = waitForTable(clusterNodes.get(1));
        tbl1.insert(tbl1.tupleBuilder().set("key", uuid).set("affKey", 42)
            .set("valStr", "String value").set("valInt", 73).set("valNullable", null).build());

        // Get data on node 2.
        Table tbl2 = waitForTable(clusterNodes.get(2));
        final Tuple val = tbl2.get(tbl2.tupleBuilder().set("key", uuid).set("affKey", 42).build());

        assertEquals("String value", val.value("valStr"));
        assertEquals(73, (int)val.value("valInt"));
        assertNull(val.value("valNullable"));
    }
}

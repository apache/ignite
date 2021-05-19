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
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Ignition interface tests.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-14578")
class TableCreationTest {
    /** Nodes bootstrap configuration with preconfigured tables. */
    private final String[] nodesBootstrapCfg =
        {
            "{\n" +
                "  \"node\": {\n" +
                "    \"name\":node0,\n" +
                "    \"metastorageNodes\":[ \"node0\", \"node1\" ]\n" +
                "  },\n" +
                "  \"network\": {\n" +
                "    \"port\":3344,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
                "  },\n" +
                "  \"table\": {\n" +
                "       \"tables\": {\n" +
                "           \"tbl1\": {\n" +
                "               \"partitions\":10,\n" +
                "               \"replicas\":2,\n" +
                "               \"columns\": { \n" +
                "                   \"key\": {\n" +
                "                       \"type\": {" +
                "                           \"type\":UUID\n" +
                "                       },\n" +
                "                       \"nullable\":false\n" +
                "                   },\n" +
                "                   \"affKey\": {\n" +
                "                       \"type\": {" +
                "                           \"type\":INT64\n" +
                "                       },\n" +
                "                       \"nullable\":false\n" +
                "                   },\n" +
                "                   \"valString\": {\n" +
                "                       \"type\": {" +
                "                           \"type\":String\n" +
                "                       },\n" +
                "                       \"nullable\":false\n" +
                "                   },\n" +
                "                   \"valInt\": {\n" +
                "                       \"type\": {" +
                "                           \"type\":INT32\n" +
                "                       },\n" +
                "                       \"nullable\":false\n" +
                "                   },\n" +
                "                   \"valNullable\": {\n" +
                "                       \"type\": {" +
                "                           \"type\":String\n" +
                "                       },\n" +
                "                       \"nullable\":true\n" +
                "                   }\n" +
                "               },\n" + /* Columns. */
                "               \"indices\": {\n" +
                "                   \"PK\": {\n" +
                "                       \"type\":PRIMARY,\n" +
                "                       \"columns\": {\n" +
                "                           \"key\": {\n" +
                "                               \"asc\":true\n" +
                "                           },\n" +
                "                           \"affKey\": {}\n" +
                "                       },\n" + /* Columns. */
                "                       \"affinityColumns\":[ \"affKey\" ]\n" +
                "                   }\n" +
                "               }\n" + /* Indices. */
                "           },\n" + /* Table. */
                "\n" +
                "           \"tbl2\": {\n" + // Table minimal configuration.
                "               \"columns\": { \n" +
                "                   \"key\": {\n" +
                "                       \"type\": {" +
                "                           \"type\":INT64\n" +
                "                       },\n" +
                "                   },\n" +
                "                   \"val\": {\n" +
                "                       \"type\": {" +
                "                           \"type\":INT64\n" +
                "                       },\n" +
                "                   }\n" +
                "               },\n" + /* Columns. */
                "               \"indices\": {\n" +
                "                   \"PK\": {\n" +
                "                       \"type\":PRIMARY,\n" +
                "                       \"columns\": {\n" +
                "                           \"key\": {}\n" +
                "                       },\n" + /* Columns. */
                "                   }\n" +
                "               }\n" + /* Indices. */
                "           }\n" + /* Table. */
                "       }\n" + /* Tables. */
                "  }\n" + /* Root. */
                "}",

            "{\n" +
                "  \"node\": {\n" +
                "    \"name\":node1,\n" +
                "    \"metastorageNodes\":[ \"node0\", \"node1\" ]\n" +
                "  },\n" +
                "  \"network\": {\n" +
                "    \"port\":3345,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
                "  }\n" +
                "}",

            "{\n" +
                "  \"node\": {\n" +
                "    \"name\":node2,\n" +
                "    \"metastorageNodes\":[ \"node0\", \"node1\" ]\n" +
                "  },\n" +
                "  \"network\": {\n" +
                "    \"port\":3346,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
                "  }\n" +
                "}",
        };

    /**
     * Check table creation via bootstrap configuration with pre-configured table.
     */
    @Test
    void testInitialSimpleTableConfiguration() {
        List<Ignite> clusterNodes = new ArrayList<>();

        for (String nodeBootstrapCfg : nodesBootstrapCfg)
            clusterNodes.add(IgnitionManager.start(nodeBootstrapCfg));

        assertEquals(3, clusterNodes.size());

        clusterNodes.forEach(Assertions::assertNotNull);

        { /* Table 1.*/
            Table tbl1 = clusterNodes.get(1).tables().table("tbl1");
            tbl1.insert(tbl1.tupleBuilder().set("key", 1L).set("val", 111L).build());

            Table tbl2 = clusterNodes.get(2).tables().table("tbl1");
            assertEquals(111L, tbl2.get(tbl2.tupleBuilder().set("key", 1L).build()));
        }

        { /* Table 2. */
            final UUID uuid = UUID.randomUUID();

            // Put data on node 1.
            Table tbl1 = clusterNodes.get(1).tables().table("tbl1");
            tbl1.insert(tbl1.tupleBuilder().set("key", uuid).set("affKey", 42L)
                .set("valStr", "String value").set("valInt", 73L).set("valNullable", null).build());

            // Get data on node 2.
            Table tbl2 = clusterNodes.get(2).tables().table("tbl1");
            final Tuple val = tbl2.get(tbl1.tupleBuilder().set("key", uuid).set("affKey", 42L).build());

            assertEquals("String value", val.value("valStr"));
            assertEquals(73L, (Long)val.value("valInt"));
            assertNull(val.value("valNullable"));
        }
    }
}

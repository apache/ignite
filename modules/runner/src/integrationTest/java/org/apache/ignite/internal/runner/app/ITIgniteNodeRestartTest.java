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

import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.IgnitionManager;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * These tests check node restart scenarios.
 */
public class ITIgniteNodeRestartTest extends IgniteAbstractTest {
    /** Default node port. */
    private static final int DEFAULT_NODE_PORT = 47500;

    /** Test table name. */
    private static final String TABLE_NAME = "Table1";

    /**
     * Restarts empty node.
     */
    @Test
    public void emptyNodeTest(TestInfo testInfo) {
        String nodeName = testNodeName(testInfo, DEFAULT_NODE_PORT);

        IgniteImpl ignite = (IgniteImpl)IgnitionManager.start(nodeName, null, workDir);

        int nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(DEFAULT_NODE_PORT, nodePort);

        IgnitionManager.stop(ignite.name());

        ignite = (IgniteImpl)IgnitionManager.start(nodeName, null, workDir);

        nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(DEFAULT_NODE_PORT, nodePort);

        IgnitionManager.stop(ignite.name());
    }

    /**
     * Restarts a node with changing configuration.
     */
    @Test
    public void changeConfigurationOnStartTest(TestInfo testInfo) {
        String nodeName = testNodeName(testInfo, DEFAULT_NODE_PORT);

        IgniteImpl ignite = (IgniteImpl)IgnitionManager.start(nodeName, null, workDir);

        int nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(DEFAULT_NODE_PORT, nodePort);

        IgnitionManager.stop(ignite.name());

        int newPort = 3322;

        String updateCfg = "network.port=" + newPort;

        ignite = (IgniteImpl)IgnitionManager.start(nodeName, updateCfg, workDir);

        nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(newPort, nodePort);

        IgnitionManager.stop(ignite.name());
    }

    /**
     * Checks that the only one non-default property overwrites after another configuration is passed on the node
     * restart.
     */
    @Test
    public void twoCustomPropertiesTest(TestInfo testInfo) {
        String nodeName = testNodeName(testInfo, 3344);

        String startCfg = "network: {\n" +
            "  port:3344,\n" +
            "  netClusterNodes:[ \"localhost:3344\" ]\n" +
            "}";

        IgniteImpl ignite = (IgniteImpl)IgnitionManager.start(nodeName, startCfg, workDir);

        assertEquals(
            3344,
            ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value()
        );

        assertArrayEquals(
            new String[] {"localhost:3344"},
            ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).netClusterNodes().value()
        );

        IgnitionManager.stop(ignite.name());

        ignite = (IgniteImpl)IgnitionManager.start(
            nodeName,
            "network.netClusterNodes=[ \"localhost:3344\", \"localhost:3343\" ]",
            workDir
        );

        assertEquals(
            3344,
            ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value()
        );

        assertArrayEquals(
            new String[] {"localhost:3344", "localhost:3343"},
            ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).netClusterNodes().value()
        );

        IgnitionManager.stop(ignite.name());
    }

    /**
     * Restarts the node which stores some data.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15255")
    public void nodeWithDataTest(TestInfo testInfo) {
        String nodeName = testNodeName(testInfo, 3344);

        Ignite ignite = IgnitionManager.start(nodeName, "{\n" +
            "  \"node\": {\n" +
            "    \"metastorageNodes\":[ " + nodeName + " ]\n" +
            "  },\n" +
            "  \"network\": {\n" +
            "    \"port\":3344,\n" +
            "    \"netClusterNodes\":[ \"localhost:3344\" ]\n" +
            "  }\n" +
            "}", workDir);

        TableDefinition scmTbl1 = SchemaBuilders.tableBuilder("PUBLIC", TABLE_NAME).columns(
            SchemaBuilders.column("id", ColumnType.INT32).asNonNull().build(),
            SchemaBuilders.column("name", ColumnType.string()).asNullable().build()
        ).withPrimaryKey(
            SchemaBuilders.primaryKey()
                .withColumns("id")
                .build()
        ).build();

        Table table = ignite.tables().createTableIfNotExists(
            scmTbl1.canonicalName(), tbl -> SchemaConfigurationConverter.convert(scmTbl1, tbl).changePartitions(10));

        for (int i = 0; i < 100; i++) {
            Tuple key = Tuple.create().set("id", i);
            Tuple val = Tuple.create().set("name", "name " + i);

            table.keyValueView().put(key, val);
        }

        IgnitionManager.stop(nodeName);

        ignite = IgnitionManager.start(nodeName, null, workDir);

        assertNotNull(ignite.tables().table(TABLE_NAME));

        for (int i = 0; i < 100; i++) {
            assertEquals("name " + i, table.keyValueView().get(Tuple.create()
                .set("id", i))
                .stringValue("name"));
        }

        IgnitionManager.stop(nodeName);
    }
}

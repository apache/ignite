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

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.table.Table;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The test checks that no threads left after one node stopped.
 */
public class ITNoThreadsLeftTest extends IgniteAbstractTest {
    /** Schema name. */
    public static final String SCHEMA = "PUBLIC";

    /** Short table name. */
    public static final String SHORT_TABLE_NAME = "tbl1";

    /** Table name. */
    public static final String TABLE_NAME = SCHEMA + "." + SHORT_TABLE_NAME;

    /** One node cluster configuration. */
    private static Function<String, String> NODE_CONFIGURATION = metastorageNodeName -> "{\n" +
        "  \"node\": {\n" +
        "    \"metastorageNodes\":[ " + metastorageNodeName + " ]\n" +
        "  },\n" +
        "  \"network\": {\n" +
        "    \"port\":3344,\n" +
        "    \"nodeFinder\": {\n" +
        "      \"netClusterNodes\":[ \"localhost:3344\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}";

    /**
     * Starts one node and stops it and
     * checks that the amount of thread equivalent as before.
     *
     * @param testInfo JUnit meta info for the test.
     * @throws Exception If failed.
     */
    @Test
    public void test(TestInfo testInfo) throws Exception {
        Set<Thread> threadsBefore = getCurrentThreads();

        String nodeName = IgniteTestUtils.testNodeName(testInfo, 0);

        Ignite ignite = IgnitionManager.start(
            nodeName,
            NODE_CONFIGURATION.apply(nodeName),
            workDir.resolve(nodeName));

        Table tbl = createTable(ignite, SCHEMA, SHORT_TABLE_NAME);

        assertNotNull(tbl);

        Set<Thread> threadsInTime = getCurrentThreads();

        assertTrue(threadsBefore.size() < threadsInTime.size(), threadsBefore.stream()
            .filter(thread -> !threadsInTime.contains(thread)).map(Thread::getName)
            .collect(Collectors.joining(",")));

        ignite.close();

        Set<Thread> threadsAfter = getCurrentThreads();

        assertEquals(threadsBefore.size(), threadsAfter.size(), threadsAfter.stream().
            filter(thread -> !threadsBefore.contains(thread)).map(Thread::getName)
            .collect(Collectors.joining(", ")));
    }

    /**
     * Creates a table.
     *
     * @param node           Cluster node.
     * @param schemaName     Schema name.
     * @param shortTableName Table name.
     */
    protected Table createTable(Ignite node, String schemaName, String shortTableName) {
        return node.tables().createTable(
            schemaName + "." + shortTableName, tblCh -> convert(SchemaBuilders.tableBuilder(schemaName, shortTableName).columns(
                SchemaBuilders.column("key", ColumnType.INT64).asNonNull().build(),
                SchemaBuilders.column("valInt", ColumnType.INT32).asNullable().build(),
                SchemaBuilders.column("valStr", ColumnType.string()).withDefaultValueExpression("default").build()
                ).withPrimaryKey("key").build(),
                tblCh).changeReplicas(2).changePartitions(10)
        );
    }

    /**
     * Get a set threads.
     * TODO: IGNITE-15161. Filter will be removed after the stopping for all components is implemented.
     *
     * @return Set of threads.
     */
    @NotNull private Set<Thread> getCurrentThreads() {
        return Thread.getAllStackTraces().keySet().stream()
            .filter(thread ->
                !thread.getName().startsWith("nioEventLoopGroup") &&
                !thread.getName().startsWith("globalEventExecutor") &&
                !thread.getName().startsWith("ForkJoinPool") &&
                !thread.getName().startsWith("parallel"))
            .collect(Collectors.toSet());
    }
}

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

package org.apache.ignite.util;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static java.lang.String.format;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.cache.CheckIndexesInlineSizeOnNodeJoinMultiJvmTest.getSqlStatements;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Checks utility output if check_index_inline_sizes command found problems.
 */
@WithSystemProperty(key = IGNITE_MAX_INDEX_PAYLOAD_SIZE, value = "1")
public class GridCommandHandlerCheckIndexesInlineSizeTest extends GridCommandHandlerAbstractTest {
    /** */
    private static final String INDEX_PROBLEM_FMT =
        "Full index name: PUBLIC#TEST_TABLE#%s nodes: [%s] inline size: 1, nodes: [%s] inline size: 2";

    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** Max payload inline index size for local node. */
    private static final int INITIAL_PAYLOAD_SIZE = 1;

    /** Max payload inline index size. */
    private int payloadSize;

    /** */
    private static final UUID remoteNodeId = UUID.randomUUID();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (isRemoteJvm(igniteInstanceName))
            cfg.setNodeId(remoteNodeId);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        cleanPersistenceDir();

        assertEquals(INITIAL_PAYLOAD_SIZE, Integer.parseInt(System.getProperty(IGNITE_MAX_INDEX_PAYLOAD_SIZE)));

        startGrids(NODES_CNT).cluster().active(true);

        for (Map.Entry<String, Object[]> entry : getSqlStatements().entrySet())
            executeSql(grid(0), entry.getKey(), entry.getValue());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmArgs() {
        List<String> args = super.additionalRemoteJvmArgs();

        args.add("-D" + IGNITE_MAX_INDEX_PAYLOAD_SIZE + "=" + payloadSize);

        return args;
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(int idx) throws Exception {
        payloadSize = getMaxPayloadSize(idx);

        return super.startGrid(idx);
    }

    /**
     * See class description.
     */
    @Test
    public void test() {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "check_index_inline_sizes"));

        checkUtilityOutput(log, testOut.toString(), grid(0).localNode().id(), remoteNodeId);
    }

    /** */
    private int getMaxPayloadSize(int nodeId) {
        return INITIAL_PAYLOAD_SIZE + nodeId;
    }

    /** */
    private static List<List<?>> executeSql(IgniteEx node, String stmt, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(stmt).setArgs(args), true).getAll();
    }

    /**
     * Checks that control.sh output as expected.
     *
     * @param log Logger.
     * @param output Uitlity output.
     * @param localNodeId Local node id.
     * @param remoteNodeId Remote node id.
     */
    public static void checkUtilityOutput(IgniteLogger log, String output, UUID localNodeId, UUID remoteNodeId) {
        assertContains(log, output, "Found 4 secondary indexes.");
        assertContains(log, output, "3 index(es) have different effective inline size on nodes. It can lead to performance degradation in SQL queries.");
        assertContains(log, output, "Index(es):");
        assertContains(log, output, format(INDEX_PROBLEM_FMT, "L_IDX", localNodeId, remoteNodeId));
        assertContains(log, output, format(INDEX_PROBLEM_FMT, "S1_IDX", localNodeId, remoteNodeId));
        assertContains(log, output, format(INDEX_PROBLEM_FMT, "I_IDX", localNodeId, remoteNodeId));
        assertContains(log, output, "  Check that value of property IGNITE_MAX_INDEX_PAYLOAD_SIZE are the same on all nodes.");
        assertContains(log, output, "  Recreate indexes (execute DROP INDEX, CREATE INDEX commands) with different inline size.");
    }
}

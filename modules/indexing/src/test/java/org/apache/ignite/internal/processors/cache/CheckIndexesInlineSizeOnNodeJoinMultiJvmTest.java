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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.lang.String.format;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE;
import static org.apache.ignite.internal.processors.query.GridQueryProcessor.INLINE_SIZES_DIFFER_WARN_MSG_FORMAT;

/**
 * Checks that node prints warn message on join node, if indexes inlize size are different on the nodes.
 */
@WithSystemProperty(key = IGNITE_MAX_INDEX_PAYLOAD_SIZE, value = "1")
public class CheckIndexesInlineSizeOnNodeJoinMultiJvmTest extends GridCommonAbstractTest {
    /** */
    private static final String STR = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    /** */
    private static final String INDEXES_WARN_MSG_FORMAT = "PUBLIC#TEST_TABLE#L_IDX(%d,%d),PUBLIC#TEST_TABLE#S1_IDX(%d,%d),PUBLIC#TEST_TABLE#I_IDX(%d,%d)";

    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** Max payload inline index size for local node. */
    private static final int INITIAL_PAYLOAD_SIZE = 1;

    /** */
    private ListeningTestLogger testLog;

    /** Max payload inline index size. */
    private int payloadSize;

    /** */
    private UUID nodeId;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setFailureHandler(new StopNodeOrHaltFailureHandler())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            );

        if (!isRemoteJvm(igniteInstanceName))
            cfg.setGridLogger(testLog);

        if (nodeId != null)
            cfg.setNodeId(nodeId);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        assertEquals(INITIAL_PAYLOAD_SIZE, Integer.parseInt(System.getProperty(IGNITE_MAX_INDEX_PAYLOAD_SIZE)));

        testLog = new ListeningTestLogger(false, log);

        startGrids(NODES_CNT).cluster().active(true);

        for (Map.Entry<String, Object[]> entry : getSqlStatements().entrySet())
            executeSql(grid(0), entry.getKey(), entry.getValue());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
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
     * Checks that warn messages are presented in the logs on joining node.
     *
     * @throws Exception If something goes wrong.
     */
    @Test
    public void testWarnOnJoiningNode() throws Exception {
        performTestScenario(0);
    }

    /**
     * Checks that warn messages are presented in the logs on the nodes in cluster.
     *
     * @throws Exception If something goes wrong.
     */
    @Test
    public void testWarnOnNodeInCluster() throws Exception {
        performTestScenario(1);
    }

    /**
     * Restarts node with given index and check messages in the log after node restart.
     *
     * @param restartNodeIdx Index of restarting node.
     * @throws Exception If something goes wrong.
     */
    private void performTestScenario(int restartNodeIdx) throws Exception {
        assertTrue("Wrong restart node index: " + restartNodeIdx, restartNodeIdx >= 0 && restartNodeIdx < NODES_CNT);

        Map<Integer, UUID> nodeIdxToNodeId =
            IntStream.range(0, NODES_CNT).boxed().collect(Collectors.toMap(i -> i, i -> grid(i).localNode().id()));

        stopGrid(restartNodeIdx);

        nodeId = UUID.randomUUID();

        nodeIdxToNodeId.put(restartNodeIdx, nodeId);

        Collection<LogListener> listeners = registerListeners(restartNodeIdx, nodeIdxToNodeId);

        assertFalse(F.isEmpty(listeners));

        startGrid(restartNodeIdx);

        awaitPartitionMapExchange();

        for (LogListener listener : listeners)
            assertTrue(listener.toString(), listener.check());
    }

    /** */
    private Collection<LogListener> registerListeners(int restartNodeIdx, Map<Integer, UUID> nodeIdxToNodeId) {
        List<String> msgs = new ArrayList<>(NODES_CNT - 1);

        int payloadSize = getMaxPayloadSize(restartNodeIdx);

        if (restartNodeIdx == 0) {
            for (Integer idx : nodeIdxToNodeId.keySet()) {
                if (idx != 0)
                    msgs.add(generateIndexesWarnMessage(nodeIdxToNodeId.get(idx), payloadSize, getMaxPayloadSize(idx)));
            }
        }
        else
            msgs.add(generateIndexesWarnMessage(nodeIdxToNodeId.get(restartNodeIdx), getMaxPayloadSize(0), payloadSize));

        Collection<LogListener> listeners = msgs.stream().map(m -> LogListener.matches(m).build()).collect(Collectors.toList());

        listeners.forEach(l -> testLog.registerListener(l));

        return listeners;
    }

    /** */
    private String generateIndexesWarnMessage(UUID nodeId, int payloadSize1, int payloadSize2) {
        String indexesInfo = format(INDEXES_WARN_MSG_FORMAT, payloadSize1, payloadSize2, payloadSize1, payloadSize2, payloadSize1, payloadSize2);

        return format(INLINE_SIZES_DIFFER_WARN_MSG_FORMAT, nodeId, indexesInfo);
    }

    /** */
    private int getMaxPayloadSize(int nodeId) {
        return INITIAL_PAYLOAD_SIZE + nodeId;
    }

    /** */
    private static List<List<?>> executeSql(IgniteEx node, String stmt, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(stmt).setArgs(args), true).getAll();
    }

    /** */
    public static Map<String, Object[]> getSqlStatements() {
        Object[] EMPTY = new Object[0];

        Map<String, Object[]> stmts = new LinkedHashMap<>();

        stmts.put("CREATE TABLE TEST_TABLE (i INT, l LONG, s0 VARCHAR, s1 VARCHAR, PRIMARY KEY (i, s0)) WITH \"backups=1\"", EMPTY);

        for (int i = 0; i < 10; i++)
            stmts.put("INSERT INTO TEST_TABLE (i, l, s0, s1) VALUES (?, ?, ?, ?)", Stream.of(i, i * i, STR + i, STR + i * i).toArray());

        stmts.put("CREATE INDEX i_idx ON TEST_TABLE(i)", EMPTY);
        stmts.put("CREATE INDEX l_idx ON TEST_TABLE(l)", EMPTY);
        stmts.put("CREATE INDEX s0_idx ON TEST_TABLE(s0) INLINE_SIZE 10", EMPTY);
        stmts.put("CREATE INDEX s1_idx ON TEST_TABLE(s1)", EMPTY);

        return stmts;
    }
}

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

package org.apache.ignite.internal.processors.query.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.QueryEngineConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.schema.management.IndexDescriptor;
import org.apache.ignite.internal.processors.query.schema.message.SchemaFinishDiscoveryMessage;
import org.apache.ignite.internal.processors.query.schema.message.SchemaOperationStatusMessage;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexCreateOperation;
import org.apache.ignite.internal.util.lang.ConsumerX;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.schema.IndexWithSameNameTestBase.SchemaFinishListeningTcpDiscoverySpi.discoSpi;
import static org.apache.ignite.internal.util.lang.GridFunc.asSet;
import static org.apache.ignite.internal.util.lang.GridFunc.t;

/** */
@SuppressWarnings("deprecation")
public abstract class IndexWithSameNameTestBase extends GridCommonAbstractTest {
    /** Test index. */
    public static final String TEST_INDEX = "TEST_IDX";

    /** Baseline size. */
    public static final int BASELINE_SIZE = 3;

    /** All nodes count, including non-baseline nodes. */
    public static final int NODES_COUNT = BASELINE_SIZE + 2;

    /** Client node index. */
    public static final int CLIENT_INDEX = NODES_COUNT - 1;

    /** Correct index fields. */
    public static final Set<String> CORRECT_FIELDS = asSet("K1", "V1");

    /** Fields of duplicate index. */
    public static final Set<String> DUPLICATE_FIELDS = asSet("K2", "V2");

    /** Schema finish latch. */
    public static CountDownLatch schemaFinishLatch;

    /** Index create operation identifiers. */
    public static Map<Set<String>, UUID> idxOps = new ConcurrentHashMap<>();

    /** Schema status latch. */
    private CountDownLatch schemaStatusLatch;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        idxOps.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        SchemaFinishListeningTcpDiscoverySpi discoSpi = new SchemaFinishListeningTcpDiscoverySpi();
        discoSpi.setIpFinder(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder());

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        commSpi.record((n, msg) -> {
            if (msg instanceof SchemaOperationStatusMessage) {
                schemaStatusLatch.countDown();

                return true;
            }

            return false;
        });

        return cfg.setDiscoverySpi(discoSpi)
            .setCommunicationSpi(commSpi)
            .setSqlConfiguration(new SqlConfiguration()
                .setQueryEnginesConfiguration(getEngineConfiguration()))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));
    }

    /** */
    protected abstract QueryEngineConfiguration getEngineConfiguration();

    /** */
    @Test
    public void testSeparateRequests() throws Exception {
        doTestWithRestart(qryProc -> {
            checkIndexCreate(qryProc, t("TABLE1", CORRECT_FIELDS, /* nop */ false));
            checkIndexCreate(qryProc, t("TABLE2", DUPLICATE_FIELDS, /* nop */ true));
        });
    }

    /** */
    @Test
    public void testMultiLineRequest() throws Exception {
        doTestWithRestart(qryProc -> checkIndexCreate(
            qryProc,
            t("TABLE1", CORRECT_FIELDS, /* nop */ false),
            t("TABLE2", DUPLICATE_FIELDS, /* nop */ true)));
    }

    /** */
    public void doTestWithRestart(ConsumerX<GridQueryProcessor> idxCreateAction) throws Exception {
        // Ensure coordinator.
        IgniteEx crd = startGrid(0);

        startGridsMultiThreaded(1, BASELINE_SIZE - 1);

        waitForTopology(BASELINE_SIZE);

        crd.cluster().state(ClusterState.ACTIVE);
        awaitPartitionMapExchange();

        // Non-baseline nodes.
        startGrid(BASELINE_SIZE);
        startClientGrid(CLIENT_INDEX);

        waitForTopology(NODES_COUNT);
        assertEquals(BASELINE_SIZE, crd.cluster().currentBaselineTopology().size());

        GridQueryProcessor qryProc = crd.context().query();

        qryProc.querySqlFields(new SqlFieldsQuery("CREATE TABLE TABLE1 (K1 INT PRIMARY KEY, V1 INT)"), true).getAll();
        qryProc.querySqlFields(new SqlFieldsQuery("CREATE TABLE TABLE2 (K2 INT PRIMARY KEY, V2 INT)"), true).getAll();

        idxCreateAction.accept(qryProc);

        assertSingleIndex(NODES_COUNT);

        crd.cluster().state(ClusterState.INACTIVE);
        awaitPartitionMapExchange();
        stopAllGrids();

        startGrids(BASELINE_SIZE);

        assertSingleIndex(BASELINE_SIZE);
    }

    /**
     * @param qryProc Query processor.
     * @param idxParams Expected index parameters.
     */
    private void checkIndexCreate(GridQueryProcessor qryProc, GridTuple3<String, Set<String>, Boolean>... idxParams)
        throws InterruptedException {
        List<GridTuple3<String, Set<String>, Boolean>> idxParams0 = Arrays.asList(idxParams);

        // Multi-statement index create.
        String idxCreateSql = idxParams0.stream()
            .map(t -> String.format("CREATE INDEX IF NOT EXISTS %s ON %s (%s)", TEST_INDEX,
                t.get1(),
                String.join(",", t.get2())))
            .collect(Collectors.joining("\n;"));

        // SchemaFinishDiscoveryMessage is processed twice on coordinator.
        schemaFinishLatch = new CountDownLatch((NODES_COUNT + 1) * idxParams0.size());

        // SchemaOperationStatusMessage is not sent by coordinator and client.
        schemaStatusLatch = new CountDownLatch((NODES_COUNT - 2) * idxParams0.size());

        qryProc.querySqlFields(new SqlFieldsQuery(idxCreateSql), true, false)
            .forEach(FieldsQueryCursor::getAll);

        schemaStatusLatch.await(getTestTimeout(), MILLISECONDS);
        schemaFinishLatch.await(getTestTimeout(), MILLISECONDS);

        checkNoOpMessages(idxParams0);
    }

    /**
     * @param idxParams Expected index parameters.
     */
    private void checkNoOpMessages(List<GridTuple3<String, Set<String>, Boolean>> idxParams) {
        for (int i = 0; i < NODES_COUNT; i++) {
            List<Object> commMsgs = spi(grid(i)).recordedMessages(false);

            boolean crdOrClient = i == 0 || i == CLIENT_INDEX;

            String commMsgErr = "Unexpected SchemaOperationStatusMessage count on node: igniteIndex=" + i +
                ", commMsgsCnt=" + commMsgs.size();

            int sqlCnt = idxParams.size();

            // SchemaOperationStatusMessage is not sent by coordinator and client.
            assertEquals(commMsgErr, crdOrClient ? 0 : sqlCnt, commMsgs.size());

            List<SchemaFinishDiscoveryMessage> discoMsgs = discoSpi(grid(i)).recordedMessages();

            String discoMsgErr = "Unexpected SchemaFinishDiscoveryMessage count on node: igniteIndex=" + i +
                ", discoMsgsCnt=" + discoMsgs.size();

            // SchemaFinishDiscoveryMessage is processed twice on coordinator.
            assertEquals(discoMsgErr, i == 0 ? 2 * sqlCnt : sqlCnt, discoMsgs.size());

            for (GridTuple3<String, Set<String>, Boolean> param : idxParams) {
                Set<String> expIdxFields = param.get2();
                boolean expNop = param.get3();

                UUID opId = idxOps.get(expIdxFields);

                String commNopErr = String.format(
                    "Unexpected no-op flag in communication messages: opId=%s, igniteIndex=%d, expNop=%b, recordedMsgs=%s",
                    opId, i, expNop, commMsgs);

                assertTrue(commNopErr, commMsgs.stream()
                    .map(msg -> ((SchemaOperationStatusMessage)msg))
                    .filter(msg -> opId.equals(msg.operationId()))
                    .allMatch(msg -> msg.nop() == expNop));

                String discoNopErr = String.format(
                    "Unexpected no-op flag in discovery messages: opId=%s, igniteIndex=%d, expNop=%b, recordedMsgs=%s",
                    opId, i, expNop, discoMsgs);

                assertTrue(discoNopErr, discoMsgs.stream()
                    .filter(msg -> opId.equals(msg.operation().id()))
                    .allMatch(msg -> msg.nop() == expNop));
            }
        }
    }

    /** */
    private void assertSingleIndex(int nodesCnt) {
        for (int i = 0; i < nodesCnt; i++) {
            Collection<IndexDescriptor> indexes = grid(i).context()
                .query()
                .schemaManager()
                .allIndexes();

            List<IndexDescriptor> filteredIdxs = indexes.stream()
                .filter(idx -> TEST_INDEX.equalsIgnoreCase(idx.name()))
                .collect(Collectors.toList());

            assertEquals("There should be only one index", 1, filteredIdxs.size());

            Set<String> actualFields = filteredIdxs.get(0)
                .keyDefinitions()
                .keySet()
                .stream()
                .filter(f -> !KEY_FIELD_NAME.equalsIgnoreCase(f))
                .collect(Collectors.toSet());

            assertEqualsCollectionsIgnoringOrder(CORRECT_FIELDS, actualFields);
        }
    }

    /** */
    public static class SchemaFinishListeningTcpDiscoverySpi extends TcpDiscoverySpi {
        /** Filtered messages. */
        private final List<SchemaFinishDiscoveryMessage> recordedMsgs = new CopyOnWriteArrayList<>();

        /** {@inheritDoc} */
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (msg instanceof TcpDiscoveryCustomEventMessage) {
                try {
                    TcpDiscoveryCustomEventMessage msg0 = (TcpDiscoveryCustomEventMessage)msg;
                    msg0.finishUnmarhal(marshaller(), U.gridClassLoader());

                    DiscoveryCustomMessage discoCustomMsg = GridTestUtils.unwrap(msg0.message());

                    if (discoCustomMsg instanceof SchemaFinishDiscoveryMessage) {
                        SchemaFinishDiscoveryMessage finishMsg = (SchemaFinishDiscoveryMessage)discoCustomMsg;

                        SchemaIndexCreateOperation op = (SchemaIndexCreateOperation)finishMsg.operation();

                        idxOps.putIfAbsent(new HashSet<>(op.index().getFieldNames()), op.id());

                        recordedMsgs.add(finishMsg);

                        schemaFinishLatch.countDown();
                    }
                }
                catch (Throwable e) {
                    log.error("Unexpected error", e);

                    fail(e.getMessage());
                }
            }
        }

        /** */
        public static SchemaFinishListeningTcpDiscoverySpi discoSpi(Ignite ignite) {
            return (SchemaFinishListeningTcpDiscoverySpi)ignite.configuration().getDiscoverySpi();
        }

        /** */
        public List<SchemaFinishDiscoveryMessage> recordedMessages() {
            List<SchemaFinishDiscoveryMessage> recordedMsgs0 = new ArrayList<>(recordedMsgs);

            recordedMsgs.clear();

            return recordedMsgs0;
        }
    }
}

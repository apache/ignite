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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.QueryEngineConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.schema.management.IndexDescriptor;
import org.apache.ignite.internal.processors.query.schema.message.SchemaFinishDiscoveryMessage;
import org.apache.ignite.internal.processors.query.schema.message.SchemaOperationStatusMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.schema.IndexWithSameNameTestBase.SchemaFinishListeningTcpDiscoverySpi.discoSpi;
import static org.apache.ignite.internal.util.lang.GridFunc.asSet;

/** */
public abstract class IndexWithSameNameTestBase extends GridCommonAbstractTest {
    /** Test index. */
    public static final String TEST_INDEX = "TEST_IDX";

    /** Baseline size. */
    public static final int BASELINE_SIZE = 3;

    /** All nodes count, including non-baseline nodes. */
    public static final int NODES_COUNT = BASELINE_SIZE + 2;

    /** Schema finish latch. */
    public static CountDownLatch schemaFinishLatch;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
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
        commSpi.record(SchemaOperationStatusMessage.class);

        return cfg.setDiscoverySpi(discoSpi)
            .setCommunicationSpi(commSpi)
            .setSqlConfiguration(new SqlConfiguration()
                .setQueryEnginesConfiguration(getEngineConfiguration()))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));
    }

    /**
     *
     */
    protected abstract QueryEngineConfiguration getEngineConfiguration();

    /**
     *
     */
    @Test
    @SuppressWarnings("deprecation")
    public void testWithRestart() throws Exception {
        startGrids(BASELINE_SIZE)
            .cluster()
            .state(ClusterState.ACTIVE);

        // Non-baseline nodes.
        startGrid(BASELINE_SIZE);
        startClientGrid(NODES_COUNT - 1);

        GridQueryProcessor qryProc = grid(0).context().query();

        qryProc.querySqlFields(new SqlFieldsQuery("CREATE TABLE T1 (k1 INT PRIMARY KEY, v1 INT)"), true).getAll();
        qryProc.querySqlFields(new SqlFieldsQuery("CREATE TABLE T2 (k2 INT PRIMARY KEY, v2 INT)"), true).getAll();

        Set<String> correctFields = asSet("K1", "V1");
        Set<String> duplicateFields = asSet("K2", "V2");

        // This index will be created.
        checkIndexCreate(qryProc, "T1", correctFields, false);

        // This index must not be created. Query must be no-op.
        checkIndexCreate(qryProc, "T2", duplicateFields, true);

        assertSingleIndex(correctFields);

        grid(0).cluster().state(ClusterState.INACTIVE);
        awaitPartitionMapExchange();
        stopAllGrids();

        startGrids(BASELINE_SIZE);

        assertSingleIndex(correctFields);
    }

    /**
     * @param qryProc Query processor.
     * @param tblName Table name.
     * @param idxFields Index fields.
     * @param expNop Expected no-op flag.
     */
    private void checkIndexCreate(GridQueryProcessor qryProc, String tblName, Set<String> idxFields, boolean expNop)
        throws InterruptedException {
        // SchemaFinishDiscoveryMessage is processed twice on coordinator.
        schemaFinishLatch = new CountDownLatch(NODES_COUNT + 1);

        String createIdxSql = String.format("CREATE INDEX IF NOT EXISTS %s ON %s (%s)", TEST_INDEX, tblName,
            String.join(",", idxFields));

        qryProc.querySqlFields(new SqlFieldsQuery(createIdxSql), true).getAll();

        schemaFinishLatch.await();

        checkNoOpMessages(expNop);
    }

    /**
     * @param expNop Expected no-op flag.
     */
    private void checkNoOpMessages(boolean expNop) {
        for (int i = 0; i < NODES_COUNT; i++) {
            assertTrue(spi(grid(i)).recordedMessages(false)
                .stream()
                .allMatch(msg -> ((SchemaOperationStatusMessage)msg).nop() == expNop));

            assertTrue(discoSpi(grid(i)).recordedMessages()
                .stream()
                .allMatch(msg -> msg.nop() == expNop));
        }
    }

    /**
     * @param expIdxFields Expected index fields.
     */
    private void assertSingleIndex(Collection<String> expIdxFields) {
        for (int i = 0; i < BASELINE_SIZE; i++) {
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

            assertEqualsCollectionsIgnoringOrder(expIdxFields, actualFields);
        }
    }

    /**
     *
     */
    public static class SchemaFinishListeningTcpDiscoverySpi extends TcpDiscoverySpi {
        /** Filtered messages. */
        private final List<SchemaFinishDiscoveryMessage> recordedMsgs = new CopyOnWriteArrayList<>();

        /** {@inheritDoc} */
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (msg instanceof TcpDiscoveryCustomEventMessage) {
                try {
                    DiscoverySpiCustomMessage spiCustomMsg = ((TcpDiscoveryCustomEventMessage)msg).message(marshaller(),
                        U.resolveClassLoader(ignite().configuration()));

                    DiscoveryCustomMessage discoCustomMsg = ((CustomMessageWrapper)spiCustomMsg).delegate();

                    if (discoCustomMsg instanceof SchemaFinishDiscoveryMessage) {
                        recordedMsgs.add((SchemaFinishDiscoveryMessage)discoCustomMsg);

                        schemaFinishLatch.countDown();
                    }
                }
                catch (Throwable e) {
                    log.error("Unexpected error", e);

                    fail(e.getMessage());
                }
            }
        }

        /**
         * @param ignite Ignite.
         */
        public static SchemaFinishListeningTcpDiscoverySpi discoSpi(Ignite ignite) {
            return (SchemaFinishListeningTcpDiscoverySpi)ignite.configuration().getDiscoverySpi();
        }

        /**
         *
         */
        public List<SchemaFinishDiscoveryMessage> recordedMessages() {
            List<SchemaFinishDiscoveryMessage> recordedMsgs0 = new ArrayList<>(recordedMsgs);

            recordedMsgs.clear();

            return recordedMsgs0;
        }
    }
}

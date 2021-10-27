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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.ITUtils;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.WatchEvent;
import org.apache.ignite.internal.metastorage.client.WatchListener;
import org.apache.ignite.internal.metastorage.watch.AggregatedWatch;
import org.apache.ignite.internal.metastorage.watch.WatchAggregator;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.platform.commons.util.ReflectionUtils;
import org.mockito.Mockito;

import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;

/**
 * Integration tests to check consistent of java API on different nodes.
 */
public class ITTablesApiTest extends IgniteAbstractTest {
    /** Schema name. */
    public static final String SCHEMA = "PUBLIC";

    /** Short table name. */
    public static final String SHORT_TABLE_NAME = "tbl1";

    /** Table name. */
    public static final String TABLE_NAME = SCHEMA + "." + SHORT_TABLE_NAME;

    /** Nodes bootstrap configuration. */
    private final ArrayList<Function<String, String>> nodesBootstrapCfg = new ArrayList<>() {{
        add((metastorageNodeName) -> "{\n" +
            "  \"node\": {\n" +
            "    \"metastorageNodes\":[ " + metastorageNodeName + " ]\n" +
            "  },\n" +
            "  \"network\": {\n" +
            "    \"port\":3344,\n" +
            "    \"nodeFinder\": {\n" +
            "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}");

        add((metastorageNodeName) -> "{\n" +
            "  \"node\": {\n" +
            "    \"metastorageNodes\":[ " + metastorageNodeName + " ]\n" +
            "  },\n" +
            "  \"network\": {\n" +
            "    \"port\":3345,\n" +
            "    \"nodeFinder\": {\n" +
            "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}");

        add((metastorageNodeName) -> "{\n" +
            "  \"node\": {\n" +
            "    \"metastorageNodes\":[ " + metastorageNodeName + " ]\n" +
            "  },\n" +
            "  \"network\": {\n" +
            "    \"port\":3346,\n" +
            "    \"nodeFinder\": {\n" +
            "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}");
    }};

    /** Cluster nodes. */
    private List<Ignite> clusterNodes;

    /**
     * @throws Exception If failed.
     */
    @BeforeEach
    void beforeEach(TestInfo testInfo) throws Exception {
        String metastorageNodeName = IgniteTestUtils.testNodeName(testInfo, 0);

        clusterNodes = IntStream.range(0, nodesBootstrapCfg.size()).mapToObj(value -> {
                String nodeName = IgniteTestUtils.testNodeName(testInfo, value);

                return IgnitionManager.start(
                    nodeName,
                    nodesBootstrapCfg.get(value).apply(metastorageNodeName),
                    workDir.resolve(nodeName));
            }
        ).collect(Collectors.toList());
    }

    /**
     * @throws Exception If failed.
     */
    @AfterEach
    void afterEach() throws Exception {
        IgniteUtils.closeAll(ITUtils.reverse(clusterNodes));
    }

    /**
     * Checks that if a table would be created/dropped into any cluster node, this is visible to all other nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateDropTable() throws Exception {
        clusterNodes.forEach(ign -> assertNull(ign.tables().table(TABLE_NAME)));

        Ignite ignite1 = clusterNodes.get(1);

        WatchListenerInhibitor ignite1Inhibitor = metastorageEventsInhibitor(ignite1);

        ignite1Inhibitor.startInhibit();

        Table table = createTable(clusterNodes.get(0), SCHEMA, SHORT_TABLE_NAME);

        IgniteUuid tblId = ((TableImpl)table).tableId();

        CompletableFuture<Table> tableByNameFut = CompletableFuture.supplyAsync(() -> {
            return ignite1.tables().table(TABLE_NAME);
        });

        CompletableFuture<Table> tableByIdFut = CompletableFuture.supplyAsync(() -> {
            try {
                return ((IgniteTablesInternal)ignite1.tables()).table(tblId);
            }
            catch (NodeStoppingException e) {
                throw new AssertionError(e.getMessage());
            }
        });

        // Because the event inhibitor was started, last metastorage updates do not reach to one node.
        // Therefore the table still doesn't exists locally, but API prevents getting null and waits events.
        for (Ignite ignite: clusterNodes) {
            if (ignite != ignite1) {
                assertNotNull(ignite.tables().table(TABLE_NAME));

                assertNotNull(((IgniteTablesInternal)ignite.tables()).table(tblId));
            }
        }

        assertFalse(tableByNameFut.isDone());
        assertFalse(tableByIdFut.isDone());

        ignite1Inhibitor.stopInhibit();

        assertNotNull(tableByNameFut.get(10, TimeUnit.SECONDS));
        assertNotNull(tableByIdFut.get(10, TimeUnit.SECONDS));

        ignite1Inhibitor.startInhibit();

        clusterNodes.get(0).tables().dropTable(TABLE_NAME);

        // Because the event inhibitor was started, last metastorage updates do not reach to one node.
        // Therefore the table still exists locally, but API prevents getting it.
        for (Ignite ignite : clusterNodes) {
            assertNull(ignite.tables().table(TABLE_NAME));

            assertNull(((IgniteTablesInternal)ignite.tables()).table(tblId));
        }

        ignite1Inhibitor.stopInhibit();
    }

    /**
     * Creates the specific listener which can inhibit events for real metastorage listener.
     *
     * @param ignite Ignite.
     * @return Listener inhibitor.
     * @throws Exception If something wrong when creating the listener inhibitor.
     */
    private WatchListenerInhibitor metastorageEventsInhibitor(Ignite ignite) throws Exception {
        //TODO: IGNITE-15723 After a component factory will be implemented, need to got rid of reflection here.
        MetaStorageManager metaMngr = (MetaStorageManager)ReflectionUtils.tryToReadFieldValue(
            IgniteImpl.class,
            "metaStorageMgr",
            (IgniteImpl)ignite
        ).get();

        assertNotNull(metaMngr);

        WatchAggregator aggregator = (WatchAggregator)ReflectionUtils.tryToReadFieldValue(
            MetaStorageManager.class,
            "watchAggregator",
            metaMngr
        ).get();

        assertNotNull(aggregator);

        WatchAggregator aggregatorSpy = Mockito.spy(aggregator);

        WatchListenerInhibitor inhibitor = new WatchListenerInhibitor();

        doAnswer(mock -> {
            Optional<AggregatedWatch> op = (Optional<AggregatedWatch>)mock.callRealMethod();

            assertTrue(op.isPresent());

            inhibitor.setRealListener(op.get().listener());

            return Optional.of(new AggregatedWatch(op.get().keyCriterion(), op.get().revision(), inhibitor));
        }).when(aggregatorSpy).watch(anyLong(), any());

        IgniteTestUtils.setFieldValue(metaMngr, "watchAggregator", aggregatorSpy);

        // Redeploy metastorage watch. The Watch inhibitor will be used after.
        metaMngr.unregisterWatch(-1);

        return inhibitor;
    }

    /**
     * Listener which wraps another one to inhibit events.
     */
    private static class WatchListenerInhibitor implements WatchListener {
        /** Inhibited events. */
        private final ArrayList<WatchEvent> inhibitEvents = new ArrayList<>();

        /** Inhibit flag. */
        private boolean inhibit = false;

        /** Wrapped listener */
        private WatchListener realListener;

        /**
         * Sets a wrapped listener.
         *
         * @param realListener Listener to wrap.
         */
        public void setRealListener(WatchListener realListener) {
            this.realListener = realListener;
        }

        /** {@inheritDoc} */
        @Override public synchronized boolean onUpdate(WatchEvent evt) {
            if (!inhibit)
                return realListener.onUpdate(evt);

            return inhibitEvents.add(evt);
        }

        /** {@inheritDoc} */
        @Override public synchronized void onError(Throwable e) {
            realListener.onError(e);
        }

        /**
         * Starts inhibit events.
         */
        synchronized void startInhibit() {
            inhibit = true;
        }

        /**
         * Stops inhibit events.
         */
        synchronized void stopInhibit() {
            inhibit = false;

            for (WatchEvent evt : inhibitEvents)
                realListener.onUpdate(evt);

            inhibitEvents.clear();
        }
    }

    /**
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
}

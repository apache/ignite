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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.systemview.view.SqlQueryHistoryView;
import org.apache.ignite.spi.systemview.view.SqlQueryView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.IgniteApplicationAttributesAware.ReservedApplicationAttributes.QUERY_LABEL;
import static org.apache.ignite.internal.processors.query.running.HeavyQueriesTracker.LONG_QUERY_EXEC_MSG;
import static org.apache.ignite.internal.processors.query.running.RunningQueryManager.SQL_QRY_HIST_VIEW;
import static org.apache.ignite.internal.processors.query.running.RunningQueryManager.SQL_QRY_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Class for testing local labelled queries executed on remote nodes as {@link IgniteCompute} tasks.
 */
@RunWith(Parameterized.class)
public class ComputeWithLabelledQueryTest extends GridCommonAbstractTest {
    /** Query label. */
    private static final String LABEL = "Label 1";

    /** Key for INSERT queries. */
    private static final AtomicInteger KEY = new AtomicInteger(10);

    /** Long query warning timeout. */
    private static final int LONG_QUERY_WARNING_TIMEOUT = 1000;

    /** Sleep duration. */
    private static final int SLEEP = 300;

    /** Sleep duration for INSERT queries. */
    private static final int SLEEP_INSERT = 2000;

    /** Logger. */
    private ListeningTestLogger log;

    /** Ignite instance #1. */
    private IgniteEx ignite0;

    /** Ignite instance #2. */
    private IgniteEx ignite1;

    /** Client node. */
    private IgniteEx client;

    /** SQL engine. */
    @Parameterized.Parameter
    public String sqlEngine;

    /** */
    @Parameterized.Parameters(name = "sqlEngine={0}")
    public static Object[] params() {
        return new Object[] {CalciteQueryEngineConfiguration.ENGINE_NAME, IndexingQueryEngineConfiguration.ENGINE_NAME};
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(log)
            .setCacheConfiguration(new CacheConfiguration<>("A")
                .setIndexedTypes(Integer.class, Integer.class)
                .setSqlFunctionClasses(GridTestUtils.SqlTestFunctions.class))
            .setSqlConfiguration(new SqlConfiguration()
                .setLongQueryWarningTimeout(LONG_QUERY_WARNING_TIMEOUT)
                .setQueryEnginesConfiguration(sqlEngine.equals(CalciteQueryEngineConfiguration.ENGINE_NAME) ?
                    new CalciteQueryEngineConfiguration() : new IndexingQueryEngineConfiguration()));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        log = new ListeningTestLogger(log());

        ignite0 = startGrid(0);
        ignite1 = startGrid(1);
        client = startClientGrid(2);

        IgniteCache<Integer, Integer> cache = client.cache("A");

        for (int i = 0; i < 10; i++)
            cache.put(i, i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        client.close();

        stopAllGrids();
    }

    /**
     * Verifies that local query labels are correctly displayed in log messages and system views when remote SELECT
     * tasks are executed.
     */
    @Test
    public void testComputeWithLabelSelect() throws Exception {
        runComputeWithLabelTest("select * from A.Integer where _key < sleep(?)");
    }

    /**
     * Verifies that local query labels are correctly displayed in log messages and system views when remote INSERT
     * tasks are executed.
     */
    @Test
    public void testComputeWithLabelInsert() throws Exception {
        runComputeWithLabelTest("insert into A.Integer (_key, _val) values (?, sleep(?))");
    }

    /**
     * Verifies that local query labels are correctly displayed in log messages and system views when remote UPDATE
     * tasks are executed.
     */
    @Test
    public void testComputeWithLabelUpdate() throws Exception {
        runComputeWithLabelTest("update A.Integer set _val = 1 where _key < sleep(?)");
    }

    /**
     * Verifies that local query labels are correctly displayed in log messages and system views when remote DELETE
     * tasks are executed.
     */
    @Test
    public void testComputeWithLabelDelete() throws Exception {
        runComputeWithLabelTest("delete from A.Integer where _key < sleep(?)");
    }

    /**
     * @param sql SQL query.
     */
    public void runComputeWithLabelTest(String sql) throws Exception {
        LogListener logLsnr = LogListener.matches(LONG_QUERY_EXEC_MSG).andMatches(LABEL).build();

        log.registerListener(logLsnr);

        IgniteFuture<Void> fut = client.compute(client.cluster().forServers()).broadcastAsync(new RemoteTask(sql));

        viewChecker(SqlQueryView.class, view -> assertEquals(LABEL, F.first(view).label()), SQL_QRY_VIEW);

        fut.get();

        viewChecker(SqlQueryHistoryView.class, view -> assertEquals(LABEL, F.first(view).label()), SQL_QRY_HIST_VIEW);

        assertTrue(logLsnr.check(1000));
    }

    /**
     * @param clazz View class.
     * @param check View check task.
     * @param viewName View name.
     */
    @SuppressWarnings("unused")
    private <T> void viewChecker(
        Class<T> clazz,
        Consumer<SystemView<T>> check,
        String viewName
    ) throws IgniteInterruptedCheckedException {
        for (IgniteEx grid : Set.of(ignite0, ignite1)) {
            assertTrue(waitForCondition(() -> grid.context().systemView().view(viewName).size() == 1, 1000));

            SystemView<T> view = grid.context().systemView().view(viewName);

            check.accept(view);
        }
    }

    /** */
    private static class RemoteTask implements IgniteRunnable {
        /** Ignite instance. */
        @IgniteInstanceResource
        Ignite ignite;

        /** SQL query. */
        private final String sql;

        /**
         * @param sql SQL query.
         */
        public RemoteTask(String sql) {
            this.sql = sql;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            Ignite gridWithAttrs = ignite.withApplicationAttributes(F.asMap(QUERY_LABEL, LABEL));

            Supplier<Object> key = KEY::getAndIncrement;

            Object[] args = sql.startsWith("insert") ? new Object[] {key.get(), SLEEP_INSERT} : new Object[] {SLEEP};

            gridWithAttrs.cache("A").query(new SqlFieldsQuery(sql).setArgs(args).setLocal(true)).getAll();
        }
    }
}

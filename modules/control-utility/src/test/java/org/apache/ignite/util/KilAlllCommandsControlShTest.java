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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.GridCacheDistributedQueryManager;
import org.apache.ignite.internal.util.GridTestClockTimer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.systemview.view.SqlQueryView;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.query.running.RunningQueryManager.SQL_QRY_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Test for mass queries cancellation.
 */
public class KilAlllCommandsControlShTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** Operations timeout. */
    public static final int TIMEOUT = 10_000;

    /** */
    private static CountDownLatch latch;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgniteCache<Object, Object> cache = client.getOrCreateCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setIndexedTypes(Integer.class, Integer.class)
                .setSqlFunctionClasses(SqlTestFunctions.class));

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        latch = new CountDownLatch(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        latch.countDown();
    }

    /** */
    @Test
    public void testKillAllSql() {
        String sql = "SELECT * FROM Integer WHERE latch()";

        checkKillAll("sql", () -> new SqlFieldsQuery(sql), KilAlllCommandsControlShTest::sqlQueriesCnt);
    }

    /** */
    @Test
    public void testKillAllScan() {
        checkKillAll("scan", () -> new ScanQuery<>().setPageSize(1).setFilter((k, v) -> {
            try {
                latch.await(10, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return true;
        }), KilAlllCommandsControlShTest::scanQueriesCnt);
    }

    /** */
    @Test
    public void testKillAllContinuous() {
        assertTrue(SERVER_NODE_CNT >= 2);

        client.cache(DEFAULT_CACHE_NAME).query(new ContinuousQuery<>().setLocalListener(evts -> {}));
        grid(0).cache(DEFAULT_CACHE_NAME).query(new ContinuousQuery<>().setLocalListener(evts -> {}));
        grid(1).cache(DEFAULT_CACHE_NAME).query(new ContinuousQuery<>().setLocalListener(evts -> {}));

        // Kill all queries using --node-id argument.
        assertEquals(EXIT_CODE_OK, execute("--kill", "all", "continuous",
            "--node-id", grid(0).context().localNodeId().toString()));

        assertEquals(1, client.context().continuous().localRoutineInfos().size());
        assertEquals(0, grid(0).context().continuous().localRoutineInfos().size());
        assertEquals(1, grid(1).context().continuous().localRoutineInfos().size());
        assertEquals(0, client.context().continuous().remoteRoutineInfos().size());
        assertEquals(2, grid(0).context().continuous().remoteRoutineInfos().size());
        assertEquals(1, grid(1).context().continuous().remoteRoutineInfos().size());

        // Kill all queries without arguments.
        assertEquals(EXIT_CODE_OK, execute("--kill", "all", "continuous"));

        assertEquals(0, client.context().continuous().localRoutineInfos().size());
        assertEquals(0, grid(0).context().continuous().localRoutineInfos().size());
        assertEquals(0, grid(1).context().continuous().localRoutineInfos().size());
        assertEquals(0, client.context().continuous().remoteRoutineInfos().size());
        assertEquals(0, grid(0).context().continuous().remoteRoutineInfos().size());
        assertEquals(0, grid(1).context().continuous().remoteRoutineInfos().size());
    }

    /** */
    public void checkKillAll(String target, Supplier<Query<?>> qryFactory, ToIntFunction<IgniteEx> qryCntProvider) {
        try {
            assertTrue(SERVER_NODE_CNT >= 2);

            long ts = U.currentTimeMillis();
            GridTestClockTimer.timeSupplier(() -> ts);

            List<QueryCursor<?>> curs = new ArrayList<>();

            curs.add(client.cache(DEFAULT_CACHE_NAME).query(qryFactory.get()));

            for (int i = 0; i < 2; i++)
                curs.add(grid(i).cache(DEFAULT_CACHE_NAME).query(qryFactory.get()));

            GridTestClockTimer.timeSupplier(() -> ts + 1001L);

            curs.add(client.cache(DEFAULT_CACHE_NAME).query(qryFactory.get()));

            for (int i = 0; i < 2; i++)
                curs.add(grid(i).cache(DEFAULT_CACHE_NAME).query(qryFactory.get()));

            injectTestSystemOut();

            // Kill all queries using both --min-duration and --node-id arguments.
            assertEquals(EXIT_CODE_OK, execute("--kill", "all", target, "--min-duration", "1",
                "--node-id", client.context().localNodeId().toString()));

            assertContains(log, testOut.toString(), "Node ID: " + client.context().localNodeId() + " Killed: 1");
            assertContains(log, testOut.toString(), "Total killed: 1");

            assertEquals(1, qryCntProvider.applyAsInt(client));
            assertEquals(2, qryCntProvider.applyAsInt(grid(0)));
            assertEquals(2, qryCntProvider.applyAsInt(grid(1)));
            assertThrows(log, () -> curs.get(0).getAll(), Exception.class, "");

            testOut.reset();

            // Kill all queries using --min-duration argument.
            assertEquals(EXIT_CODE_OK, execute("--kill", "all", target, "--min-duration", "1"));
            assertContains(log, testOut.toString(), "Node ID: " + grid(0).context().localNodeId() + " Killed: 1");
            assertContains(log, testOut.toString(), "Node ID: " + grid(1).context().localNodeId() + " Killed: 1");
            assertContains(log, testOut.toString(), "Total killed: 2");

            assertEquals(1, qryCntProvider.applyAsInt(client));
            assertEquals(1, qryCntProvider.applyAsInt(grid(0)));
            assertEquals(1, qryCntProvider.applyAsInt(grid(1)));

            assertThrows(log, () -> curs.get(1).getAll(), Exception.class, "");
            assertThrows(log, () -> curs.get(2).getAll(), Exception.class, "");

            testOut.reset();

            // Kill all queries using --node-id argument.
            assertEquals(EXIT_CODE_OK, execute("--kill", "all", target, "--node-id",
                grid(0).context().localNodeId().toString()));

            assertContains(log, testOut.toString(), "Node ID: " + grid(0).context().localNodeId() + " Killed: 1");
            assertContains(log, testOut.toString(), "Total killed: 1");

            assertEquals(1, qryCntProvider.applyAsInt(client));
            assertEquals(0, qryCntProvider.applyAsInt(grid(0)));
            assertEquals(1, qryCntProvider.applyAsInt(grid(1)));

            assertThrows(log, () -> curs.get(4).getAll(), Exception.class, "");

            testOut.reset();

            // Kill all queries without arguments.
            assertEquals(EXIT_CODE_OK, execute("--kill", "all", target));

            assertContains(log, testOut.toString(), "Node ID: " + client.context().localNodeId() + " Killed: 1");
            assertContains(log, testOut.toString(), "Node ID: " + grid(1).context().localNodeId() + " Killed: 1");
            assertContains(log, testOut.toString(), "Total killed: 2");

            assertEquals(0, qryCntProvider.applyAsInt(client));
            assertEquals(0, qryCntProvider.applyAsInt(grid(0)));
            assertEquals(0, qryCntProvider.applyAsInt(grid(1)));

            assertThrows(log, () -> curs.get(3).getAll(), Exception.class, "");
            assertThrows(log, () -> curs.get(5).getAll(), Exception.class, "");
        }
        finally {
            GridTestClockTimer.timeSupplier(GridTestClockTimer.DFLT_TIME_SUPPLIER);
        }
    }

    /** */
    private static int sqlQueriesCnt(IgniteEx ignite) {
        return F.size(ignite.context().systemView().<SqlQueryView>view(SQL_QRY_VIEW).iterator(), v -> !v.mapQuery());
    }

    /** */
    private static int scanQueriesCnt(IgniteEx ignite) {
        return ((GridCacheDistributedQueryManager<?, ?>)ignite.cachex(DEFAULT_CACHE_NAME).context().queries())
            .distributedQueryFutures().size();
    }

    /** */
    public static class SqlTestFunctions {
        /** */
        @QuerySqlFunction
        public static boolean latch() {
            try {
                latch.await(TIMEOUT, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ignored) {
                return false;
            }

            return true;
        }
    }
}

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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests distributed SQL queries cancel by user or timeout.
 */
public class IgniteCacheDistributedQueryStopOnCancelOrTimeoutSelfTest extends GridCommonAbstractTest {
    /** Grids count. */
    private static final int GRIDS_CNT = 4;

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache size. */
    public static final int CACHE_SIZE = 10_000;

    /** Value size. */
    public static final int VAL_SIZE = 16;

    /** */
    private static final String QRY_1 = "select a._val, b._val from String a, String b";

    /** */
    private static final String QRY_2 = "select a._key, count(*) from String a group by a._key";

    /** */
    private static final String QRY_3 = "select a._val from String a";

    /** */
    private static final String CANCELLED_BY_CLIENT = "reason=Cancelled by client";

    /** */
    private static final String WITH_TIMEOUT_WAS_CANCELLED = "reason=Statement with timeout was cancelled";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(GRIDS_CNT);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        TcpDiscoverySpi spi = (TcpDiscoverySpi)cfg.getDiscoverySpi();
        spi.setIpFinder(IP_FINDER);

        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        ccfg.setIndexedTypes(Integer.class, String.class);

        cfg.setCacheConfiguration(ccfg);

        if ("client".equals(igniteInstanceName))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (Ignite g : G.allGrids())
            g.cache(DEFAULT_CACHE_NAME).removeAll();
    }

    /** */
    public void testRemoteQueryExecutionTimeout() throws Exception {
        testQueryCancel(CACHE_SIZE, VAL_SIZE, QRY_1, 10, TimeUnit.MILLISECONDS, true,
            WITH_TIMEOUT_WAS_CANCELLED);
    }

    /** */
    public void testRemoteQueryWithMergeTableTimeout0() throws Exception {
        testQueryCancel(CACHE_SIZE, VAL_SIZE, QRY_2, 3, TimeUnit.MILLISECONDS, true,
            WITH_TIMEOUT_WAS_CANCELLED);
    }

    /** Query possibly could be executed faster than timeout*/
    public void testRemoteQueryWithMergeTableTimeout1() throws Exception {
        testQueryCancel(CACHE_SIZE, VAL_SIZE, QRY_2, 25, TimeUnit.MILLISECONDS, true,
            WITH_TIMEOUT_WAS_CANCELLED);
    }

    /** */
    public void testRemoteQueryExecutionCancel0() throws Exception {
        testQueryCancel(CACHE_SIZE, VAL_SIZE, QRY_1, 1, TimeUnit.MILLISECONDS, false,
            CANCELLED_BY_CLIENT);
    }

    /** */
    public void testRemoteQueryExecutionCancel1() throws Exception {
        testQueryCancel(CACHE_SIZE, VAL_SIZE, QRY_1, 10, TimeUnit.MILLISECONDS, false,
            CANCELLED_BY_CLIENT);
    }

    /** */
    public void testRemoteQueryExecutionCancel2() throws Exception {
        testQueryCancel(CACHE_SIZE, VAL_SIZE, QRY_1, 1, TimeUnit.SECONDS, false,
            CANCELLED_BY_CLIENT);
    }

    /** */
    public void testRemoteQueryExecutionCancel3() throws Exception {
        testQueryCancel(CACHE_SIZE, VAL_SIZE, QRY_1, 3, TimeUnit.SECONDS, false,
            CANCELLED_BY_CLIENT);
    }

    /** */
    public void testRemoteQueryWithMergeTableCancel0() throws Exception {
        testQueryCancel(CACHE_SIZE, VAL_SIZE, QRY_2, 1, TimeUnit.MILLISECONDS, false,
            CANCELLED_BY_CLIENT);
    }

    /** Query with far less complex sql and expected to be executed faster than timeout*/
    public void testRemoteQueryWithMergeTableCancel1() throws Exception {
        testQueryCancel(CACHE_SIZE, VAL_SIZE, QRY_2, 500, TimeUnit.MILLISECONDS, false, null);
    }

    /** Query with far less complex sql and expected to be executed faster than timeout*/
    public void testRemoteQueryWithMergeTableCancel2() throws Exception {
        testQueryCancel(CACHE_SIZE, VAL_SIZE, QRY_2, 1_500, TimeUnit.MILLISECONDS, false, null);
    }

    /** Query with far less complex sql and expected to be executed faster than timeout*/
    public void testRemoteQueryWithMergeTableCancel3() throws Exception {
        testQueryCancel(CACHE_SIZE, VAL_SIZE, QRY_2, 3, TimeUnit.SECONDS, false, null);
    }

    /** Query possibly could be executed faster than timeout*/
    public void testRemoteQueryWithoutMergeTableCancel0() throws Exception {
        testQueryCancel(2*CACHE_SIZE, VAL_SIZE, QRY_3, 1, TimeUnit.MILLISECONDS, false,
            CANCELLED_BY_CLIENT);
    }

    /** Query with far less complex sql and expected to be executed faster than timeout*/
    public void testRemoteQueryWithoutMergeTableCancel1() throws Exception {
        testQueryCancel(CACHE_SIZE, VAL_SIZE, QRY_3, 500, TimeUnit.MILLISECONDS, false, null);
    }

    /** Query with far less complex sql and expected to be executed faster than timeout*/
    public void testRemoteQueryWithoutMergeTableCancel2() throws Exception {
        testQueryCancel(CACHE_SIZE, VAL_SIZE, QRY_3, 1_000, TimeUnit.MILLISECONDS, false, null);
    }

    /** Query with far less complex sql and expected to be executed faster than timeout*/
    public void testRemoteQueryWithoutMergeTableCancel3() throws Exception {
        testQueryCancel(CACHE_SIZE, VAL_SIZE, QRY_3, 3, TimeUnit.SECONDS, false, null);
    }

    /** Query with far less complex sql and expected to be executed faster than timeout*/
    public void testRemoteQueryAlreadyFinishedStop() throws Exception {
        testQueryCancel(100, VAL_SIZE, QRY_3, 3, TimeUnit.SECONDS, false, null);
    }

    /** */
    private void testQueryCancel(int keyCnt, int valSize, String sql, int timeoutUnits, TimeUnit timeUnit,
                                 boolean timeout, String cause) throws Exception {
        try (Ignite client = startGrid("client")) {

            IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

            assertEquals(0, cache.localSize());

            int p = 1;
            for (int i = 1; i <= keyCnt; i++) {
                char[] tmp = new char[valSize];
                Arrays.fill(tmp, ' ');
                cache.put(i, new String(tmp));

                if (i/(float)keyCnt >= p/10f) {
                    log().info("Loaded " + i + " of " + keyCnt);

                    p++;
                }
            }

            assertEquals(0, cache.localSize());

            SqlFieldsQuery qry = new SqlFieldsQuery(sql);

            final QueryCursor<List<?>> cursor;
            if (timeout) {
                qry.setTimeout(timeoutUnits, timeUnit);

                cursor = cache.query(qry);
            } else {
                cursor = cache.query(qry);

                client.scheduler().runLocal(new Runnable() {
                    @Override public void run() {
                        cursor.close();
                    }
                }, timeoutUnits, timeUnit);
            }

            try(QueryCursor<List<?>> ignored = cursor) {
                cursor.iterator();

                if (!F.isEmpty(cause))
                    fail("No exception caught");
            }
            catch (CacheException ex) {
                log().error("Got exception", ex);

                log().error( "Cause of exception", ex.getCause());

                assertTrue("Must throw correct exception", ex.getCause() instanceof QueryCancelledException);

                assertTrue( "Cause message "+ex.getCause().getMessage(), ex.getCause().getMessage().contains(cause));
            }finally {

                // Give some time to clean up.
                Thread.sleep(TimeUnit.MILLISECONDS.convert(timeoutUnits, timeUnit) + 3_000);

                checkCleanState();
            }
        }
    }

    /**
     * Validates clean state on all participating nodes after query cancellation.
     */
    @SuppressWarnings("unchecked")
    private void checkCleanState() throws IgniteCheckedException {
        for (int i = 0; i < GRIDS_CNT; i++) {
            IgniteEx grid = grid(i);

            // Validate everything was cleaned up.
            ConcurrentMap<UUID, ?> map = U.field(((IgniteH2Indexing)U.field((GridProcessor)U.field(
                grid.context(), "qryProc"), "idx")).mapQueryExecutor(), "qryRess");

            String msg = "Map executor state is not cleared";

            // TODO FIXME Current implementation leaves map entry for each node that's ever executed a query.
            for (Object result : map.values()) {
                Map<Long, ?> m = U.field(result, "res");

                assertEquals(msg, 0, m.size());
            }
        }
    }
}

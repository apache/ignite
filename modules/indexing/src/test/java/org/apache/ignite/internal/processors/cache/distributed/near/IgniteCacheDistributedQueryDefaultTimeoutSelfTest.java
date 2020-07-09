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
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests distributed SQL queries default timeouts.
 */
public class IgniteCacheDistributedQueryDefaultTimeoutSelfTest extends GridCommonAbstractTest {
    /** Grids count. */
    private static final int GRIDS_CNT = 3;

    /** Cache size. */
    public static final int CACHE_SIZE = 10_000;

    /** Default query timeout */
    private static final long DEFAULT_QUERY_TIMEOUT = 1000;

    /** Value size. */
    public static final int VAL_SIZE = 16;

    /** */
    private static final String QRY_1 = "select a._val, b._val from String a, String b";

    /** */
    private static final String QRY_2 = "select a._key, count(*) from String a group by a._key";

    /** */
    private static final String QRY_3 = "select a._val from String a";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(GRIDS_CNT);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        ccfg.setIndexedTypes(Integer.class, String.class);

        cfg.setCacheConfiguration(ccfg);
        cfg.setSqlConfiguration(new SqlConfiguration().setDefaultQueryTimeout(DEFAULT_QUERY_TIMEOUT));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        grid(0).cache(DEFAULT_CACHE_NAME).removeAll();
    }

    /** */
    @Test
    public void testRemoteQueryExecutionTimeout() throws Exception {
        testQueryCancel(CACHE_SIZE, VAL_SIZE, QRY_1, 500, TimeUnit.MILLISECONDS, true, true);
    }

    /** */
    @Test
    public void testRemoteQueryWithMergeTableTimeout() throws Exception {
        testQueryCancel(CACHE_SIZE, VAL_SIZE, QRY_2, 500, TimeUnit.MILLISECONDS, true, false);
    }

    /** */
    @Test
    public void testRemoteQueryExecutionCancel0() throws Exception {
        testQueryCancel(CACHE_SIZE, VAL_SIZE, QRY_1, 1, TimeUnit.MILLISECONDS, false, true);
    }

    /** */
    private void testQueryCancel(int keyCnt, int valSize, String sql, int timeoutUnits, TimeUnit timeUnit,
        boolean timeout, boolean checkCanceled) throws Exception {
        try (Ignite client = startClientGrid("client")) {
            IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

            assertEquals(0, cache.localSize());

            int p = 1;
            for (int i = 1; i <= keyCnt; i++) {
                char[] tmp = new char[valSize];
                Arrays.fill(tmp, ' ');
                cache.put(i, new String(tmp));

                if (i / (float)keyCnt >= p / 10f) {
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
            }
            else {
                cursor = cache.query(qry);

                client.scheduler().runLocal(new Runnable() {
                    @Override public void run() {
                        cursor.close();
                    }
                }, timeoutUnits, timeUnit);
            }

            try (QueryCursor<List<?>> ignored = cursor) {
                cursor.getAll();

                if (checkCanceled)
                    fail("Query not canceled");
            }
            catch (CacheException ex) {
                error("Got expected exception", ex);

                assertNotNull("Must throw correct exception", X.cause(ex, QueryCancelledException.class));
            }

            // Give some time to clean up.
            Thread.sleep(TimeUnit.MILLISECONDS.convert(timeoutUnits, timeUnit) + 3_000);

            checkCleanState();
        }
    }

    /**
     * Validates clean state on all participating nodes after query cancellation.
     */
    private void checkCleanState() throws IgniteCheckedException {
        for (int i = 0; i < GRIDS_CNT; i++) {
            IgniteEx grid = grid(i);

            // Validate everything was cleaned up.
            ConcurrentMap<UUID, ?> map = U.field(((IgniteH2Indexing)U.field((GridProcessor)U.field(
                grid.context(), "qryProc"), "idx")).mapQueryExecutor(), "qryRess");

            String msg = "Map executor state is not cleared";

            for (Object result : map.values()) {
                Map<Long, ?> m = U.field(result, "res");

                assertEquals(msg, 0, m.size());
            }
        }
    }
}

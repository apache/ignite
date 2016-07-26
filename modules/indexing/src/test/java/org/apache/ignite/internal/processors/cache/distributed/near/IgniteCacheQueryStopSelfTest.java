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
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractQuerySelfTest;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.QueryCancelledException;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests distributed fields query resources cleanup on cancellation by various reasons.
 */
public class IgniteCacheQueryStopSelfTest extends IgniteCacheAbstractQuerySelfTest {
    /** Value size. */
    public static final int VAL_SIZE = 16;

    /** */
    private static final String QUERY_1 = "select a._key, b._key from String a, String b";

    /** */
    private static final String QUERY_2 = "select a._key, count(*) from String a group by a._key";

    /** */
    private static final String QUERY_3 = "select a._val from String a";

    /** */
    private static final String QUERY_4 = "select a._key from String a";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** */
    public void testRemoteQueryExecutionTimeout() throws Exception {
        testQueryCancel(10_000, VAL_SIZE, QUERY_1, 500, TimeUnit.MILLISECONDS, true);
    }

    /** */
    public void testRemoteQueryWithMergeTableTimeout() throws Exception {
        testQueryCancel(100_000, VAL_SIZE, QUERY_2, 500, TimeUnit.MILLISECONDS, true);
    }

    /** */
    public void testRemoteQueryExecutionCancel1() throws Exception {
        testQueryCancel(10_000, VAL_SIZE, QUERY_1, 500, TimeUnit.MILLISECONDS, false);
    }

    /** */
    public void testRemoteQueryExecutionCancel2() throws Exception {
        testQueryCancel(10_000, VAL_SIZE, QUERY_1, 1_000, TimeUnit.MILLISECONDS, false);
    }

    /** */
    public void testRemoteQueryExecutionCancel3() throws Exception {
        testQueryCancel(10_000, VAL_SIZE, QUERY_1, 3_000, TimeUnit.MILLISECONDS, false);
    }

    /** */
    public void testRemoteQueryWithMergeTableCancel1() throws Exception {
        testQueryCancel(100_000, VAL_SIZE, QUERY_2, 500, TimeUnit.MILLISECONDS, false);
    }

    /** */
    public void testRemoteQueryWithMergeTableCancel2() throws Exception {
        testQueryCancel(100_000, VAL_SIZE, QUERY_2, 1_500, TimeUnit.MILLISECONDS, false);
    }

    /** */
    public void testRemoteQueryWithMergeTableCancel3() throws Exception {
        testQueryCancel(100_000, VAL_SIZE, QUERY_2, 3_000, TimeUnit.MILLISECONDS, false);
    }

    /** */
    public void testRemoteQueryWithoutMergeTableCancel1() throws Exception {
        testQueryCancel(100_000, VAL_SIZE, QUERY_3, 500, TimeUnit.MILLISECONDS, false);
    }

    /** */
    public void testRemoteQueryWithoutMergeTableCancel2() throws Exception {
        testQueryCancel(100_000, VAL_SIZE, QUERY_3, 1_000, TimeUnit.MILLISECONDS, false);
    }

    /** */
    public void testRemoteQueryWithoutMergeTableCancel3() throws Exception {
        testQueryCancel(100_000, VAL_SIZE, QUERY_3, 3_000, TimeUnit.MILLISECONDS, false);
    }

    /** */
    public void testRemoteQueryAlreadyFinishedStop() throws Exception {
        testQueryCancel(100, VAL_SIZE, QUERY_4, 3000, TimeUnit.MILLISECONDS, false);
    }

    /** */
    private void testQueryCancel(int keyCnt, int valSize, String sql, int timeoutMillis, TimeUnit timeUnit,
        boolean timeout) throws Exception {
        try (Ignite client = startGrid("client")) {

            IgniteCache<Object, Object> cache = client.cache(null);

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
                qry.setTimeout(timeoutMillis, timeUnit);

                cursor = cache.query(qry);
            } else {
                cursor = cache.query(qry);

                ignite().scheduler().runLocal(new Runnable() {
                    @Override public void run() {
                        cursor.close();
                    }
                }, timeoutMillis, timeUnit);
            }

            try {
                cursor.iterator();
            }
            catch (CacheException ex) {
                log().error("Got expected exception", ex);

                assertTrue("Must throw correct exception", ex.getCause() instanceof QueryCancelledException);
            }

            Thread.sleep(TimeUnit.MILLISECONDS.convert(timeoutMillis, timeUnit) + 3_000);

            checkCleanState();
        }
    }

    /**
     * Validates clean state on all participating nodes after query cancellation.
     */
    private void checkCleanState() {
        int total = gridCount();

        for (int i = 0; i < total; i++) {
            IgniteEx grid = grid(i);

            // Validate everything was cleaned up.
            ConcurrentMap<UUID, ConcurrentMap<Long, ?>> map = U.field(((IgniteH2Indexing)U.field(U.field(
                grid.context(), "qryProc"), "idx")).mapQueryExecutor(), "qryRess");

            String msg = "Executor state is not cleared";

            if (map.size() == 1)
                assertEquals(msg, 0, map.entrySet().iterator().next().getValue().size());
            else
                assertEquals(msg, 0, map.size());
        }
    }
}
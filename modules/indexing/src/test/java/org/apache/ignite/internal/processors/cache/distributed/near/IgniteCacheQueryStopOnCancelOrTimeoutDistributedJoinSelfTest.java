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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

/**
 * Test for cancel of query containing distributed joins.
 */
public class IgniteCacheQueryStopOnCancelOrTimeoutDistributedJoinSelfTest extends IgniteCacheQueryAbstractDistributedJoinSelfTest {
    /** */
    @Test
    public void testCancel1() throws Exception {
        testQueryCancel(grid(0), "pe", QRY_LONG, 1, TimeUnit.MILLISECONDS, false, true);
    }

    /** */
    @Test
    public void testCancel2() throws Exception {
        testQueryCancel(grid(0), "pe", QRY_LONG, 50, TimeUnit.MILLISECONDS, false, true);
    }

    /** */
    @Test
    public void testCancel3() throws Exception {
        testQueryCancel(grid(0), "pe", QRY_LONG, 100, TimeUnit.MILLISECONDS, false, false);
    }

    /** */
    @Test
    public void testCancel4() throws Exception {
        testQueryCancel(grid(0), "pe", QRY_LONG, 500, TimeUnit.MILLISECONDS, false, false);
    }

    /** */
    @Test
    public void testTimeout1() throws Exception {
        testQueryCancel(grid(0), "pe", QRY_LONG, 1, TimeUnit.MILLISECONDS, true, true);
    }

    /** */
    @Test
    public void testTimeout2() throws Exception {
        testQueryCancel(grid(0), "pe", QRY_LONG, 50, TimeUnit.MILLISECONDS, true, true);
    }

    /** */
    @Test
    public void testTimeout3() throws Exception {
        testQueryCancel(grid(0), "pe", QRY_LONG, 100, TimeUnit.MILLISECONDS, true, false);
    }

    /** */
    @Test
    public void testTimeout4() throws Exception {
        testQueryCancel(grid(0), "pe", QRY_LONG, 500, TimeUnit.MILLISECONDS, true, false);
    }

    /** */
    private void testQueryCancel(Ignite ignite, String cacheName, String sql, int timeoutUnits, TimeUnit timeUnit,
                           boolean timeout, boolean checkCanceled) throws Exception {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setDistributedJoins(true);

        IgniteCache<Object, Object> cache = ignite.cache(cacheName);

        final QueryCursor<List<?>> cursor;
        if (timeout) {
            qry.setTimeout(timeoutUnits, timeUnit);

            cursor = cache.query(qry);
        } else {
            cursor = cache.query(qry);

            ignite.scheduler().runLocal(new Runnable() {
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
            log().error("Got expected exception", ex);

            assertNotNull("Must throw correct exception", X.cause(ex, QueryCancelledException.class));
        }

        // Give some time to clean up.
        Thread.sleep(TimeUnit.MILLISECONDS.convert(timeoutUnits, timeUnit) + 3_000);

        checkCleanState();
    }

    /**
     * Validates clean state on all participating nodes after query cancellation.
     */
    private void checkCleanState() throws IgniteCheckedException {
        for (int i = 0; i < GRID_CNT; i++) {
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

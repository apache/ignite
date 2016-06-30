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
import java.util.concurrent.CountDownLatch;
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
import org.apache.ignite.internal.processors.query.h2.twostep.GridRemoteQueryCancelledException;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CachePeekMode.ALL;

/**
 * Tests distributed fields query resources cleanup on cancellation by various reasons.
 */
public class IgniteCacheQueryStopSelfTest extends IgniteCacheAbstractQuerySelfTest {
    /** */
    private static final String KEY_CROSS_JOIN_QRY = "select a._key, b._key from String a, String b";

    /** */
    private static final String GROUP_QRY = "select a._key, count(*) from String a group by a._key";

    /** */
    private static final String VAL_CROSS_JOIN_QRY = "select a._val, b._val from String a, String b";

    /** */
    private static final String SIMPLE_QRY = "select a._key from String a";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * Tests stopping two-step long query on timeout.
     */
    public void testRemoteQueryExecutionTimeout() throws Exception {
        testQueryTimeout(10_000, 4, KEY_CROSS_JOIN_QRY, 3);
    }

    /**
     * Tests stopping two-step long query while result set is being generated on remote nodes.
     */
    public void testRemoteQueryExecutionStop1() throws Exception {
        testQueryStop(10_000, 4, KEY_CROSS_JOIN_QRY, 500);
    }

    /**
     * Tests stopping two-step long query while result set is being generated on remote nodes.
     */
    public void testRemoteQueryExecutionStop2() throws Exception {
        testQueryStop(10_000, 4, KEY_CROSS_JOIN_QRY, 1000);
    }

    /**
     * Tests stopping two-step long query while result set is being generated on remote nodes.
     */
    public void testRemoteQueryExecutionStop3() throws Exception {
        testQueryStop(10_000, 4, KEY_CROSS_JOIN_QRY, 3000);
    }

    /**
     * Tests stopping two step query while reducing.
     */
    public void testRemoteQueryReducingStop1() throws Exception {
        testQueryStop(100_000, 4, GROUP_QRY, 500);
    }

    /**
     * Tests stopping two step query while reducing.
     */
    public void testRemoteQueryReducingStop2() throws Exception {
        testQueryStop(100_000, 4, GROUP_QRY, 1_500);
    }

    /**
     * Tests stopping two step query while reducing.
     */
    public void testRemoteQueryReducingStop3() throws Exception {
        testQueryStop(100_000, 4, GROUP_QRY, 3_000);
    }

    /**
     * Tests stopping two step query while fetching data from remote nodes.
     */
    public void testRemoteQueryFetchngStop1() throws Exception {
        testQueryStop(100_000, 512, VAL_CROSS_JOIN_QRY, 500);
    }

    /**
     * Tests stopping two step query while fetching data from remote nodes.
     */
    public void testRemoteQueryFetchngStop2() throws Exception {
        testQueryStop(100_000, 512, VAL_CROSS_JOIN_QRY, 1_500);
    }

    /**
     * Tests stopping two step query while fetching data from remote nodes.
     */
    public void testRemoteQueryFetchngStop3() throws Exception {
        testQueryStop(100_000, 512, VAL_CROSS_JOIN_QRY, 3000);
    }

    /**
     * Tests stopping two step query after it was finished.
     */
    public void testRemoteQueryAlreadyFinishedStop() throws Exception {
        testQueryStop(100, 4, SIMPLE_QRY, 3000);
    }

    /**
     * Tests stopping two step query while fetching result set from remote nodes.
     */
    private void testQueryStop(int keyCnt, int valSize, String sql, long cancelTimeout) throws Exception {
        try (Ignite client = startGrid("client")) {

            IgniteCache<Object, Object> cache = client.cache(null);

            assertEquals(0, cache.localSize());

            for (int i = 0; i < keyCnt; i++) {
                char[] tmp = new char[valSize];
                Arrays.fill(tmp, ' ');
                cache.put(i, new String(tmp));
            }

            assertEquals(0, cache.localSize());

            final QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery(sql));

            final CountDownLatch l = new CountDownLatch(1);

            ignite().scheduler().runLocal(new Runnable() {
                @Override public void run() {
                    cursor.close();

                    l.countDown();

                }
            }, cancelTimeout, TimeUnit.MILLISECONDS);

            try {
                // Trigger distributed execution.
                cursor.iterator();
            }
            catch (CacheException ex) {
                log().error("Got expected exception", ex);

                // Expecting instance of GridRemoteQueryCancelledException.
                assertTrue("Must throw correct exception", ex.getCause() instanceof GridRemoteQueryCancelledException);
            }

            l.await();

            // Give some time to clean up after query cancellation.
            Thread.sleep(3000);

            // Validate nodes query result buffer.
            checkCleanState();
        }
    }

    /**
     * Tests stopping two step query on timeout.
     */
    private void testQueryTimeout(int keyCnt, int valSize, String sql, int qryTimeoutSecs) throws Exception {
        try (Ignite client = startGrid("client")) {

            IgniteCache<Object, Object> cache = client.cache(null);

            assertEquals(0, cache.localSize());

            for (int i = 0; i < keyCnt; i++) {
                char[] tmp = new char[valSize];
                Arrays.fill(tmp, ' ');
                cache.put(i, new String(tmp));
            }

            assertEquals(0, cache.localSize(ALL));

            SqlFieldsQuery sqlFldQry = new SqlFieldsQuery(sql);
            sqlFldQry.setTimeout(qryTimeoutSecs, TimeUnit.SECONDS);

            final QueryCursor<?> cursor = cache.query(sqlFldQry);

            try {
                // Trigger distributed execution.
                cursor.iterator();
            }
            catch (CacheException ex) {
                log().error("Got expected exception", ex);
            }

            // Validate nodes query result buffer.
            checkCleanState();
        }
    }

    /**
     * Validates clean state on all participating nodes after query execution stopping.
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
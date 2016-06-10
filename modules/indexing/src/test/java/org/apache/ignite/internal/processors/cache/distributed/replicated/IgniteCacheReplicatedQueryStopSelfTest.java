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

package org.apache.ignite.internal.processors.cache.distributed.replicated;

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
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractQuerySelfTest;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.twostep.GridRemoteQueryCancelledException;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CachePeekMode.ALL;

/**
 * Tests distributed query cancellation.
 */
public class IgniteCacheReplicatedQueryStopSelfTest extends IgniteCacheAbstractQuerySelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * Tests stopping two-step long query while result set is being generated on remote nodes.
     */
    public void testRemoteQueryExecutionStop1() throws Exception {
        testQueryStop(10_000, 4, "select a._key, b._key from String a, String b", 500);
    }

    /**
     * Tests stopping two-step long query while result set is being generated on remote nodes.
     */
    public void testRemoteQueryExecutionStop2() throws Exception {
        testQueryStop(10_000, 4, "select a._key, b._key from String a, String b", 1000);
    }

    /**
     * Tests stopping two-step long query while result set is being generated on remote nodes.
     */
    public void testRemoteQueryExecutionStop3() throws Exception {
        testQueryStop(10_000, 4, "select a._key, b._key from String a, String b", 3000);
    }

    /**
     * Tests stopping two step query while reducing.
     */
    public void testRemoteQueryReducingStop1() throws Exception {
        testQueryStop(100_000, 4, "select a._key, count(*) from String a group by a._key", 500);
    }

    /**
     * Tests stopping two step query while reducing.
     */
    public void testRemoteQueryReducingStop2() throws Exception {
        testQueryStop(100_000, 4, "select a._key, count(*) from String a group by a._key", 1_500);
    }

    /**
     * Tests stopping two step query while reducing.
     */
    public void testRemoteQueryReducingStop3() throws Exception {
        testQueryStop(100_000, 4, "select a._key, count(*) from String a group by a._key", 3_000);
    }

    /**
     * Tests stopping two step query while fetching data from remote nodes.
     */
    public void testRemoteQueryFetchngStop1() throws Exception {
        testQueryStop(100_000, 512, "select a._val, b._val from String a, String b", 500);
    }

    /**
     * Tests stopping two step query while fetching data from remote nodes.
     */
    public void testRemoteQueryFetchngStop2() throws Exception {
        testQueryStop(100_000, 512, "select a._val, b._val from String a, String b", 1_500);
    }

    /**
     * Tests stopping two step query while fetching data from remote nodes.
     */
    public void testRemoteQueryFetchngStop3() throws Exception {
        testQueryStop(100_000, 512, "select a._val, b._val from String a, String b", 3000);
    }

    /**
     * Tests stopping two step query after it was finished.
     */
    public void testRemoteQueryAlreadyFinishedStop() throws Exception {
        testQueryStop(100, 4, "select a._key from String a", 3000);
    }

    /**
     * Tests stopping two-step query on initiator node fail.
     */
    public void testRemoteQueryExecutionStopClientNodeFail1() throws Exception {
        testQueryStopOnClientNodeFail(10_000, 4, "select a._key, b._key from String a, String b", 500);
    }

    /**
     * Tests stopping two-step query on initiator node fail.
     */
    public void testRemoteQueryExecutionStopClientNodeFail2() throws Exception {
        testQueryStopOnClientNodeFail(10_000, 4, "select a._key, b._key from String a, String b", 3_000);
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

            assertEquals(0, cache.localSize(ALL));

            final QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery(sql));

            final CountDownLatch l = new CountDownLatch(1);

            ignite().scheduler().runLocal(new Runnable() {
                @Override public void run() {
                    qry.close();

                    l.countDown();

                }
            }, cancelTimeout, TimeUnit.MILLISECONDS);

            try {
                // Trigger distributed execution.
                qry.iterator();
            }
            catch (CacheException ex) {
                log().error("Got expected exception", ex);

                // Expecting instance of GridRemoteQueryCancelledException.
                assertTrue(ex.getCause() instanceof GridRemoteQueryCancelledException);
            }

            l.await();

            // Give some time to clean up after query cancellation.
            Thread.sleep(3000);

            // Validate nodes query result buffer.
            checkCleanState();
        }
    }

    /**
     * Tests stopping two step query while fetching result set from remote nodes.
     */
    private void testQueryStopOnClientNodeFail(int keyCnt, int valSize, String sql, long failTimeout) throws Exception {
        try (final Ignite client = startGrid("client")) {

            IgniteCache<Object, Object> cache = client.cache(null);

            assertEquals(0, cache.localSize());

            for (int i = 0; i < keyCnt; i++) {
                char[] tmp = new char[valSize];
                Arrays.fill(tmp, ' ');
                cache.put(i, new String(tmp));
            }

            assertEquals(0, cache.localSize(ALL));

            final QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery(sql));

            final CountDownLatch l = new CountDownLatch(1);

            ignite().scheduler().runLocal(new Runnable() {
                @Override public void run() {
                    try {
                        failNode(client);
                    }
                    catch (Exception e) {
                        log().error("Cannot fail node", e);
                    }

                    l.countDown();
                }
            }, failTimeout, TimeUnit.MILLISECONDS);

            try {
                // Trigger distributed execution.
                qry.iterator();
            }
            catch (CacheException ex) {
                log().error("Got expected exception", ex);
            }

            l.await();

            // Give some time to clean up after query cancellation.
            Thread.sleep(3000);

            // Validate nodes query result buffer.
            checkCleanState();
        }
    }

    /**
     *
     */
    private void checkCleanState() {
        int total = gridCount();

        for (int i = 0; i < total; i++) {
            // Validate everything was cleaned up.
            ConcurrentMap<UUID, ConcurrentMap<Long, ?>> map = U.field(((IgniteH2Indexing)U.field(U.field(
                grid(i).context(), "qryProc"), "idx")).mapQueryExecutor(), "qryRess");

            if (map.size() == 1)
                assertEquals(0, map.entrySet().iterator().next().getValue().size());
            else
                assertEquals(0, map.size());

        }
    }

    /**
     * @param ignite Ignite.
     * @throws Exception In case of error.
     */
    private void failNode(Ignite ignite) throws Exception {
        grid(0).configuration().getDiscoverySpi().failNode(ignite.cluster().localNode().id(), "Kicked by grid 0");
    }
}
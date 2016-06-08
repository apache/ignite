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
import org.apache.ignite.cluster.ClusterNode;
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
        return 3;
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
     *
     */
    public void testRemoteQueryExecutionStop2() throws Exception {
        testQueryStop(10_000, 4, "select a._key, b._key from String a, String b", 1000);
    }

    /**
     *
     */
    public void testRemoteQueryExecutionStop3() throws Exception {
        testQueryStop(10_000, 4, "select a._key, b._key from String a, String b", 3000);
    }

    /**
     * Tests stopping two-step long query while result set is being generated on remote nodes.
     */
    public void testRemoteQueryExecutionStopNodeFail1() throws Exception {
        testQueryStopOnNodeFail(10_000, 4, "select a._key, b._key from String a, String b", 2000);
    }

    /**
     * Tests stopping two step query while reducing.
     */
    public void testRemoteQueryReducingStop() throws Exception {
        testQueryStop(100_000, 4, "select a._key, count(*) from String a group by a._key", 3000);
    }

    /**
     * Tests stopping two step query while fetching data from remote nodes.
     */
    public void testRemoteQueryFetchngStop() throws Exception {
        testQueryStop(100_000, 512, "select a._val, b._val from String a, String b", 3000);
    }

    /**
     * Tests stopping two step query after it was finished.
     */
    public void testRemoteQueryAlreadyFinishedStop() throws Exception {
        testQueryStop(100, 4, "select a._key from String a", 3000);
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
            checkCleanState(-1);
        }
    }

    /**
     * Tests stopping two step query while fetching result set from remote nodes.
     */
    private void testQueryStopOnNodeFail(int keyCnt, int valSize, String sql, long failTimeout) throws Exception {
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
                    ClusterNode node = grid(1).localNode();

                    grid(0).configuration().getDiscoverySpi().failNode(node.id(), "Triggered by grid 0");
                }
            }, failTimeout, TimeUnit.MILLISECONDS);

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
            checkCleanState(1);
        }
    }

    /**
     *
     */
    private void checkCleanState(int excludeIdx) {
        int total = gridCount();

        for (int i = 0; i < total; i++) {
            if (excludeIdx == i) continue;

            // Validate everything was cleaned up.
            ConcurrentMap<UUID, ConcurrentMap<Long, ?>> map = U.field(((IgniteH2Indexing)U.field(U.field(
                grid(i).context(), "qryProc"), "idx")).mapQueryExecutor(), "qryRess");

            assertEquals(1, map.size());

            assertEquals(0, map.entrySet().iterator().next().getValue().size());
        }
    }
}
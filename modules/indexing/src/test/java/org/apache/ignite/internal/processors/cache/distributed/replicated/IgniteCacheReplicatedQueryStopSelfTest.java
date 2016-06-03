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
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractQuerySelfTest;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;

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

            final AtomicBoolean first = new AtomicBoolean();

            ignite().scheduler().runLocal(new Runnable() {
                @Override public void run() {
                    boolean res = first.compareAndSet(false, true);
                    if (res)
                        qry.close();

                    l.countDown();
                }
            }, cancelTimeout, TimeUnit.MILLISECONDS);

            try {
                // Trigger remote execution.
                qry.iterator();

                // Fail if close was already invoked.
                boolean res = first.compareAndSet(false, true);

                if (!res)
                    fail();
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

            final AtomicBoolean first = new AtomicBoolean();

            ignite().scheduler().runLocal(new Runnable() {
                @Override public void run() {
                    boolean res = first.compareAndSet(false, true);
                    if (res)
                        qry.close();

                    l.countDown();
                }
            }, failTimeout, TimeUnit.MILLISECONDS);

            try {
                // Trigger remote execution.
                qry.iterator();

                // Fail if close was already invoked.
                boolean res = first.compareAndSet(false, true);

                if (!res)
                    fail();
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

            assertEquals(1, map.size());

            assertEquals(0, map.entrySet().iterator().next().getValue().size());
        }
    }
}
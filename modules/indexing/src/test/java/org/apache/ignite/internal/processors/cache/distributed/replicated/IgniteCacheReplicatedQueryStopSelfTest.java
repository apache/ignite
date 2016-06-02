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
import org.apache.ignite.internal.util.typedef.G;
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
     * Tests stopping two-step long query.
     */
    public void testRemoteQueryExecutionStop() throws Exception {
        try (Ignite client = startGrid("client")) {

            int keyCnt = 10_000;

            IgniteCache<Object, Object> cache = client.cache(null);

            assertEquals(0, cache.localSize());

            for (int i = 0; i < keyCnt; i++)
                cache.put(i, "val" + i);

            assertEquals(0, cache.localSize(ALL));

            final QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery("select a._key, b._key from String a, String b"));

            ignite().scheduler().runLocal(new Runnable() {
                @Override public void run() {
                    qry.close();
                }
            }, 3, TimeUnit.SECONDS);

            // Trigger remote execution.
            try {
                qry.iterator();

                // Iterator should not return.
                fail();
            }
            catch (CacheException ex) {
                log().error("Got expected exception", ex);
            }

            // Give some time to clean up.
            Thread.sleep(3000);

            checkCleanState();
        }
    }

    /**
     * Tests stopping two step short query.
     */
    public void testRemoteQueryAlreadyFinishedStop() throws Exception {
        try (Ignite client = startGrid("client")) {

            int keyCnt = 100;

            IgniteCache<Object, Object> cache = client.cache(null);

            assertEquals(0, cache.localSize());

            for (int i = 0; i < keyCnt; i++)
                cache.put(i, "val" + i);

            assertEquals(0, cache.localSize(ALL));

            final QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery("select a._key from String a"));

            final CountDownLatch l = new CountDownLatch(1);

            ignite().scheduler().runLocal(new Runnable() {
                @Override public void run() {
                    // Query should be finished at this point.
                    qry.close();

                    l.countDown();
                }
            }, 3, TimeUnit.SECONDS);

            try {
                List<List<?>> all = qry.getAll();

                assertEquals(keyCnt, all.size());
            }
            catch (CacheException ex) {
                fail("Query should be finished without errors.");
            }

            // Test should complete without any exceptions.
            l.await();

            checkCleanState();
        }
    }

    /**
     * Tests stopping two step short query.
     */
    public void testRemoteQueryFetchngStop() throws Exception {
        try (Ignite client = startGrid("client")) {

            int keyCnt = 1_000;

            IgniteCache<Object, Object> cache = client.cache(null);

            assertEquals(0, cache.localSize());

            int buf = 512;

            for (int i = 0; i < keyCnt; i++) {
                char[] tmp = new char[buf];
                Arrays.fill(tmp, ' ');
                cache.put(i, new String(tmp));
            }

            assertEquals(0, cache.localSize(ALL));

            final QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery("select a._val, b._val from String a, String b"));

            ignite().scheduler().runLocal(new Runnable() {
                @Override public void run() {
                    qry.close();
                }
            }, 3, TimeUnit.SECONDS);

            // Trigger remote execution.
            try {
                qry.iterator();

                // Iterator should not return.
                fail();
            }
            catch (CacheException ex) {
                log().error("Got expected exception", ex);
            }

            // Give some time to clean up.
            Thread.sleep(3000);

            checkCleanState();
        }
    }

    /**
     * Tests stopping two step short query.
     */
    public void testRemoteQueryReducingStop() throws Exception {
        try (Ignite client = startGrid("client")) {

            int keyCnt = 2_000;

            IgniteCache<Object, Object> cache = client.cache(null);

            for (int i = 0; i < keyCnt; i++)
                cache.put(i, "val" + i);

            assertEquals(0, cache.localSize(ALL));

            final QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery("select a._key from String a"));

            // Trigger remote execution.
            Iterator<List<?>> iter = qry.iterator();

            // For reduce part close currently does nothing.
            qry.close();

            int c = 0;
            while (iter.hasNext()) {
                iter.next();

                c++;
            }

            assertEquals(keyCnt, c);

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
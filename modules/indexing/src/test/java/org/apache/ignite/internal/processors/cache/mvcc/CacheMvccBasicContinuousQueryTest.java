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
package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryManager;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccCachingManager.TX_SIZE_THRESHOLD;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Basic continuous queries test with enabled mvcc.
 */
public class CacheMvccBasicContinuousQueryTest extends CacheMvccAbstractTest  {
    /** */
    private static final long LATCH_TIMEOUT = 5000;

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // Wait for all routines are unregistered
        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                for (Ignite node : G.allGrids()) {
                    GridContinuousProcessor proc = ((IgniteEx)node).context().continuous();

                    if(((Map)U.field(proc, "rmtInfos")).size() > 0)
                        return false;
                }

                return true;
            }
        }, 3000);

        for (Ignite node : G.allGrids()) {
            GridContinuousProcessor proc = ((IgniteEx)node).context().continuous();

            assertEquals(0, ((Map)U.field(proc, "locInfos")).size());
            assertEquals(0, ((Map)U.field(proc, "rmtInfos")).size());
            assertEquals(0, ((Map)U.field(proc, "startFuts")).size());
            assertEquals(0, ((Map)U.field(proc, "stopFuts")).size());
            assertEquals(0, ((Map)U.field(proc, "bufCheckThreads")).size());

            CacheContinuousQueryManager mgr = ((IgniteEx)node).context().cache().internalCache(DEFAULT_CACHE_NAME).context().continuousQueries();

            assertEquals(0, ((Map)U.field(mgr, "lsnrs")).size());

            MvccCachingManager cachingMgr = ((IgniteEx)node).context().cache().context().mvccCaching();

            assertEquals(0, ((Map)U.field(cachingMgr, "enlistCache")).size());
            assertEquals(0, ((Map)U.field(cachingMgr, "cntrs")).size());
        }

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAllEntries() throws Exception {
        Ignite node = startGrids(3);

        final IgniteCache cache = node.createCache(
            cacheConfiguration(cacheMode(), FULL_SYNC, 1, 2)
                .setCacheMode(CacheMode.REPLICATED)
                .setIndexedTypes(Integer.class, Integer.class));

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        final Map<Integer, List<Integer>> map = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(5);

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> e : evts) {
                    synchronized (map) {
                        List<Integer> vals = map.get(e.getKey());

                        if (vals == null) {
                            vals = new ArrayList<>();

                            map.put(e.getKey(), vals);
                        }

                        vals.add(e.getValue());
                    }

                    latch.countDown();
                }
            }
        });

        try (QueryCursor<Cache.Entry<Integer, Integer>> ignored = cache.query(qry)) {

            try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                String dml = "INSERT INTO Integer (_key, _val) values (1,1),(2,2)";

                cache.query(new SqlFieldsQuery(dml)).getAll();

                tx.commit();
            }

            try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                String dml1 = "MERGE INTO Integer (_key, _val) values (3,3)";

                cache.query(new SqlFieldsQuery(dml1)).getAll();

                String dml2 = "DELETE FROM Integer WHERE _key = 2";

                cache.query(new SqlFieldsQuery(dml2)).getAll();

                String dml3 = "UPDATE Integer SET _val = 10 WHERE _key = 1";

                cache.query(new SqlFieldsQuery(dml3)).getAll();

                tx.commit();
            }

            try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                String dml = "INSERT INTO Integer (_key, _val) values (4,4),(5,5)";

                cache.query(new SqlFieldsQuery(dml)).getAll();

                tx.rollback();
            }

            assert latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(3, map.size());

            List<Integer> vals = map.get(1);

            assertNotNull(vals);
            assertEquals(2, vals.size());
            assertEquals(1, (int)vals.get(0));
            assertEquals(10, (int)vals.get(1));

            vals = map.get(2);

            assertNotNull(vals);
            assertEquals(2, vals.size());
            assertEquals(2, (int)vals.get(0));
            assertEquals(2, (int)vals.get(1));

            vals = map.get(3);

            assertNotNull(vals);
            assertEquals(1, vals.size());
            assertEquals(3, (int)vals.get(0));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCachingMaxSize() throws Exception {
        Ignite node = startGrids(1);

        final IgniteCache cache = node.createCache(
            cacheConfiguration(cacheMode(), FULL_SYNC, 1, 2)
                .setCacheMode(CacheMode.PARTITIONED)
                .setIndexedTypes(Integer.class, Integer.class));

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                // No-op.
            }
        });

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (QueryCursor<Cache.Entry<Integer, Integer>> ignored = cache.query(qry)) {
                    try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        for (int i = 0; i < TX_SIZE_THRESHOLD + 1; i++)
                            cache.query(new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (" + i + ", 1)")).getAll();

                        tx.commit();
                    }
                }

                return null;
            }
        },  CacheException.class, "Failed to run update. Transaction is too large. Consider reducing transaction size");
    }
}

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.util.lang.GridInClosure3;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SQL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test for MVCC caches update counters behaviour.
 */
@SuppressWarnings("unchecked")
public class CacheMvccSqlUpdateCountersTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdateCountersInsertSimple() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        Ignite node = startGridsMultiThreaded(3);

        IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

        Affinity aff = affinity(cache);

        Integer key1 = 1;

        int part1 = aff.partition(key1);

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (" + key1 + ",1)");

            cache.query(qry).getAll();

            tx.commit();
        }

        checkUpdateCounters(DEFAULT_CACHE_NAME, part1, 1);

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            SqlFieldsQuery qry = new SqlFieldsQuery("UPDATE Integer SET _val=2 WHERE _key=" + key1);

            cache.query(qry).getAll();

            tx.commit();
        }

        checkUpdateCounters(DEFAULT_CACHE_NAME, part1, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdateCountersDoubleUpdate() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        Ignite node = startGridsMultiThreaded(3);

        IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

        Affinity aff = affinity(cache);

        int key1 = 1;

        int part1 = aff.partition(key1);

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (" + key1 + ",1)");

            cache.query(qry).getAll();

            qry = new SqlFieldsQuery("UPDATE Integer SET _val=2 WHERE _key=" + key1);

            cache.query(qry).getAll();

            tx.commit();
        }

        checkUpdateCounters(DEFAULT_CACHE_NAME, part1, 1);

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            SqlFieldsQuery qry = new SqlFieldsQuery("MERGE INTO Integer (_key, _val) values (" + key1 + ",1)");

            cache.query(qry).getAll();

            qry = new SqlFieldsQuery("UPDATE Integer SET _val=2 WHERE _key=" + key1);

            cache.query(qry).getAll();

            tx.commit();
        }

        checkUpdateCounters(DEFAULT_CACHE_NAME, part1, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdateCountersRollback() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        Ignite node = startGridsMultiThreaded(3);

        IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

        Affinity aff = affinity(cache);

        int key1 = 1;

        int part1 = aff.partition(key1);

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (" + key1 + ",1)");

            cache.query(qry).getAll();

            qry = new SqlFieldsQuery("UPDATE Integer SET _val=2 WHERE _key=" + key1);

            cache.query(qry).getAll();

            tx.rollback();
        }

        checkUpdateCounters(DEFAULT_CACHE_NAME, part1, 0);

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            SqlFieldsQuery qry = new SqlFieldsQuery("MERGE INTO Integer (_key, _val) values (" + key1 + ",1)");

            cache.query(qry).getAll();

            qry = new SqlFieldsQuery("UPDATE Integer SET _val=2 WHERE _key=" + key1);

            cache.query(qry).getAll();

            tx.rollback();
        }

        checkUpdateCounters(DEFAULT_CACHE_NAME, part1, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeleteOwnKey() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, 1)
            .setCacheMode(CacheMode.REPLICATED)
            .setIndexedTypes(Integer.class, Integer.class);

        Ignite node = startGridsMultiThreaded(1);

        IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

        Affinity aff = affinity(cache);

        int key1 = 1;

        int part1 = aff.partition(key1);

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (" + key1 + ",1)");

            cache.query(qry).getAll();

            qry = new SqlFieldsQuery("MERGE INTO Integer (_key, _val) values (" + key1 + ",2)");

            cache.query(qry).getAll();

            qry = new SqlFieldsQuery("MERGE INTO Integer (_key, _val) values (" + key1 + ",3)");

            cache.query(qry).getAll();

            qry = new SqlFieldsQuery("DELETE FROM Integer WHERE _key=" + key1);

            cache.query(qry).getAll();

            qry = new SqlFieldsQuery("MERGE INTO Integer (_key, _val) values (" + key1 + ",4)");

            cache.query(qry).getAll();

            qry = new SqlFieldsQuery("MERGE INTO Integer (_key, _val) values (" + key1 + ",5)");

            cache.query(qry).getAll();

            qry = new SqlFieldsQuery("DELETE FROM Integer WHERE _key=" + key1);

            cache.query(qry).getAll();

            tx.commit();
        }

        checkUpdateCounters(DEFAULT_CACHE_NAME, part1, 0);

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            SqlFieldsQuery qry = new SqlFieldsQuery("MERGE INTO Integer (_key, _val) values (" + key1 + ",1)");

            cache.query(qry).getAll();

            tx.commit();
        }

        checkUpdateCounters(DEFAULT_CACHE_NAME, part1, 1);

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            SqlFieldsQuery qry = new SqlFieldsQuery("DELETE FROM Integer WHERE _key=" + key1);

            cache.query(qry).getAll();

            tx.commit();
        }

        checkUpdateCounters(DEFAULT_CACHE_NAME, part1, 2);

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            SqlFieldsQuery qry = new SqlFieldsQuery("DELETE FROM Integer WHERE _key=" + key1);

            cache.query(qry).getAll();

            tx.commit();
        }

        checkUpdateCounters(DEFAULT_CACHE_NAME, part1, 2);

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            SqlFieldsQuery qry  = new SqlFieldsQuery("MERGE INTO Integer (_key, _val) values (" + key1 + ",5)");

            cache.query(qry).getAll();

            tx.commit();
        }

        checkUpdateCounters(DEFAULT_CACHE_NAME, part1, 3);

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            SqlFieldsQuery qry = new SqlFieldsQuery("DELETE FROM Integer WHERE _key=" + key1);

            cache.query(qry).getAll();

            qry  = new SqlFieldsQuery("MERGE INTO Integer (_key, _val) values (" + key1 + ",5)");

            cache.query(qry).getAll();

            qry = new SqlFieldsQuery("DELETE FROM Integer WHERE _key=" + key1);

            cache.query(qry).getAll();

            tx.commit();
        }

        checkUpdateCounters(DEFAULT_CACHE_NAME, part1, 4);

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            SqlFieldsQuery qry  = new SqlFieldsQuery("MERGE INTO Integer (_key, _val) values (" + key1 + ",6)");

            cache.query(qry).getAll();

            tx.commit();
        }

        checkUpdateCounters(DEFAULT_CACHE_NAME, part1, 5);

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            SqlFieldsQuery qry = new SqlFieldsQuery("DELETE FROM Integer WHERE _key=" + key1);

            cache.query(qry).getAll();

            qry  = new SqlFieldsQuery("MERGE INTO Integer (_key, _val) values (" + key1 + ",5)");

            cache.query(qry).getAll();

            tx.commit();
        }

        checkUpdateCounters(DEFAULT_CACHE_NAME, part1, 6);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdateCountersMultithreaded() throws Exception {
        final int writers = 4;

        final int readers = 0;

        int parts = 8;

        int keys = 20;

        final Map<Integer, AtomicLong> tracker = new ConcurrentHashMap<>();

        for (int i = 0; i< keys; i++)
            tracker.put(i, new AtomicLong(1));

        final IgniteInClosure<IgniteCache<Object, Object>> init = new IgniteInClosure<IgniteCache<Object, Object>>() {
            @Override public void apply(IgniteCache<Object, Object> cache) {
                final IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();
                try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO MvccTestAccount(_key, val, updateCnt) VALUES " +
                        "(?, 0, 1)");

                    for (int i = 0; i < keys; i++) {
                        try (FieldsQueryCursor<List<?>> cur = cache.query(qry.setArgs(i))) {
                            assertEquals(1L, cur.iterator().next().get(0));
                        }

                        tx.commit();
                    }
                }
            }
        };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Map<Integer, AtomicLong> acc = new HashMap<>();

                    int v = 0;

                    while (!stop.get()) {
                        int cnt = rnd.nextInt(keys / 3);

                        if (cnt == 0)
                            cnt = 2;

                        // Generate key set to be changed in tx.
                        while (acc.size() < cnt)
                            acc.put(rnd.nextInt(cnt), new AtomicLong());

                        TestCache<Integer, Integer> cache = randomCache(caches, rnd);

                        boolean success = true;

                        try {
                            IgniteTransactions txs = cache.cache.unwrap(Ignite.class).transactions();

                            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                Map<Integer, MvccTestAccount> allVals = readAllByMode(cache.cache, tracker.keySet(), SQL, ACCOUNT_CODEC);

                                boolean rmv = allVals.size() > keys * 2 / 3;

                                for (Map.Entry<Integer, AtomicLong> e : acc.entrySet()) {
                                    int key = e.getKey();

                                    AtomicLong accCntr = e.getValue();

                                    boolean exists = allVals.containsKey(key);

                                    int delta = 0;

                                    boolean createdInTx = false;

                                    if (rmv && rnd.nextBoolean()) {
                                        if (exists)
                                            delta = 1;

                                        SqlFieldsQuery qry = new SqlFieldsQuery("DELETE FROM MvccTestAccount WHERE _key=" + key);

                                        cache.cache.query(qry).getAll();
                                    }
                                    else {
                                        delta = 1;

                                        if (!exists)
                                            createdInTx = true;

                                        SqlFieldsQuery qry = new SqlFieldsQuery("MERGE INTO MvccTestAccount " +
                                            "(_key, val, updateCnt) VALUES (" + key + ",  " + rnd.nextInt(100) + ", 1)");

                                        cache.cache.query(qry).getAll();
                                    }

                                    if (rnd.nextBoolean()) {
                                        if (createdInTx)
                                            delta = 0; // Do not count cases when key created and removed in the same tx.

                                        SqlFieldsQuery qry = new SqlFieldsQuery("DELETE FROM MvccTestAccount WHERE _key=" + key);

                                        cache.cache.query(qry).getAll();
                                    }
                                    else {
                                        delta = 1;

                                        SqlFieldsQuery qry = new SqlFieldsQuery("MERGE INTO MvccTestAccount " +
                                            "(_key, val, updateCnt) VALUES (" + key + ",  " + rnd.nextInt(100) + ", 1)");

                                        cache.cache.query(qry).getAll();
                                    }

                                    accCntr.addAndGet(delta);
                                }

                                tx.commit();
                            }
                        }
                        catch (Exception e) {
                            handleTxException(e);

                            success = false;

                            int r= 0;

                            for (Map.Entry<Integer, AtomicLong> en : acc.entrySet()) {
                                if (((IgniteCacheProxy)cache.cache).context().affinity().partition(en.getKey()) == 0)
                                    r += en.getValue().intValue();
                            }
                        }
                        finally {
                            cache.readUnlock();

                            if (success) {
                                v++;

                                for (Map.Entry<Integer, AtomicLong> e : acc.entrySet()) {
                                    int k = e.getKey();
                                    long updCntr = e.getValue().get();

                                    tracker.get(k).addAndGet(updCntr);
                                }

                                int r= 0;

                                for (Map.Entry<Integer, AtomicLong> en : acc.entrySet()) {
                                    if (((IgniteCacheProxy)cache.cache).context().affinity().partition(en.getKey()) == 0)
                                        r += en.getValue().intValue();
                                }
                            }

                            acc.clear();
                        }
                    }

                    info("Writer done, updates: " + v);
                }
            };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    // No-op.
                }
            };

        readWriteTest(
            null,
            4,
            1,
            2,
            parts,
            writers,
            readers,
            30_000,
            new InitIndexing(Integer.class, MvccTestAccount.class),
            init,
            writer,
            reader);

        Map<Integer, AtomicLong> updPerParts = new HashMap<>(parts);

        Affinity aff = grid(1).cachex(DEFAULT_CACHE_NAME).affinity();

        for (Map.Entry<Integer, AtomicLong> e : tracker.entrySet()) {
            int k = e.getKey();
            long updCntr = e.getValue().get();

            int p = aff.partition(k);

            AtomicLong cntr = updPerParts.get(p);

            if (cntr == null) {
                cntr = new AtomicLong();

                updPerParts.putIfAbsent(p, cntr);
            }

            cntr.addAndGet(updCntr);
        }

        for (Map.Entry<Integer, AtomicLong> e : updPerParts.entrySet())
            checkUpdateCounters(DEFAULT_CACHE_NAME, e.getKey(), e.getValue().get());
    }

    /**
     * Checks update counter value on all nodes.
     *
     * @param cacheName Cache name.
     * @param p Part number.
     * @param val Expected partition counter value.
     */
    private void checkUpdateCounters(String cacheName, int p, long val) {
        for (Ignite node : G.allGrids()) {
            if (!node.configuration().isClientMode()) {
                IgniteCacheProxy cache = (IgniteCacheProxy)node.cache(cacheName);

                GridDhtLocalPartition part = cache.context().topology().localPartition(p);

                if (!cache.context().mvccEnabled() || part == null)
                    continue;

                assertEquals("part=" + p, val, part.updateCounter());
            }
        }
    }
}

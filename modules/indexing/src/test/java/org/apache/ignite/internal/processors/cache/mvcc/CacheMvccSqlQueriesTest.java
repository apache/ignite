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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.lang.GridInClosure3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * TODO IGNITE-3478: text/spatial indexes with mvcc. TODO IGNITE-3478: indexingSpi with mvcc. TODO IGNITE-3478:
 * setQueryParallelism with mvcc. TODO IGNITE-3478: dynamic index create.
 */
@SuppressWarnings("unchecked")
public class CacheMvccSqlQueriesTest extends CacheMvccAbstractTest {
    /** */
    private static final int TX_TIMEOUT = 3000;

    /** */
    private CacheConfiguration ccfg;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (ccfg != null)
            cfg.setCacheConfiguration(ccfg);

        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_SingleNode_SinglePartition() throws Exception {
        accountsTxReadAll(1, 0, 0, 1, new InitIndexing(Integer.class, MvccTestAccount.class), false, ReadMode.SQL_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_WithRemoves_SingleNode_SinglePartition() throws Exception {
        accountsTxReadAll(1, 0, 0, 1, new InitIndexing(Integer.class, MvccTestAccount.class), true, ReadMode.SQL_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_SingleNode() throws Exception {
        accountsTxReadAll(1, 0, 0, 64, new InitIndexing(Integer.class, MvccTestAccount.class), false, ReadMode.SQL_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_SingleNode_Persistence() throws Exception {
        persistence = true;

        testAccountsTxSql_SingleNode();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSumSql_SingleNode() throws Exception {
        accountsTxReadAll(1, 0, 0, 64, new InitIndexing(Integer.class, MvccTestAccount.class), false, ReadMode.SQL_SUM);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_WithRemoves_SingleNode() throws Exception {
        accountsTxReadAll(1, 0, 0, 64, new InitIndexing(Integer.class, MvccTestAccount.class), true, ReadMode.SQL_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_WithRemoves_SingleNode_Persistence() throws Exception {
        persistence = true;

        testAccountsTxSql_WithRemoves_SingleNode();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_ClientServer_Backups2() throws Exception {
        accountsTxReadAll(4, 2, 2, 64, new InitIndexing(Integer.class, MvccTestAccount.class), false, ReadMode.SQL_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdateSingleValue_SingleNode() throws Exception {
        updateSingleValue(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdateSingleValue_LocalQuery_SingleNode() throws Exception {
        updateSingleValue(true, true);
    }


    /**
     * @throws Exception If failed.
     */
    public void testQueryInsertStaticCache() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {

            SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)");

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (4,4),(5,5),(6,6)");

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            tx.commit();
        }

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));

        assertEquals(4, cache.get(4));
        assertEquals(5, cache.get(5));
        assertEquals(6, cache.get(6));
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryDeleteStaticCache() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(1,1,2,2,3,3));

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {

            SqlFieldsQuery qry = new SqlFieldsQuery("DELETE FROM Integer WHERE 1 = 1");

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            tx.commit();
        }

        assertNull(cache.get(1));
        assertNull(cache.get(2));
        assertNull(cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryUpdateStaticCache() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(1,1,2,2,3,3));

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {

            SqlFieldsQuery qry = new SqlFieldsQuery("UPDATE Integer SET _val = (_key * 10)");

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            tx.commit();
        }

        assertEquals(10, cache.get(1));
        assertEquals(20, cache.get(2));
        assertEquals(30, cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryDeadlock() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(2);

        client = true;

        startGridsMultiThreaded(2, 2);

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicInteger idx = new AtomicInteger();
        final AtomicReference<Exception> ex = new AtomicReference<>();


        multithreaded(new Runnable() {
            @Override public void run() {
                int id = idx.getAndIncrement();

                IgniteEx node = grid(id);

                try {
                    try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        tx.timeout(TX_TIMEOUT);

                        IgniteCache<Object, Object> cache0 = node.cache(DEFAULT_CACHE_NAME);

                        String qry1 = "INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)";
                        String qry2 = "INSERT INTO Integer (_key, _val) values (4,4),(5,5),(6,6)";

                        SqlFieldsQuery qry = new SqlFieldsQuery((id % 2) == 0 ? qry1 : qry2);

                        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                            cur.getAll();
                        }

                        barrier.await();

                        qry = new SqlFieldsQuery((id % 2) == 0 ? qry2 : qry1);

                        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                            cur.getAll();
                        }

                        tx.commit();
                    }
                }
                catch (Exception e) {
                    onException(ex, e);
                }
            }
        }, 2);

        Exception ex0 = ex.get();

        assertNotNull(ex0);

        if (!X.hasCause(ex0, IgniteTxTimeoutCheckedException.class))
            throw ex0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryInsertClient() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGrid(0);

        client = true;

        startGrid(1);

        awaitPartitionMapExchange();

        Ignite checkNode  = grid(0);
        Ignite updateNode = grid(1);

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {

            SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)");

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (4,4),(5,5),(6,6)");

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            tx.commit();
        }

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));

        assertEquals(4, cache.get(4));
        assertEquals(5, cache.get(5));
        assertEquals(6, cache.get(6));
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryInsertMultithread() throws Exception {
        final int THREAD_CNT = 8;
        final int BATCH_SIZE = 1000;
        final int ROUNDS = 10;

        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(2);

        client = true;

        startGridsMultiThreaded(2, 2);

        final AtomicInteger seq = new AtomicInteger();

        multithreaded(new Runnable() {
            @Override public void run() {
                for (int r = 0; r < ROUNDS; r++) {
                    StringBuilder bldr = new StringBuilder("INSERT INTO Integer (_key, _val) values ");

                    int start = seq.getAndAdd(BATCH_SIZE);

                    for (int i = start, end = start + BATCH_SIZE; i < end; i++) {
                        if (i != start)
                            bldr.append(',');

                        bldr
                            .append('(')
                            .append(i)
                            .append(',')
                            .append(i)
                            .append(')');
                    }

                    Random rnd = ThreadLocalRandom.current();

                    Ignite checkNode = grid(rnd.nextInt(4));
                    Ignite updateNode = grid(rnd.nextInt(4));

                    IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

                    try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {

                        SqlFieldsQuery qry = new SqlFieldsQuery(bldr.toString());

                        IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

                        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                            assertEquals((long)BATCH_SIZE, cur.iterator().next().get(0));
                        }

                        tx.commit();
                    }

                    for (int i = start, end = start + BATCH_SIZE; i < end; i++)
                        assertEquals(i, cache.get(i));
                }

            }
        }, THREAD_CNT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryInsertUpdateMiltithread() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(2);

        final Phaser phaser = new Phaser(2);
        final AtomicReference<Exception> ex = new AtomicReference<>();

        GridCompoundFuture fut = new GridCompoundFuture();

        fut.add(multithreadedAsync(new Runnable() {
            @Override public void run() {
                IgniteEx node = grid(0);

                try {
                    try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        tx.timeout(TX_TIMEOUT);

                        IgniteCache<Object, Object> cache0 = node.cache(DEFAULT_CACHE_NAME);

                        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)");

                        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                            cur.getAll();
                        }

                        awaitPhase(phaser, 2);

                        qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (4,4),(5,5),(6,6)");

                        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                            cur.getAll();
                        }

                        tx.commit();
                    }
                }
                catch (Exception e) {
                    onException(ex, e);
                }
            }
        }, 1));

        fut.add(multithreadedAsync(new Runnable() {
            @Override public void run() {
                IgniteEx node = grid(1);

                try {
                    phaser.arriveAndAwaitAdvance();

                    try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        tx.timeout(TX_TIMEOUT);

                        IgniteCache<Integer, Integer> cache0 = node.cache(DEFAULT_CACHE_NAME);

                        cache0.invokeAllAsync(F.asSet(1, 2, 3, 4, 5, 6), new EntryProcessor<Integer, Integer, Void>() {
                            @Override
                            public Void process(MutableEntry<Integer, Integer> entry,
                                Object... arguments) throws EntryProcessorException {
                                entry.setValue(entry.getValue() * 10);

                                return null;
                            }
                        });

                        phaser.arrive();

                        tx.commit();
                    }
                }
                catch (Exception e) {
                    onException(ex, e);
                }
            }
        }, 1));

        fut.markInitialized();

        try {
            fut.get(TX_TIMEOUT);
        }
        catch (IgniteCheckedException e) {
            onException(ex, e);
        }

        Exception ex0 = ex.get();

        if (ex0 != null)
            throw ex0;

        IgniteCache cache = grid(0).cache(DEFAULT_CACHE_NAME);

        assertEquals(10, cache.get(1));
        assertEquals(20, cache.get(2));
        assertEquals(30, cache.get(3));
        assertEquals(40, cache.get(4));
        assertEquals(50, cache.get(5));
        assertEquals(60, cache.get(6));
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryInsertVersionConflict() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(2);

        IgniteCache cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(1, 1, 2, 2, 3, 3));

        final Phaser phaser = new Phaser(2);
        final AtomicReference<Exception> ex = new AtomicReference<>();

        GridCompoundFuture fut = new GridCompoundFuture();

        fut.add(multithreadedAsync(new Runnable() {
            @Override public void run() {
                IgniteEx node = grid(0);

                try {
                    phaser.arriveAndAwaitAdvance();

                    try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        IgniteCache<Object, Object> cache0 = node.cache(DEFAULT_CACHE_NAME);

                        SqlFieldsQuery qry = new SqlFieldsQuery("UPDATE Integer SET _val = (_key * 10)");

                        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                            assertEquals(3L, cur.iterator().next().get(0));
                        }

                        tx.commit();
                    }

                    phaser.arrive();
                }
                catch (Exception e) {
                    onException(ex, e);
                }
            }
        }, 1));

        fut.add(multithreadedAsync(new Runnable() {
            @Override public void run() {
                IgniteEx node = grid(1);

                try {
                    try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        IgniteCache<Object, Object> cache0 = node.cache(DEFAULT_CACHE_NAME);

                        SqlFieldsQuery qry = new SqlFieldsQuery("UPDATE Integer SET _val = (_key * 10) where _key > 3");

                        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                            assertEquals(0L, cur.iterator().next().get(0));
                        }

                        awaitPhase(phaser, 2);

                        qry = new SqlFieldsQuery("UPDATE Integer SET _val = (_key * 10)");

                        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                            assertEquals(0L, cur.iterator().next().get(0));
                        }

                        tx.commit();
                    }
                }
                catch (Exception e) {
                    onException(ex, e);
                }
            }
        }, 1));

        fut.markInitialized();

        try {
            fut.get(TX_TIMEOUT);
        }
        catch (IgniteCheckedException e) {
            onException(ex, e);
        }

        Exception ex0 = ex.get();

        assertNotNull(ex0);

        assertEquals("Failed to run update. Mvcc version mismatch.", ex0.getMessage());
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryInsertRollback() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {

            SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)");

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (4,4),(5,5),(6,6)");

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            tx.rollback();
        }

        for (int i = 1; i <= 6; i++)
            assertNull(cache.get(1));
    }


    /**
     * TODO IGNITE-6938
     * @throws Exception If failed.
     */
    public void testQueryInsertRollbackOnKeysConflict() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {

            SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)");

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)");

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                cur.getAll();
            }

            tx.commit();
        }
        catch (Throwable e) {
             assertEquals("Entry is already enlisted.", e.getMessage());
        }

        for (int i = 1; i <= 6; i++)
            assertNull(cache.get(1));
    }

    /**
     * @param ex Exception holder.
     * @param e Exception.
     */
    private void onException(AtomicReference<Exception> ex, Exception e) {
        if (!ex.compareAndSet(null, e))
            ex.get().addSuppressed(e);
    }

    /**
     * @param phaser Phaser.
     * @param phase Phase to wait for.
     */
    private void awaitPhase(Phaser phaser, int phase) {
        int p;
        do {
            p = phaser.arriveAndAwaitAdvance();
        } while (p < phase);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdateSingleValue_ClientServer() throws Exception {
        updateSingleValue(false, false);
    }

    /**
     * @param singleNode {@code True} for test with single node.
     * @param locQry Local query flag.
     * @throws Exception If failed.
     */
    private void updateSingleValue(boolean singleNode, final boolean locQry) throws Exception {
        final int VALS = 100;

        final int writers = 4;

        final int readers = 4;

        final int INC_BY = 110;

        final IgniteInClosure<IgniteCache<Object, Object>> init = new IgniteInClosure<IgniteCache<Object, Object>>() {
            @Override public void apply(IgniteCache<Object, Object> cache) {
                Map<Integer, MvccTestSqlIndexValue> vals = new HashMap<>();

                for (int i = 0; i < VALS; i++)
                    vals.put(i, new MvccTestSqlIndexValue(i));

                cache.putAll(vals);
            }
        };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int cnt = 0;

                    while (!stop.get()) {
                        TestCache<Integer, MvccTestSqlIndexValue> cache = randomCache(caches, rnd);

                        try {
                            Integer key = rnd.nextInt(VALS);

                            cache.cache.invoke(key, new CacheEntryProcessor<Integer, MvccTestSqlIndexValue, Object>() {
                                @Override public Object process(MutableEntry<Integer, MvccTestSqlIndexValue> e, Object... args) {
                                    Integer key = e.getKey();

                                    MvccTestSqlIndexValue val = e.getValue();

                                    int newIdxVal;

                                    if (val.idxVal1 < INC_BY) {
                                        assertEquals(key.intValue(), val.idxVal1);

                                        newIdxVal = val.idxVal1 + INC_BY;
                                    }
                                    else {
                                        assertEquals(INC_BY + key, val.idxVal1);

                                        newIdxVal = key;
                                    }

                                    e.setValue(new MvccTestSqlIndexValue(newIdxVal));

                                    return null;
                                }
                            });
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }

                    info("Writer finished, updates: " + cnt);
                }
            };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    List<SqlFieldsQuery> fieldsQrys = new ArrayList<>();

                    fieldsQrys.add(
                        new SqlFieldsQuery("select _key, idxVal1 from MvccTestSqlIndexValue where idxVal1=?").setLocal(locQry));

                    fieldsQrys.add(new SqlFieldsQuery("select _key, idxVal1 from MvccTestSqlIndexValue where idxVal1=? or idxVal1=?").setLocal(locQry));

                    fieldsQrys.add(new SqlFieldsQuery("select _key, idxVal1 from MvccTestSqlIndexValue where _key=?").setLocal(locQry));

                    List<SqlQuery<Integer, MvccTestSqlIndexValue>> sqlQrys = new ArrayList<>();

                    sqlQrys.add(new SqlQuery<Integer, MvccTestSqlIndexValue>(MvccTestSqlIndexValue.class, "idxVal1=?").setLocal(locQry));

                    sqlQrys.add(new SqlQuery<Integer, MvccTestSqlIndexValue>(MvccTestSqlIndexValue.class, "idxVal1=? or idxVal1=?").setLocal(locQry));

                    sqlQrys.add(new SqlQuery<Integer, MvccTestSqlIndexValue>(MvccTestSqlIndexValue.class, "_key=?").setLocal(locQry));

                    while (!stop.get()) {
                        Integer key = rnd.nextInt(VALS);

                        int qryIdx = rnd.nextInt(3);

                        TestCache<Integer, MvccTestSqlIndexValue> cache = randomCache(caches, rnd);

                        List<List<?>> res;

                        try {
                            if (rnd.nextBoolean()) {
                                SqlFieldsQuery qry = fieldsQrys.get(qryIdx);

                                if (qryIdx == 1)
                                    qry.setArgs(key, key + INC_BY);
                                else
                                    qry.setArgs(key);

                                res = cache.cache.query(qry).getAll();
                            }
                            else {
                                SqlQuery<Integer, MvccTestSqlIndexValue> qry = sqlQrys.get(qryIdx);

                                if (qryIdx == 1)
                                    qry.setArgs(key, key + INC_BY);
                                else
                                    qry.setArgs(key);

                                res = new ArrayList<>();

                                for (IgniteCache.Entry<Integer, MvccTestSqlIndexValue> e : cache.cache.query(qry).getAll()) {
                                    List<Object> row = new ArrayList<>(2);

                                    row.add(e.getKey());
                                    row.add(e.getValue().idxVal1);

                                    res.add(row);
                                }
                            }
                        }
                        finally {
                            cache.readUnlock();
                        }

                        assertTrue(qryIdx == 0 || !res.isEmpty());

                        if (!res.isEmpty()) {
                            assertEquals(1, res.size());

                            List<?> resVals = res.get(0);

                            Integer key0 = (Integer)resVals.get(0);
                            Integer val0 = (Integer)resVals.get(1);

                            assertEquals(key, key0);
                            assertTrue(val0.equals(key) || val0.equals(key + INC_BY));
                        }
                    }

                    if (idx == 0) {
                        SqlFieldsQuery qry = new SqlFieldsQuery("select _key, idxVal1 from MvccTestSqlIndexValue");

                        TestCache<Integer, MvccTestSqlIndexValue> cache = randomCache(caches, rnd);

                        List<List<?>> res;

                        try {
                            res = cache.cache.query(qry).getAll();
                        }
                        finally {
                            cache.readUnlock();
                        }

                        assertEquals(VALS, res.size());

                        for (List<?> vals : res)
                            info("Value: " + vals);
                    }
                }
            };

        int srvs;
        int clients;

        if (singleNode) {
            srvs = 1;
            clients = 0;
        }
        else {
            srvs = 4;
            clients = 2;
        }

        readWriteTest(
            null,
            srvs,
            clients,
            0,
            DFLT_PARTITION_COUNT,
            writers,
            readers,
            DFLT_TEST_TIME,
            new InitIndexing(Integer.class, MvccTestSqlIndexValue.class),
            init,
            writer,
            reader);

        for (Ignite node : G.allGrids())
            checkActiveQueriesCleanup(node);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinTransactional_SingleNode() throws Exception {
        joinTransactional(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinTransactional_ClientServer() throws Exception {
        joinTransactional(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinTransactional_DistributedJoins_ClientServer() throws Exception {
        joinTransactional(false, true);
    }

    /**
     * @param singleNode {@code True} for test with single node.
     * @param distributedJoin {@code True} to test distributed joins.
     * @throws Exception If failed.
     */
    private void joinTransactional(boolean singleNode, final boolean distributedJoin) throws Exception {
        final int KEYS = 100;

        final int writers = 4;

        final int readers = 4;

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int cnt = 0;

                    while (!stop.get()) {
                        TestCache<Object, Object> cache = randomCache(caches, rnd);

                        IgniteTransactions txs = cache.cache.unwrap(Ignite.class).transactions();

                        try {
                            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                Integer key = rnd.nextInt(KEYS);

                                JoinTestChildKey childKey = new JoinTestChildKey(key);

                                JoinTestChild child = (JoinTestChild)cache.cache.get(childKey);

                                if (child == null) {
                                    Integer parentKey = distributedJoin ? key + 100 : key;

                                    child = new JoinTestChild(parentKey);

                                    cache.cache.put(childKey, child);

                                    JoinTestParent parent = new JoinTestParent(parentKey);

                                    cache.cache.put(new JoinTestParentKey(parentKey), parent);
                                }
                                else {
                                    cache.cache.remove(childKey);

                                    cache.cache.remove(new JoinTestParentKey(child.parentId));
                                }

                                tx.commit();
                            }

                            cnt++;
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }

                    info("Writer finished, updates: " + cnt);
                }
            };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    List<SqlFieldsQuery> qrys = new ArrayList<>();

                    qrys.add(new SqlFieldsQuery("select c.parentId, p.id from " +
                        "JoinTestChild c left outer join JoinTestParent p on (c.parentId = p.id)").
                        setDistributedJoins(distributedJoin));

                    qrys.add(new SqlFieldsQuery("select c.parentId, p.id from " +
                        "JoinTestChild c left outer join JoinTestParent p on (c.parentId = p.id) where p.id = 10").
                        setDistributedJoins(distributedJoin));

                    qrys.add(new SqlFieldsQuery("select c.parentId, p.id from " +
                        "JoinTestChild c left outer join JoinTestParent p on (c.parentId = p.id) where p.id != 10").
                        setDistributedJoins(distributedJoin));

                    while (!stop.get()) {
                        TestCache<Object, Object> cache = randomCache(caches, rnd);

                        try {
                            for (SqlFieldsQuery qry : qrys) {
                                List<List<?>> res = cache.cache.query(qry).getAll();

                                if (!res.isEmpty()) {
                                    for (List<?> resRow : res) {
                                        Integer parentId = (Integer)resRow.get(1);

                                        assertNotNull(parentId);
                                    }
                                }
                            }
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }

                    if (idx == 0) {
                        TestCache<Object, Object> cache = randomCache(caches, rnd);

                        try {
                            List<List<?>> res = cache.cache.query(qrys.get(0)).getAll();

                            info("Reader finished, result: " + res);
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }
                }
            };

        int srvs;
        int clients;

        if (singleNode) {
            srvs = 1;
            clients = 0;
        }
        else {
            srvs = 4;
            clients = 2;
        }

        readWriteTest(
            null,
            srvs,
            clients,
            0,
            DFLT_PARTITION_COUNT,
            writers,
            readers,
            DFLT_TEST_TIME,
            new InitIndexing(JoinTestParentKey.class, JoinTestParent.class,
                JoinTestChildKey.class, JoinTestChild.class),
            null,
            writer,
            reader);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinTransactional_DistributedJoins_ClientServer2() throws Exception {
        final int KEYS = 100;

        final int writers = 1;

        final int readers = 4;

        final int CHILDREN_CNT = 10;

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int cnt = 0;

                    while (!stop.get()) {
                        TestCache<Object, Object> cache = randomCache(caches, rnd);

                        IgniteTransactions txs = cache.cache.unwrap(Ignite.class).transactions();

                        try {
                            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                Integer key = rnd.nextInt(KEYS);

                                JoinTestParentKey parentKey = new JoinTestParentKey(key);

                                JoinTestParent parent = (JoinTestParent)cache.cache.get(parentKey);

                                if (parent == null) {
                                    for (int i = 0; i < CHILDREN_CNT; i++)
                                        cache.cache.put(new JoinTestChildKey(key * 10_000 + i), new JoinTestChild(key));

                                    cache.cache.put(parentKey, new JoinTestParent(key));
                                }
                                else {
                                    for (int i = 0; i < CHILDREN_CNT; i++)
                                        cache.cache.remove(new JoinTestChildKey(key * 10_000 + i));

                                    cache.cache.remove(parentKey);
                                }

                                tx.commit();
                            }

                            cnt++;
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }

                    info("Writer finished, updates: " + cnt);
                }
            };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    SqlFieldsQuery qry = new SqlFieldsQuery("select c.parentId, p.id from " +
                        "JoinTestChild c left outer join JoinTestParent p on (c.parentId = p.id) where p.id=?").
                        setDistributedJoins(true);

                    int cnt = 0;

                    while (!stop.get()) {
                        TestCache<Object, Object> cache = randomCache(caches, rnd);

                        qry.setArgs(rnd.nextInt(KEYS));

                        try {
                            List<List<?>> res = cache.cache.query(qry).getAll();

                            if (!res.isEmpty())
                                assertEquals(CHILDREN_CNT, res.size());

                            cnt++;
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }

                    info("Reader finished, read count: " + cnt);
                }
            };

        readWriteTest(
            null,
            4,
            2,
            0,
            DFLT_PARTITION_COUNT,
            writers,
            readers,
            DFLT_TEST_TIME,
            new InitIndexing(JoinTestParentKey.class, JoinTestParent.class,
                JoinTestChildKey.class, JoinTestChild.class),
            null,
            writer,
            reader);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedJoinSimple() throws Exception {
        startGridsMultiThreaded(4);

        Ignite srv0 = ignite(0);

        int[] backups = {0, 1, 2};

        for (int b : backups) {
            IgniteCache<Object, Object> cache = srv0.createCache(
                cacheConfiguration(PARTITIONED, FULL_SYNC, b, DFLT_PARTITION_COUNT).
                    setIndexedTypes(JoinTestParentKey.class, JoinTestParent.class, JoinTestChildKey.class, JoinTestChild.class));

            int cntr = 0;

            int expCnt = 0;

            for (int i = 0; i < 10; i++) {
                JoinTestParentKey parentKey = new JoinTestParentKey(i);

                cache.put(parentKey, new JoinTestParent(i));

                for (int c = 0; c < i; c++) {
                    JoinTestChildKey childKey = new JoinTestChildKey(cntr++);

                    cache.put(childKey, new JoinTestChild(i));

                    expCnt++;
                }
            }

            SqlFieldsQuery qry = new SqlFieldsQuery("select c.parentId, p.id from " +
                "JoinTestChild c join JoinTestParent p on (c.parentId = p.id)").
                setDistributedJoins(true);

            Map<Integer, Integer> resMap = new HashMap<>();

            List<List<?>> res = cache.query(qry).getAll();

            assertEquals(expCnt, res.size());

            for (List<?> resRow : res) {
                Integer parentId = (Integer)resRow.get(0);

                Integer cnt = resMap.get(parentId);

                if (cnt == null)
                    resMap.put(parentId, 1);
                else
                    resMap.put(parentId, cnt + 1);
            }

            for (int i = 1; i < 10; i++)
                assertEquals(i, (Object)resMap.get(i));

            srv0.destroyCache(cache.getName());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheRecreate() throws Exception {
        cacheRecreate(new InitIndexing(Integer.class, MvccTestAccount.class));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheRecreateChangeIndexedType() throws Exception {
        Ignite srv0 = startGrid(0);

        final int PARTS = 64;

        {
            CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, PARTS).
                setIndexedTypes(Integer.class, MvccTestAccount.class);

            IgniteCache<Integer, MvccTestAccount> cache = (IgniteCache)srv0.createCache(ccfg);

            for (int k = 0; k < PARTS * 2; k++) {
                assertNull(cache.get(k));

                int vals = k % 3 + 1;

                for (int v = 0; v < vals; v++)
                    cache.put(k, new MvccTestAccount(v, 1));

                assertEquals(vals - 1, cache.get(k).val);
            }

            assertEquals(PARTS * 2, cache.query(new SqlQuery<>(MvccTestAccount.class, "true")).getAll().size());

            srv0.destroyCache(cache.getName());
        }

        {
            CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, PARTS).
                setIndexedTypes(Integer.class, MvccTestSqlIndexValue.class);

            IgniteCache<Integer, MvccTestSqlIndexValue> cache = (IgniteCache)srv0.createCache(ccfg);

            for (int k = 0; k < PARTS * 2; k++) {
                assertNull(cache.get(k));

                int vals = k % 3 + 1;

                for (int v = 0; v < vals; v++)
                    cache.put(k, new MvccTestSqlIndexValue(v));

                assertEquals(vals - 1, cache.get(k).idxVal1);
            }

            assertEquals(PARTS * 2, cache.query(new SqlQuery<>(MvccTestSqlIndexValue.class, "true")).getAll().size());

            srv0.destroyCache(cache.getName());
        }

        {
            CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, PARTS).
                setIndexedTypes(Long.class, Long.class);

            IgniteCache<Long, Long> cache = (IgniteCache)srv0.createCache(ccfg);

            for (int k = 0; k < PARTS * 2; k++) {
                assertNull(cache.get((long)k));

                int vals = k % 3 + 1;

                for (int v = 0; v < vals; v++)
                    cache.put((long)k, (long)v);

                assertEquals((long)(vals - 1), (Object)cache.get((long)k));
            }

            assertEquals(PARTS * 2, cache.query(new SqlQuery<>(Long.class, "true")).getAll().size());

            srv0.destroyCache(cache.getName());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testChangeValueType1() throws Exception {
        Ignite srv0 = startGrid(0);

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT).
            setIndexedTypes(Integer.class, MvccTestSqlIndexValue.class, Integer.class, Integer.class);

        IgniteCache<Object, Object> cache = srv0.createCache(ccfg);

        cache.put(1, new MvccTestSqlIndexValue(1));
        cache.put(1, new MvccTestSqlIndexValue(2));

        checkSingleResult(cache, new SqlFieldsQuery("select idxVal1 from MvccTestSqlIndexValue"), 2);

        cache.put(1, 1);

        assertEquals(0, cache.query(new SqlFieldsQuery("select idxVal1 from MvccTestSqlIndexValue")).getAll().size());

        checkSingleResult(cache, new SqlFieldsQuery("select _val from Integer"), 1);

        cache.put(1, 2);

        checkSingleResult(cache, new SqlFieldsQuery("select _val from Integer"), 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testChangeValueType2() throws Exception {
        Ignite srv0 = startGrid(0);

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT).
            setIndexedTypes(Integer.class, MvccTestSqlIndexValue.class, Integer.class, Integer.class);

        IgniteCache<Object, Object> cache = srv0.createCache(ccfg);

        cache.put(1, new MvccTestSqlIndexValue(1));
        cache.put(1, new MvccTestSqlIndexValue(2));

        checkSingleResult(cache, new SqlFieldsQuery("select idxVal1 from MvccTestSqlIndexValue"), 2);

        cache.remove(1);

        assertEquals(0, cache.query(new SqlFieldsQuery("select idxVal1 from MvccTestSqlIndexValue")).getAll().size());

        cache.put(1, 1);

        assertEquals(0, cache.query(new SqlFieldsQuery("select idxVal1 from MvccTestSqlIndexValue")).getAll().size());

        checkSingleResult(cache, new SqlFieldsQuery("select _val from Integer"), 1);

        cache.put(1, 2);

        checkSingleResult(cache, new SqlFieldsQuery("select _val from Integer"), 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCountTransactional_SingleNode() throws Exception {
        countTransactional(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCountTransactional_ClientServer() throws Exception {
        countTransactional(false);
    }

    /**
     * @param singleNode {@code True} for test with single node.
     * @throws Exception If failed.
     */
    private void countTransactional(boolean singleNode) throws Exception {
        final int writers = 4;

        final int readers = 4;

        final int THREAD_KEY_RANGE = 100;

        final int VAL_RANGE = 10;

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int min = idx * THREAD_KEY_RANGE;
                    int max = min + THREAD_KEY_RANGE;

                    info("Thread range [min=" + min + ", max=" + max + ']');

                    int cnt = 0;

                    Set<Integer> keys = new LinkedHashSet<>();

                    while (!stop.get()) {
                        TestCache<Integer, MvccTestSqlIndexValue> cache = randomCache(caches, rnd);

                        try {
                            // Add or remove 10 keys.
                            if (!keys.isEmpty() && (keys.size() == THREAD_KEY_RANGE || rnd.nextInt(3) == 0)) {
                                Set<Integer> rmvKeys = new HashSet<>();

                                for (Integer key : keys) {
                                    rmvKeys.add(key);

                                    if (rmvKeys.size() == 10)
                                        break;
                                }

                                assertEquals(10, rmvKeys.size());

                                cache.cache.removeAll(rmvKeys);

                                keys.removeAll(rmvKeys);
                            }
                            else {
                                TreeMap<Integer, MvccTestSqlIndexValue> map = new TreeMap<>();

                                while (map.size() != 10) {
                                    Integer key = rnd.nextInt(min, max);

                                    if (keys.add(key))
                                        map.put(key, new MvccTestSqlIndexValue(rnd.nextInt(VAL_RANGE)));
                                }

                                assertEquals(10, map.size());

                                cache.cache.putAll(map);
                            }
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }

                    info("Writer finished, updates: " + cnt);
                }
            };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    List<SqlFieldsQuery> qrys = new ArrayList<>();

                    qrys.add(new SqlFieldsQuery("select count(*) from MvccTestSqlIndexValue"));

                    qrys.add(new SqlFieldsQuery(
                        "select count(*) from MvccTestSqlIndexValue where idxVal1 >= 0 and idxVal1 <= " + VAL_RANGE));

                    while (!stop.get()) {
                        TestCache<Integer, MvccTestSqlIndexValue> cache = randomCache(caches, rnd);

                        try {
                            for (SqlFieldsQuery qry : qrys) {
                                List<List<?>> res = cache.cache.query(qry).getAll();

                                assertEquals(1, res.size());

                                Long cnt = (Long)res.get(0).get(0);

                                assertTrue(cnt % 10 == 0);
                            }
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }
                }
            };

        int srvs;
        int clients;

        if (singleNode) {
            srvs = 1;
            clients = 0;
        }
        else {
            srvs = 4;
            clients = 2;
        }

        readWriteTest(
            null,
            srvs,
            clients,
            0,
            DFLT_PARTITION_COUNT,
            writers,
            readers,
            DFLT_TEST_TIME,
            new InitIndexing(Integer.class, MvccTestSqlIndexValue.class),
            null,
            writer,
            reader);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxMinTransactional_SingleNode() throws Exception {
        maxMinTransactional(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxMinTransactional_ClientServer() throws Exception {
        maxMinTransactional(false);
    }

    /**
     * @param singleNode {@code True} for test with single node.
     * @throws Exception If failed.
     */
    private void maxMinTransactional(boolean singleNode) throws Exception {
        final int writers = 1;

        final int readers = 1;

        final int THREAD_OPS = 10;

        final int OP_RANGE = 10;

        final int THREAD_KEY_RANGE = OP_RANGE * THREAD_OPS;

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int min = idx * THREAD_KEY_RANGE;

                    info("Thread range [start=" + min + ']');

                    int cnt = 0;

                    boolean add = true;

                    int op = 0;

                    while (!stop.get()) {
                        TestCache<Integer, MvccTestSqlIndexValue> cache = randomCache(caches, rnd);

                        try {
                            int startKey = min + op * OP_RANGE;

                            if (add) {
                                Map<Integer, MvccTestSqlIndexValue> vals = new HashMap<>();

                                for (int i = 0; i < 10; i++) {
                                    Integer key = startKey + i + 1;

                                    vals.put(key, new MvccTestSqlIndexValue(key));
                                }

                                cache.cache.putAll(vals);

                                // info("put " + vals.keySet());
                            }
                            else {
                                Set<Integer> rmvKeys = new HashSet<>();

                                for (int i = 0; i < 10; i++)
                                    rmvKeys.add(startKey + i + 1);

                                cache.cache.removeAll(rmvKeys);

                                // info("remove " + rmvKeys);
                            }

                            if (++op == THREAD_OPS) {
                                add = !add;

                                op = 0;
                            }
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }

                    info("Writer finished, updates: " + cnt);
                }
            };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    List<SqlFieldsQuery> maxQrys = new ArrayList<>();
                    List<SqlFieldsQuery> minQrys = new ArrayList<>();

                    maxQrys.add(new SqlFieldsQuery("select max(idxVal1) from MvccTestSqlIndexValue"));
                    maxQrys.add(new SqlFieldsQuery("select max(idxVal1) from MvccTestSqlIndexValue where idxVal1 >= 0"));

                    minQrys.add(new SqlFieldsQuery("select min(idxVal1) from MvccTestSqlIndexValue"));
                    minQrys.add(new SqlFieldsQuery("select min(idxVal1) from MvccTestSqlIndexValue where idxVal1 >= 0"));

                    while (!stop.get()) {
                        TestCache<Integer, MvccTestSqlIndexValue> cache = randomCache(caches, rnd);

                        try {
                            for (SqlFieldsQuery qry : maxQrys) {
                                List<List<?>> res = cache.cache.query(qry).getAll();

                                assertEquals(1, res.size());

                                Integer m = (Integer)res.get(0).get(0);

                                assertTrue(m == null || m % 10 == 0);
                            }

                            for (SqlFieldsQuery qry : minQrys) {
                                List<List<?>> res = cache.cache.query(qry).getAll();

                                assertEquals(1, res.size());

                                Integer m = (Integer)res.get(0).get(0);

                                assertTrue(m == null || m % 10 == 1);
                            }
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }
                }
            };

        int srvs;
        int clients;

        if (singleNode) {
            srvs = 1;
            clients = 0;
        }
        else {
            srvs = 4;
            clients = 2;
        }

        readWriteTest(
            null,
            srvs,
            clients,
            0,
            DFLT_PARTITION_COUNT,
            writers,
            readers,
            DFLT_TEST_TIME,
            new InitIndexing(Integer.class, MvccTestSqlIndexValue.class),
            null,
            writer,
            reader);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlQueriesWithMvcc() throws Exception {
        Ignite srv0 = startGrid(0);

        IgniteCache<Integer, MvccTestSqlIndexValue> cache = (IgniteCache)srv0.createCache(
            cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT).
                setIndexedTypes(Integer.class, MvccTestSqlIndexValue.class));

        for (int i = 0; i < 10; i++)
            cache.put(i, new MvccTestSqlIndexValue(i));

        sqlQueriesWithMvcc(cache, true);

        sqlQueriesWithMvcc(cache, false);

        startGrid(1);

        awaitPartitionMapExchange();

        sqlQueriesWithMvcc(cache, false);
    }

    /**
     * @param cache Cache.
     * @param loc Local query flag.
     */
    private void sqlQueriesWithMvcc(IgniteCache<Integer, MvccTestSqlIndexValue> cache, boolean loc) {
        assertEquals(10,
            cache.query(new SqlQuery<>(MvccTestSqlIndexValue.class, "true").setLocal(loc)).getAll().size());

        assertEquals(10,
            cache.query(new SqlFieldsQuery("select idxVal1 from MvccTestSqlIndexValue").setLocal(loc)).getAll().size());

        assertEquals(10,
            cache.query(new SqlFieldsQuery("" +
                "select (select count (*) from MvccTestSqlIndexValue where idxVal1 = t1.idxVal1) as c1," +
                " (select 0 from dual) as c2" +
                " from MvccTestSqlIndexValue as t1" +
                " join (select * from MvccTestSqlIndexValue) as t2 on t1.idxVal1 = t2.idxVal1").setLocal(loc)).getAll().size());

        checkSingleResult(cache,
            new SqlFieldsQuery("select max(idxVal1) from MvccTestSqlIndexValue").setLocal(loc), 9);

        checkSingleResult(cache,
            new SqlFieldsQuery("select max(idxVal1) from MvccTestSqlIndexValue where idxVal1 > 0").setLocal(loc), 9);

        checkSingleResult(cache,
            new SqlFieldsQuery("select max(idxVal1) from MvccTestSqlIndexValue where idxVal1 < 5").setLocal(loc), 4);

        checkSingleResult(cache,
            new SqlFieldsQuery("select min(idxVal1) from MvccTestSqlIndexValue").setLocal(loc), 0);

        checkSingleResult(cache,
            new SqlFieldsQuery("select min(idxVal1) from MvccTestSqlIndexValue where idxVal1 < 100").setLocal(loc), 0);

        checkSingleResult(cache,
            new SqlFieldsQuery("select min(idxVal1) from MvccTestSqlIndexValue where idxVal1 < 5").setLocal(loc), 0);

        checkSingleResult(cache,
            new SqlFieldsQuery("select min(idxVal1) from MvccTestSqlIndexValue where idxVal1 > 5").setLocal(loc), 6);

        checkSingleResult(cache,
            new SqlFieldsQuery("select count(*) from MvccTestSqlIndexValue").setLocal(loc), 10L);

        checkSingleResult(cache,
            new SqlFieldsQuery("select count(*) from MvccTestSqlIndexValue where idxVal1 >= 0").setLocal(loc), 10L);

        checkSingleResult(cache,
            new SqlFieldsQuery("select count(*) from MvccTestSqlIndexValue where idxVal1 >= 0 and idxVal1 < 100").setLocal(loc), 10L);

        checkSingleResult(cache,
            new SqlFieldsQuery("select count(*) from MvccTestSqlIndexValue where idxVal1 >0 and idxVal1 < 5").setLocal(loc), 4L);

        checkSingleResult(cache,
            new SqlFieldsQuery("select count(*) from MvccTestSqlIndexValue where idxVal1 >= 1").setLocal(loc), 9L);

        checkSingleResult(cache,
            new SqlFieldsQuery("select count(*) from MvccTestSqlIndexValue where idxVal1 > 100").setLocal(loc), 0L);

        checkSingleResult(cache,
            new SqlFieldsQuery("select count(*) from MvccTestSqlIndexValue where idxVal1 = 1").setLocal(loc), 1L);
    }

    /**
     * @param cache Cache.
     * @param qry Query.
     * @param exp Expected value.
     */
    private void checkSingleResult(IgniteCache cache, SqlFieldsQuery qry, Object exp) {
        List<List<?>> res = cache.query(qry).getAll();

        assertEquals(1, res.size());

        List<?> row = res.get(0);

        assertEquals(1, row.size());

        assertEquals(exp, row.get(0));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlSimple() throws Exception {
        startGrid(0);

        for (int i = 0; i < 4; i++)
            sqlSimple(i * 512);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 5; i++)
            sqlSimple(rnd.nextInt(2048));
    }

    /**
     * @param inlineSize Inline size.
     * @throws Exception If failed.
     */
    private void sqlSimple(int inlineSize) throws Exception {
        Ignite srv0 = ignite(0);

        IgniteCache<Integer, MvccTestSqlIndexValue> cache =  (IgniteCache)srv0.createCache(
            cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT).
                setIndexedTypes(Integer.class, MvccTestSqlIndexValue.class).
                setSqlIndexMaxInlineSize(inlineSize));

        Map<Integer, Integer> expVals = new HashMap<>();

        checkValues(expVals, cache);

        cache.put(1, new MvccTestSqlIndexValue(1));
        expVals.put(1, 1);

        checkValues(expVals, cache);

        cache.put(1, new MvccTestSqlIndexValue(2));
        expVals.put(1, 2);

        checkValues(expVals, cache);

        cache.put(2, new MvccTestSqlIndexValue(1));
        expVals.put(2, 1);
        cache.put(3, new MvccTestSqlIndexValue(1));
        expVals.put(3, 1);
        cache.put(4, new MvccTestSqlIndexValue(1));
        expVals.put(4, 1);

        checkValues(expVals, cache);

        cache.remove(1);
        expVals.remove(1);

        checkValues(expVals, cache);

        checkNoValue(1, cache);

        cache.put(1, new MvccTestSqlIndexValue(10));
        expVals.put(1, 10);

        checkValues(expVals, cache);

        checkActiveQueriesCleanup(srv0);

        srv0.destroyCache(cache.getName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlSimplePutRemoveRandom() throws Exception {
        startGrid(0);

        testSqlSimplePutRemoveRandom(0);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 3; i++)
            testSqlSimplePutRemoveRandom(rnd.nextInt(2048));
    }

    /**
     * @param inlineSize Inline size.
     * @throws Exception If failed.
     */
    private void testSqlSimplePutRemoveRandom(int inlineSize) throws Exception {
        Ignite srv0 = grid(0);

        IgniteCache<Integer, MvccTestSqlIndexValue> cache = (IgniteCache) srv0.createCache(
            cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT).
                setIndexedTypes(Integer.class, MvccTestSqlIndexValue.class).
                setSqlIndexMaxInlineSize(inlineSize));

        Map<Integer, Integer> expVals = new HashMap<>();

        final int KEYS = 100;
        final int VALS = 10;

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        long stopTime = System.currentTimeMillis() + 5_000;

        for (int i = 0; i < 100_000; i++) {
            Integer key = rnd.nextInt(KEYS);

            if (rnd.nextInt(5) == 0) {
                cache.remove(key);

                expVals.remove(key);
            }
            else {
                Integer val = rnd.nextInt(VALS);

                cache.put(key, new MvccTestSqlIndexValue(val));

                expVals.put(key, val);
            }

            checkValues(expVals, cache);

            if (System.currentTimeMillis() > stopTime) {
                info("Stop test, iteration: " + i);

                break;
            }
        }

        for (int i = 0; i < KEYS; i++) {
            if (!expVals.containsKey(i))
                checkNoValue(i, cache);
        }

        checkActiveQueriesCleanup(srv0);

        srv0.destroyCache(cache.getName());
    }

    /**
     * @param key Key.
     * @param cache Cache.
     */
    private void checkNoValue(Object key, IgniteCache cache) {
        SqlQuery<Integer, MvccTestSqlIndexValue> qry;

        qry = new SqlQuery<>(MvccTestSqlIndexValue.class, "_key = ?");

        qry.setArgs(key);

        List<IgniteCache.Entry<Integer, MvccTestSqlIndexValue>> res = cache.query(qry).getAll();

        assertTrue(res.isEmpty());
    }

    /**
     * @param expVals Expected values.
     * @param cache Cache.
     */
    private void checkValues(Map<Integer, Integer> expVals, IgniteCache<Integer, MvccTestSqlIndexValue> cache) {
        SqlFieldsQuery cntQry = new SqlFieldsQuery("select count(*) from MvccTestSqlIndexValue");

        Long cnt = (Long)cache.query(cntQry).getAll().get(0).get(0);

        assertEquals((long)expVals.size(), (Object)cnt);

        SqlQuery<Integer, MvccTestSqlIndexValue> qry;

        qry = new SqlQuery<>(MvccTestSqlIndexValue.class, "true");

        Map<Integer, Integer> vals = new HashMap<>();

        for (IgniteCache.Entry<Integer, MvccTestSqlIndexValue> e : cache.query(qry).getAll())
            assertNull(vals.put(e.getKey(), e.getValue().idxVal1));

        assertEquals(expVals, vals);

        qry = new SqlQuery<>(MvccTestSqlIndexValue.class, "_key >= 0");

        vals = new HashMap<>();

        for (IgniteCache.Entry<Integer, MvccTestSqlIndexValue> e : cache.query(qry).getAll())
            assertNull(vals.put(e.getKey(), e.getValue().idxVal1));

        assertEquals(expVals, vals);

        qry = new SqlQuery<>(MvccTestSqlIndexValue.class, "idxVal1 >= 0");

        vals = new HashMap<>();

        for (IgniteCache.Entry<Integer, MvccTestSqlIndexValue> e : cache.query(qry).getAll())
            assertNull(vals.put(e.getKey(), e.getValue().idxVal1));

        assertEquals(expVals, vals);

        Map<Integer, Set<Integer>> expIdxVals = new HashMap<>();

        for (Map.Entry<Integer, Integer> e : expVals.entrySet()) {
            qry = new SqlQuery<>(MvccTestSqlIndexValue.class, "_key = ?");

            qry.setArgs(e.getKey());

            List<IgniteCache.Entry<Integer, MvccTestSqlIndexValue>> res = cache.query(qry).getAll();

            assertEquals(1, res.size());
            assertEquals(e.getKey(), res.get(0).getKey());
            assertEquals(e.getValue(), (Integer)res.get(0).getValue().idxVal1);

            SqlFieldsQuery fieldsQry = new SqlFieldsQuery("select _key, idxVal1 from MvccTestSqlIndexValue where _key=?");
            fieldsQry.setArgs(e.getKey());

            List<List<?>> fieldsRes = cache.query(fieldsQry).getAll();

            assertEquals(1, fieldsRes.size());
            assertEquals(e.getKey(), fieldsRes.get(0).get(0));
            assertEquals(e.getValue(), fieldsRes.get(0).get(1));

            Integer val = e.getValue();

            Set<Integer> keys = expIdxVals.get(val);

            if (keys == null)
                expIdxVals.put(val, keys = new HashSet<>());

            assertTrue(keys.add(e.getKey()));
        }

        for (Map.Entry<Integer, Set<Integer>> expE : expIdxVals.entrySet()) {
            qry = new SqlQuery<>(MvccTestSqlIndexValue.class, "idxVal1 = ?");
            qry.setArgs(expE.getKey());

            vals = new HashMap<>();

            for (IgniteCache.Entry<Integer, MvccTestSqlIndexValue> e : cache.query(qry).getAll()) {
                assertNull(vals.put(e.getKey(), e.getValue().idxVal1));

                assertEquals(expE.getKey(), (Integer)e.getValue().idxVal1);

                assertTrue(expE.getValue().contains(e.getKey()));
            }

            assertEquals(expE.getValue().size(), vals.size());
        }
    }

    /**
     *
     */
    static class JoinTestParentKey implements Serializable {
        /** */
        private int key;

        /**
         * @param key Key.
         */
        JoinTestParentKey(int key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            JoinTestParentKey that = (JoinTestParentKey)o;

            return key == that.key;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key;
        }
    }

    /**
     *
     */
    static class JoinTestParent {
        /** */
        @QuerySqlField(index = true)
        private int id;

        /**
         * @param id ID.
         */
        JoinTestParent(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(JoinTestParent.class, this);
        }
    }

    /**
     *
     */
    static class JoinTestChildKey implements Serializable {
        /** */
        @QuerySqlField(index = true)
        private int key;

        /**
         * @param key Key.
         */
        JoinTestChildKey(int key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            JoinTestChildKey that = (JoinTestChildKey)o;

            return key == that.key;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key;
        }
    }

    /**
     *
     */
    static class JoinTestChild {
        /** */
        @QuerySqlField(index = true)
        private int parentId;

        /**
         * @param parentId Parent ID.
         */
        JoinTestChild(int parentId) {
            this.parentId = parentId;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(JoinTestChild.class, this);
        }
    }

    /**
     *
     */
    static class MvccTestSqlIndexValue implements Serializable {
        /** */
        @QuerySqlField(index = true)
        private int idxVal1;

        /**
         * @param idxVal1 Indexed value 1.
         */
        MvccTestSqlIndexValue(int idxVal1) {
            this.idxVal1 = idxVal1;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MvccTestSqlIndexValue.class, this);
        }
    }

    /**
     *
     */
    static class InitIndexing implements IgniteInClosure<CacheConfiguration> {
        /** */
        private final Class[] idxTypes;

        /**
         * @param idxTypes Indexed types.
         */
        InitIndexing(Class<?>... idxTypes) {
            this.idxTypes = idxTypes;
        }

        /** {@inheritDoc} */
        @Override public void apply(CacheConfiguration cfg) {
            cfg.setIndexedTypes(idxTypes);
        }
    }
}

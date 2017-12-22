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
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests for transactional SQL.
 */
public class CacheMvccSqlTxQueriesTest extends CacheMvccAbstractTest {
    /** */
    private static final int TIMEOUT = 3000;

    /**
     * @throws Exception If failed.
     */
    public void testQueryInsertStaticCache() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TIMEOUT);

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
    public void testQueryInsertStaticCacheImplicit() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)")
            .setTimeout(TIMEOUT, TimeUnit.MILLISECONDS);

        IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
            assertEquals(3L, cur.iterator().next().get(0));
        }

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryDeleteStaticCache() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
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
            tx.timeout(TIMEOUT);

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
    public void testQueryDeleteStaticCacheImplicit() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
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

        SqlFieldsQuery qry = new SqlFieldsQuery("DELETE FROM Integer WHERE 1 = 1")
            .setTimeout(TIMEOUT, TimeUnit.MILLISECONDS);

        IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
            assertEquals(3L, cur.iterator().next().get(0));
        }

        assertNull(cache.get(1));
        assertNull(cache.get(2));
        assertNull(cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryUpdateStaticCache() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
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
            tx.timeout(TIMEOUT);

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
    public void testQueryUpdateStaticCacheImplicit() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
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

        SqlFieldsQuery qry = new SqlFieldsQuery("UPDATE Integer SET _val = (_key * 10)")
            .setTimeout(TIMEOUT, TimeUnit.MILLISECONDS);

        IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
            assertEquals(3L, cur.iterator().next().get(0));
        }

        assertEquals(10, cache.get(1));
        assertEquals(20, cache.get(2));
        assertEquals(30, cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryDeadlock() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
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
                        tx.timeout(TIMEOUT);

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
    public void testQueryDeadlockImplicit() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(2);

        final Phaser phaser = new Phaser(2);
        final AtomicReference<Exception> ex = new AtomicReference<>();

        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                IgniteEx node = grid(0);

                try {
                    try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        IgniteCache<Object, Object> cache0 = node.cache(DEFAULT_CACHE_NAME);

                        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)");

                        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                            cur.getAll();
                        }

                        awaitPhase(phaser, 2);

                        tx.commit();
                    }
                }
                catch (Exception e) {
                    onException(ex, e);
                }
                finally {
                    phaser.arrive();
                }
            }
        });

        phaser.arriveAndAwaitAdvance();

        IgniteEx node = grid(1);

        IgniteCache<Object, Object> cache0 = node.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)")
            .setTimeout(TIMEOUT, TimeUnit.MILLISECONDS);

        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
            cur.getAll();
        }
        catch (Exception e) {
            phaser.arrive();

            onException(ex, e);
        }

        phaser.arriveAndAwaitAdvance();

        Exception ex0 = ex.get();

        assertNotNull(ex0);

        if (!X.hasCause(ex0, IgniteTxTimeoutCheckedException.class))
            throw ex0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryInsertClient() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGrid(0);

        client = true;

        startGrid(1);

        awaitPartitionMapExchange();

        Ignite checkNode  = grid(0);
        Ignite updateNode = grid(1);

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TIMEOUT);

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
    public void testQueryInsertClientImplicit() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGrid(0);

        client = true;

        startGrid(1);

        awaitPartitionMapExchange();

        Ignite checkNode  = grid(0);
        Ignite updateNode = grid(1);

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)")
            .setTimeout(TIMEOUT, TimeUnit.MILLISECONDS);

        IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
            assertEquals(3L, cur.iterator().next().get(0));
        }

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryInsertSubquery() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class, Integer.class, MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        awaitPartitionMapExchange();

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1, new MvccTestSqlIndexValue(1),
            2, new MvccTestSqlIndexValue(2),
            3, new MvccTestSqlIndexValue(3)));

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TIMEOUT);

            SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) SELECT _key * 10, idxVal1 FROM MvccTestSqlIndexValue");

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
    public void testQueryInsertSubqueryImplicit() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class, Integer.class, MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        awaitPartitionMapExchange();

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1, new MvccTestSqlIndexValue(1),
            2, new MvccTestSqlIndexValue(2),
            3, new MvccTestSqlIndexValue(3)));

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) SELECT _key * 10, idxVal1 FROM MvccTestSqlIndexValue")
            .setTimeout(TIMEOUT, TimeUnit.MILLISECONDS);

        IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
            assertEquals(3L, cur.iterator().next().get(0));
        }

        assertEquals(10, cache.get(1));
        assertEquals(20, cache.get(2));
        assertEquals(30, cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryUpdateSubquery() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class, Integer.class, MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        awaitPartitionMapExchange();

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1, new MvccTestSqlIndexValue(1),
            2, new MvccTestSqlIndexValue(2),
            3, new MvccTestSqlIndexValue(3)));

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TIMEOUT);

            SqlFieldsQuery qry = new SqlFieldsQuery("UPDATE MvccTestSqlIndexValue SET (idxVal1) = SELECT t.idxVal1 * 10 FROM MvccTestSqlIndexValue as t");

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            tx.commit();
        }

        assertEquals(10, ((MvccTestSqlIndexValue)cache.get(1)).idxVal1);
        assertEquals(20, ((MvccTestSqlIndexValue)cache.get(2)).idxVal1);
        assertEquals(30, ((MvccTestSqlIndexValue)cache.get(3)).idxVal1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryUpdateSubqueryImplicit() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class, Integer.class, MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        awaitPartitionMapExchange();

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1, new MvccTestSqlIndexValue(1),
            2, new MvccTestSqlIndexValue(2),
            3, new MvccTestSqlIndexValue(3)));


        SqlFieldsQuery qry = new SqlFieldsQuery("UPDATE MvccTestSqlIndexValue SET (idxVal1) = SELECT t.idxVal1 * 10 FROM MvccTestSqlIndexValue as t")
            .setTimeout(TIMEOUT, TimeUnit.MILLISECONDS);

        IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
            assertEquals(3L, cur.iterator().next().get(0));
        }

        assertEquals(10, ((MvccTestSqlIndexValue)cache.get(1)).idxVal1);
        assertEquals(20, ((MvccTestSqlIndexValue)cache.get(2)).idxVal1);
        assertEquals(30, ((MvccTestSqlIndexValue)cache.get(3)).idxVal1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryInsertMultithread() throws Exception {
        final int THREAD_CNT = 8;
        final int BATCH_SIZE = 1000;
        final int ROUNDS = 10;

        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
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
                        tx.timeout(TIMEOUT);

                        SqlFieldsQuery qry = new SqlFieldsQuery(bldr.toString()).setPageSize(100);

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
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
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
                        tx.timeout(TIMEOUT);

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
                        tx.timeout(TIMEOUT);

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
            fut.get(TIMEOUT);
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
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
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
                        tx.timeout(TIMEOUT);

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
                        tx.timeout(TIMEOUT);

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
            fut.get(TIMEOUT);
        }
        catch (IgniteCheckedException e) {
            onException(ex, e);
        }

        Exception ex0 = ex.get();

        assertNotNull(ex0);

        if(!"Failed to run update. Mvcc version mismatch.".equals(ex0.getMessage()))
            throw ex0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryInsertRollback() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TIMEOUT);

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
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        final Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        assertThrows(log(), new Callable<Void>() {
            @Override public Void call() {
                try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    tx.timeout(TIMEOUT);

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

                return null;
            }
        }, IgniteSQLException.class, "Failed to INSERT some keys because they are already in cache");

        for (int i = 1; i <= 6; i++)
            assertNull(cache.get(1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSelectProducesTransaction() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite node  = grid(rnd.nextInt(4));

        IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO MvccTestSqlIndexValue (_key, idxVal1) values (1,1),(2,2),(3,3)");

        try (FieldsQueryCursor<List<?>> cur = cache.query(qry)) {
            assertEquals(3L, cur.iterator().next().get(0));
        }

        SqlFieldsQueryEx qryEx = new SqlFieldsQueryEx("SELECT * FROM MvccTestSqlIndexValue", true);

        qryEx.setAutoCommit(false);

        try (FieldsQueryCursor<List<?>> cur = cache.query(qryEx)) {
            assertEquals(3, cur.getAll().size());
        }

        try (GridNearTxLocal tx = cache.unwrap(IgniteEx.class).context().cache().context().tm().userTx()) {
            assertNotNull(tx);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRepeatableRead() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        IgniteCache<Object, Object> cache = grid(rnd.nextInt(4)).cache(DEFAULT_CACHE_NAME);

        try (FieldsQueryCursor<List<?>> cur = cache.query(
            new SqlFieldsQuery("INSERT INTO MvccTestSqlIndexValue (_key, idxVal1) values (1,1),(2,2),(3,3)"))) {
            assertEquals(3L, cur.iterator().next().get(0));
        }

        Ignite node = grid(rnd.nextInt(4));
        IgniteCache<Object, Object> cache0 = node.cache(DEFAULT_CACHE_NAME);
        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM MvccTestSqlIndexValue");

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TIMEOUT);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3, cur.getAll().size());
            }

            runAsync(new Runnable() {
                @Override public void run() {
                    IgniteCache<Object, Object> cache = grid(ThreadLocalRandom.current().nextInt(4))
                        .cache(DEFAULT_CACHE_NAME);

                    try (FieldsQueryCursor<List<?>> cur = cache.query(
                        new SqlFieldsQuery("INSERT INTO MvccTestSqlIndexValue (_key, idxVal1) values (4,4),(5,5),(6,6)"))) {
                        assertEquals(3L, cur.iterator().next().get(0));
                    }
                }
            }).get(TIMEOUT);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3, cur.getAll().size());
            }
        }

        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
            assertEquals(6, cur.getAll().size());
        }
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
}

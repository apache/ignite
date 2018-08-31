/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreaded;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests for transactional SQL.
 */
public abstract class CacheMvccSqlTxQueriesWithReducerAbstractTest extends CacheMvccAbstractTest  {
    /** */
    private static final int TIMEOUT = 3000;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ccfgs = null;
        ccfg = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryReducerInsert() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache<Integer, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue> cache =
            checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1,new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1),
            2,new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(2),
            3,new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3)));

        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1), cache.get(1));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(2), cache.get(2));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3), cache.get(3));

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TIMEOUT);

            String sqlText = "INSERT INTO MvccTestSqlIndexValue (_key, idxVal1) " +
                "SELECT DISTINCT _key + 3, idxVal1 + 3 FROM MvccTestSqlIndexValue";

            SqlFieldsQuery qry = new SqlFieldsQuery(sqlText);

            qry.setDistributedJoins(true);

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            tx.commit();
        }

        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1), cache.get(1));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(2), cache.get(2));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3), cache.get(3));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(4), cache.get(4));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(5), cache.get(5));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(6), cache.get(6));
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryReducerInsertDuplicateKey() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache<Integer, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue> cache =
            checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1,new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1),
            2,new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(2),
            3,new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3)));

        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1), cache.get(1));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(2), cache.get(2));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3), cache.get(3));

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TIMEOUT);

            String sqlText = "INSERT INTO MvccTestSqlIndexValue (_key, idxVal1) " +
                "SELECT DISTINCT _key, idxVal1 FROM MvccTestSqlIndexValue";

            SqlFieldsQuery qry = new SqlFieldsQuery(sqlText);

            qry.setDistributedJoins(true);

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                @Override public Object call() {
                    return cache0.query(qry);
                }
            }, IgniteSQLException.class, "Duplicate key");

            tx.rollback();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryReducerMerge() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache<Integer, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue> cache =
            checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1,new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1),
            2,new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(2),
            3,new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3)));

        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1), cache.get(1));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(2), cache.get(2));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3), cache.get(3));

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TIMEOUT);

            String sqlText = "MERGE INTO MvccTestSqlIndexValue (_key, idxVal1) " +
                "SELECT DISTINCT _key * 2, idxVal1 FROM MvccTestSqlIndexValue";

            SqlFieldsQuery qry = new SqlFieldsQuery(sqlText);

            qry.setDistributedJoins(true);

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            tx.commit();
        }

        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1), cache.get(1));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1), cache.get(2));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3), cache.get(3));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(2), cache.get(4));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3), cache.get(6));
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryReducerMultiBatchPerNodeServer() throws Exception {
        checkMultiBatchPerNode(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryReducerMultiBatchPerNodeClient() throws Exception {
        checkMultiBatchPerNode(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkMultiBatchPerNode(boolean client) throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue.class);

        Ignite checkNode;
        Ignite updateNode;

        Random rnd = ThreadLocalRandom.current();

        if (client) {
            startGridsMultiThreaded(3);

            updateNode = grid(rnd.nextInt(3));

            this.client = true;

            checkNode = startGrid(4);
        }
        else {
            startGridsMultiThreaded(4);

            checkNode  = grid(rnd.nextInt(4));
            updateNode = grid(rnd.nextInt(4));
        }

        IgniteCache<Integer, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue> cache =
            checkNode.cache(DEFAULT_CACHE_NAME);

        final int count = 6;

        Map<Integer, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue> vals = new HashMap<>(count);

        for (int idx = 1; idx <= count; ++idx)
            vals.put(idx, new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(idx));

        cache.putAll(vals);

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TIMEOUT);

            String sqlText = "INSERT INTO MvccTestSqlIndexValue (_key, idxVal1) " +
                "SELECT DISTINCT _key + 6, idxVal1 + 6 FROM MvccTestSqlIndexValue";

            SqlFieldsQuery qry = new SqlFieldsQuery(sqlText);

            qry.setDistributedJoins(true);
            qry.setPageSize(1);

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals((long)count, cur.iterator().next().get(0));
            }

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryReducerDelete() throws Exception {
        ccfgs = new CacheConfiguration[] {
            cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
                .setName("int")
                .setIndexedTypes(Integer.class, Integer.class),
            cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
                .setIndexedTypes(Integer.class,
                CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue.class),
        };

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache<Integer, Integer> cache = checkNode.cache("int");

        cache.putAll(F.asMap(1, 1, 3, 3, 5, 5));

        final int count = 6;

        Map<Integer, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue> vals = new HashMap<>(count);

        for (int idx = 1; idx <= count; ++idx)
            vals.put(idx, new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(idx));

        IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

        cache0.putAll(vals);

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TIMEOUT);

            String sqlText = "DELETE FROM MvccTestSqlIndexValue t " +
                "WHERE EXISTS (SELECT 1 FROM \"int\".Integer WHERE t._key = _key)";

            SqlFieldsQuery qry = new SqlFieldsQuery(sqlText);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryReducerUpdate() throws Exception {
        ccfgs = new CacheConfiguration[] {
            cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
                .setName("int")
                .setIndexedTypes(Integer.class, Integer.class),
            cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
                .setIndexedTypes(Integer.class,
                CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue.class),
        };

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache<Integer, Integer> cache = checkNode.cache("int");

        cache.putAll(F.asMap(1, 5, 3, 1, 5, 3));

        final int count = 6;

        Map<Integer, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue> vals = new HashMap<>(count);

        for (int idx = 1; idx <= count; ++idx)
            vals.put(idx, new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(idx));

        IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

        cache0.putAll(vals);

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TIMEOUT);

            String sqlText = "UPDATE MvccTestSqlIndexValue t SET idxVal1=" +
                "(SELECT _val FROM \"int\".Integer WHERE t._key = _key)" +
                " WHERE EXISTS (SELECT 1 FROM \"int\".Integer WHERE t._key = _key)";

            SqlFieldsQuery qry = new SqlFieldsQuery(sqlText);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryReducerImplicitTxInsert() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache<Integer, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue> cache =
            checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1,new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1),
            2,new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(2),
            3,new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3)));

        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1), cache.get(1));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(2), cache.get(2));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3), cache.get(3));

        String sqlText = "INSERT INTO MvccTestSqlIndexValue (_key, idxVal1) " +
                "SELECT DISTINCT _key + 3, idxVal1 + 3 FROM MvccTestSqlIndexValue";

        SqlFieldsQuery qry = new SqlFieldsQuery(sqlText);

        qry.setTimeout(TX_TIMEOUT, TimeUnit.MILLISECONDS);

        qry.setDistributedJoins(true);

        IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
            assertEquals(3L, cur.iterator().next().get(0));
        }

        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1), cache.get(1));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(2), cache.get(2));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3), cache.get(3));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(4), cache.get(4));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(5), cache.get(5));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(6), cache.get(6));
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryReducerRollbackInsert() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache<Integer, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue> cache =
            checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1,new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1),
            2,new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(2),
            3,new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3)));

        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1), cache.get(1));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(2), cache.get(2));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3), cache.get(3));

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TIMEOUT);

            String sqlText = "INSERT INTO MvccTestSqlIndexValue (_key, idxVal1) " +
                "SELECT DISTINCT _key + 3, idxVal1 + 3 FROM MvccTestSqlIndexValue";

            SqlFieldsQuery qry = new SqlFieldsQuery(sqlText);

            qry.setDistributedJoins(true);

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            tx.rollback();
        }

        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1), sqlGet(1, cache).get(0).get(0));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(2), sqlGet(2, cache).get(0).get(0));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3), sqlGet(3, cache).get(0).get(0));
        assertTrue(sqlGet(4, cache).isEmpty());
        assertTrue(sqlGet(5, cache).isEmpty());
        assertTrue(sqlGet(6, cache).isEmpty());
    }

    /**
     * @param key Key.
     * @param cache Cache.
     * @return Result.
     */
    private List<List> sqlGet(int key, IgniteCache cache) {
        return cache.query(new SqlFieldsQuery("SELECT _val from MvccTestSqlIndexValue WHERE _key=" + key)).getAll();
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryReducerDeadlockInsert() throws Exception {
        ccfgs = new CacheConfiguration[] {
            cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
                .setName("int")
                .setIndexedTypes(Integer.class, Integer.class),
            cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
                .setIndexedTypes(Integer.class,
                CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue.class),
        };

        startGridsMultiThreaded(2);

        client = true;

        startGridsMultiThreaded(2, 2);

        Ignite checkNode  = grid(2);

        IgniteCache<Integer, Integer> cache = checkNode.cache("int");

        HashMap<Integer, Integer> vals = new HashMap<>(100);

        for (int idx = 0; idx < 100; ++idx)
            vals.put(idx, idx);

        cache.putAll(vals);

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicInteger idx = new AtomicInteger(2);
        final AtomicReference<Exception> ex = new AtomicReference<>();

        multithreaded(new Runnable() {
            @Override public void run() {
                int id = idx.getAndIncrement();

                IgniteEx node = grid(id);

                try {
                    try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        tx.timeout(TIMEOUT);

                        String sqlText = "INSERT INTO MvccTestSqlIndexValue (_key, idxVal1) " +
                            "SELECT DISTINCT _key, _val FROM \"int\".Integer ORDER BY _key";

                        String sqlAsc = sqlText + " ASC";
                        String sqlDesc = sqlText + " DESC";

                        SqlFieldsQuery qry = new SqlFieldsQuery((id % 2) == 0 ? sqlAsc : sqlDesc);

                        IgniteCache<Object, Object> cache0 = node.cache(DEFAULT_CACHE_NAME);

                        cache0.query(qry).getAll();

                        barrier.await();

                        qry = new SqlFieldsQuery((id % 2) == 0 ? sqlDesc : sqlAsc);

                        cache0.query(qry).getAll();

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
    public void testQueryReducerInsertVersionConflict() throws Exception {
        ccfgs = new CacheConfiguration[] {
            cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
                .setName("int")
                .setIndexedTypes(Integer.class, Integer.class),
            cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
                .setIndexedTypes(Integer.class,
                CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue.class),
        };

        startGridsMultiThreaded(2);

        client = true;

        final Ignite checkNode  = startGrid(2);

        IgniteCache<Integer, Integer> cache = checkNode.cache("int");

        HashMap<Integer, Integer> vals = new HashMap<>(100);

        for (int idx = 0; idx < 10; ++idx)
            vals.put(idx, idx);

        cache.putAll(vals);

        awaitPartitionMapExchange();

        IgniteCache cache0 = checkNode.cache(DEFAULT_CACHE_NAME);

        cache0.query(new SqlFieldsQuery("INSERT INTO MvccTestSqlIndexValue (_key, idxVal1) " +
            "SELECT _key, _val FROM \"int\".Integer")).getAll();

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicReference<Exception> ex = new AtomicReference<>();

        runMultiThreaded(new Runnable() {
            @Override public void run() {
                try {
                    try (Transaction tx = checkNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        tx.timeout(TX_TIMEOUT);

                        barrier.await();

                        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM MvccTestSqlIndexValue");

                        cache0.query(qry).getAll();

                        barrier.await();

                        String sqlText = "UPDATE MvccTestSqlIndexValue t SET idxVal1=" +
                            "(SELECT _val FROM \"int\".Integer WHERE _key >= 5 AND _key <= 5 ORDER BY _key) WHERE _key = 5";

                        qry = new SqlFieldsQuery(sqlText);

                        cache0.query(qry).getAll();

                        tx.commit();
                    }
                }
                catch (Exception e) {
                    onException(ex, e);
                }
            }
        }, 2, "tx-thread");

        IgniteSQLException ex0 = X.cause(ex.get(), IgniteSQLException.class);

        assertNotNull("Exception has not been thrown.", ex0);
        assertEquals("Mvcc version mismatch.", ex0.getMessage());
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryReducerInsertValues() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite node  = grid(rnd.nextInt(4));

        IgniteCache<Object, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue> cache = node.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO MvccTestSqlIndexValue (_key, idxVal1)" +
            " values (1,?),(2,?),(3,?)");

        qry.setArgs(1, 2, 3);

        try (FieldsQueryCursor<List<?>> cur = cache.query(qry)) {
            assertEquals(3L, cur.iterator().next().get(0));
        }

        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1), cache.get(1));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(2), cache.get(2));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3), cache.get(3));

        qry = new SqlFieldsQuery("INSERT INTO MvccTestSqlIndexValue (_key, idxVal1) values (4,4)");

        try (FieldsQueryCursor<List<?>> cur = cache.query(qry)) {
            assertEquals(1L, cur.iterator().next().get(0));
        }

        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(4), cache.get(4));
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryReducerMergeValues() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite node  = grid(rnd.nextInt(4));

        IgniteCache<Object, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue> cache = node.cache(DEFAULT_CACHE_NAME);

        cache.put(1, new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1));
        cache.put(3, new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3));

        SqlFieldsQuery qry = new SqlFieldsQuery("MERGE INTO MvccTestSqlIndexValue (_key, idxVal1)" +
            " values (1,?),(2,?),(3,?)");

        qry.setArgs(1, 4, 6);

        try (FieldsQueryCursor<List<?>> cur = cache.query(qry)) {
            assertEquals(3L, cur.iterator().next().get(0));
        }

        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1), cache.get(1));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(4), cache.get(2));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(6), cache.get(3));

        qry = new SqlFieldsQuery("MERGE INTO MvccTestSqlIndexValue (_key, idxVal1) values (4,4)");

        try (FieldsQueryCursor<List<?>> cur = cache.query(qry)) {
            assertEquals(1L, cur.iterator().next().get(0));
        }

        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(4), cache.get(4));
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryReducerFastUpdate() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache<Object, Object> cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(1,1,2,2,3,3));

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));

        IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("UPDATE Integer SET _val = 8 WHERE _key = ?").setArgs(1);

        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
             assertEquals(1L, cur.iterator().next().get(0));
        }

        qry = new SqlFieldsQuery("UPDATE Integer SET _val = 9 WHERE _key = 2");

        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
            assertEquals(1L, cur.iterator().next().get(0));
        }

        assertEquals(8, cache.get(1));
        assertEquals(9, cache.get(2));
        assertEquals(3, cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryReducerFastDelete() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache<Object, Object> cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1,new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1),
            2,new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(2),
            3,new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3)));

        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(1), cache.get(1));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(2), cache.get(2));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3), cache.get(3));

        IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("DELETE FROM MvccTestSqlIndexValue WHERE _key = ?")
            .setArgs(1);

        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
            assertEquals(1L, cur.iterator().next().get(0));
        }

        qry = new SqlFieldsQuery("DELETE FROM MvccTestSqlIndexValue WHERE _key = 2");

        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
            assertEquals(1L, cur.iterator().next().get(0));
        }

        assertNull(cache.get(1));
        assertNull(cache.get(2));
        assertEquals(new CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue(3), cache.get(3));
    }

    /**
     * @param ex Exception holder.
     * @param e Exception.
     */
    private void onException(AtomicReference<Exception> ex, Exception e) {
        if (!ex.compareAndSet(null, e))
            ex.get().addSuppressed(e);
    }
}

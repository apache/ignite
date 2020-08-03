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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionDuplicateKeyException;
import org.apache.ignite.transactions.TransactionSerializationException;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SQL;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SQL_SUM;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.WriteMode.DML;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreaded;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests for transactional SQL.
 */
public abstract class CacheMvccSqlTxQueriesAbstractTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setTransactionConfiguration(new TransactionConfiguration().setDeadlockTimeout(0));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSql_SingleNode_SinglePartition() throws Exception {
        accountsTxReadAll(1, 0, 0, 1,
            new InitIndexing(Integer.class, MvccTestAccount.class), false, SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSql_WithRemoves_SingleNode_SinglePartition() throws Exception {
        accountsTxReadAll(1, 0, 0, 1,
            new InitIndexing(Integer.class, MvccTestAccount.class), true, SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSql_SingleNode() throws Exception {
        accountsTxReadAll(1, 0, 0, 64,
            new InitIndexing(Integer.class, MvccTestAccount.class), false, SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSql_SingleNode_Persistence() throws Exception {
        persistence = true;

        testAccountsTxDmlSql_SingleNode();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSumSql_SingleNode() throws Exception {
        accountsTxReadAll(1, 0, 0, 64,
            new InitIndexing(Integer.class, MvccTestAccount.class), false, SQL_SUM, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSumSql_WithRemoves_SingleNode() throws Exception {
        accountsTxReadAll(1, 0, 0, 64,
            new InitIndexing(Integer.class, MvccTestAccount.class), true, SQL_SUM, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSumSql_WithRemoves__ClientServer_Backups0() throws Exception {
        accountsTxReadAll(4, 2, 0, 64,
            new InitIndexing(Integer.class, MvccTestAccount.class), true, SQL_SUM, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSumSql_ClientServer_Backups2() throws Exception {
        accountsTxReadAll(4, 2, 2, 64,
            new InitIndexing(Integer.class, MvccTestAccount.class), true, SQL_SUM, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSql_WithRemoves_SingleNode() throws Exception {
        accountsTxReadAll(1, 0, 0, 64,
            new InitIndexing(Integer.class, MvccTestAccount.class), true, SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSql_WithRemoves_SingleNode_Persistence() throws Exception {
        persistence = true;

        testAccountsTxDmlSql_WithRemoves_SingleNode();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSql_ClientServer_Backups0() throws Exception {
        accountsTxReadAll(4, 2, 0, 64,
            new InitIndexing(Integer.class, MvccTestAccount.class), false, SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSql_WithRemoves_ClientServer_Backups0() throws Exception {
        accountsTxReadAll(4, 2, 0, 64,
            new InitIndexing(Integer.class, MvccTestAccount.class), true, SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSql_WithRemoves_ClientServer_Backups0_Persistence() throws Exception {
        persistence = true;

        testAccountsTxDmlSql_WithRemoves_ClientServer_Backups0();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSql_ClientServer_Backups1() throws Exception {
        accountsTxReadAll(3, 0, 1, 64,
            new InitIndexing(Integer.class, MvccTestAccount.class), false, SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSql_WithRemoves_ClientServer_Backups1() throws Exception {
        accountsTxReadAll(4, 2, 1, 64,
            new InitIndexing(Integer.class, MvccTestAccount.class), true, SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSql_WithRemoves_ClientServer_Backups1_Persistence() throws Exception {
        persistence = true;

        testAccountsTxDmlSql_WithRemoves_ClientServer_Backups1();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSql_ClientServer_Backups2() throws Exception {
        accountsTxReadAll(4, 2, 2, 64,
            new InitIndexing(Integer.class, MvccTestAccount.class), false, SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSql_WithRemoves_ClientServer_Backups2() throws Exception {
        accountsTxReadAll(4, 2, 2, 64,
            new InitIndexing(Integer.class, MvccTestAccount.class), true, SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxDmlSql_ClientServer_Backups2_Persistence() throws Exception {
        persistence = true;

        testAccountsTxDmlSql_ClientServer_Backups2();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testParsingErrorHasNoSideEffect() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 0, 4)
            .setIndexedTypes(Integer.class, Integer.class);

        IgniteEx node = startGrid(0);

        IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1),(2,2),(3,3)");

            try {
                try (FieldsQueryCursor<List<?>> cur = cache.query(qry)) {
                    fail("We should not get there.");
                }
            }
            catch (CacheException ex) {
                IgniteSQLException cause = X.cause(ex, IgniteSQLException.class);

                assertNotNull(cause);
                assertEquals(IgniteQueryErrorCode.PARSING, cause.statusCode());

                assertFalse(tx.isRollbackOnly());
            }

            qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (4,4),(5,5),(6,6)");

            try (FieldsQueryCursor<List<?>> cur = cache.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            tx.commit();
        }

        assertNull(cache.get(1));
        assertNull(cache.get(2));
        assertNull(cache.get(3));
        assertEquals(4, cache.get(4));
        assertEquals(5, cache.get(5));
        assertEquals(6, cache.get(6));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryInsertStaticCache() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

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
    @Test
    public void testQueryInsertStaticCacheImplicit() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)")
            .setTimeout(TX_TIMEOUT, TimeUnit.MILLISECONDS);

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
    @Test
    public void testQueryDeleteStaticCache() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)")
            .setTimeout(TX_TIMEOUT, TimeUnit.MILLISECONDS);

        IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
            assertEquals(3L, cur.iterator().next().get(0));
        }

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            qry = new SqlFieldsQuery("DELETE FROM Integer WHERE 1 = 1");

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
    @Test
    public void testQueryFastDeleteStaticCache() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)")
            .setTimeout(TX_TIMEOUT, TimeUnit.MILLISECONDS);

        IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
            assertEquals(3L, cur.iterator().next().get(0));
        }
        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            qry = new SqlFieldsQuery("DELETE FROM Integer WHERE _key = 1");

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(1L, cur.iterator().next().get(0));
            }

            tx.commit();
        }

        assertNull(cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryFastUpdateStaticCache() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)")
            .setTimeout(TX_TIMEOUT, TimeUnit.MILLISECONDS);

        IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
            assertEquals(3L, cur.iterator().next().get(0));
        }
        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            qry = new SqlFieldsQuery("UPDATE Integer SET _val = 8 WHERE _key = 1");

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(1L, cur.iterator().next().get(0));
            }

            tx.commit();
        }

        assertEquals(8, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryFastDeleteObjectStaticCache() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1, new MvccTestSqlIndexValue(1),
            2, new MvccTestSqlIndexValue(2),
            3, new MvccTestSqlIndexValue(3)));

        assertEquals(new MvccTestSqlIndexValue(1), cache.get(1));
        assertEquals(new MvccTestSqlIndexValue(2), cache.get(2));
        assertEquals(new MvccTestSqlIndexValue(3), cache.get(3));

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            SqlFieldsQuery qry = new SqlFieldsQuery("DELETE FROM MvccTestSqlIndexValue WHERE _key = 1");

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(1L, cur.iterator().next().get(0));
            }

            tx.commit();
        }

        assertNull(cache.get(1));
        assertEquals(new MvccTestSqlIndexValue(2), cache.get(2));
        assertEquals(new MvccTestSqlIndexValue(3), cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryFastUpdateObjectStaticCache() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1, new MvccTestSqlIndexValue(1),
            2, new MvccTestSqlIndexValue(2),
            3, new MvccTestSqlIndexValue(3)));

        assertEquals(new MvccTestSqlIndexValue(1), cache.get(1));
        assertEquals(new MvccTestSqlIndexValue(2), cache.get(2));
        assertEquals(new MvccTestSqlIndexValue(3), cache.get(3));

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            SqlFieldsQuery qry = new SqlFieldsQuery("UPDATE MvccTestSqlIndexValue SET idxVal1 = 8 WHERE _key = 1");

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(1L, cur.iterator().next().get(0));
            }

            tx.commit();
        }

        assertEquals(new MvccTestSqlIndexValue(8), cache.get(1));
        assertEquals(new MvccTestSqlIndexValue(2), cache.get(2));
        assertEquals(new MvccTestSqlIndexValue(3), cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryDeleteStaticCacheImplicit() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(1, 1, 2, 2, 3, 3));

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));

        SqlFieldsQuery qry = new SqlFieldsQuery("DELETE FROM Integer WHERE 1 = 1")
            .setTimeout(TX_TIMEOUT, TimeUnit.MILLISECONDS);

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
    @Test
    public void testQueryUpdateStaticCache() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(1, 1, 2, 2, 3, 3));

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

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
    @Test
    public void testQueryUpdateStaticCacheImplicit() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(1, 1, 2, 2, 3, 3));

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));

        SqlFieldsQuery qry = new SqlFieldsQuery("UPDATE Integer SET _val = (_key * 10)")
            .setTimeout(TX_TIMEOUT, TimeUnit.MILLISECONDS);

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
    @Test
    public void testQueryDeadlockWithTxTimeout() throws Exception {
        checkQueryDeadlock(TimeoutMode.TX);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryDeadlockWithStmtTimeout() throws Exception {
        checkQueryDeadlock(TimeoutMode.STMT);
    }

    /** */
    private enum TimeoutMode {
        /** */
        TX,
        /** */
        STMT
    }

    /** */
    private void checkQueryDeadlock(TimeoutMode timeoutMode) throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
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
                        if (timeoutMode == TimeoutMode.TX)
                            tx.timeout(TX_TIMEOUT);

                        IgniteCache<Object, Object> cache0 = node.cache(DEFAULT_CACHE_NAME);

                        String qry1 = "INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)";
                        String qry2 = "INSERT INTO Integer (_key, _val) values (4,4),(5,5),(6,6)";

                        SqlFieldsQuery qry = new SqlFieldsQuery((id % 2) == 0 ? qry1 : qry2);

                        if (timeoutMode == TimeoutMode.STMT)
                            qry.setTimeout(TX_TIMEOUT, TimeUnit.MILLISECONDS);

                        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                            cur.getAll();
                        }

                        barrier.await();

                        qry = new SqlFieldsQuery((id % 2) == 0 ? qry2 : qry1);

                        if (timeoutMode == TimeoutMode.STMT)
                            qry.setTimeout(TX_TIMEOUT, TimeUnit.MILLISECONDS);

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
    @Test
    public void testQueryDeadlockImplicit() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 0, DFLT_PARTITION_COUNT)
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
            .setTimeout(TX_TIMEOUT, TimeUnit.MILLISECONDS);

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
    @Test
    public void testQueryInsertClient() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGrid(0);

        client = true;

        startGrid(1);

        awaitPartitionMapExchange();

        Ignite checkNode = grid(0);
        Ignite updateNode = grid(1);

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

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
    @Test
    public void testQueryInsertClientImplicit() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGrid(0);

        client = true;

        startGrid(1);

        awaitPartitionMapExchange();

        Ignite checkNode = grid(0);
        Ignite updateNode = grid(1);

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)")
            .setTimeout(TX_TIMEOUT, TimeUnit.MILLISECONDS);

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
    @Test
    public void testQueryInsertSubquery() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class, Integer.class, MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        awaitPartitionMapExchange();

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1, new MvccTestSqlIndexValue(1),
            2, new MvccTestSqlIndexValue(2),
            3, new MvccTestSqlIndexValue(3)));

        IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val)" +
                " SELECT _key * 10, idxVal1 FROM MvccTestSqlIndexValue");

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            tx.commit();
        }

        assertEquals(1, cache0.get(10));
        assertEquals(2, cache0.get(20));
        assertEquals(3, cache0.get(30));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryInsertSubqueryImplicit() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class, Integer.class, MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        awaitPartitionMapExchange();

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1, new MvccTestSqlIndexValue(1),
            2, new MvccTestSqlIndexValue(2),
            3, new MvccTestSqlIndexValue(3)));

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val)" +
            " SELECT _key * 10, idxVal1 FROM MvccTestSqlIndexValue")
            .setTimeout(TX_TIMEOUT, TimeUnit.MILLISECONDS);

        IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
            assertEquals(3L, cur.iterator().next().get(0));
        }

        assertEquals(1, cache0.get(10));
        assertEquals(2, cache0.get(20));
        assertEquals(3, cache0.get(30));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryUpdateSubquery() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class, Integer.class, MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        awaitPartitionMapExchange();

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1, new MvccTestSqlIndexValue(1),
            2, new MvccTestSqlIndexValue(2),
            3, new MvccTestSqlIndexValue(3)));

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            SqlFieldsQuery qry = new SqlFieldsQuery("UPDATE MvccTestSqlIndexValue AS t " +
                "SET (idxVal1) = (SELECT idxVal1*10 FROM MvccTestSqlIndexValue WHERE t._key = _key)");

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
    @Test
    public void testQueryUpdateSubqueryImplicit() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class, Integer.class, MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        awaitPartitionMapExchange();

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache<Object, Object> cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1, new MvccTestSqlIndexValue(1),
            2, new MvccTestSqlIndexValue(2),
            3, new MvccTestSqlIndexValue(3)));

        SqlFieldsQuery qry = new SqlFieldsQuery("UPDATE MvccTestSqlIndexValue AS t " +
            "SET (idxVal1) = (SELECT idxVal1*10 FROM MvccTestSqlIndexValue WHERE t._key = _key)")
            .setTimeout(TX_TIMEOUT, TimeUnit.MILLISECONDS);

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
    @Test
    public void testQueryInsertMultithread() throws Exception {
        // Reopen https://issues.apache.org/jira/browse/IGNITE-10764 if test starts failing with timeout
        final int THREAD_CNT = 8;
        final int BATCH_SIZE = 1000;
        final int ROUNDS = 10;

        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
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

                    IgniteCache<Object, Object> cache = checkNode.cache(DEFAULT_CACHE_NAME);

                    // no tx timeout here, deadlocks should not happen because all keys are unique
                    try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
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
    @Test
    public void testQueryInsertUpdateMultithread() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(2);

        final Phaser phaser = new Phaser(2);
        final AtomicReference<Exception> ex = new AtomicReference<>();

        GridCompoundFuture fut = new GridCompoundFuture();

        fut.add(multithreadedAsync(new Runnable() {
            @Override public void run() {
                IgniteEx node = grid(0);

                try {
                    while (true) {
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

                            break;
                        }
                        catch (CacheException e) {
                            MvccFeatureChecker.assertMvccWriteConflict(e);
                        }
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

                    while (true) {
                        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            tx.timeout(TX_TIMEOUT);

                            IgniteCache<Integer, Integer> cache0 = node.cache(DEFAULT_CACHE_NAME);

                            cache0.invokeAllAsync(F.asSet(1, 2, 3, 4, 5, 6), new EntryProcessor<Integer, Integer, Void>() {
                                @Override public Void process(MutableEntry<Integer, Integer> entry,
                                    Object... arguments) throws EntryProcessorException {
                                    entry.setValue(entry.getValue() * 10);

                                    return null;
                                }
                            });

                            phaser.arrive();

                            tx.commit();

                            break;
                        }
                        catch (Exception e) {
                            assertTrue(e instanceof TransactionSerializationException);
                        }
                    }
                }
                catch (Exception e) {
                    onException(ex, e);
                }
            }
        }, 1));

        try {
            fut.markInitialized();

            fut.get(TX_TIMEOUT);
        }
        catch (IgniteCheckedException e) {
            onException(ex, e);
        }
        finally {
            phaser.forceTermination();
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
    @Test
    public void testQueryInsertVersionConflict() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(2);

        IgniteCache cache = grid(0).cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1)");

        try (FieldsQueryCursor<List<?>> cur = cache.query(qry)) {
            assertEquals(1L, cur.iterator().next().get(0));
        }

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicReference<Exception> ex = new AtomicReference<>();

        runMultiThreaded(new Runnable() {
            @Override public void run() {
                IgniteEx node = grid(0);

                try {
                    try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        tx.timeout(TX_TIMEOUT);

                        barrier.await(TX_TIMEOUT, TimeUnit.MILLISECONDS);

                        IgniteCache<Object, Object> cache0 = node.cache(DEFAULT_CACHE_NAME);

                        SqlFieldsQuery qry;

                        synchronized (barrier) {
                            qry = new SqlFieldsQuery("SELECT * FROM Integer");

                            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                                assertEquals(1, cur.getAll().size());
                            }
                        }

                        barrier.await(TX_TIMEOUT, TimeUnit.MILLISECONDS);

                        qry = new SqlFieldsQuery("UPDATE Integer SET _val = (_key * 10)");

                        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                            assertEquals(1L, cur.iterator().next().get(0));
                        }

                        tx.commit();
                    }
                }
                catch (Exception e) {
                    onException(ex, e);
                }
            }
        }, 2, "tx-thread");

        MvccFeatureChecker.assertMvccWriteConflict(ex.get());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInsertAndFastDeleteWithoutVersionConflict() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(2);

        IgniteCache<?, ?> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);

        try (Transaction tx1 = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            // obtain tx version
            cache0.query(new SqlFieldsQuery("select * from Integer where _key = 1"));

            runAsync(() -> {
                cache0.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, ?)").setArgs(1, 1));
            }).get();

            cache0.query(new SqlFieldsQuery("delete from Integer where _key = ?").setArgs(1));

            tx1.commit();
        }
        catch (Exception e) {
            e.printStackTrace();

            fail("Exception is not expected here");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInsertAndFastUpdateWithoutVersionConflict() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(2);

        IgniteCache<?, ?> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);

        try (Transaction tx1 = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            // obtain tx version
            cache0.query(new SqlFieldsQuery("select * from Integer where _key = 1"));

            runAsync(() -> {
                cache0.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, ?)").setArgs(1, 1));
            }).get();

            cache0.query(new SqlFieldsQuery("update Integer set _val = ? where _key = ?").setArgs(1, 1));

            tx1.commit();
        }
        catch (Exception e) {
            e.printStackTrace();

            fail("Exception is not expected here");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInsertFastUpdateConcurrent() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(2);

        IgniteCache<?, ?> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);

        try {
            for (int i = 0; i < 100; i++) {
                int key = i;
                CompletableFuture.allOf(
                    CompletableFuture.runAsync(() -> {
                        cache0.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, ?)").setArgs(key, key));
                    }),
                    CompletableFuture.runAsync(() -> {
                        cache0.query(new SqlFieldsQuery("update Integer set _val = ? where _key = ?").setArgs(key, key));
                    })
                ).get();
            }
        }
        catch (Exception e) {
            e.printStackTrace();

            fail("Exception is not expected here");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryInsertRollback() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

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
            assertTrue(cache.query(new SqlFieldsQuery("SELECT * FROM Integer WHERE _key = 1")).getAll().isEmpty());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryInsertUpdateSameKeys() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode = grid(rnd.nextInt(4));
        final Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)");

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            qry = new SqlFieldsQuery("UPDATE Integer SET _val = (_key * 10)");

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                cur.getAll();
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
    @Test
    public void testQueryInsertUpdateSameKeysInSameOperation() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        final Ignite updateNode = grid(rnd.nextInt(4));

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    tx.timeout(TX_TIMEOUT);

                    SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1),(1,2),(1,3)");

                    IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

                    cache0.query(qry).getAll();

                    tx.commit();
                }

                return null;
            }
        }, TransactionDuplicateKeyException.class, "Duplicate key during INSERT [key=KeyCacheObjectImpl");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryPendingUpdates() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode = grid(rnd.nextInt(4));
        final Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)");

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry.setSql("UPDATE Integer SET _val = (_key * 10)"))) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            for (List<?> row : cache0.query(qry.setSql("SELECT _key, _val FROM Integer")).getAll()) {
                assertEquals((Integer)row.get(0) * 10, row.get(1));
            }

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry.setSql("UPDATE Integer SET _val = 15 where _key = 2"))) {
                assertEquals(1L, cur.iterator().next().get(0));
            }

            for (List<?> row : cache0.query(qry.setSql("SELECT _key, _val FROM Integer")).getAll()) {
                if ((Integer)row.get(0) == 2)
                    assertEquals(15, row.get(1));
                else
                    assertEquals((Integer)row.get(0) * 10, row.get(1));
            }

            GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    SqlFieldsQuery qry = new SqlFieldsQuery("SELECT _key, _val FROM Integer");

                    assertTrue(cache.query(qry).getAll().isEmpty());
                }
            }).get(TX_TIMEOUT);

            cache0.query(qry.setSql("DELETE FROM Integer")).getAll();

            assertTrue(cache0.query(qry.setSql("SELECT _key, _val FROM Integer")).getAll().isEmpty());

            assertEquals(3L, cache0.query(qry.setSql("INSERT INTO Integer (_key, _val) values (1,1),(2,2),(3,3)")).getAll().iterator().next().get(0));

            tx.commit();
        }

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSelectProducesTransaction() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite node = grid(rnd.nextInt(4));

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
    @Test
    public void testRepeatableRead() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
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
            tx.timeout(TX_TIMEOUT);

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
            }).get(TX_TIMEOUT);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3, cur.getAll().size());
            }
        }

        try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
            assertEquals(6, cur.getAll().size());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateExplicitPartitionsWithoutReducer() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, 10)
            .setIndexedTypes(Integer.class, Integer.class);

        Ignite ignite = startGridsMultiThreaded(4);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        Affinity<Object> affinity = internalCache0(cache).affinity();

        int keysCnt = 10, retryCnt = 0;

        Integer test = 0;

        Map<Integer, Integer> vals = new LinkedHashMap<>();

        while (vals.size() < keysCnt) {
            int partition = affinity.partition(test);

            if (partition == 1 || partition == 2)
                vals.put(test, 0);
            else
                assertTrue("Maximum retry number exceeded", ++retryCnt < 1000);

            test++;
        }

        cache.putAll(vals);

        SqlFieldsQuery qry = new SqlFieldsQuery("UPDATE Integer set _val=2").setPartitions(1,2);

        List<List<?>> all = cache.query(qry).getAll();

        assertEquals(Long.valueOf(keysCnt), all.stream().findFirst().orElseThrow(AssertionError::new).get(0));

        List<List<?>> rows = cache.query(new SqlFieldsQuery("SELECT _val FROM Integer")).getAll();

        assertEquals(keysCnt, rows.size());
        assertTrue(rows.stream().map(r -> r.get(0)).map(Integer.class::cast).allMatch(v -> v == 2));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateExplicitPartitionsWithReducer() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, 10)
            .setIndexedTypes(Integer.class, Integer.class);

        Ignite ignite = startGridsMultiThreaded(4);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        Affinity<Object> affinity = internalCache0(cache).affinity();

        int keysCnt = 10, retryCnt = 0;

        Integer test = 0;

        Map<Integer, Integer> vals = new LinkedHashMap<>();

        while (vals.size() < keysCnt) {
            int partition = affinity.partition(test);

            if (partition == 1 || partition == 2)
                vals.put(test, 0);
            else
                assertTrue("Maximum retry number exceeded", ++retryCnt < 1000);

            test++;
        }

        cache.putAll(vals);

        SqlFieldsQuery qry = new SqlFieldsQuery("UPDATE Integer set _val=(SELECT 2 FROM DUAL)").setPartitions(1,2);

        List<List<?>> all = cache.query(qry).getAll();

        assertEquals(Long.valueOf(keysCnt), all.stream().findFirst().orElseThrow(AssertionError::new).get(0));

        List<List<?>> rows = cache.query(new SqlFieldsQuery("SELECT _val FROM Integer")).getAll();

        assertEquals(keysCnt, rows.size());
        assertTrue(rows.stream().map(r -> r.get(0)).map(Integer.class::cast).allMatch(v -> v == 2));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFastInsertUpdateConcurrent() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        Ignite ignite = startGridsMultiThreaded(4);

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 1000; i++) {
            int key = i;
            CompletableFuture.allOf(
                CompletableFuture.runAsync(() -> {
                    cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, ?)").setArgs(key, key));
                }),
                CompletableFuture.runAsync(() -> {
                    cache.query(new SqlFieldsQuery("update Integer set _val = ? where _key = ?").setArgs(key, key));
                })
            ).join();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIterator() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        startGrid(getConfiguration("grid").setMvccVacuumFrequency(Integer.MAX_VALUE));

        Ignite client = startClientGrid(getConfiguration("client"));

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);
        cache.put(4, 4);

        List<List<?>> res;

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            res = cache.query(new SqlFieldsQuery("UPDATE Integer SET _val = CASE _key " +
                "WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE 30 END")).getAll();

            assertEquals(4L, res.get(0).get(0));

            tx.rollback();
        }

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            res = cache.query(new SqlFieldsQuery("UPDATE Integer SET _val = CASE _val " +
                "WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE 30 END")).getAll();

            assertEquals(4L, res.get(0).get(0));

            res = cache.query(new SqlFieldsQuery("UPDATE Integer SET _val = CASE _val " +
                "WHEN 10 THEN 100 WHEN 20 THEN 200 ELSE 300 END")).getAll();

            assertEquals(4L, res.get(0).get(0));

            res = cache.query(new SqlFieldsQuery("DELETE FROM Integer WHERE _key = 4")).getAll();

            assertEquals(1L, res.get(0).get(0));

            tx.commit();
        }

        IgniteCache<Integer, Integer> cache0 = client.cache(DEFAULT_CACHE_NAME);

        Iterator<Cache.Entry<Integer, Integer>> it = cache0.iterator();

        Map<Integer, Integer> map = new HashMap<>();

        while (it.hasNext()) {
            Cache.Entry<Integer, Integer> e = it.next();

            assertNull("duplicate key returned from iterator", map.putIfAbsent(e.getKey(), e.getValue()));
        }

        assertEquals(3, map.size());

        assertEquals(100, map.get(1).intValue());
        assertEquals(200, map.get(2).intValue());
        assertEquals(300, map.get(3).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHints() throws Exception {
        persistence = true;

        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        Ignite node = startGrid(getConfiguration("grid").setMvccVacuumFrequency(100));

        node.cluster().active(true);

        Ignite client = startClientGrid(getConfiguration("client"));

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        List<List<?>> res;

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            res = cache.query(new SqlFieldsQuery("INSERT INTO Integer (_key, _val) " +
                "VALUES (1, 1), (2, 2), (3, 3), (4, 4)")).getAll();

            assertEquals(4L, res.get(0).get(0));

            tx.commit();
        }

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            res = cache.query(new SqlFieldsQuery("UPDATE Integer SET _val = CASE _key " +
                "WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE 30 END")).getAll();

            assertEquals(4L, res.get(0).get(0));

            tx.rollback();
        }

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            res = cache.query(new SqlFieldsQuery("UPDATE Integer SET _val = CASE _val " +
                "WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE 30 END")).getAll();

            assertEquals(4L, res.get(0).get(0));

            res = cache.query(new SqlFieldsQuery("UPDATE Integer SET _val = CASE _val " +
                "WHEN 10 THEN 100 WHEN 20 THEN 200 ELSE 300 END")).getAll();

            assertEquals(4L, res.get(0).get(0));

            res = cache.query(new SqlFieldsQuery("DELETE FROM Integer WHERE _key = 4")).getAll();

            assertEquals(1L, res.get(0).get(0));

            tx.commit();
        }

        mvccProcessor(node).runVacuum().get(TX_TIMEOUT);

        checkAllVersionsHints(node.cache(DEFAULT_CACHE_NAME));
    }

    /** */
    private void checkAllVersionsHints(IgniteCache cache) throws IgniteCheckedException {
        IgniteCacheProxy cache0 = (IgniteCacheProxy)cache;
        GridCacheContext cctx = cache0.context();

        assert cctx.mvccEnabled();

        for (Object e : cache) {
            IgniteBiTuple entry = (IgniteBiTuple)e;

            KeyCacheObject key = cctx.toCacheKeyObject(entry.getKey());

            GridCursor<CacheDataRow> cur = cctx.offheap().mvccAllVersionsCursor(cctx, key, CacheDataRowAdapter.RowData.LINK_WITH_HEADER);

            while (cur.next()) {
                CacheDataRow row = cur.get();

                assertTrue(row.mvccTxState() != 0);
            }
        }
    }

    /**
     * @param ex Exception holder.
     * @param e Exception.
     */
    private <T extends Throwable> void onException(AtomicReference<T> ex, T e) {
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
        }
        while (p < phase && p >= 0 /* check termination */ );
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
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            MvccTestSqlIndexValue value = (MvccTestSqlIndexValue)o;
            return idxVal1 == value.idxVal1;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(idxVal1);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MvccTestSqlIndexValue.class, this);
        }
    }
}

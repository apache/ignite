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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.GET;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SQL;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.WriteMode.DML;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.WriteMode.PUT;

/**
 * Test basic mvcc bulk cache operations.
 */
public class MvccRepeatableReadBulkOpsTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** */
    private int nodesCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startGridsMultiThreaded(nodesCount() - 1);

        client = true;

        startGrid(nodesCount() - 1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        grid(0).createCache(cacheConfiguration(cacheMode(), FULL_SYNC, 1, 32).
            setIndexedTypes(Integer.class, MvccTestAccount.class));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).destroyCache(DEFAULT_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRepeatableReadIsolationGetPut() throws Exception {
        checkOperations(GET, GET, PUT, true);
        checkOperations(GET, GET, PUT, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRepeatableReadIsolationSqlPut() throws Exception {
        checkOperations(SQL, SQL, PUT, true);
        checkOperations(SQL, SQL, PUT, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRepeatableReadIsolationSqlDml() throws Exception {
        checkOperations(SQL, SQL, DML, true);
        checkOperations(SQL, SQL, DML, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRepeatableReadIsolationGetDml() throws Exception {
        checkOperations(GET, GET, DML, true);
        checkOperations(GET, GET, DML, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRepeatableReadIsolationMixedPut() throws Exception {
        checkOperations(SQL, GET, PUT, false);
        checkOperations(SQL, GET, PUT, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRepeatableReadIsolationMixedPut2() throws Exception {
        checkOperations(GET, SQL, PUT, false);
        checkOperations(GET, SQL, PUT, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRepeatableReadIsolationMixedDml() throws Exception {
        checkOperations(SQL, GET, DML, false);
        checkOperations(SQL, GET, DML, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRepeatableReadIsolationMixedDml2() throws Exception {
        checkOperations(GET, SQL, DML, false);
        checkOperations(GET, SQL, DML, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOperationConsistency() throws Exception {
        checkOperationsConsistency(false);
        checkOperationsConsistency(true);
    }

    /**
     * Checks SQL and CacheAPI operation isolation consistency.
     *
     * @param readModeBefore read mode used before value updated.
     * @param readModeBefore read mode used after value updated.
     * @param writeMode write mode used for update.
     * @throws Exception If failed.
     */
    private void checkOperations(ReadMode readModeBefore, ReadMode readModeAfter,
        WriteMode writeMode, boolean requestFromClient) throws Exception {
        Ignite node1 = grid(requestFromClient ? nodesCount() - 1 : 0);
        Ignite node2 = grid(requestFromClient ? 0 : nodesCount() - 1);

        TestCache<Integer, MvccTestAccount> cache1 = new TestCache<>(node1.cache(DEFAULT_CACHE_NAME));
        TestCache<Integer, MvccTestAccount> cache2 = new TestCache<>(node2.cache(DEFAULT_CACHE_NAME));

        final Set<Integer> keys = new HashSet<>();

        {
            keys.add(primaryKey(grid(0).cache(DEFAULT_CACHE_NAME)));
            keys.add(backupKey(grid(0).cache(DEFAULT_CACHE_NAME)));
            keys.add(nearKey(grid(0).cache(DEFAULT_CACHE_NAME)));
        }

        final Map<Integer, MvccTestAccount> initialVals = keys.stream().collect(
            Collectors.toMap(k -> k, k -> new MvccTestAccount(k, 1)));

        cache1.cache.putAll(initialVals);

        IgniteTransactions txs1 = node1.transactions();
        IgniteTransactions txs2 = node2.transactions();

        CountDownLatch updateStart = new CountDownLatch(1);
        CountDownLatch updateFinish = new CountDownLatch(1);

        IgniteInternalFuture<Void> updater = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                updateStart.await();

                try (Transaction tx = txs2.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                    Map<Integer, MvccTestAccount> batch = keys.stream().collect(Collectors.toMap(Function.identity(),
                        k -> new MvccTestAccount(k, 2)));

                    updateEntries(cache2, batch, writeMode);

                    tx.commit();
                }

                updateFinish.countDown();

                return null;
            }
        });

        IgniteInternalFuture<Void> reader = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                try (Transaction tx = txs1.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                    assertEquals(initialVals, getEntries(cache1, keys, readModeBefore));

                    updateStart.countDown();
                    updateFinish.await();

                    assertEquals(initialVals, getEntries(cache1, keys, readModeAfter));

                    tx.commit();
                }

                return null;
            }
        });

        try {
            updater.get(3_000, TimeUnit.MILLISECONDS);
            reader.get(3_000, TimeUnit.MILLISECONDS);
        }
        catch (Throwable e) {
            throw new AssertionError(e);
        }
        finally {
            updateStart.countDown();
            updateFinish.countDown();
        }

        Map<Integer, MvccTestAccount> updatedVals = keys.stream().collect(Collectors.toMap(k -> k, k -> new MvccTestAccount(k, 2)));

        assertEquals(updatedVals, cache1.cache.getAll(keys));
    }

    /**
     * Checks SQL and CacheAPI operation see consistent results before and after update.
     *
     * @throws Exception If failed.
     */
    private void checkOperationsConsistency(boolean requestFromClient) throws Exception {
        Ignite node = grid(requestFromClient ? nodesCount() - 1 : 0);

        TestCache<Integer, MvccTestAccount> cache = new TestCache<>(node.cache(DEFAULT_CACHE_NAME));

        final Set<Integer> keys = new HashSet<>();

        {
            keys.add(primaryKey(grid(0).cache(DEFAULT_CACHE_NAME)));
            keys.add(backupKey(grid(0).cache(DEFAULT_CACHE_NAME)));
            keys.add(nearKey(grid(0).cache(DEFAULT_CACHE_NAME)));
        }

        int updCnt = 1;

        final Map<Integer, MvccTestAccount> initialVals = keys.stream().collect(
            Collectors.toMap(k -> k, k -> new MvccTestAccount(k, 1)));

        cache.cache.putAll(initialVals);

        IgniteTransactions txs = node.transactions();

        Map<Integer, MvccTestAccount> updatedVals = null;

        try (Transaction tx = txs.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            Map<Integer, MvccTestAccount> vals1 = getEntries(cache, keys, GET);
            Map<Integer, MvccTestAccount> vals2 = getEntries(cache, keys, SQL);

            assertEquals(initialVals, vals1);
            assertEquals(initialVals, vals2);

            for (ReadMode readMode : new ReadMode[] {GET, SQL}) {
                for (WriteMode writeMode : new WriteMode[] {PUT, DML}) {

                    int updCnt0 = updCnt++;

                    updatedVals = keys.stream().collect(Collectors.toMap(Function.identity(),
                        k -> new MvccTestAccount(k, updCnt0)));

                    updateEntries(cache, updatedVals, writeMode);

                    //TODO: IGNITE-7764: Add remove operation checks.

                    assertEquals(updatedVals, getEntries(cache, keys, readMode));
                }
            }

            tx.commit();
        }

        try (Transaction tx = txs.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            assertEquals(updatedVals, getEntries(cache, keys, GET));
            assertEquals(updatedVals, getEntries(cache, keys, SQL));

            tx.commit();
        }
    }

    /**
     * Gets values with given read mode.
     *
     * @param cache Cache.
     * @param keys Key to be read.
     * @param readMode Read mode.
     * @return Key-value result map.
     */
    protected Map<Integer, MvccTestAccount> getEntries(
        TestCache<Integer, MvccTestAccount> cache,
        Set<Integer> keys,
        ReadMode readMode) {
        switch (readMode) {
            case GET:
                return cache.cache.getAll(keys);
            case SQL:
                return getAllSql(cache);
            default:
                fail();
        }

        return null;
    }

    /**
     * Updates entries with given write mode.
     *
     * @param cache Cache.
     * @param entries Entries to be updated.
     * @param writeMode Write mode.
     */
    protected void updateEntries(
        TestCache<Integer, MvccTestAccount> cache,
        Map<Integer, MvccTestAccount> entries,
        WriteMode writeMode) {
        switch (writeMode) {
            case PUT: {
                cache.cache.putAll(entries);

                break;
            }
            case DML: {
                for (Map.Entry<Integer, MvccTestAccount> e : entries.entrySet())
                    mergeSql(cache, e.getKey(), e.getValue().val, e.getValue().updateCnt);

                break;
            }
            default:
                fail();
        }
    }

    /**
     * Updates entries with given write mode.
     *
     * @param cache Cache.
     * @param keys Key to be deleted.
     * @param writeMode Write mode.
     */
    protected void removeEntries(
        TestCache<Integer, MvccTestAccount> cache,
        Set<Integer> keys,
        WriteMode writeMode) {
        switch (writeMode) {
            case PUT: {
                cache.cache.removeAll(keys);

                break;
            }
            case DML: {
                for (Integer key : keys)
                    removeSql(cache, key);

                break;
            }
            default:
                fail();
        }
    }
}

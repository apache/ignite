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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.GET;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.INVOKE;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SQL;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.WriteMode.DML;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.WriteMode.PUT;

/**
 * Test basic mvcc bulk cache operations.
 */
@RunWith(JUnit4.class)
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
    @Test
    public void testRepeatableReadIsolationGetPut() throws Exception {
        checkOperations(GET, GET, PUT, true);
        checkOperations(GET, GET, PUT, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRepeatableReadIsolationInvoke() throws Exception {
        checkOperations(GET, GET, WriteMode.INVOKE, true);
        checkOperations(GET, GET, WriteMode.INVOKE, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRepeatableReadIsolationSqlPut() throws Exception {
        checkOperations(SQL, SQL, PUT, true);
        checkOperations(SQL, SQL, PUT, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRepeatableReadIsolationSqlInvoke() throws Exception {
        checkOperations(SQL, SQL, WriteMode.INVOKE, true);
        checkOperations(SQL, SQL, WriteMode.INVOKE, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRepeatableReadIsolationSqlDml() throws Exception {
        checkOperations(SQL, SQL, DML, true);
        checkOperations(SQL, SQL, DML, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRepeatableReadIsolationGetDml() throws Exception {
        checkOperations(GET, GET, DML, true);
        checkOperations(GET, GET, DML, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRepeatableReadIsolationMixedPut() throws Exception {
        checkOperations(SQL, GET, PUT, false);
        checkOperations(SQL, GET, PUT, true);
        checkOperations(SQL, GET, WriteMode.INVOKE, false);
        checkOperations(SQL, GET, WriteMode.INVOKE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRepeatableReadIsolationMixedPut2() throws Exception {
        checkOperations(GET, SQL, PUT, false);
        checkOperations(GET, SQL, PUT, true);
        checkOperations(GET, SQL, WriteMode.INVOKE, false);
        checkOperations(GET, SQL, WriteMode.INVOKE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRepeatableReadIsolationMixedDml() throws Exception {
        checkOperations(SQL, GET, DML, false);
        checkOperations(SQL, GET, DML, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRepeatableReadIsolationMixedDml2() throws Exception {
        checkOperations(GET, SQL, DML, false);
        checkOperations(GET, SQL, DML, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOperationConsistency() throws Exception {
        checkOperationsConsistency(PUT, false);
        checkOperationsConsistency(DML, false);
        checkOperationsConsistency(WriteMode.INVOKE, false);
        checkOperationsConsistency(PUT, true);
        checkOperationsConsistency(DML, true);
        checkOperationsConsistency(WriteMode.INVOKE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeConsistency() throws Exception {
        Ignite node = grid(/*requestFromClient ? nodesCount() - 1 :*/ 0);

        TestCache<Integer, MvccTestAccount> cache = new TestCache<>(node.cache(DEFAULT_CACHE_NAME));

        final Set<Integer> keys1 = new HashSet<>(3);
        final Set<Integer> keys2 = new HashSet<>(3);

        Set<Integer> allKeys = generateKeySet(cache.cache, keys1, keys2);

        final Map<Integer, MvccTestAccount> map1 = keys1.stream().collect(
            Collectors.toMap(k -> k, k -> new MvccTestAccount(k, 1)));

        final Map<Integer, MvccTestAccount> map2 = keys2.stream().collect(
            Collectors.toMap(k -> k, k -> new MvccTestAccount(k, 1)));

        assertEquals(0, cache.cache.size());

        updateEntries(cache, map1, WriteMode.INVOKE);
        assertEquals(3, cache.cache.size());

        updateEntries(cache, map1, WriteMode.INVOKE);
        assertEquals(3, cache.cache.size());

        getEntries(cache, allKeys, INVOKE);
        assertEquals(3, cache.cache.size());

        updateEntries(cache, map2, WriteMode.INVOKE);
        assertEquals(6, cache.cache.size());

        getEntries(cache, keys2, INVOKE);
        assertEquals(6, cache.cache.size());

        removeEntries(cache, keys1, WriteMode.INVOKE);
        assertEquals(3, cache.cache.size());

        removeEntries(cache, keys1, WriteMode.INVOKE);
        assertEquals(3, cache.cache.size());

        getEntries(cache, allKeys, INVOKE);
        assertEquals(3, cache.cache.size());

        updateEntries(cache, map1, WriteMode.INVOKE);
        assertEquals(6, cache.cache.size());

        removeEntries(cache, allKeys, WriteMode.INVOKE);
        assertEquals(0, cache.cache.size());

        getEntries(cache, allKeys, INVOKE);
        assertEquals(0, cache.cache.size());
    }

    /**
     * Checks SQL and CacheAPI operation isolation consistency.
     *
     * @param readModeBefore read mode used before value updated.
     * @param readModeAfter read mode used after value updated.
     * @param writeMode write mode used for update.
     * @throws Exception If failed.
     */
    private void checkOperations(ReadMode readModeBefore, ReadMode readModeAfter,
        WriteMode writeMode, boolean readFromClient) throws Exception {
        Ignite node1 = grid(readFromClient ? nodesCount() - 1 : 0);
        Ignite node2 = grid(readFromClient ? 0 : nodesCount() - 1);

        TestCache<Integer, MvccTestAccount> cache1 = new TestCache<>(node1.cache(DEFAULT_CACHE_NAME));
        TestCache<Integer, MvccTestAccount> cache2 = new TestCache<>(node2.cache(DEFAULT_CACHE_NAME));

        final Set<Integer> keysForUpdate = new HashSet<>(3);
        final Set<Integer> keysForRemove = new HashSet<>(3);

        final Set<Integer> allKeys = generateKeySet(grid(0).cache(DEFAULT_CACHE_NAME), keysForUpdate, keysForRemove);

        final Map<Integer, MvccTestAccount> initialMap = allKeys.stream().collect(
            Collectors.toMap(k -> k, k -> new MvccTestAccount(k, 1)));

        final Map<Integer, MvccTestAccount> updateMap = keysForUpdate.stream().collect(Collectors.toMap(Function.identity(),
            k -> new MvccTestAccount(k, 2))); /* Removed keys are excluded. */

        cache1.cache.putAll(initialMap);

        IgniteTransactions txs1 = node1.transactions();
        IgniteTransactions txs2 = node2.transactions();

        CountDownLatch updateStart = new CountDownLatch(1);
        CountDownLatch updateFinish = new CountDownLatch(1);

        // Start concurrent transactions and check isolation.
        IgniteInternalFuture<Void> updater = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                updateStart.await();

                assertEquals(initialMap.size(), cache2.cache.size());

                try (Transaction tx = txs2.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                    tx.timeout(TX_TIMEOUT);

                    updateEntries(cache2, updateMap, writeMode);
                    removeEntries(cache2, keysForRemove, writeMode);

                    assertEquals(updateMap, cache2.cache.getAll(allKeys));

                    tx.commit();
                }
                finally {
                    updateFinish.countDown();
                }

                assertEquals(updateMap.size(), cache2.cache.size());

                return null;
            }
        });

        IgniteInternalFuture<Void> reader = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                try (Transaction tx = txs1.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                    assertEquals(initialMap, getEntries(cache1, allKeys, readModeBefore));

                    checkContains(cache1, true, allKeys);

                    updateStart.countDown();
                    updateFinish.await();

                    assertEquals(initialMap, getEntries(cache1, allKeys, readModeAfter));

                    checkContains(cache1, true,allKeys);

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

        assertEquals(updateMap, cache1.cache.getAll(allKeys));
    }

    /**
     * Generate 2 sets of keys. Each set contains primary, backup and non-affinity key for given node cache.
     *
     * @param cache Cache.
     * @param keySet1 Key set.
     * @param keySet2 Key set.
     * @return All keys.
     * @throws IgniteCheckedException If failed.
     */
    protected Set<Integer> generateKeySet(IgniteCache<?, ?> cache, Set<Integer> keySet1,
        Set<Integer> keySet2) throws IgniteCheckedException {
        LinkedHashSet<Integer> allKeys = new LinkedHashSet<>();

        allKeys.addAll(primaryKeys(cache, 2));
        allKeys.addAll(backupKeys(cache, 2, 1));
        allKeys.addAll(nearKeys(cache, 2, 1));

        List<Integer> keys0 = new ArrayList<>(allKeys);

        for (int i = 0; i < 6; i++) {
            if (i % 2 == 0)
                keySet1.add(keys0.get(i));
            else
                keySet2.add(keys0.get(i));
        }

        assert allKeys.size() == 6; // Expects no duplicates.

        return allKeys;
    }

    /**
     * Checks SQL and CacheAPI operation see consistent results before and after update.
     *
     * @throws Exception If failed.
     */
    private void checkOperationsConsistency(WriteMode writeMode, boolean requestFromClient) throws Exception {
        Ignite node = grid(requestFromClient ? nodesCount() - 1 : 0);

        TestCache<Integer, MvccTestAccount> cache = new TestCache<>(node.cache(DEFAULT_CACHE_NAME));


            final Set<Integer> keysForUpdate = new HashSet<>(3);
            final Set<Integer> keysForRemove = new HashSet<>(3);

            final Set<Integer> allKeys = generateKeySet(grid(0).cache(DEFAULT_CACHE_NAME), keysForUpdate, keysForRemove);

        try {
            int updCnt = 1;

            final Map<Integer, MvccTestAccount> initialVals = allKeys.stream().collect(
                Collectors.toMap(k -> k, k -> new MvccTestAccount(k, 1)));

            updateEntries(cache, initialVals, writeMode);

            assertEquals(initialVals.size(), cache.cache.size());

            IgniteTransactions txs = node.transactions();

            Map<Integer, MvccTestAccount> updatedVals = null;

            try (Transaction tx = txs.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                Map<Integer, MvccTestAccount> vals1 = getEntries(cache, allKeys, GET);
                Map<Integer, MvccTestAccount> vals2 = getEntries(cache, allKeys, SQL);
                Map<Integer, MvccTestAccount> vals3 = getEntries(cache, allKeys, ReadMode.INVOKE);

                assertEquals(initialVals, vals1);
                assertEquals(initialVals, vals2);
                assertEquals(initialVals, vals3);

                assertEquals(initialVals.size(), cache.cache.size());

                for (ReadMode readMode : new ReadMode[] {GET, SQL, INVOKE}) {
                    int updCnt0 = ++updCnt;

                    updatedVals = allKeys.stream().collect(Collectors.toMap(Function.identity(),
                        k -> new MvccTestAccount(k, updCnt0)));

                    updateEntries(cache, updatedVals, writeMode);
                    assertEquals(allKeys.size(), cache.cache.size());

                    removeEntries(cache, keysForRemove, writeMode);

                    for (Integer key : keysForRemove)
                        updatedVals.remove(key);

                    assertEquals(String.valueOf(readMode), updatedVals, getEntries(cache, allKeys, readMode));
                }

                tx.commit();
            }

            try (Transaction tx = txs.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                assertEquals(updatedVals, getEntries(cache, allKeys, GET));
                assertEquals(updatedVals, getEntries(cache, allKeys, SQL));
                assertEquals(updatedVals, getEntries(cache, allKeys, INVOKE));

                tx.commit();
            }

            assertEquals(updatedVals.size(), cache.cache.size());
        }
        finally {
            cache.cache.removeAll(keysForUpdate);
        }

        assertEquals(0, cache.cache.size());
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
            case INVOKE: {
                Map<Integer, MvccTestAccount> res = new HashMap<>();

                CacheEntryProcessor<Integer, MvccTestAccount, MvccTestAccount> ep = new GetEntryProcessor<>();

                Map<Integer, EntryProcessorResult<MvccTestAccount>> invokeRes = cache.cache.invokeAll(keys, ep);

                for (Map.Entry<Integer, EntryProcessorResult<MvccTestAccount>> e : invokeRes.entrySet())
                    res.put(e.getKey(), e.getValue().get());

                return res;
            }
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
            case INVOKE: {
                CacheEntryProcessor<Integer, MvccTestAccount, MvccTestAccount> ep =
                    new GetAndPutEntryProcessor<Integer, MvccTestAccount>(){
                    /** {@inheritDoc} */
                    @Override MvccTestAccount newValForKey(Integer key) {
                        return entries.get(key);
                    }
                };

                cache.cache.invokeAll(entries.keySet(), ep);

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
            case INVOKE: {
                CacheEntryProcessor<Integer, MvccTestAccount, MvccTestAccount> ep = new RemoveEntryProcessor<>();

                cache.cache.invokeAll(keys, ep);

                break;
            }
            default:
                fail();
        }
    }

    /**
     * Check cache contains entries.
     *
     * @param cache Cache.
     * @param expected Expected result.
     * @param keys Keys to check.
     */
    protected void checkContains(TestCache<Integer, MvccTestAccount> cache, boolean expected, Set<Integer> keys) {
        assertEquals(expected, cache.cache.containsKeys(keys));
    }

    /**
     * Applies get operation.
     */
    static class GetEntryProcessor<K, V> implements CacheEntryProcessor<K, V, V> {
        /** {@inheritDoc} */
        @Override public V process(MutableEntry<K, V> entry,
            Object... arguments) throws EntryProcessorException {
            return entry.getValue();
        }
    }

    /**
     * Applies remove operation.
     */
    static class RemoveEntryProcessor<K, V, R> implements CacheEntryProcessor<K, V, R> {
        /** {@inheritDoc} */
        @Override public R process(MutableEntry<K, V> entry,
            Object... arguments) throws EntryProcessorException {
            entry.remove();

            return null;
        }
    }

    /**
     * Applies get and put operation.
     */
    static class GetAndPutEntryProcessor<K, V> implements CacheEntryProcessor<K, V, V> {
        /** {@inheritDoc} */
        @Override public V process(MutableEntry<K, V> entry,
            Object... args) throws EntryProcessorException {
            V newVal = !F.isEmpty(args) ? (V)args[0] : newValForKey(entry.getKey());

            V oldVal = entry.getValue();
            entry.setValue(newVal);

            return oldVal;
        }

        /**
         * @param key Key.
         * @return New value.
         */
        V newValForKey(K key){
            throw new UnsupportedOperationException();
        }
    }
}

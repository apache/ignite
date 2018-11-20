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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.GET;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SQL;

/**
 * Test basic mvcc cache operation operations.
 */
public class MvccRepeatableReadOperationsTest extends MvccRepeatableReadBulkOpsTest {
    /** {@inheritDoc} */
    @Override protected Map<Integer, MvccTestAccount> getEntries(
        TestCache<Integer, MvccTestAccount> cache,
        Set<Integer> keys,
        ReadMode readMode) {

        switch (readMode) {
            case GET: {
                Map<Integer, MvccTestAccount> res = new HashMap<>();

                for (Integer key : keys) {
                    MvccTestAccount val = cache.cache.get(key);

                    if(val != null)
                        res.put(key, val);
                }

                return res;
            }

            case SQL:
                return getAllSql(cache);

            case INVOKE: {
                Map<Integer, MvccTestAccount> res = new HashMap<>();

                CacheEntryProcessor<Integer, MvccTestAccount, MvccTestAccount> ep = new GetEntryProcessor();

                for (Integer key : keys) {
                    MvccTestAccount val = cache.cache.invoke(key, ep);

                    if(val != null)
                        res.put(key, val);
                }

                return res;
            }
            default:
                fail();
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override protected void updateEntries(
        TestCache<Integer, MvccTestAccount> cache,
        Map<Integer, MvccTestAccount> entries,
        WriteMode writeMode) {
        switch (writeMode) {
            case PUT: {
                for (Map.Entry<Integer, MvccTestAccount> e : entries.entrySet())
                    if (e.getValue() == null)
                        cache.cache.remove(e.getKey());
                    else
                        cache.cache.put(e.getKey(), e.getValue());

                break;
            }

            case DML: {
                for (Map.Entry<Integer, MvccTestAccount> e : entries.entrySet()) {
                    if (e.getValue() == null)
                        removeSql(cache, e.getKey());
                    else
                        mergeSql(cache, e.getKey(), e.getValue().val, e.getValue().updateCnt);
                }
                break;
            }

            case INVOKE: {
                GetAndPutEntryProcessor<Integer, MvccTestAccount> ep = new GetAndPutEntryProcessor<>();

                for (final Map.Entry<Integer, MvccTestAccount> e : entries.entrySet())
                    cache.cache.invoke(e.getKey(), ep, e.getValue());

                break;
            }

            default:
                fail();
        }
    }

    /** {@inheritDoc} */
    @Override protected void removeEntries(
        TestCache<Integer, MvccTestAccount> cache,
        Set<Integer> keys,
        WriteMode writeMode) {
        switch (writeMode) {
            case PUT: {
                for (Integer key : keys)
                    cache.cache.remove(key);

                break;
            }
            case DML: {
                for (Integer key : keys)
                    removeSql(cache, key);

                break;
            }
            case INVOKE: {
                CacheEntryProcessor<Integer, MvccTestAccount, MvccTestAccount> ep = new RemoveEntryProcessor<>();

                for (Integer key : keys)
                    cache.cache.invoke(key, ep);

                break;
            }
            default:
                fail();
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkContains(TestCache<Integer, MvccTestAccount> cache, boolean expected,
        Set<Integer> keys) {
        for (Integer key : keys)
            assertEquals(expected, cache.cache.containsKey(key));
    }

    /**
     * Check getAndPut/getAndRemove operations consistency.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void testGetAndUpdateOperations() throws IgniteCheckedException {
        Ignite node1 = grid(0);

        TestCache<Integer, MvccTestAccount> cache1 = new TestCache<>(node1.cache(DEFAULT_CACHE_NAME));

        final Set<Integer> keysForUpdate = new HashSet<>(3);
        final Set<Integer> keysForRemove = new HashSet<>(3);

        final Set<Integer> allKeys = generateKeySet(grid(0).cache(DEFAULT_CACHE_NAME), keysForUpdate, keysForRemove);

        final Map<Integer, MvccTestAccount> initialMap = keysForRemove.stream().collect(
            Collectors.toMap(k -> k, k -> new MvccTestAccount(k, 1)));

        Map<Integer, MvccTestAccount> updateMap = keysForUpdate.stream().collect(
            Collectors.toMap(k -> k, k -> new MvccTestAccount(k, 3)));

        cache1.cache.putAll(initialMap);

        IgniteTransactions txs = node1.transactions();
        try (Transaction tx = txs.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            for (Integer key : keysForUpdate) {
                MvccTestAccount newVal1 = new MvccTestAccount(key, 1);

                assertNull(cache1.cache.getAndPut(key, newVal1)); // Check create.

                MvccTestAccount newVal2 = new MvccTestAccount(key, 2);

                assertEquals(newVal1, cache1.cache.getAndPut(key, newVal2)); // Check update.
            }

            for (Integer key : keysForRemove) {
                assertEquals(initialMap.get(key), cache1.cache.getAndRemove(key)); // Check remove existed.

                assertNull(cache1.cache.getAndRemove(key)); // Check remove non-existed.
            }

            for (Integer key : allKeys) {
                MvccTestAccount oldVal = new MvccTestAccount(key, 2);
                MvccTestAccount newVal = new MvccTestAccount(key, 3);

                if (keysForRemove.contains(key))
                    assertNull(cache1.cache.getAndReplace(key, newVal)); // Omit update 'null'.
                else
                    assertEquals(oldVal, cache1.cache.getAndReplace(key, newVal)); // Check updated.
            }

            assertEquals(updateMap, getEntries(cache1, allKeys, SQL));
            assertEquals(updateMap, getEntries(cache1, allKeys, GET));

            tx.commit();
        }

        assertEquals(updateMap, getEntries(cache1, allKeys, SQL));
        assertEquals(updateMap, getEntries(cache1, allKeys, GET));
    }

    /**
     * Check getAndPut/getAndRemove operations consistency.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void testPutIfAbsentConsistency() throws IgniteCheckedException {
        Ignite node1 = grid(0);

        TestCache<Integer, MvccTestAccount> cache1 = new TestCache<>(node1.cache(DEFAULT_CACHE_NAME));

        final Set<Integer> keysForCreate = new HashSet<>(3);
        final Set<Integer> keysForUpdate = new HashSet<>(3);

        final Set<Integer> allKeys = generateKeySet(grid(0).cache(DEFAULT_CACHE_NAME), keysForCreate, keysForUpdate);

        final Map<Integer, MvccTestAccount> initialMap = keysForUpdate.stream().collect(
            Collectors.toMap(k -> k, k -> new MvccTestAccount(k, 1)));

        Map<Integer, MvccTestAccount> updatedMap = allKeys.stream().collect(
            Collectors.toMap(k -> k, k -> new MvccTestAccount(k, 1)));

        cache1.cache.putAll(initialMap);

        IgniteTransactions txs = node1.transactions();
        try (Transaction tx = txs.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            for (Integer key : keysForUpdate)
                assertFalse(cache1.cache.putIfAbsent(key, new MvccTestAccount(key, 2))); // Check update.

            for (Integer key : keysForCreate)
                assertTrue(cache1.cache.putIfAbsent(key, new MvccTestAccount(key, 1))); // Check create.

            assertEquals(updatedMap, getEntries(cache1, allKeys, SQL));

            tx.commit();
        }

        assertEquals(updatedMap, getEntries(cache1, allKeys, SQL));
        assertEquals(updatedMap, getEntries(cache1, allKeys, GET));
    }

    /**
     * Check getAndPut/getAndRemove operations consistency.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void testReplaceConsistency() throws IgniteCheckedException {
        Ignite node1 = grid(0);

        TestCache<Integer, MvccTestAccount> cache1 = new TestCache<>(node1.cache(DEFAULT_CACHE_NAME));

        final Set<Integer> existedKeys = new HashSet<>(3);
        final Set<Integer> nonExistedKeys = new HashSet<>(3);

        final Set<Integer> allKeys = generateKeySet(grid(0).cache(DEFAULT_CACHE_NAME), existedKeys, nonExistedKeys);

        final Map<Integer, MvccTestAccount> initialMap = existedKeys.stream().collect(
            Collectors.toMap(k -> k, k -> new MvccTestAccount(k, 1)));

        Map<Integer, MvccTestAccount> updateMap = existedKeys.stream().collect(
            Collectors.toMap(k -> k, k -> new MvccTestAccount(k, 3)));

        cache1.cache.putAll(initialMap);

        IgniteTransactions txs = node1.transactions();
        try (Transaction tx = txs.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            for (Integer key : allKeys) {
                MvccTestAccount newVal = new MvccTestAccount(key, 2);

                if(existedKeys.contains(key)) {
                    assertTrue(cache1.cache.replace(key, new MvccTestAccount(key, 1), newVal));

                    assertEquals(newVal, cache1.cache.getAndReplace(key, new MvccTestAccount(key, 3)));
                }
                else {
                    assertFalse(cache1.cache.replace(key, new MvccTestAccount(key, 1), newVal));

                    assertNull(cache1.cache.getAndReplace(key, new MvccTestAccount(key, 3)));
                }
            }

            assertEquals(updateMap, getEntries(cache1, allKeys, SQL));
            assertEquals(updateMap, getEntries(cache1, allKeys, GET));

            tx.commit();
        }

        assertEquals(updateMap, getEntries(cache1, allKeys, SQL));
        assertEquals(updateMap, getEntries(cache1, allKeys, GET));
    }
}
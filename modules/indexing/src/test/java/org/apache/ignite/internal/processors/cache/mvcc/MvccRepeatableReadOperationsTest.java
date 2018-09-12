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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

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

                for (Integer key : keys)
                    res.put(key, cache.cache.get(key));

                return res;
            }
            case SQL:
                return getAllSql(cache);
            default:
                fail();
        }

        return null;
    }

    /** {@inheritDoc} */
    protected void updateEntries(
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
            default:
                fail();
        }
    }

    /** {@inheritDoc} */
    protected void removeEntries(
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
            default:
                fail();
        }
    }

    //TODO: IGNITE-7764: Add getAndPut and getAndRemove consistency test.
    public void testName() throws IgniteCheckedException {
        Ignite node1 = grid(0);

        TestCache<Integer, MvccTestAccount> cache1 = new TestCache<>(node1.cache(DEFAULT_CACHE_NAME));

        final Set<Integer> keys = new HashSet<>();

        {
            keys.add(primaryKey(grid(0).cache(DEFAULT_CACHE_NAME)));
            keys.add(backupKey(grid(0).cache(DEFAULT_CACHE_NAME)));
            keys.add(nearKey(grid(0).cache(DEFAULT_CACHE_NAME)));
        }

        IgniteTransactions txs = node1.transactions();
        try (Transaction tx = txs.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {

            for (Integer key : keys) {
                MvccTestAccount res = cache1.cache.getAndPut(key, new MvccTestAccount(0, 1));

                assertNull(res);

                res = cache1.cache.getAndPut(key, new MvccTestAccount(1, 1));

                assertEquals(new MvccTestAccount(0, 1), res);
            }

            tx.commit();
        }
    }

    //TODO: IGNITE-7764: Add putIfAbsent consistency test.
}
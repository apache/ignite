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

package org.apache.ignite.cache.store;

import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * This class tests that redundant calls of {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)}
 * and {@link CacheStoreSessionListener#onSessionEnd(CacheStoreSession, boolean)} are not executed.
 */
public class CacheStoreListenerRWThroughDisabledTransactionalCacheTest extends CacheStoreSessionListenerReadWriteThroughDisabledAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * Tests {@link IgniteCache#get(Object)} with disabled read-through and write-through modes.
     */
    public void testTransactionalLookup() {
        testTransactionalLookup(OPTIMISTIC, READ_COMMITTED);
        testTransactionalLookup(OPTIMISTIC, REPEATABLE_READ);
        testTransactionalLookup(OPTIMISTIC, SERIALIZABLE);

        testTransactionalLookup(PESSIMISTIC, READ_COMMITTED);
        testTransactionalLookup(PESSIMISTIC, REPEATABLE_READ);
        testTransactionalLookup(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @param concurrency Transaction concurrency level.
     * @param isolation Transaction isolation level.
     */
    private void testTransactionalLookup(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        IgniteCache cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        Random r = new Random();

        try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
            for (int i = 0; i < CNT; ++i)
                cache.get(r.nextInt());

            tx.commit();
        }
    }

    /**
     * Tests {@link IgniteCache#put(Object, Object)} with disabled read-through and write-through modes.
     */
    public void testTransactionalUpdate() {
        testTransactionalUpdate(OPTIMISTIC, READ_COMMITTED);
        testTransactionalUpdate(OPTIMISTIC, REPEATABLE_READ);
        testTransactionalUpdate(OPTIMISTIC, SERIALIZABLE);

        testTransactionalUpdate(PESSIMISTIC, READ_COMMITTED);
        testTransactionalUpdate(PESSIMISTIC, REPEATABLE_READ);
        testTransactionalUpdate(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @param concurrency Transaction concurrency level.
     * @param isolation Transaction isolation level.
     */
    private void testTransactionalUpdate(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        IgniteCache cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        Random r = new Random();

        try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
            for (int i = 0; i < CNT; ++i)
                cache.put(r.nextInt(), "test-value");

            tx.commit();
        }
    }

    /**
     * Tests {@link IgniteCache#remove(Object)} with disabled read-through and write-through modes.
     */
    public void testTransactionalRemove() {
        testTransactionalRemove(OPTIMISTIC, READ_COMMITTED);
        testTransactionalRemove(OPTIMISTIC, REPEATABLE_READ);
        testTransactionalRemove(OPTIMISTIC, SERIALIZABLE);

        testTransactionalRemove(PESSIMISTIC, READ_COMMITTED);
        testTransactionalRemove(PESSIMISTIC, REPEATABLE_READ);
        testTransactionalRemove(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @param concurrency Transaction concurrency level.
     * @param isolation Transaction isolation level.
     */
    private void testTransactionalRemove(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        IgniteCache cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        Random r = new Random();

        try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
            for (int i = 0; i < CNT; ++i) {
                int key = r.nextInt();

                cache.put(key, "test-value");

                cache.remove(key, "test-value");
            }

            tx.commit();
        }
    }
}

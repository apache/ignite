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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CachePeekMode.BACKUP;
import static org.apache.ignite.cache.CachePeekMode.PRIMARY;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 *
 */
public class IgniteCacheTxInvokeTest extends IgniteCacheInvokeAbstractTest {
    /** Removes the entry and returns its previous value. */
    private static final CacheEntryProcessor<Integer, Integer, Integer> REMOVE = (entry, args) -> {
        Integer oldVal = entry.getValue();

        entry.remove();

        return oldVal;
    };

    /** Inserts a value and reports whether the entry was absent. */
    private static final CacheEntryProcessor<Integer, Integer, Boolean> INSERT = (entry, args) -> {
        if (entry.exists())
            return false;

        entry.setValue(20);

        return true;
    };

    /** */
    @Test
    public void testInvokeInsertAfterRemoveResult() {
        IgniteCache<Integer, Integer> cache = jcache();
        int key = primaryKey(cache);

        cache.put(key, 10);

        try (Transaction ignored = grid(0).transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertEquals(Integer.valueOf(10), cache.invoke(key, REMOVE));
            assertEquals(Boolean.TRUE, cache.invoke(key, INSERT));
        }
    }

    /** */
    @Test
    public void testReadAfterInvokeRemoveAndInsert() {
        IgniteCache<Integer, Integer> cache = jcache();
        int key = primaryKey(cache);

        cache.put(key, 10);

        try (Transaction ignored = grid(0).transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            cache.invoke(key, REMOVE);
            cache.invoke(key, INSERT);

            assertEquals(Integer.valueOf(20), cache.get(key));
        }
    }

    /** */
    @Test
    public void testInvokeRemoveAndInsertCommittedOnBackup() {
        IgniteCache<Integer, Integer> cache = jcache();
        int key = primaryKey(cache);

        IgniteCache<Integer, Integer> backup = backupCache(key, cache.getName());

        cache.put(key, 10);

        assertEquals(Integer.valueOf(10), backup.localPeek(key, BACKUP));

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            cache.invoke(key, REMOVE);
            cache.invoke(key, INSERT);

            tx.commit();
        }

        assertEquals(Integer.valueOf(20), cache.localPeek(key, PRIMARY));
        assertEquals(Integer.valueOf(20), backup.localPeek(key, BACKUP));
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }
}

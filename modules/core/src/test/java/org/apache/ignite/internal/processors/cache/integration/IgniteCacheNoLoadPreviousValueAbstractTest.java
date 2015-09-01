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

package org.apache.ignite.internal.processors.cache.integration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Test for configuration property {@link CacheConfiguration#isLoadPreviousValue()}.
 */
public abstract class IgniteCacheNoLoadPreviousValueAbstractTest extends IgniteCacheAbstractTest {
    /** */
    private Integer lastKey = 0;

    /** {@inheritDoc} */
    @Override protected Factory<CacheStore> cacheStoreFactory() {
        return new TestStoreFactory();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.getTransactionConfiguration().setTxSerializableEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setReadThrough(true);

        ccfg.setWriteThrough(true);

        ccfg.setLoadPreviousValue(false);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoLoadPreviousValue() throws Exception {
        IgniteCache<Integer, Integer> cache = jcache(0);

        for (Integer key : keys()) {
            log.info("Test [key=" + key + ']');

            storeMap.put(key, key);

            assertEquals(key, cache.get(key));

            assertEquals(key, storeMap.get(key));

            cache.remove(key);

            assertNull(storeMap.get(key));

            storeMap.put(key, key);

            assertNull("Invalid for key: " + key, cache.getAndPut(key, -1));

            assertEquals(-1, storeMap.get(key));

            cache.remove(key);

            assertNull(storeMap.get(key));

            storeMap.put(key, key);

            assertTrue(cache.putIfAbsent(key, -1));

            assertEquals(-1, storeMap.get(key));

            cache.remove(key);

            assertNull(storeMap.get(key));

            storeMap.put(key, key);

            assertNull(cache.getAndRemove(key));

            assertNull(storeMap.get(key));

            storeMap.put(key, key);

            assertNull(cache.getAndPutIfAbsent(key, -1));

            assertEquals(-1, storeMap.get(key));

            cache.remove(key);

            assertNull(storeMap.get(key));

            storeMap.put(key, key);

            assertFalse(cache.replace(key, -1));

            assertEquals(key, storeMap.get(key));

            assertNull(cache.getAndReplace(key, -1));

            assertEquals(key, storeMap.get(key));

            assertFalse(cache.replace(key, key, -1));

            assertEquals(key, storeMap.get(key));
        }

        Map<Integer, Integer> expData = new HashMap<>();

        for (int i = 1000_0000; i < 1000_0000 + 1000; i++) {
            storeMap.put(i, i);

            expData.put(i, i);
        }

        assertEquals(expData, cache.getAll(expData.keySet()));

        if (atomicityMode() == TRANSACTIONAL) {
            for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    for (Integer key : keys()) {
                        log.info("Test tx [key=" + key +
                            ", concurrency=" + concurrency +
                            ", isolation=" + isolation + ']');

                        storeMap.put(key, key);

                        try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                            assertNull("Invalid value [concurrency=" + concurrency + ", isolation=" + isolation + ']',
                                cache.getAndPut(key, -1));

                            tx.commit();
                        }

                        assertEquals(-1, storeMap.get(key));

                        cache.remove(key);

                        assertNull(storeMap.get(key));

                        storeMap.put(key, key);

                        try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                            assertTrue(cache.putIfAbsent(key, -1));

                            tx.commit();
                        }

                        assertEquals(-1, storeMap.get(key));

                        try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                            assertEquals(expData, cache.getAll(expData.keySet()));

                            tx.commit();
                        }
                    }
                }
            }
        }
    }

    /**
     * @return Test keys.
     * @throws Exception If failed.
     */
    protected Collection<Integer> keys() throws Exception {
        IgniteCache<Integer, Object> cache = jcache(0);

        Collection<Integer> keys = new ArrayList<>();

        keys.add(primaryKeys(cache, 1, lastKey).get(0));

        if (gridCount() > 1) {
            keys.add(backupKeys(cache, 1, lastKey).get(0));

            if (cache.getConfiguration(CacheConfiguration.class).getCacheMode() != REPLICATED)
                keys.add(nearKeys(cache, 1, lastKey).get(0));
        }

        lastKey = Collections.max(keys) + 1;

        return keys;
    }
}
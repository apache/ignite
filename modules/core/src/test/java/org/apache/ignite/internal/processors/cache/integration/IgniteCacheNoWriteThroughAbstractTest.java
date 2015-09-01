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
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
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
 * Test for configuration property {@link CacheConfiguration#isWriteThrough}.
 */
public abstract class IgniteCacheNoWriteThroughAbstractTest extends IgniteCacheAbstractTest {
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

        ccfg.setWriteThrough(false);

        ccfg.setLoadPreviousValue(true);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("UnnecessaryLocalVariable")
    public void testNoWriteThrough() throws Exception {
        IgniteCache<Integer, Integer> cache = jcache(0);

        for (Integer key : keys()) {
            log.info("Test [key=" + key + ']');

            final Integer storeVal = key;

            storeMap.put(key, storeVal );

            assertEquals(key, cache.get(key));

            cache.remove(key);

            assertEquals(storeVal, storeMap.get(key));

            storeMap.remove(key);

            assertNull(cache.get(key));

            assertTrue(cache.putIfAbsent(key, key));

            assertNull(storeMap.get(key));

            assertEquals(key, cache.get(key));

            cache.remove(key);

            storeMap.put(key, storeVal);

            Integer val = key + 1;

            assertFalse(cache.putIfAbsent(key, val));

            assertEquals(storeVal, storeMap.get(key));

            cache.put(key, val);

            assertEquals(val, cache.get(key));

            assertEquals(storeVal, storeMap.get(key));

            val = val + 1;

            assertTrue(cache.replace(key, val));

            assertEquals(val, cache.get(key));

            assertEquals(storeVal, storeMap.get(key));

            cache.remove(key);

            assertEquals(storeVal, storeMap.get(key));

            storeMap.remove(key);

            assertNull(cache.get(key));

            storeMap.put(key, storeVal);

            val = val + 1;

            assertEquals(storeVal, cache.getAndPut(key, val));

            assertEquals(storeVal, storeMap.get(key));

            assertEquals(val, cache.get(key));

            cache.remove(key);

            assertEquals(storeVal, storeMap.get(key));

            assertEquals(storeVal, cache.getAndRemove(key));

            cache.remove(key);

            assertEquals(storeVal, storeMap.get(key));

            Object ret = cache.invoke(key, new EntryProcessor<Integer, Integer, Object>() {
                @Override public Object process(MutableEntry<Integer, Integer> entry, Object... args) {
                    Integer val = entry.getValue();

                    entry.setValue(val + 1);

                    return String.valueOf(val);
                }
            });

            assertEquals(String.valueOf(storeVal), ret);

            assertEquals(storeVal + 1, (int)cache.get(key));

            assertEquals(storeVal, storeMap.get(key));

            assertTrue(cache.replace(key, storeVal + 1, storeVal + 2));

            assertEquals(storeVal, storeMap.get(key));

            assertEquals(storeVal + 2, (int) cache.get(key));
        }

        Map<Integer, Integer> expData = new HashMap<>();

        for (int i = 1000_0000; i < 1000_0000 + 1000; i++) {
            storeMap.put(i, i);

            expData.put(i, i);
        }

        assertEquals(expData, cache.getAll(expData.keySet()));

        storeMap.clear();

        cache.putAll(expData);

        assertTrue(storeMap.isEmpty());

        assertEquals(expData, cache.getAll(expData.keySet()));

        Map<Integer, Integer> expData0 = new HashMap<>();

        for (int i = 1000_0000; i < 1000_0000 + 1000; i++)
            expData0.put(i, 1);

        cache.invokeAll(expData.keySet(), new EntryProcessor<Integer, Integer, Object>() {
            @Override public Object process(MutableEntry<Integer, Integer> entry, Object... args)  {
                entry.setValue(1);

                return null;
            }
        });

        assertEquals(expData0, cache.getAll(expData0.keySet()));

        assertTrue(storeMap.isEmpty());

        storeMap.putAll(expData);

        cache.removeAll(expData.keySet());

        assertEquals(1000, storeMap.size());

        storeMap.clear();

        assertTrue(cache.getAll(expData.keySet()).isEmpty());

        if (atomicityMode() == TRANSACTIONAL) {
            for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    for (Integer key : keys()) {
                        log.info("Test tx [key=" + key +
                            ", concurrency=" + concurrency +
                            ", isolation=" + isolation + ']');

                        storeMap.put(key, key);

                        try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                            assertEquals("For concurrency=" + concurrency +
                                ", isolation=" + isolation + ']', key, cache.getAndPut(key, -1));

                            tx.commit();
                        }

                        assertEquals(-1, (int)cache.get(key));

                        assertEquals(key, storeMap.get(key));

                        try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                            cache.put(key, -2);

                            tx.commit();
                        }

                        assertEquals(-2, (int)cache.get(key));

                        assertEquals(key, storeMap.get(key));

                        try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                            assertEquals(-2, (int)cache.getAndRemove(key));

                            tx.commit();
                        }

                        assertEquals(key, storeMap.get(key));

                        storeMap.remove(key);

                        assertNull(cache.get(key));

                        storeMap.put(key, key);

                        cache.put(key, key);

                        try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                            assertTrue(cache.replace(key, -1));

                            tx.commit();
                        }

                        assertEquals(-1, (int)cache.get(key));

                        assertEquals(key, storeMap.get(key));

                        cache.remove(key);

                        storeMap.clear();

                        try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                            cache.putAll(expData);

                            tx.commit();
                        }

                        assertTrue(storeMap.isEmpty());

                        assertEquals(expData, cache.getAll(expData.keySet()));

                        try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                            cache.invokeAll(expData.keySet(), new EntryProcessor<Integer, Integer, Object>() {
                                @Override public Object process(MutableEntry<Integer, Integer> entry, Object... args)  {
                                    entry.setValue(1);

                                    return null;
                                }
                            });

                            tx.commit();
                        }

                        assertEquals(expData0, cache.getAll(expData.keySet()));

                        assertTrue(storeMap.isEmpty());

                        storeMap.putAll(expData);

                        try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                            cache.removeAll(expData.keySet());

                            tx.commit();
                        }

                        assertEquals(1000, storeMap.size());

                        storeMap.clear();

                        assertTrue(cache.getAll(expData.keySet()).isEmpty());
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
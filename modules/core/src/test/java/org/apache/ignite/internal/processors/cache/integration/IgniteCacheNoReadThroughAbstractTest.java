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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.cache.configuration.Factory;
import javax.cache.integration.CompletionListenerFuture;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Test for configuration property {@link CacheConfiguration#isReadThrough}.
 */
public abstract class IgniteCacheNoReadThroughAbstractTest extends IgniteCacheAbstractTest {
    /** */
    private Integer lastKey = 0;

    /** */
    private static boolean allowLoad;

    /** {@inheritDoc} */
    @Override protected Factory<CacheStore> cacheStoreFactory() {
        return new NoReadThroughStoreFactory();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getTransactionConfiguration().setTxSerializableEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(igniteInstanceName);

        ccfg.setReadThrough(false);

        ccfg.setWriteThrough(true);

        ccfg.setLoadPreviousValue(true);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        allowLoad = false;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoReadThrough() throws Exception {
        IgniteCache<Integer, Integer> cache = jcache(0);

        for (Integer key : keys()) {
            log.info("Test [key=" + key + ']');

            storeMap.put(key, key);

            assertNull(cache.get(key));

            assertEquals(key, storeMap.get(key));

            assertNull(cache.getAndPut(key, -1));

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

            Object ret = cache.invoke(key, new EntryProcessor<Integer, Integer, Object>() {
                @Override public Object process(MutableEntry<Integer, Integer> e, Object... args) {
                    Integer val = e.getValue();

                    assertFalse(e.exists());

                    assertNull(val);

                    e.setValue(-1);

                    return String.valueOf(val);
                }
            });

            assertEquals("null", ret);

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

        Set<Integer> keys = new HashSet<>();

        for (int i = 1000_0000; i < 1000_0000 + 1000; i++) {
            keys.add(i);

            storeMap.put(i, i);
        }

        assertTrue(cache.getAll(keys).isEmpty());

        if (atomicityMode() == TRANSACTIONAL) {
            for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    for (Integer key : keys()) {
                        log.info("Test tx [key=" + key +
                            ", concurrency=" + concurrency +
                            ", isolation=" + isolation + ']');

                        storeMap.put(key, key);

                        try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                            assertNull(cache.get(key));

                            tx.commit();
                        }

                        assertEquals(key, storeMap.get(key));

                        try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                            assertNull(cache.getAndPut(key, -1));

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

                        cache.remove(key);

                        assertNull(storeMap.get(key));

                        storeMap.put(key, key);

                        try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                            Object ret = cache.invoke(key, new EntryProcessor<Integer, Integer, Object>() {
                                @Override public Object process(MutableEntry<Integer, Integer> e, Object... args) {
                                    Integer val = e.getValue();

                                    assertFalse(e.exists());

                                    assertNull(val);

                                    e.setValue(-1);

                                    return String.valueOf(val);
                                }
                            });

                            assertEquals("null", ret);

                            tx.commit();
                        }

                        assertEquals(-1, storeMap.get(key));

                        try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                            assertTrue(cache.getAll(keys).isEmpty());

                            tx.commit();
                        }
                    }
                }
            }
        }

        // Check can load cache when read-through is disabled.

        allowLoad = true;

        Integer key = 1;

        cache.remove(key);

        storeMap.clear();

        storeMap.put(key, 10);

        cache.loadCache(null);

        assertEquals(10, (int)cache.get(key));

        cache.remove(key);

        storeMap.put(key, 11);

        CompletionListenerFuture fut = new CompletionListenerFuture();

        cache.loadAll(F.asSet(key), true, fut);

        fut.get();

        assertEquals(11, (int)cache.get(key));
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

    /**
     *
     */
    private static class NoReadThroughStoreFactory implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return new TestStore() {
                @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
                    if (!allowLoad)
                        fail();

                    super.loadCache(clo, args);
                }

                @Override public Object load(Object key) {
                    if (!allowLoad)
                        fail();

                    return super.load(key);
                }

                @Override public Map<Object, Object> loadAll(Iterable<?> keys) {
                    if (!allowLoad)
                        fail();

                    return super.loadAll(keys);
                }
            };
        }
    }
}

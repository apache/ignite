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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.cache.configuration.Factory;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public abstract class IgniteCacheInvokeReadThroughAbstractTest extends GridCommonAbstractTest {
    /** */
    private static volatile boolean failed;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        failed = false;

        startNodes();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        IgniteCacheAbstractTest.storeMap.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        ignite(0).destroyCache(DEFAULT_CACHE_NAME);
    }

    /**
     * @return Store factory.
     */
    protected Factory<CacheStore> cacheStoreFactory() {
        return new IgniteCacheAbstractTest.TestStoreFactory();
    }

    /**
     * @param data Data.
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    protected void putDataInStore(Map<Object, Object> data, String cacheName) throws Exception {
        IgniteCacheAbstractTest.storeMap.putAll(data);
    }

    /**
     * @throws Exception If failed.
     */
    protected abstract void startNodes() throws Exception;

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected void invokeReadThrough(CacheConfiguration ccfg) throws Exception {
        Ignite ignite0 = ignite(0);

        ignite0.createCache(ccfg);

        int key = 0;

        for (Ignite node : G.allGrids()) {
            if (node.configuration().isClientMode() && ccfg.getNearConfiguration() != null)
                node.createNearCache(ccfg.getName(), ccfg.getNearConfiguration());
        }

        for (Ignite node : G.allGrids()) {
            log.info("Test for node: " + node.name());

            IgniteCache<Object, Object> cache = node.cache(ccfg.getName());

            for (int i = 0; i < 50; i++)
                checkReadThrough(cache, key++, null, null);

            Set<Object> keys = new HashSet<>();

            for (int i = 0; i < 5; i++)
                keys.add(key++);

            checkReadThroughInvokeAll(cache, keys, null, null);

            keys = new HashSet<>();

            for (int i = 0; i < 100; i++)
                keys.add(key++);

            checkReadThroughInvokeAll(cache, keys, null, null);

            if (ccfg.getAtomicityMode() == TRANSACTIONAL) {
                for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        log.info("Test tx [concurrency=" + concurrency + ", isolation=" + isolation + ']');

                        for (int i = 0; i < 50; i++)
                            checkReadThrough(cache, key++, concurrency, isolation);

                        keys = new HashSet<>();

                        for (int i = 0; i < 5; i++)
                            keys.add(key++);

                        checkReadThroughInvokeAll(cache, keys, concurrency, isolation);

                        keys = new HashSet<>();

                        for (int i = 0; i < 100; i++)
                            keys.add(key++);

                        checkReadThroughInvokeAll(cache, keys, concurrency, isolation);
                    }
                }

                for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        log.info("Test tx2 [concurrency=" + concurrency + ", isolation=" + isolation + ']');

                        for (int i = 0; i < 50; i++)
                            checkReadThroughGetAndInvoke(cache, key++, concurrency, isolation);
                    }
                }
            }
        }

        ignite0.cache(ccfg.getName()).removeAll();
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @throws Exception If failed.
     */
    private void checkReadThrough(IgniteCache<Object, Object> cache,
        Object key,
        @Nullable TransactionConcurrency concurrency,
        @Nullable TransactionIsolation isolation) throws Exception {
        putDataInStore(Collections.singletonMap(key, key), cache.getName());

        Transaction tx = isolation != null ? cache.unwrap(Ignite.class).transactions().txStart(concurrency, isolation)
            : null;

        try {
            Object ret = cache.invoke(key, new TestEntryProcessor());

            assertEquals(key, ret);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        checkValue(cache.getName(), key, (Integer)key + 1);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @throws Exception If failed.
     */
    private void checkReadThroughGetAndInvoke(IgniteCache<Object, Object> cache,
        Object key,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws Exception {
        putDataInStore(Collections.singletonMap(key, key), cache.getName());

        try (Transaction tx = cache.unwrap(Ignite.class).transactions().txStart(concurrency, isolation)) {
            cache.get(key);

            Object ret = cache.invoke(key, new TestEntryProcessor());

            assertEquals(key, ret);

            tx.commit();
        }

        checkValue(cache.getName(), key, (Integer)key + 1);
    }

    /**
     * @param cache Cache.
     * @param keys Key.
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @throws Exception If failed.
     */
    private void checkReadThroughInvokeAll(IgniteCache<Object, Object> cache,
        Set<Object> keys,
        @Nullable TransactionConcurrency concurrency,
        @Nullable TransactionIsolation isolation) throws Exception {
        Map<Object, Object> data = U.newHashMap(keys.size());

        for (Object key : keys)
            data.put(key, key);

        putDataInStore(data, cache.getName());

        Transaction tx = isolation != null ? cache.unwrap(Ignite.class).transactions().txStart(concurrency, isolation)
            : null;

        try {
            Map<Object, EntryProcessorResult<Object>> ret = cache.invokeAll(keys, new TestEntryProcessor());

            assertEquals(ret.size(), keys.size());

            for (Object key : keys) {
                EntryProcessorResult<Object> res = ret.get(key);

                assertNotNull(res);
                assertEquals(key, res.get());
            }

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        for (Object key : keys)
            checkValue(cache.getName(), key, (Integer)key + 1);
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Expected value.
     */
    private void checkValue(String cacheName, Object key, Object val) {
        for (Ignite ignite : G.allGrids()) {
            assertEquals("Unexpected value for node: " + ignite.name(),
                val,
                ignite.cache(cacheName).get(key));
        }

        assertFalse(failed);
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param backups Number of backups.
     * @param nearCache Near cache flag.
     * @return Cache configuration.
     */
    @SuppressWarnings("unchecked")
    protected CacheConfiguration cacheConfiguration(CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        int backups,
        boolean nearCache) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setReadThrough(true);
        ccfg.setWriteThrough(true);
        ccfg.setCacheStoreFactory(cacheStoreFactory());
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        if (nearCache)
            ccfg.setNearConfiguration(new NearCacheConfiguration());

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     *
     */
    static class TestEntryProcessor implements EntryProcessor<Object, Object, Object> {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> entry, Object... args) {
            if (!entry.exists()) {
                failed = true;

                fail();
            }

            Integer val = (Integer)entry.getValue();

            if (!val.equals(entry.getKey())) {
                failed = true;

                assertEquals(val, entry.getKey());
            }

            entry.setValue(val + 1);

            return val;
        }
    }
}

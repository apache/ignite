/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionIsolation.values;

/**
 *
 */
public class IgniteCacheReadThroughStoreCallTest extends GridCommonAbstractTest {
    /** */
    private static final Map<Object, Object> storeMap = new ConcurrentHashMap<>();

    /** */
    protected boolean client;

    /** */
    @Before
    public void beforeIgniteCacheReadThroughStoreCallTest() {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        storeMap.clear();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultiNode() throws Exception {
        startGridsMultiThreaded(4);

        client = true;

        startGrid(4);

        checkLoadCount(cacheConfiguration(PARTITIONED, ATOMIC, 0));

        checkLoadCount(cacheConfiguration(PARTITIONED, ATOMIC, 1));

        checkLoadCount(cacheConfiguration(PARTITIONED, ATOMIC, 2));

        checkLoadCount(cacheConfiguration(PARTITIONED, TRANSACTIONAL, 0));

        checkLoadCount(cacheConfiguration(PARTITIONED, TRANSACTIONAL, 1));

        checkLoadCount(cacheConfiguration(PARTITIONED, TRANSACTIONAL, 2));
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void checkLoadCount(CacheConfiguration<Object, Object> ccfg) throws Exception {
        storeMap.clear();

        Ignite ignite0 = ignite(0);

        ignite0.createCache(ccfg);

        try {
            int key = 0;

            for (Ignite node : G.allGrids()) {
                log.info("Test for node: " + node.name());

                final IgniteCache<Object, Object> cache = node.cache(ccfg.getName());

                for (int i = 0; i < 50; i++) {
                    final int k = key++;

                    checkReadThrough(cache, new IgniteRunnable() {
                        @Override public void run() {
                            cache.invoke(k, new TestEntryProcessor());
                        }
                    }, null, null, 1);
                }

                for (int i = 0; i < 50; i++) {
                    final int k = key++;

                    checkReadThrough(cache, new IgniteRunnable() {
                        @Override public void run() {
                            cache.put(k, k);
                        }
                    }, null, null, 0);
                }

                if (ccfg.getAtomicityMode() == TRANSACTIONAL) {
                    for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                        for (TransactionIsolation isolation : values()) {
                            log.info("Test tx [concurrency=" + concurrency + ", isolation=" + isolation + ']');

                            for (int i = 0; i < 50; i++) {
                                final int k = key++;

                                checkReadThrough(cache, new IgniteRunnable() {
                                    @Override public void run() {
                                        cache.invoke(k, new TestEntryProcessor());
                                    }
                                }, concurrency, isolation, 2);
                            }
                        }
                    }
                }
            }

            ignite0.cache(ccfg.getName()).removeAll();
        }
        finally {
            ignite0.destroyCache(ccfg.getName());
        }
    }

    /**
     * @param cache Cache.
     * @param c Cache operation Closure.
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @param expLoadCnt Expected number of store 'load' calls.
     * @throws Exception If failed.
     */
    private void checkReadThrough(IgniteCache<Object, Object> cache,
        IgniteRunnable c,
        @Nullable TransactionConcurrency concurrency,
        @Nullable TransactionIsolation isolation,
        int expLoadCnt) throws Exception {
        TestStore.loadCnt.set(0);

        Transaction tx = isolation != null ? cache.unwrap(Ignite.class).transactions().txStart(concurrency, isolation)
            : null;

        try {
            c.run();

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assertEquals(expLoadCnt, TestStore.loadCnt.get());
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param backups Number of backups.
     * @return Cache configuration.
     */
    @SuppressWarnings("unchecked")
    protected CacheConfiguration<Object, Object> cacheConfiguration(CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        int backups) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setReadThrough(true);
        ccfg.setWriteThrough(true);
        ccfg.setCacheStoreFactory(new TestStoreFactory());
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     *
     */
    public static class TestStoreFactory implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return new TestStore();
        }
    }

    /**
     *
     */
    public static class TestStore extends CacheStoreAdapter<Object, Object> {
        /** */
        static AtomicInteger loadCnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
            fail();
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            loadCnt.incrementAndGet();

            return storeMap.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) {
            storeMap.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            storeMap.remove(key);
        }
    }

    /**
     *
     */
    static class TestEntryProcessor implements EntryProcessor<Object, Object, Object> {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> entry, Object... args) {
            Object val = entry.getValue();

            entry.setValue(entry.getKey());

            return val;
        }
    }
}

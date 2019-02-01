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

package org.apache.ignite.internal.processors.cache.eviction;

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Tests that cache handles {@code setAllowEmptyEntries} flag correctly.
 */
public abstract class GridCacheEmptyEntriesAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private EvictionPolicy<?, ?> plc;

    /** */
    private EvictionPolicy<?, ?> nearPlc;

    /** Test store. */
    private CacheStore<String, String> testStore;

    /** Tx concurrency to use. */
    private TransactionConcurrency txConcurrency;

    /** Tx isolation to use. */
    private TransactionIsolation txIsolation;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        TransactionConfiguration txCfg = c.getTransactionConfiguration();

        txCfg.setDefaultTxConcurrency(txConcurrency);
        txCfg.setDefaultTxIsolation(txIsolation);
        txCfg.setTxSerializableEnabled(true);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode());
        cc.setAtomicityMode(TRANSACTIONAL);

        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        cc.setEvictionPolicy(plc);
        cc.setOnheapCacheEnabled(true);

        if (testStore != null) {
            cc.setCacheStoreFactory(singletonFactory(testStore));
            cc.setReadThrough(true);
            cc.setWriteThrough(true);
            cc.setLoadPreviousValue(true);
        }
        else
            cc.setCacheStoreFactory(null);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * Starts grids depending on testing cache.
     *
     * @return First grid node.
     * @throws Exception If failed.
     */
    protected abstract Ignite startGrids() throws Exception;

    /** @return Cache mode for particular test. */
    protected abstract CacheMode cacheMode();

    /**
     * Tests FIFO eviction policy.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFifo() throws Exception {
        FifoEvictionPolicy plc = new FifoEvictionPolicy();
        plc.setMaxSize(50);
        this.plc = plc;

        FifoEvictionPolicy nearPlc = new FifoEvictionPolicy();
        nearPlc.setMaxSize(50);
        this.nearPlc = nearPlc;

        checkPolicy();
    }

    /**
     * Checks policy with and without store queue.
     *
     * @throws Exception If failed.
     */
    private void checkPolicy() throws Exception {
        testStore = null;

        checkPolicy0();

        testStore = new CacheStoreAdapter<String, String>() {
            @Override public String load(String key) {
                return null;
            }

            @Override public void write(javax.cache.Cache.Entry<? extends String, ? extends String> e) {
                // No-op.
            }

            @Override public void delete(Object key) {
                // No-op.
            }
        };

        checkPolicy0();
    }

    /**
     * Tests preset eviction policy.
     *
     * @throws Exception If failed.
     */
    private void checkPolicy0() throws Exception {
        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            txConcurrency = concurrency;

            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                txIsolation = isolation;

                if (MvccFeatureChecker.forcedMvcc() && !MvccFeatureChecker.isSupported(concurrency, isolation))
                    continue;

                Ignite g = startGrids();

                IgniteCache<String, String> cache = g.cache(DEFAULT_CACHE_NAME);

                try {
                    info(">>> Checking policy [txConcurrency=" + txConcurrency + ", txIsolation=" + txIsolation +
                        ", plc=" + plc + ", nearPlc=" + nearPlc + ']');

                    checkExplicitTx(g, cache);

                    checkImplicitTx(cache);
                }
                finally {
                    stopAllGrids();
                }
            }
        }
    }

    /**
     * Checks that gets work for implicit txs.
     *
     * @param cache Cache to test.
     * @throws Exception If failed.
     */
    private void checkImplicitTx(IgniteCache<String, String> cache) throws Exception {
        assertNull(cache.get("key1"));

        assertNull(cache.getAsync("key2").get());

        assertTrue(cache.getAll(F.asSet("key3", "key4")).isEmpty());

        assertTrue(((Map)cache.getAllAsync(F.asSet("key5", "key6")).get()).isEmpty());

        cache.put("key7", "key7");
        cache.remove("key7", "key7");
        assertNull(cache.get("key7"));

        checkEmpty(cache);
    }

    /**
     * Checks that gets work for implicit txs.
     *
     * @param ignite Ignite instance.
     * @param cache Cache to test.
     * @throws Exception If failed.
     */
    private void checkExplicitTx(Ignite ignite, IgniteCache<String, String> cache) throws Exception {
        Transaction tx = ignite.transactions().txStart();

        try {
            assertNull(cache.get("key1"));

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = ignite.transactions().txStart();

        try {
            assertNull(cache.getAsync("key2").get());

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = ignite.transactions().txStart();

        try {
            assertTrue(cache.getAll(F.asSet("key3", "key4")).isEmpty());

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = ignite.transactions().txStart();

        try {
            assertTrue(((Map)cache.getAllAsync(F.asSet("key5", "key6")).get()).isEmpty());

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = ignite.transactions().txStart();

        try {
            cache.put("key7", "key7");

            cache.remove("key7");

            assertNull(cache.get("key7"));

            tx.commit();
        }
        finally {
            tx.close();
        }

        checkEmpty(cache);
    }

    /**
     * Checks that cache is empty.
     *
     * @param cache Cache to check.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If interrupted while sleeping.
     */
    @SuppressWarnings({"ErrorNotRethrown", "TypeMayBeWeakened"})
    private void checkEmpty(IgniteCache<String, String> cache) throws IgniteInterruptedCheckedException {
        for (int i = 0; i < 3; i++) {
            try {
                assertTrue(!cache.iterator().hasNext());

                break;
            }
            catch (AssertionError e) {
                if (i == 2)
                    throw e;

                info(">>> Cache is not empty, flushing evictions.");

                U.sleep(1000);
            }
        }
    }
}

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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import javax.cache.Cache;

/**
 *
 */
public class IgniteCacheTxIteratorSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "testCache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        final TransactionConfiguration txCfg = new TransactionConfiguration();

        txCfg.setDefaultTxIsolation(TransactionIsolation.READ_COMMITTED);

        cfg.setTransactionConfiguration(txCfg);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<String, TestClass> cacheConfiguration(
        CacheMode mode,
        CacheAtomicityMode atomMode,
        boolean nearEnabled,
        boolean useEvictPlc
    ) {
        final CacheConfiguration<String, TestClass> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setAtomicityMode(atomMode);
        ccfg.setCacheMode(mode);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        if (nearEnabled)
            ccfg.setNearConfiguration(new NearCacheConfiguration<String, TestClass>());

        if (useEvictPlc) {
            ccfg.setEvictionPolicy(new FifoEvictionPolicy(50));
            ccfg.setOnheapCacheEnabled(true);
        }

        return ccfg;
    }

    /**
     * @throws Exception if failed.
     */
    public void testModesSingleNode() throws Exception {
        checkModes(1);
    }

    /**
     * @throws Exception if failed.
     */
    public void testModesMultiNode() throws Exception {
        checkModes(3);
    }

    /**
     * @throws Exception if failed.
     */
    public void checkModes(int gridCnt) throws Exception {
        startGrids(gridCnt);

        try {
            for (CacheMode mode : CacheMode.values()) {
                for (CacheAtomicityMode atomMode : CacheAtomicityMode.values()) {
                    if (mode == CacheMode.PARTITIONED) {
                        // Near cache makes sense only for partitioned cache.
                        checkTxCache(CacheMode.PARTITIONED, atomMode, true, false);
                    }

                    checkTxCache(CacheMode.PARTITIONED, atomMode, false, true);

                    checkTxCache(CacheMode.PARTITIONED, atomMode, false, false);
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkTxCache(
        CacheMode mode,
        CacheAtomicityMode atomMode,
        boolean nearEnabled,
        boolean useEvicPlc
    ) throws Exception {
        final Ignite ignite = grid(0);

        final CacheConfiguration<String, TestClass> ccfg = cacheConfiguration(
            mode,
            atomMode,
            nearEnabled,
            useEvicPlc);

        final IgniteCache<String, TestClass> cache = ignite.createCache(ccfg);

        info("Checking cache [mode=" + mode + ", atomMode=" + atomMode + ", near=" + nearEnabled +
            ", evict=" + useEvicPlc + ']');

        try {
            for (int i = 0; i < 30; i++) {
                final TestClass val = new TestClass("data");
                final String key = "key-" + i;

                cache.put(key, val);

                assertEquals(i + 1, cache.size());

                for (TransactionIsolation iso : TransactionIsolation.values()) {
                    for (TransactionConcurrency con : TransactionConcurrency.values()) {
                        try (Transaction transaction = ignite.transactions().txStart(con, iso)) {
                            assertEquals(val, cache.get(key));

                            transaction.commit();
                        }

                        int cnt = iterateOverKeys(cache);

                        assertEquals("Failed [con=" + con + ", iso=" + iso + ']', i + 1, cnt);

                        assertEquals("Failed [con=" + con + ", iso=" + iso + ']', i + 1, cache.size());
                    }
                }
            }
        }
        finally {
            grid(0).destroyCache(CACHE_NAME);
        }
    }

    /**
     * @param cache Cache.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private int iterateOverKeys(final IgniteCache<String, TestClass> cache) {
        int cnt = 0;

        for (final Cache.Entry<String, TestClass> ignore : cache)
            cnt++;

        return cnt;
    }

    /**
     *
     */
    private static class TestClass {
        /** */
        private String data;

        /**
         * @param data Data.
         */
        private TestClass(String data) {
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            final TestClass testCls = (TestClass)o;

            return data != null ? data.equals(testCls.data) : testCls.data == null;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return data != null ? data.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestClass.class, this);
        }
    }
}
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

package org.apache.ignite.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;

import javax.cache.Cache;

/**
 *
 */
public class CacheTXTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If fail.
     */
    public void testTxCache() throws Exception {
        final IgniteConfiguration cfg = getIgniteConfiguration();

        final Ignite ignite = G.start(cfg);

        final CacheConfiguration<String, TestClass> ccfg = getCacheConfiguration();

        final IgniteCache<String, TestClass> cache = ignite.getOrCreateCache(ccfg);

        final TestClass val = new TestClass("data");
        final String key = "key";

        cache.put(key, val);

        assertEquals(1, cache.size());

        final Transaction transaction = ignite.transactions().txStart();

        assertEquals(val, cache.get(key));

        transaction.commit();

        iterateOverKeys(cache);

        assertEquals(1, cache.size());
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<String, TestClass> getCacheConfiguration() {
        final CacheConfiguration<String, TestClass> ccfg = new CacheConfiguration<>();

        ccfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setBackups(0);

        return ccfg;
    }

    /**
     * @return Ignite configuration.
     */
    private IgniteConfiguration getIgniteConfiguration() {
        final IgniteConfiguration cfg = new IgniteConfiguration();

        final TransactionConfiguration txCfg = new TransactionConfiguration();

        txCfg.setDefaultTxIsolation(TransactionIsolation.READ_COMMITTED);

        cfg.setTransactionConfiguration(txCfg);

        return cfg;
    }

    /**
     * @param cache Cache.
     */
    private void iterateOverKeys(final IgniteCache<String, TestClass> cache) {
        for (final Cache.Entry<String, TestClass> entry : cache)
            System.out.println(entry);
    }

    /**
     *
     */
    public static class TestClass {
        /** */
        private String data;

        /**
         * @param data Data.
         */
        public TestClass(String data) {
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final TestClass testCls = (TestClass) o;

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

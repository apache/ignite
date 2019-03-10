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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Test tx spanning multiple tx and dht caches.
 */
public class TxLocalDhtMixedCacheModesTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration().
            setName(DEFAULT_CACHE_NAME).
            setCacheMode(LOCAL).
            setAtomicityMode(TRANSACTIONAL));

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testTxLocalDhtMixedCacheModes() throws Exception {
        try {
            IgniteEx g0 = startGrid(0);

            IgniteCache cache1 = g0.cache(DEFAULT_CACHE_NAME);
            IgniteCache cache2 = g0.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME + 1).
                setAtomicityMode(TRANSACTIONAL));

            cache1.put(1, 1);
            cache2.put(1, 2);

            // Commit.
            try (Transaction tx = g0.transactions().txStart()) {
                cache1.put(1, 10);
                cache2.put(1, 20);

                tx.commit();
            }

            assertEquals(10, cache1.get(1));
            assertEquals(20, cache2.get(1));

            // Rollback.
            try (Transaction tx = g0.transactions().txStart()) {
                cache1.put(1, 100);
                cache2.put(1, 200);
            }

            assertEquals(10, cache1.get(1));
            assertEquals(20, cache2.get(1));
        }
        finally {
            stopAllGrids();
        }
    }
}

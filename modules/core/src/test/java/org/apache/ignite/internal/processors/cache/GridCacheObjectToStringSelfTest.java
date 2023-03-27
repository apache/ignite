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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests that common cache objects' toString() methods do not lead to stack overflow.
 */
public class GridCacheObjectToStringSelfTest extends GridCommonAbstractTest {
    /** Cache mode for test. */
    private CacheMode cacheMode;

    /** Cache eviction policy. */
    private EvictionPolicy evictionPlc;

    /** Near enabled flag. */
    private boolean nearEnabled;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EVICTION);

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EVICTION);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(cacheMode);
        cacheCfg.setEvictionPolicy(evictionPlc);
        cacheCfg.setOnheapCacheEnabled(true);
        cacheCfg.setNearConfiguration(nearEnabled ? new NearCacheConfiguration() : null);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        evictionPlc = null;
    }

    /** @throws Exception If failed. */
    @Test
    public void testReplicatedCacheFifoEvictionPolicy() throws Exception {
        cacheMode = REPLICATED;
        evictionPlc = new FifoEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    @Test
    public void testReplicatedCacheLruEvictionPolicy() throws Exception {
        cacheMode = REPLICATED;
        evictionPlc = new LruEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    @Test
    public void testPartitionedCacheFifoEvictionPolicy() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = true;
        evictionPlc = new FifoEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    @Test
    public void testPartitionedCacheLruEvictionPolicy() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = true;
        evictionPlc = new LruEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    @Test
    public void testColocatedCacheFifoEvictionPolicy() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = false;
        evictionPlc = new FifoEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    @Test
    public void testColocatedCacheLruEvictionPolicy() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = false;
        evictionPlc = new LruEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    private void checkToString() throws Exception {
        Ignite g = startGrid(0);

        try {
            IgniteCache<Object, Object> cache = g.cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < 10; i++)
                cache.put(i, i);

            for (int i = 0; i < 10; i++) {
                GridCacheEntryEx entry = ((IgniteKernal)g).context().cache().internalCache(DEFAULT_CACHE_NAME).peekEx(i);

                if (entry != null)
                    assertFalse("Entry is locked after implicit transaction commit: " + entry, entry.lockedByAny());
            }

            assertFalse(cache.toString().isEmpty());
            assertFalse(cache.iterator().toString().isEmpty());

            try (Transaction tx = g.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                assertEquals(1, cache.get(1));

                cache.put(2, 22);

                assertFalse(tx.toString().isEmpty());

                assertFalse(cache.toString().isEmpty());

                tx.commit();
            }
        }
        finally {
            stopAllGrids();
        }
    }
}

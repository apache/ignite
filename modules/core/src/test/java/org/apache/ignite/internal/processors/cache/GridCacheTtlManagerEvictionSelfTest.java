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

import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * TTL manager eviction self test.
 */
public class GridCacheTtlManagerEvictionSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int ENTRIES_TO_PUT = 10_100;

    /** */
    private static final int ENTRIES_LIMIT = 1_000;

    /** Cache mode. */
    private volatile CacheMode cacheMode;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EXPIRATION);
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EVICTION);

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setEagerTtl(true);
        ccfg.setEvictionPolicy(new FifoEvictionPolicy(ENTRIES_LIMIT, 100));
        ccfg.setOnheapCacheEnabled(true);
        ccfg.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.HOURS, 12)));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalEviction() throws Exception {
        checkEviction(CacheMode.LOCAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionedEviction() throws Exception {
        checkEviction(CacheMode.PARTITIONED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplicatedEviction() throws Exception {
        checkEviction(CacheMode.REPLICATED);
    }

    /**
     * @param mode Cache mode.
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    private void checkEviction(CacheMode mode) throws Exception {
        cacheMode = mode;

        final IgniteKernal g = (IgniteKernal)startGrid(0);

        try {
            final IgniteCache<Object, Object> cache = g.cache(DEFAULT_CACHE_NAME);

            final GridCacheContext<Object, Object> cctx = g.cachex(DEFAULT_CACHE_NAME).context();

            for (int i = 1; i <= ENTRIES_TO_PUT; i++) {
                String key = "Some test entry key#" + i;
                String value = "Some test entry value#" + i;

                cache.put(key, value);
            }

            if (log.isTraceEnabled())
                cctx.ttl().printMemoryStats();

            final String firstKey = "Some test entry key#1";
            final String lastKey = "Some test entry key#" + ENTRIES_TO_PUT;

            assertNull("first key should be evicted", cache.localPeek(firstKey, CachePeekMode.ONHEAP));

            assertNotNull("last key should NOT be evicted", cache.localPeek(lastKey, CachePeekMode.ONHEAP));

            assertEquals("Ttl Manager should NOT track evicted entries", ENTRIES_LIMIT, cctx.ttl().pendingSize());
        }
        finally {
            Ignition.stopAll(true);
        }
    }
}

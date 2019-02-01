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

package org.apache.ignite.internal.processors.cache.store;

import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that write behind store is updated if client does not have store.
 */
public abstract class IgnteCacheClientWriteBehindStoreAbstractTest extends IgniteCacheAbstractTest {
    /** */
    @Before
    public void beforeCacheStoreListenerRWThroughDisabledTransactionalCacheTest() {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);

        CacheConfiguration ccfg = super.cacheConfiguration(igniteInstanceName);

        ccfg.setWriteBehindEnabled(true);
        ccfg.setWriteBehindBatchSize(10);

        if (getTestIgniteInstanceName(2).equals(igniteInstanceName)) {
            ccfg.setCacheStoreFactory(null);
            ccfg.setWriteThrough(false);
            ccfg.setReadThrough(false);
            ccfg.setWriteBehindEnabled(false);
        }

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceName(2).equals(igniteInstanceName))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected Factory<CacheStore> cacheStoreFactory() {
        return new TestStoreFactory();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientWithoutStore() throws Exception {
        Ignite client = grid(2);

        assertTrue(client.configuration().isClientMode());

        IgniteCache<Integer, Integer> cache = client.cache(DEFAULT_CACHE_NAME);

        assertNull(cache.getConfiguration(CacheConfiguration.class).getCacheStoreFactory());

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return storeMap.size() == 1000;
            }
        }, 5000);

        assertEquals(1000, storeMap.size());
    }
}

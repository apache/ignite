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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.testframework.*;

import javax.cache.configuration.*;

/**
 * Tests that write behind store is updated if client does not have store.
 */
public abstract class IgnteCacheClientWriteBehindStoreAbstractTest extends IgniteCacheAbstractTest {
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
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setWriteBehindEnabled(true);
        ccfg.setWriteBehindBatchSize(10);

        if (getTestGridName(2).equals(gridName)) {
            ccfg.setCacheStoreFactory(null);
            ccfg.setWriteThrough(false);
            ccfg.setReadThrough(false);
            ccfg.setWriteBehindEnabled(false);
        }

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (getTestGridName(2).equals(gridName))
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
    public void testClientWithoutStore() throws Exception {
        Ignite client = grid(2);

        assertTrue(client.configuration().isClientMode());

        IgniteCache<Integer, Integer> cache = client.cache(null);

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

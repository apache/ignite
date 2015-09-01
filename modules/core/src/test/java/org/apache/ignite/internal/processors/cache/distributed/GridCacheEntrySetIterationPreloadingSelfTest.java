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

import java.util.ArrayList;
import java.util.Collection;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;

/**
 * Tests entry wrappers after preloading happened.
 */
public class GridCacheEntrySetIterationPreloadingSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
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
    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.ATOMIC;
    }

    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteration()  throws Exception {
        try {
            final IgniteCache<String, Integer> cache = jcache();

            final int entryCnt = 1000;

            for (int i = 0; i < entryCnt; i++)
                cache.put(String.valueOf(i), i);

            Collection<Cache.Entry<String, Integer>> entries = new ArrayList<>(10_000);

            for (int i = 0; i < 10_000; i++)
                entries.add(cache.randomEntry());

            startGrid(1);
            startGrid(2);
            startGrid(3);

            for (Cache.Entry<String, Integer> entry : entries)
                entry.getValue();

            for (int i = 0; i < entryCnt; i++)
                cache.remove(String.valueOf(i));
        }
        finally {
            stopGrid(3);
            stopGrid(2);
            stopGrid(1);
        }
    }
}
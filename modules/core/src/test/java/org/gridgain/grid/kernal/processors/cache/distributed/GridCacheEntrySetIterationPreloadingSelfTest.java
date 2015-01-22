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

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.cache.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;

import java.util.*;

/**
 * Tests entry wrappers after preloading happened.
 */
public class GridCacheEntrySetIterationPreloadingSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return GridCacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return GridCacheDistributionMode.PARTITIONED_ONLY;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheAtomicityMode atomicityMode() {
        return GridCacheAtomicityMode.ATOMIC;
    }

    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setPreloadMode(GridCachePreloadMode.SYNC);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteration()  throws Exception {
        try {
            final GridCache<String, Integer> cache = cache();

            final int entryCnt = 1000;

            for (int i = 0; i < entryCnt; i++)
                cache.put(String.valueOf(i), i);

            Collection<GridCacheEntry<String, Integer>> entries = new ArrayList<>(10_000);

            for (int i = 0; i < 10_000; i++)
                entries.add(cache.randomEntry());

            startGrid(1);
            startGrid(2);
            startGrid(3);

            for (GridCacheEntry<String, Integer> entry : entries)
                entry.partition();

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

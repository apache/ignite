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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteState;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgniteCacheNearOffheapGetSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        if (nearEnabled())
            grid(gridCount() - 1).getOrCreateCache(new CacheConfiguration(), nearConfiguration());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected NearCacheConfiguration nearConfiguration() {
        NearCacheConfiguration nearCfg = super.nearConfiguration();

        nearCfg.setNearEvictionPolicy(new FifoEvictionPolicy(100));

        return nearCfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (getTestGridName(gridCount() - 1).equals(gridName)) {
            cfg.setClientMode(true);

            cfg.setCacheConfiguration();
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setBackups(1);
        cfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    @Override
    protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetFromNear() throws Exception {

        IgniteCache<Object, Object> nearOnly = ignite(gridCount() - 1).cache(null);

        // Start extra node.
        IgniteEx ignite = startGrid(gridCount());

        try {
            final int keyCnt = 30;

            for (int i = 0; i < keyCnt; i++)
                ignite(0).cache(null).put(i, i);

            for (int i = 0; i < keyCnt; i++)
                assertEquals(i, nearOnly.get(i));

            Collection<Integer> invalidatedKeys = new ArrayList<>();

            Affinity<Object> cacheAff = ignite.affinity(null);

            // Going to stop the last node.
            for (int i = 0; i < keyCnt; i++) {
                if (cacheAff.mapKeyToNode(i).equals(ignite.localNode()))
                    invalidatedKeys.add(i);
            }

            stopGrid(gridCount());

            for (Integer key : invalidatedKeys)
                assertEquals(key, nearOnly.get(key));
        }
        finally {
            if (Ignition.state(getTestGridName(gridCount())) == IgniteState.STARTED)
                stopGrid(gridCount());
        }
    }
}
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

import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Leak test.
 */
public class GridCacheLeakTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "data";

    /** Iterations to run. */
    private static final int ITERS = 10_000;

    /** Atomicity mode. */
    private CacheAtomicityMode atomicityMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * Gets cache configuration.
     *
     * @return Data cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(CACHE_NAME);

        cfg.setAffinity(new RendezvousAffinityFunction(false, 128));

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(1);
        cfg.setNearConfiguration(null);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setAtomicityMode(atomicityMode);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLeakTransactional() throws Exception {
        checkLeak(TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLeakAtomic() throws Exception {
        checkLeak(ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkLeak(CacheAtomicityMode mode) throws Exception {
        atomicityMode = mode;

        startGrids(3);

        try {
            int i = 0;

            IgniteCache<Object, Object> cache = grid(0).cache(CACHE_NAME);

            while (!Thread.currentThread().isInterrupted()) {
                UUID key = UUID.randomUUID();

                cache.put(key, 0);

                cache.remove(key);
                cache.remove(key);

                i++;

                if (i % 1000 == 0)
                    info("Put: " + i);

                if (i % 5000 == 0) {
                    for (int g = 0; g < 3; g++) {
                        GridCacheConcurrentMap map = ((IgniteKernal)grid(g)).internalCache(CACHE_NAME).map();

                        info("Map size for cache [g=" + g + ", size=" + map.internalSize() +
                            ", pubSize=" + map.publicSize(CU.cacheId(CACHE_NAME)) + ']');

                        assertTrue("Wrong map size: " + map.internalSize(), map.internalSize() <= 8192);
                    }
                }

                if (i == ITERS)
                    break;
            }
        }
        finally {
            stopAllGrids();
        }
    }
}

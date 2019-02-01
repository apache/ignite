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

import java.util.Map;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager.TOP_VER_BASE_TIME;

/**
 *
 */
public class GridCacheEntryVersionSelfTest extends GridCommonAbstractTest {
    /** Atomicity mode. */
    private CacheAtomicityMode atomicityMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionAtomic() throws Exception {
        atomicityMode = ATOMIC;

        checkVersion();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionTransactional() throws Exception {
        atomicityMode = TRANSACTIONAL;

        checkVersion();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionMvccTx() throws Exception {
        atomicityMode = TRANSACTIONAL_SNAPSHOT;

        checkVersion();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkVersion() throws Exception {
        startGridsMultiThreaded(3);

        try {
            Map<Integer,Integer> map = F.asMap(1, 1, 2, 2, 3, 3);

            for (Integer key : map.keySet()) {
                info("Affinity nodes [key=" + key + ", nodes=" +
                    F.viewReadOnly(grid(0).affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(key), F.node2id()) + ']');
            }

            grid(0).cache(DEFAULT_CACHE_NAME).putAll(map);

            for (int g = 0; g < 3; g++) {
                IgniteKernal grid = (IgniteKernal)grid(g);

                for (Integer key : map.keySet()) {
                    GridCacheAdapter<Object, Object> cache = grid.internalCache(DEFAULT_CACHE_NAME);

                    GridCacheEntryEx entry = cache.peekEx(key);

                    if (entry != null) {
                        GridCacheVersion ver = entry.version();

                        long order = grid.affinity(DEFAULT_CACHE_NAME).mapKeyToNode(key).order();

                        // Check topology version.
                        assertEquals(3, ver.topologyVersion() -
                            (grid.context().discovery().gridStartTime() - TOP_VER_BASE_TIME) / 1000);

                        // Check node order.
                        assertEquals("Failed for key: " + key, order, ver.nodeOrder());
                    }
                }
            }

            startGrid(3);

            grid(0).cache(DEFAULT_CACHE_NAME).putAll(map);

            for (int g = 0; g < 4; g++) {
                IgniteKernal grid = (IgniteKernal)grid(g);

                for (Integer key : map.keySet()) {
                    GridCacheAdapter<Object, Object> cache = grid.internalCache(DEFAULT_CACHE_NAME);

                    GridCacheEntryEx entry = cache.peekEx(key);

                    if (entry != null) {
                        GridCacheVersion ver = entry.version();

                        long order = grid.affinity(DEFAULT_CACHE_NAME).mapKeyToNode(key).order();

                        // Check topology version.
                        assertEquals(4, ver.topologyVersion() -
                            (grid.context().discovery().gridStartTime() - TOP_VER_BASE_TIME) / 1000);

                        // Check node order.
                        assertEquals("Failed for key: " + key, order, ver.nodeOrder());
                    }
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }
}

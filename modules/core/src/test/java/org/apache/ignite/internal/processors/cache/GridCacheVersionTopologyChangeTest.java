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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridCacheVersionTopologyChangeTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionIncreaseAtomic() throws Exception {
        checkVersionIncrease(cacheConfigurations(ATOMIC));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionIncreaseTx() throws Exception {
        checkVersionIncrease(cacheConfigurations(TRANSACTIONAL));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionIncreaseMvccTx() throws Exception {
        checkVersionIncrease(cacheConfigurations(TRANSACTIONAL_SNAPSHOT));
    }

    /**
     * @param ccfgs Cache configurations.
     * @throws Exception If failed.
     */
    private void checkVersionIncrease(List<CacheConfiguration<Object, Object>> ccfgs) throws Exception {
        try {
            assert !ccfgs.isEmpty();

            Ignite ignite = startGrid(0);

            List<IgniteCache<Object, Object>> caches = new ArrayList<>();
            List<Set<Integer>> cachesKeys = new ArrayList<>();

            for (CacheConfiguration<Object, Object> ccfg : ccfgs) {
                IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

                caches.add(cache);

                Affinity<Object> aff = ignite.affinity(ccfg.getName());

                int parts = aff.partitions();

                assert parts > 0 : parts;

                Set<Integer> keys = new HashSet<>();

                for (int p = 0; p < parts; p++) {
                    for (int k = 0; k < 100_000; k++) {
                        if (aff.partition(k) == p) {
                            assertTrue(keys.add(k));

                            break;
                        }
                    }
                }

                assertEquals(parts, keys.size());

                cachesKeys.add(keys);
            }

            List<Map<Integer, Comparable>> cachesVers = new ArrayList<>();

            for (int i = 0; i < caches.size(); i++) {
                IgniteCache<Object, Object> cache = caches.get(i);

                Map<Integer, Comparable> vers = new HashMap<>();

                for (Integer k : cachesKeys.get(i)) {
                    cache.put(k, k);

                    vers.put(k, cache.getEntry(k).version());
                }

                cachesVers.add(vers);
            }

            for (int i = 0; i < caches.size(); i++) {
                for (int k = 0; k < 10; k++)
                    checkVersionIncrease(caches.get(i), cachesVers.get(i));
            }

            int nodeIdx = 1;

            for (int n = 0; n < SF.applyLB(10, 2); n++) {
                startGrid(nodeIdx++);

                for (int i = 0; i < caches.size(); i++)
                    checkVersionIncrease(caches.get(i), cachesVers.get(i));

                awaitPartitionMapExchange();

                for (int i = 0; i < caches.size(); i++)
                    checkVersionIncrease(caches.get(i), cachesVers.get(i));
            }

            for (int n = 1; n < nodeIdx; n++) {
                log.info("Stop node: " + n);

                stopGrid(n);

                for (int i = 0; i < caches.size(); i++)
                    checkVersionIncrease(caches.get(i), cachesVers.get(i));

                awaitPartitionMapExchange();

                for (int i = 0; i < caches.size(); i++)
                    checkVersionIncrease(caches.get(i), cachesVers.get(i));
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param cache Cache.
     * @param vers Current versions.
     */
    @SuppressWarnings("unchecked")
    private void checkVersionIncrease(IgniteCache<Object, Object> cache, Map<Integer, Comparable> vers) {
        for (Integer k : vers.keySet()) {
            cache.put(k, k);

            Comparable curVer = vers.get(k);

            CacheEntry entry = cache.getEntry(k);

            if (entry != null) {
                Comparable newVer = entry.version();

                assertTrue(newVer.compareTo(curVer) > 0);

                vers.put(k, newVer);
            }
            else {
                CacheConfiguration ccfg = cache.getConfiguration(CacheConfiguration.class);

                assertEquals(0, ccfg.getBackups());
            }
        }
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configurations.
     */
    private List<CacheConfiguration<Object, Object>> cacheConfigurations(CacheAtomicityMode atomicityMode) {
        List<CacheConfiguration<Object, Object>> ccfgs = new ArrayList<>();

        ccfgs.add(cacheConfiguration("c1", atomicityMode, new RendezvousAffinityFunction(), 0));
        ccfgs.add(cacheConfiguration("c2", atomicityMode, new RendezvousAffinityFunction(), 1));
        ccfgs.add(cacheConfiguration("c3", atomicityMode, new RendezvousAffinityFunction(false, 10), 0));

        return ccfgs;
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Cache atomicity mode.
     * @param aff Affinity.
     * @param backups Backups number.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(String name,
        CacheAtomicityMode atomicityMode,
        AffinityFunction aff,
        int backups) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setBackups(backups);
        ccfg.setName(name);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setAffinity(aff);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }
}

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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;

import java.lang.reflect.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.internal.processors.cache.GridCacheAdapter.*;

/**
 * Test {@link GridCache#clearLocally()} operations in multinode environment with nodes having caches with different names.
 */
public class GridCacheClearLocallySelfTest extends GridCommonAbstractTest {
    /** Local cache. */
    private static final String CACHE_LOCAL = "cache_local";

    /** Partitioned cache. */
    private static final String CACHE_PARTITIONED = "cache_partitioned";

    /** Co-located cache. */
    private static final String CACHE_COLOCATED = "cache_colocated";

    /** Replicated cache. */
    private static final String CACHE_REPLICATED = "cache_replicated";

    /** Grid nodes count. */
    private static final int GRID_CNT = 3;

    /** VM IP finder for TCP discovery SPI. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Local caches. */
    private GridCache<Integer, Integer>[] cachesLoc;

    /** Partitioned caches. */
    private GridCache<Integer, Integer>[] cachesPartitioned;

    /** Colocated caches. */
    private GridCache<Integer, Integer>[] cachesColocated;

    /** Replicated caches. */
    private GridCache<Integer, Integer>[] cachesReplicated;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfgLoc = new CacheConfiguration();

        ccfgLoc.setName(CACHE_LOCAL);
        ccfgLoc.setCacheMode(LOCAL);
        ccfgLoc.setWriteSynchronizationMode(FULL_SYNC);
        ccfgLoc.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration ccfgPartitioned = new CacheConfiguration();

        ccfgPartitioned.setName(CACHE_PARTITIONED);
        ccfgPartitioned.setCacheMode(PARTITIONED);
        ccfgPartitioned.setBackups(1);
        ccfgPartitioned.setWriteSynchronizationMode(FULL_SYNC);
        NearCacheConfiguration nearCfg = new NearCacheConfiguration();

        ccfgPartitioned.setNearConfiguration(nearCfg);

        ccfgPartitioned.setNodeFilter(new AttributeFilter(getTestGridName(0)));

        ccfgPartitioned.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration ccfgColocated = new CacheConfiguration();

        ccfgColocated.setName(CACHE_COLOCATED);
        ccfgColocated.setCacheMode(PARTITIONED);
        ccfgColocated.setBackups(1);
        ccfgColocated.setWriteSynchronizationMode(FULL_SYNC);
        ccfgColocated.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration ccfgReplicated = new CacheConfiguration();

        ccfgReplicated.setName(CACHE_REPLICATED);
        ccfgReplicated.setCacheMode(REPLICATED);
        ccfgReplicated.setWriteSynchronizationMode(FULL_SYNC);
        ccfgReplicated.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(ccfgLoc, ccfgPartitioned, ccfgColocated, ccfgReplicated);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cachesLoc = null;
        cachesPartitioned = null;
        cachesColocated = null;
        cachesReplicated = null;
    }

    /**
     * Startup routine.
     *
     * @throws Exception If failed.
     */
    private void startUp() throws Exception {
        cachesLoc = (GridCache<Integer, Integer>[])Array.newInstance(GridCache.class, GRID_CNT);
        cachesPartitioned = (GridCache<Integer, Integer>[])Array.newInstance(GridCache.class, GRID_CNT);
        cachesColocated = (GridCache<Integer, Integer>[])Array.newInstance(GridCache.class, GRID_CNT);
        cachesReplicated = (GridCache<Integer, Integer>[])Array.newInstance(GridCache.class, GRID_CNT);

        for (int i = 0; i < GRID_CNT; i++) {
            Ignite ignite = startGrid(i);

            if (i == 1) {
                NearCacheConfiguration nearCfg = new NearCacheConfiguration();

                ignite.createNearCache(CACHE_PARTITIONED, nearCfg);
            }

            if (i == 2)
                ignite.cache(CACHE_PARTITIONED);

            cachesLoc[i] = ((IgniteKernal)ignite).getCache(CACHE_LOCAL);
            cachesPartitioned[i] = ((IgniteKernal)ignite).getCache(CACHE_PARTITIONED);
            cachesColocated[i] = ((IgniteKernal)ignite).getCache(CACHE_COLOCATED);
            cachesReplicated[i] = ((IgniteKernal)ignite).getCache(CACHE_REPLICATED);
        }
    }

    /**
     * Test {@link GridCache#clearLocally()} on LOCAL cache with no split.
     *
     * @throws Exception If failed.
     */
    public void testLocalNoSplit() throws Exception {
        test(Mode.TEST_LOCAL, CLEAR_ALL_SPLIT_THRESHOLD / 2);
    }

    /**
     * Test {@link GridCache#clearLocally()} on LOCAL cache with split.
     *
     * @throws Exception If failed.
     */
    public void testLocalSplit() throws Exception {
        test(Mode.TEST_LOCAL, CLEAR_ALL_SPLIT_THRESHOLD + 1);
    }

    /**
     * Test {@link GridCache#clearLocally()} on PARTITIONED cache with no split.
     *
     * @throws Exception If failed.
     */
    public void testPartitionedNoSplit() throws Exception {
        test(Mode.TEST_PARTITIONED, CLEAR_ALL_SPLIT_THRESHOLD / 2);
    }

    /**
     * Test {@link GridCache#clearLocally()} on PARTITIONED cache with split.
     *
     * @throws Exception If failed.
     */
    public void testPartitionedSplit() throws Exception {
        test(Mode.TEST_PARTITIONED, CLEAR_ALL_SPLIT_THRESHOLD + 1);
    }

    /**
     * Test {@link GridCache#clearLocally()} on co-located cache with no split.
     *
     * @throws Exception If failed.
     */
    public void testColocatedNoSplit() throws Exception {
        test(Mode.TEST_COLOCATED, CLEAR_ALL_SPLIT_THRESHOLD / 2);
    }

    /**
     * Test {@link GridCache#clearLocally()} on co-located cache with split.
     *
     * @throws Exception If failed.
     */
    public void testColocatedSplit() throws Exception {
        test(Mode.TEST_COLOCATED, CLEAR_ALL_SPLIT_THRESHOLD + 1);
    }

    /**
     * Test {@link GridCache#clearLocally()} on REPLICATED cache with no split.
     *
     * @throws Exception If failed.
     */
    public void testReplicatedNoSplit() throws Exception {
        test(Mode.TEST_REPLICATED, CLEAR_ALL_SPLIT_THRESHOLD / 2);
    }

    /**
     * Test {@link GridCache#clearLocally()} on REPLICATED cache with split.
     *
     * @throws Exception If failed.
     */
    public void testReplicatedSplit() throws Exception {
        test(Mode.TEST_REPLICATED, CLEAR_ALL_SPLIT_THRESHOLD + 1);
    }

    /**
     * Internal method for all tests.
     *
     * @param mode Test mode
     * @param keysCnt Keys count.
     * @throws Exception In case of exception.
     */
    private void test(Mode mode, int keysCnt) throws Exception {
        startUp();

        switch (mode) {
            case TEST_LOCAL: {
                // Check on only one node.
                GridCache<Integer, Integer> cache = cachesLoc[0];

                fillCache(cache, keysCnt);

                cache.clearLocally();

                assert cache.isEmpty();

                break;
            }

            case TEST_PARTITIONED: {
                // Take in count special case for near-only cache as well.
                fillCache(cachesPartitioned[0], keysCnt);

                // Ensure correct no-op clean of CLIENT_ONLY cache.
                warmCache(cachesPartitioned[2], keysCnt);
                assert cachesPartitioned[2].isEmpty() : cachesPartitioned[2].values();
                cachesPartitioned[2].clearLocally();
                assert cachesPartitioned[2].isEmpty();

                stopGrid(2); // Shutdown Grid in order to remove reader in NEAR_PARTITIONED cache.

                // Ensure correct clearLocally of NEA_ONLY cache.
                warmCache(cachesPartitioned[1], keysCnt);
                assert !cachesPartitioned[1].isEmpty();
                cachesPartitioned[1].clearLocally();
                assert cachesPartitioned[1].isEmpty();
                fillCache(cachesPartitioned[1], keysCnt);

                stopGrid(1); // Shutdown Grid in order to remove reader in NEAR_PARTITIONED cache.

                // Ensure correct clearLocally of NEAR_PARTITIONED cache.
                assert !cachesPartitioned[0].isEmpty();
                cachesPartitioned[0].clearLocally();
                assert cachesPartitioned[0].isEmpty();

                break;
            }

            default: {
                assert mode == Mode.TEST_COLOCATED || mode == Mode.TEST_REPLICATED;

                GridCache<Integer, Integer>[] caches = mode == Mode.TEST_COLOCATED ? cachesColocated : cachesReplicated;

                fillCache(caches[0], keysCnt);

                for (GridCache<Integer, Integer> cache : caches) {
                    assert !cache.isEmpty();

                    cache.clearLocally();

                    assert cache.isEmpty();
                }
            }
        }
    }

    /**
     * Fill cache with values.
     *
     * @param cache Cache.
     * @param keysCnt Amount of keys to put.
     * @throws Exception If failed.
     */
    private void fillCache(GridCache<Integer, Integer> cache, int keysCnt) throws Exception {
        try (Transaction tx = cache.txStart()) {
            for (int i = 0; i < keysCnt; i++)
                cache.put(i, i);

            tx.commit();
        }
    }

    /**
     * Warm cache up.
     *
     * @param cache Cache.
     * @param keysCnt Amount of keys to get.
     * @throws Exception If failed.
     */
    private void warmCache(GridCache<Integer, Integer> cache, int keysCnt) throws Exception {
        for (int i = 0; i < keysCnt; i++)
            cache.get(i);
    }

    /**
     * Test mode.
     */
    private enum Mode {
        /** Local cache. */
        TEST_LOCAL,

        /** Partitioned cache. */
        TEST_PARTITIONED,

        /** Co-located cache. */
        TEST_COLOCATED,

        /** Replicated cache. */
        TEST_REPLICATED
    }

    /**
     *
     */
    private static class AttributeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private String[] attrs;

        /**
         * @param attrs Attribute values.
         */
        private AttributeFilter(String... attrs) {
            this.attrs = attrs;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            String gridName = node.attribute(IgniteNodeAttributes.ATTR_GRID_NAME);

            for (String attr : attrs) {
                if (F.eq(attr, gridName))
                    return true;
            }

            return false;
        }
    }
}

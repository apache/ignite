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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** */
public class TestNodeRestartsAfterDeletionOfNodeFilteredCache extends GridCommonAbstractTest {
    /** */
    private static final String DYNAMIC_CACHE_NAME = "TestDynamicCache";

    /** */
    private static final String TEST_ATTRIBUTE_NAME = "TEST_ATTRIBUTE_NAME";

    /** */
    public static final IgnitePredicate<ClusterNode> NODE_FILTER = new IgnitePredicate<ClusterNode>() {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode n) {
            Boolean val = n.attribute(TEST_ATTRIBUTE_NAME);

            return val != null && val;
        }
    };

    /** */
    private boolean testAttribute = true;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setUserAttributes(F.asMap(TEST_ATTRIBUTE_NAME, testAttribute));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().
                setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        return cfg;
    }

    /** */
    @Test
    public void test0() throws Exception {
        cleanPersistenceDir();

        startFilteredGrid(0);
        startGrid(1);
        startGrid(2);

        grid(0).cluster().state(ClusterState.ACTIVE);

        createAndFillCache();

        stopAllGrids();

        startFilteredGrid(0);

        grid(0).cluster().state(ClusterState.ACTIVE);

        assertThrows(null, () -> {
            grid(0).createCache(defaultCacheConfiguration().setName(DYNAMIC_CACHE_NAME));
        }, IgniteException.class, "cache with the same name is already started");

        startGrid(1);
        startGrid(2);
    }

    /** */
    @Test
    public void testNodeRejoinsClusterAfterDeletedOfNodeFilteredCache() throws Exception {
        cleanPersistenceDir();

        startGrids(2);

        int filteredGridIdx = G.allGrids().size();

        startFilteredGrid(filteredGridIdx);

        grid(0).cluster().state(ClusterState.ACTIVE);

        grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

        int nonBaselineIdx = G.allGrids().size();

        startGrid(nonBaselineIdx);

        createAndFillCache();
        grid(0).destroyCache(DYNAMIC_CACHE_NAME);
        awaitPartitionMapExchange();

        // Try just restart grid.
        stopGrid(filteredGridIdx);
        stopGrid(nonBaselineIdx);
        startFilteredGrid(filteredGridIdx);
        startFilteredGrid(nonBaselineIdx);

        createAndFillCache();

        // Destroy cache after test node stops.
        stopGrid(filteredGridIdx);
        stopGrid(nonBaselineIdx);

        grid(0).destroyCache(DYNAMIC_CACHE_NAME);
        awaitPartitionMapExchange();

        // Ensure nodes join cluster.
        startFilteredGrid(filteredGridIdx);
        startFilteredGrid(nonBaselineIdx);
    }

    /** */
    private CacheConfiguration createAndFillCache() throws InterruptedException {
        final CacheConfiguration<Object, Object> cfg = defaultCacheConfiguration()
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setName(DYNAMIC_CACHE_NAME)
            .setNodeFilter(NODE_FILTER);

        IgniteCache<Object, Object> cache = grid(0).createCache(cfg);

        awaitPartitionMapExchange();

        try (IgniteDataStreamer<Integer, Integer> ds = grid(0).dataStreamer(DYNAMIC_CACHE_NAME)) {
            for (int i = 0; i < 100; ++i)
                ds.addData(i, i);
        }

        return cfg;
    }

    /** */
    private IgniteEx startFilteredGrid(int idx) throws Exception {
        testAttribute = false;

        IgniteEx res = startGrid(idx);

        testAttribute = true;

        return res;
    }
}

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
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class TestNodeRestartsAfterDeletionOfNodeFilteredCache extends GridCommonAbstractTest {
    /** */
    private static final String DYNAMIC_CACHE_NAME = "TestDynamicCache";

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
    public void testNodeRejoinsClusterAfterDeletedOfNodeFilteredCache() throws Exception {
        startGrid(0);
        startGrid(1);

        testAttribute = false;

        startGrid(2);

        grid(0).cluster().state(ClusterState.ACTIVE);

        final CacheConfiguration<Object, Object> cfg = defaultCacheConfiguration()
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setName(DYNAMIC_CACHE_NAME)
            .setNodeFilter(NODE_FILTER);

        IgniteCache<Object, Object> cache = grid(0).createCache(cfg);

        awaitPartitionMapExchange();

        for (int i = 0; i < 100; ++i)
            cache.put(i, i);

        // This fixes the issue!
        cache = grid(2).cache(cfg.getName());

        grid(0).destroyCache(DYNAMIC_CACHE_NAME);

        awaitPartitionMapExchange();

        // Try just restart grid.
        stopGrid(2);

        startGrid(2);
    }
}

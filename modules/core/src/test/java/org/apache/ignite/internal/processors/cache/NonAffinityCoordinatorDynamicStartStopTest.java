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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class NonAffinityCoordinatorDynamicStartStopTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_ATTRIBUTE = "test-attribute";

    /** Dummy grid name. */
    private static final String DUMMY_GRID_NAME = "dummy";

    /** Cache configuration. */
    private static final CacheConfiguration<Integer, Integer> CCFG = new CacheConfiguration<Integer, Integer>("cache")
        .setAffinity(new RendezvousAffinityFunction(false, 32))
        .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
        .setNodeFilter(new TestNodeFilter())
        .setGroupName("Group");

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(200L * 1024 * 1024));

        cfg.setDataStorageConfiguration(memCfg);

        if (gridName.contains(DUMMY_GRID_NAME))
            cfg.setUserAttributes(F.asMap(TEST_ATTRIBUTE, false));
        else
            cfg.setUserAttributes(F.asMap(TEST_ATTRIBUTE, true));

        cfg.setConsistentId(gridName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        final Ignite crd = startGrid(DUMMY_GRID_NAME);

        crd.active(true);

        startClientGrid("client");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartStop() throws Exception {
        startGrids(2);

        awaitPartitionMapExchange();

        Ignite ig = ignite(0);

        IgniteCache<Integer, Integer> cache = ig.getOrCreateCache(CCFG);

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        cache.destroy();

        grid(DUMMY_GRID_NAME).createCache(CCFG);
    }

    /** {@inheritDoc} */
    @Override protected boolean checkTopology() {
        // We start a dummy node in the beginning of the each test,
        // so default startGrids(n) will fail on wait for .
        return false;
    }

    /**
     *
     */
    private static class TestNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            return Boolean.TRUE.equals(clusterNode.attribute(TEST_ATTRIBUTE));
        }
    }
}

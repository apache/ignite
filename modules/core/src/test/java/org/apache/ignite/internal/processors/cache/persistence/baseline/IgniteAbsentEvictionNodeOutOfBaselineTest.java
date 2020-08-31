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

package org.apache.ignite.internal.processors.cache.persistence.baseline;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test absenting eviction for joined node if it is out of baseline.
 */
public class IgniteAbsentEvictionNodeOutOfBaselineTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_CACHE_NAME = "test";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalSegmentSize(512 * 1024)
            .setWalSegments(4)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(256 * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
    }

    /**
     * Removed partitions if node is out of baseline.
     */
    @Test
    public void testPartitionsRemovedIfJoiningNodeNotInBaseline() throws Exception {
        //given: start 3 nodes with data
        Ignite ignite0 = startGrids(3);

        ignite0.cluster().baselineAutoAdjustEnabled(false);
        ignite0.cluster().active(true);

        IgniteCache<Object, Object> cache = ignite0.getOrCreateCache(TEST_CACHE_NAME);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        //when: stop one node and reset baseline topology
        stopGrid(2);

        resetBaselineTopology();

        ignite0.resetLostPartitions(Collections.singleton(TEST_CACHE_NAME));

        awaitPartitionMapExchange();

        for (int i = 0; i < 200; i++)
            cache.put(i, i);

        //then: after returning stopped node to grid its partitions should be removed
        IgniteEx ignite2 = startGrid(2);

        List<GridDhtLocalPartition> partitions = ignite2.cachex(TEST_CACHE_NAME).context().topology().localPartitions();

        assertTrue("Should be empty : " + partitions, partitions.isEmpty());
    }
}

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

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test scenario: supplier is left during rebalancing leaving partition in OWNING state (but actually LOST)
 * because only one owner left.
 * <p>
 * Expected result: no assertions are triggered.
 */
@WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
public class CachePartitionLostWhileClearingTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS_CNT = 64;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(
                new DataStorageConfiguration()
                        .setWalMode(WALMode.LOG_ONLY)
                        .setWalSegmentSize(4 * 1024 * 1024)
                        .setDefaultDataRegionConfiguration(
                                new DataRegionConfiguration()
                                        .setPersistenceEnabled(true)
                                        .setMaxSize(100L * 1024 * 1024))
        );

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
                setAtomicityMode(TRANSACTIONAL).
                setCacheMode(PARTITIONED).
                setBackups(1).
                setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT)));

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLostWhileClearing_FailOnCrd() throws Exception {
        doTestPartitionLostWhileClearing(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLostWhileClearing_FailOnFullMessage() throws Exception {
        doTestPartitionLostWhileClearing(3);
    }

    /**
     * @param cnt Nodes count.
     *
     * @throws Exception If failed.
     */
    private void doTestPartitionLostWhileClearing(int cnt) throws Exception {
        try {
            IgniteEx crd = startGrids(cnt);

            crd.cluster().active(true);

            int partId = -1;
            int idx0 = 0;
            int idx1 = 1;

            for (int p = 0; p < PARTS_CNT; p++) {
                List<ClusterNode> nodes = new ArrayList<>(crd.affinity(DEFAULT_CACHE_NAME).mapPartitionToPrimaryAndBackups(p));

                if (grid(nodes.get(0)) == grid(idx0) && grid(nodes.get(1)) == grid(idx1)) {
                    partId = p;

                    break;
                }
            }

            assertTrue(partId >= 0);

            load(grid(idx0), DEFAULT_CACHE_NAME, partId, 10_000, 0);

            stopGrid(idx1);

            load(grid(idx0), DEFAULT_CACHE_NAME, partId, 10, 10_000);

            IgniteEx g1 = startGrid(idx1);

            stopGrid(idx0); // Restart supplier in the middle of clearing.

            awaitPartitionMapExchange();

            // Expecting partition in OWNING state.
            assertNotNull(counter(partId, DEFAULT_CACHE_NAME, g1.name()));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param ignite Ignite.
     * @param cache Cache.
     * @param partId Partition id.
     * @param cnt Count.
     * @param skip Skip.
     */
    private void load(IgniteEx ignite, String cache, int partId, int cnt, int skip) {
        List<Integer> keys = partitionKeys(ignite.cache(cache), partId, cnt, skip);

        try(IgniteDataStreamer<Object, Object> s = ignite.dataStreamer(cache)) {
            for (Integer key : keys)
                s.addData(key, key);
        }
    }
}

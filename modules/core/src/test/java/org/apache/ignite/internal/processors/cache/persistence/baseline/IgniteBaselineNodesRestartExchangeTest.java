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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests the local affinity recalculation exchange in case of leaving baseline nodes.
 */
@RunWith(JUnit4.class)
public class IgniteBaselineNodesRestartExchangeTest extends GridCommonAbstractTest {
    /** Grids count. */
    private static final int GRIDS_COUNT = 8;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(100L * 1024 * 1024)
                )
                .setWalSegmentSize(1024 * 1024)
        );

        if (igniteInstanceName.contains("client"))
            cfg.setClientMode(true);

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRestartBaselineNodes() throws Exception {
        Ignite ignite = startGridsMultiThreaded(GRIDS_COUNT);

        startGrid("client");

        // Payload for local affinity recalculation.
        for (int i = 0; i < 10; i++) {
            CacheConfiguration<Integer, Long> txCfg = new CacheConfiguration<Integer, Long>()
                .setName("test-cache-" + i)
                .setBackups(GRIDS_COUNT / 2);

            ignite.createCache(txCfg);
        }

        ignite.cluster().active(true);

        awaitPartitionMapExchange();

        // Kill first three nodes.
        AtomicInteger killIdx = new AtomicInteger();

        IgniteInternalFuture<Long> killFut = GridTestUtils.runMultiThreadedAsync(() ->
            stopGrid(killIdx.getAndIncrement(), true), 3, "kill-node");

        killFut.get();

        awaitPartitionMapExchange();

        // Start first three nodes and kill next three ones.
        AtomicInteger startIdx = new AtomicInteger();

        IgniteInternalFuture<Long> startFut = GridTestUtils.runMultiThreadedAsync(() ->
            startGrid(startIdx.getAndIncrement()), 3, "start-node");

        killFut = GridTestUtils.runMultiThreadedAsync(() ->
            stopGrid(killIdx.getAndIncrement(), true), 3, "kill-node");

        killFut.get();

        awaitPartitionMapExchange();

        startFut.get();

        awaitPartitionMapExchange();
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
}

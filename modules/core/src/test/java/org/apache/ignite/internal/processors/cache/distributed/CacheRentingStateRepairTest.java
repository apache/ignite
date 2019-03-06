/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
public class CacheRentingStateRepairTest extends GridCommonAbstractTest {
    /** */
    public static final int PARTS = 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS).setPartitions(64));

        ccfg.setOnheapCacheEnabled(false);

        ccfg.setBackups(1);

        ccfg.setRebalanceBatchSize(100);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setCacheConfiguration(ccfg);

        cfg.setActiveOnStart(false);

        cfg.setConsistentId(igniteInstanceName);

        long sz = 100 * 1024 * 1024;

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setPageSize(1024)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true).setInitialSize(sz).setMaxSize(sz))
            .setWalMode(WALMode.LOG_ONLY).setCheckpointFrequency(24L * 60 * 60 * 1000);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IGNITE_BASELINE_AUTO_ADJUST_ENABLED, "false");

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(IGNITE_BASELINE_AUTO_ADJUST_ENABLED);
    }

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName(int idx) {
        return "node" + idx;
    }

    /**
     *
     */
    @Test
    public void testRentingStateRepairAfterRestart() throws Exception {
        try {
            IgniteEx g0 = startGrid(0);

            startGrid(1);

            g0.cluster().active(true);

            awaitPartitionMapExchange();

            List<Integer> parts = evictingPartitionsAfterJoin(g0, g0.cache(DEFAULT_CACHE_NAME), 20);

            int toEvictPart = parts.get(0);

            int k = 0;

            while (g0.affinity(DEFAULT_CACHE_NAME).partition(k) != toEvictPart)
                k++;

            g0.cache(DEFAULT_CACHE_NAME).put(k, k);

            GridDhtPartitionTopology top = dht(g0.cache(DEFAULT_CACHE_NAME)).topology();

            GridDhtLocalPartition part = top.localPartition(toEvictPart);

            assertNotNull(part);

            // Prevent eviction.
            part.reserve();

            startGrid(2);

            g0.cluster().setBaselineTopology(3);

            // Wait until all is evicted except first partition.
            assertTrue("Failed to wait for partition eviction", waitForCondition(() -> {
                for (int i = 1; i < parts.size(); i++) { // Skip reserved partition.
                    Integer p = parts.get(i);

                    if (top.localPartition(p).state() != GridDhtPartitionState.EVICTED)
                        return false;
                }

                return true;
            }, 5000));

            /**
             * Force renting state before node stop.
             * This also could be achieved by stopping node just after RENTING state is set.
             */
            part.setState(GridDhtPartitionState.RENTING);

            assertEquals(GridDhtPartitionState.RENTING, part.state());

            stopGrid(0);

            g0 = startGrid(0);

            awaitPartitionMapExchange();

            part = dht(g0.cache(DEFAULT_CACHE_NAME)).topology().localPartition(toEvictPart);

            assertNotNull(part);

            final GridDhtLocalPartition finalPart = part;

            CountDownLatch clearLatch = new CountDownLatch(1);

            part.onClearFinished(fut -> {
                assertEquals(GridDhtPartitionState.EVICTED, finalPart.state());

                clearLatch.countDown();
            });

            assertTrue("Failed to wait for partition eviction after restart",
                clearLatch.await(5_000, TimeUnit.MILLISECONDS));
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }
}

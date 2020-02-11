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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.PartitionsEvictManagerAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/**
 *
 */
public class GridCacheScheduleResendPartitionsAfterEvictionTest extends PartitionsEvictManagerAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(false)));

        CacheConfiguration cc = new CacheConfiguration(DEFAULT_CACHE_NAME).setBackups(1);

        cfg.setCacheConfiguration(cc);

        return cfg;
    }

    /**
     * Check that listeners that schedule resend partitions after eviction doesn't added to renting future
     * uncontrollably. At most one listener is expected.
     *
     * @throws Exception If failed.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testRentingFuturesListenersNotGrowingUncontrollably() throws Exception {
        IgniteEx node1 = startGrid(0);

        startGrid(1);

        node1.cluster().baselineAutoAdjustEnabled(false);

        CountDownLatch latch = new CountDownLatch(1);

        subscribeEvictionQueueAtLatch(node1, latch, false);

        node1.cluster().state(ACTIVE);

        node1.getOrCreateCache(DEFAULT_CACHE_NAME);
        try (IgniteDataStreamer streamer = node1.dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < 100_000; i++)
                streamer.addData(i, i);
        }

        final AtomicInteger nodeIdx = new AtomicInteger(1);

        IgniteInternalFuture res = GridTestUtils.runMultiThreadedAsync(() -> {
                try {
                    startGrid(nodeIdx.incrementAndGet());
                    resetBaselineTopology();
                }
                catch (Exception e) {
                    fail("Failed to start rebalance.");
                }
            },
            3,
            "rebalanceThread");

        // Give some time for rebalance to start.
        Thread.sleep(3000);

        latch.countDown();

        // And some extra time for collecting callbacks.
        Thread.sleep(100);

        CacheGroupContext grpCtx = node1.context().cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME));

        assertNotNull(grpCtx);

        GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl)grpCtx.topology();

        List<GridDhtLocalPartition> locParts = top.localPartitions();

        for (GridDhtLocalPartition localPartition : locParts) {
            GridFutureAdapter partRentFut = IgniteUtils.field(localPartition, "rent");

            int lsnrCnt = 0;
            for (Object waitNode = IgniteUtils.field(partRentFut, "state");
                waitNode != null; waitNode = IgniteUtils.field(waitNode, "next")) {
                if (IgniteUtils.field(waitNode, "val") != null)
                    lsnrCnt++;
            }

            // At most one listener is expected.
            assertTrue(lsnrCnt <= 1);
        }

        res.get();
    }
}

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
package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class SlowHistoricalRebalanceSmallHistoryTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Slow rebalance cache name. */
    private static final String SLOW_REBALANCE_CACHE = "b13813ce";

    /** Regular cache name. */
    private static final String REGULAR_CACHE = "another-cache";

    /** Supply message latch. */
    private static final AtomicReference<CountDownLatch> SUPPLY_MESSAGE_LATCH = new AtomicReference<>();

    /** Wal history size. */
    private static final int WAL_HISTORY_SIZE = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalHistorySize(WAL_HISTORY_SIZE)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
                .setWalSegmentSize(512 * 1024)
        );

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setCommunicationSpi(new RebalanceBlockingSPI());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        SUPPLY_MESSAGE_LATCH.set(new CountDownLatch(1));

        System.setProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "1000");

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        SUPPLY_MESSAGE_LATCH.get().countDown();

        SUPPLY_MESSAGE_LATCH.set(null);

        System.clearProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD);

        cleanPersistenceDir();
    }

    /**
     * Checks that we reserve and release the same WAL index on exchange.
     */
    public void testReservation() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        ig.getOrCreateCache(new CacheConfiguration<>()
            .setName(SLOW_REBALANCE_CACHE)
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setRebalanceBatchSize(100));

        try (IgniteDataStreamer<Object, Object> streamer = ig.dataStreamer(SLOW_REBALANCE_CACHE)) {
            for (int i = 0; i < 3_000; i++)
                streamer.addData(i, new byte[5 * 1000]);

            streamer.flush();
        }

        startGrid(1);

        resetBaselineTopology();

        IgniteCache<Object, Object> anotherCache = ig.getOrCreateCache(new CacheConfiguration<>()
            .setName(REGULAR_CACHE)
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setRebalanceBatchSize(100));

        Thread.sleep(7_000); // To let distributedExchange() finish.

        for (int i = 0; i < WAL_HISTORY_SIZE; i++) {
            for (int j = 0; i < 500; i++)
                anotherCache.put(j, new byte[5 * 1000]);

            forceCheckpoint(); // Checkpoints where partition is OWNING on grid(0), MOVING on grid(1)

            for (int j = 0; i < 500; i++)
                anotherCache.put(j, new byte[5 * 1000]);
        }

        SUPPLY_MESSAGE_LATCH.get().countDown();

        waitForRebalancing(); // Partition is OWNING on grid(0) and grid(1)

        for (int i = 0; i < 2; i++) {
            for (int j = 0; i < 500; i++)
                anotherCache.put(j, new byte[5 * 1000]);

            forceCheckpoint(); // A few more checkpoints when partition is OWNING everywhere

            for (int j = 0; i < 500; i++)
                anotherCache.put(j, new byte[5 * 1000]);
        }

        stopGrid(0);

        IgniteCache<Object, Object> anotherCacheGrid1 = grid(1).cache(REGULAR_CACHE);

        for (int i = 0; i < 500; i++)
            anotherCacheGrid1.put(i, new byte[5 * 1000]);

        startGrid(0);

        waitForRebalancing();

        assertEquals(2, grid(1).context().discovery().aliveServerNodes().size());
    }

    /**
     *
     */
    private static class RebalanceBlockingSPI extends TcpCommunicationSpi {
        /** */
        public static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                int grpId = ((GridCacheGroupIdMessage)((GridIoMessage)msg).message()).groupId();

                if (grpId == CU.cacheId(SLOW_REBALANCE_CACHE)) {
                    CountDownLatch latch0 = SUPPLY_MESSAGE_LATCH.get();

                    if (latch0 != null)
                        try {
                            latch0.await();
                        }
                        catch (InterruptedException ex) {
                            throw new IgniteException(ex);
                        }
                }
            }

            super.sendMessage(node, msg);
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                int grpId = ((GridCacheGroupIdMessage)((GridIoMessage)msg).message()).groupId();

                if (grpId == CU.cacheId(SLOW_REBALANCE_CACHE)) {
                    CountDownLatch latch0 = SUPPLY_MESSAGE_LATCH.get();

                    if (latch0 != null)
                        try {
                            latch0.await();
                        }
                        catch (InterruptedException ex) {
                            throw new IgniteException(ex);
                        }
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}

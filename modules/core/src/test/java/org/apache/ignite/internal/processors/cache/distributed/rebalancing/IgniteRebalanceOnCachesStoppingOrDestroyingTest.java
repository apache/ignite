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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteRebalanceOnCachesStoppingOrDestroyingTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_1 = "cache_1";

    /** */
    private static final String CACHE_2 = "cache_2";

    /** */
    private static final String CACHE_3 = "cache_3";

    /** */
    private static final String CACHE_4 = "cache_4";

    /** */
    private static final String GROUP_1 = "group_1";

    /** */
    private static final String GROUP_2 = "group_2";

    /** */
    private static final int REBALANCE_BATCH_SIZE = 50 * 1024;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        cfg.setCommunicationSpi(new RebalanceBlockingSPI());

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setRebalanceThreadPoolSize(4);

        cfg.setTransactionConfiguration(new TransactionConfiguration()
            .setDefaultTxTimeout(1000));

        cfg.setDataStorageConfiguration(
                new DataStorageConfiguration()
                        .setWalMode(WALMode.LOG_ONLY)
                        .setDefaultDataRegionConfiguration(
                                new DataRegionConfiguration()
                                        .setPersistenceEnabled(true)
                                        .setMaxSize(100L * 1024 * 1024)));

        return cfg;
    }

    /**
     *
     */
    public void testStopCachesOnDeactivation() throws Exception {
        performTest(ig -> {
            ig.cluster().active(false);

            return null;
        });
    }

    /**
     *
     */
    public void testDestroySpecificCachesInDifferentCacheGroups() throws Exception {
        performTest(ig -> {
            ig.destroyCaches(Arrays.asList(CACHE_1, CACHE_3));

            return null;
        });
    }

    /**
     *
     */
    public void testDestroySpecificCacheAndCacheGroup() throws Exception {
        performTest(ig -> {
            ig.destroyCaches(Arrays.asList(CACHE_1, CACHE_3, CACHE_4));

            return null;
        });
    }

    /**
     * @param consumer Action that trigger stop or destroy of caches.
     */
    private void performTest(IgniteThrowableConsumer<Ignite, Void> consumer) throws Exception {
        IgniteEx ig0 = (IgniteEx)startGrids(2);

        ig0.cluster().active(true);

        stopGrid(1);

        loadData(ig0);

        startGrid(1);

        runLoad(ig0);

        consumer.accept(ig0);

        U.sleep(1000);

        awaitPartitionMapExchange(true, true, null, true);

        assertNull(grid(1).context().failure().failureContext());
    }

    /**
     * @param ig Ig.
     */
    private void loadData(Ignite ig) {
        ig.getOrCreateCaches(Arrays.asList(
                new CacheConfiguration<>(CACHE_1)
                        .setCacheMode(CacheMode.REPLICATED)
                        .setRebalanceBatchSize(REBALANCE_BATCH_SIZE)
                        .setGroupName(GROUP_1),
                new CacheConfiguration<>(CACHE_2)
                        .setCacheMode(CacheMode.REPLICATED)
                        .setRebalanceBatchSize(REBALANCE_BATCH_SIZE)
                        .setGroupName(GROUP_1),
                new CacheConfiguration<>(CACHE_3)
                        .setCacheMode(CacheMode.REPLICATED)
                        .setRebalanceBatchSize(REBALANCE_BATCH_SIZE)
                        .setGroupName(GROUP_2),
                new CacheConfiguration<>(CACHE_4)
                        .setCacheMode(CacheMode.REPLICATED)
                        .setRebalanceBatchSize(REBALANCE_BATCH_SIZE)
                        .setGroupName(GROUP_2)
        ));

        try (IgniteDataStreamer<Object, Object> streamer1 = ig.dataStreamer(CACHE_1);
             IgniteDataStreamer<Object, Object> streamer2 = ig.dataStreamer(CACHE_2);
             IgniteDataStreamer<Object, Object> streamer3 = ig.dataStreamer(CACHE_3);
             IgniteDataStreamer<Object, Object> streamer4 = ig.dataStreamer(CACHE_4)
        ) {
            for (int i = 0; i < 3_000; i++) {
                streamer1.addData(i, new byte[1024]);
                streamer2.addData(i, new byte[1024]);
                streamer3.addData(i, new byte[1024]);
                streamer4.addData(i, new byte[1024]);
            }

            streamer1.flush();
            streamer2.flush();
            streamer3.flush();
            streamer4.flush();
        }
    }

    /**
     * @param ig Ig.
     */
    private void runLoad(Ignite ig) throws Exception{
        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                String cacheName = F.rand(CACHE_1, CACHE_2, CACHE_3, CACHE_4);

                IgniteCache cache = ig.cache(cacheName);

                for (int i = 0; i < 3_000; i++) {
                    int idx = ThreadLocalRandom.current().nextInt(3_000);

                    cache.put(idx, new byte[1024]);

                    log.info("tx put" + idx);
                }
            }
        }, 4, "load-thread");
    }

    /**
     *
     */
    private static class RebalanceBlockingSPI extends TcpCommunicationSpi {
        /** */
        public static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            if (msg instanceof GridIoMessage && ((GridIoMessage) msg).message() instanceof GridDhtPartitionSupplyMessage) {
                int grpId = ((GridCacheGroupIdMessage) ((GridIoMessage) msg).message()).groupId();

                if (grpId == CU.cacheId(GROUP_1) || grpId == CU.cacheId(GROUP_2)) {
                    try {
                        U.sleep(50);
                    } catch (IgniteInterruptedCheckedException e) {
                        e.printStackTrace();
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

                if (grpId == CU.cacheId(GROUP_1) || grpId == CU.cacheId(GROUP_2)) {
                    try {
                        U.sleep(50);
                    } catch (IgniteInterruptedCheckedException e) {
                        e.printStackTrace();
                    }
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}

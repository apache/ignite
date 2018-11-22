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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class IgniteDemanderOnDeactivationTest extends GridCommonAbstractTest {
    /** */
    private static final String STOPPING_CACHE_NAME = "cache_stopping";

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private static final String CACHE_GROUP = "group";

    private static final AtomicReference<CountDownLatch> SUPPLY_MESSAGE_LATCH = new AtomicReference<>();

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        SUPPLY_MESSAGE_LATCH.set(new CountDownLatch(1));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        SUPPLY_MESSAGE_LATCH.get().countDown();

        SUPPLY_MESSAGE_LATCH.set(null);

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

        cfg.setDataStorageConfiguration(
                new DataStorageConfiguration()
                        .setWalMode(WALMode.LOG_ONLY)
                        .setDefaultDataRegionConfiguration(
                                new DataRegionConfiguration()
                                        .setPersistenceEnabled(true)
                                        .setMaxSize(100L * 1024 * 1024))
        );

        return cfg;
    }

    /**
     *
     */
    public void test() throws Exception {
        IgniteEx ig0 = (IgniteEx)startGrids(2);

        ig0.cluster().active(true);

        ig0.getOrCreateCaches(Arrays.asList(
                new CacheConfiguration<>(STOPPING_CACHE_NAME)
                        .setName(STOPPING_CACHE_NAME)
                        .setCacheMode(CacheMode.REPLICATED)
                        .setGroupName(CACHE_GROUP),
                new CacheConfiguration<>(CACHE_NAME)
                        .setCacheMode(CacheMode.REPLICATED)
                        .setGroupName(CACHE_GROUP)
        ));

        stopGrid(1);

        try (IgniteDataStreamer<Object, Object> streamer = ig0.dataStreamer(STOPPING_CACHE_NAME)) {
            for (int i = 0; i < 3_000; i++)
                streamer.addData(i, new byte[5 * 1000]);

            streamer.flush();
        }

        try (IgniteDataStreamer<Object, Object> streamer = ig0.dataStreamer(CACHE_NAME)) {
            for (int i = 0; i < 3_000; i++)
                streamer.addData(i, new byte[5 * 1000]);

            streamer.flush();
        }


        IgniteEx ig1 = startGrid(1);

        ig0.context().state().changeGlobalState(false, ig0.cluster().currentBaselineTopology(), false);

        SUPPLY_MESSAGE_LATCH.get().countDown();

        U.sleep(3000);

        assertNull(ig1.context().failure().failureContext());
    }



    private static class RebalanceBlockingSPI extends TcpCommunicationSpi {
        /** */
        public static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                int grpId = ((GridCacheGroupIdMessage)((GridIoMessage)msg).message()).groupId();

                if (grpId == CU.cacheId(STOPPING_CACHE_NAME)) {
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

                if (grpId == CU.cacheId(STOPPING_CACHE_NAME)) {
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

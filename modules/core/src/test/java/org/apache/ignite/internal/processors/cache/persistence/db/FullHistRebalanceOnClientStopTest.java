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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;

/**
 * Tests that full rebalance is not triggered instead of historical one when client node stops during PME
 */
public class FullHistRebalanceOnClientStopTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Partitions count. */
    private static final int PARTS_CNT = 16;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        CacheConfiguration<Object, Object> ccfg1 = new CacheConfiguration<>(CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));

        cfg.setCacheConfiguration(ccfg1);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(100 * 1024 * 1024)
            );

        cfg.setDataStorageConfiguration(dbCfg);

        cfg.setCommunicationSpi(new RebalanceCheckingCommunicationSpi());

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

    /**
     * @throws Exception if failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
    public void testFullRebalanceNotTriggeredWhenClientNodeStopsDuringPme() throws Exception {
        startGrids(2);

        IgniteEx ig0 = grid(0);

        ig0.cluster().active(true);

        IgniteCache<Object, Object> cache = ig0.cache(CACHE_NAME);

        startClientGrid(5);

        final int entryCnt = PARTS_CNT * 1000;

        for (int i = 0; i < entryCnt; i++)
            cache.put(i, i);

        forceCheckpoint();

        stopGrid(1);

        for (int i = 0; i < entryCnt; i++)
            cache.put(i, i + 100);

        forceCheckpoint();

        final CountDownLatch exchangeLatch = new CountDownLatch(1);

        final CountDownLatch hangingPmeStartedLatch = new CountDownLatch(1);

        ig0.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onInitAfterTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                try {
                    hangingPmeStartedLatch.countDown();

                    exchangeLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            try {
                startGrid(1);

                awaitPartitionMapExchange();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });

        IgniteInternalFuture clientStopFut = GridTestUtils.runAsync(() -> {
            try {
                hangingPmeStartedLatch.await();

                stopGrid(5);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }, "client-starter");

        assertFalse(fut.isDone());

        exchangeLatch.countDown();

        clientStopFut.get();

        fut.get();

        awaitPartitionMapExchange();

        boolean histRebalanceInvoked = RebalanceCheckingCommunicationSpi.histRebalances();

        boolean fullRebalanceInvoked = RebalanceCheckingCommunicationSpi.fullRebalances();

        RebalanceCheckingCommunicationSpi.cleanup();

        assertTrue("Historical rebalance hasn't been invoked.", histRebalanceInvoked);

        assertFalse("Full rebalance has been invoked.", fullRebalanceInvoked);
    }

    /**
     * Wrapper of communication spi to detect if rebalance has happened.
     */
    public static class RebalanceCheckingCommunicationSpi extends TestRecordingCommunicationSpi {
        /** */
        private static boolean topVersForHist = false;

        /** */
        private static boolean topVersForFull = false;

        /** Lock object. */
        private static final Object mux = new Object();

        /**
         * @return {@code true} if WAL rebalance has been used.
         */
        static boolean histRebalances() {
            synchronized (mux) {
                return topVersForHist;
            }
        }

        /**
         * @return {@code true} if full rebalance has been used.
         */
        static boolean fullRebalances() {
            synchronized (mux) {
                return topVersForFull;
            }
        }

        /**
         * Cleans all rebalances histories.
         */
        public static void cleanup() {
            synchronized (mux) {
                topVersForHist = false;
                topVersForFull = false;
            }
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (((GridIoMessage)msg).message() instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage demandMsg = (GridDhtPartitionDemandMessage)((GridIoMessage)msg).message();

                IgniteDhtDemandedPartitionsMap map = demandMsg.partitions();

                if (!map.historicalMap().isEmpty()) {
                    synchronized (mux) {
                        topVersForHist = true;
                    }
                }

                if (!map.fullSet().isEmpty()) {
                    synchronized (mux) {
                        topVersForFull = true;
                    }
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}

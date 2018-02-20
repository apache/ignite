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
package org.apache.ignite.compatibility.rebalance;

import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.junit.Assert;

/**
 * An simple test to check compatibility during rebalance process.
 */
public class RebalanceCompatibilityTest extends IgniteCompatibilityAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Latch name. */
    private static final String LATCH_NAME = "check-finished";

    /**
     * Test rebalance compatibility.
     *
     * @throws Exception If failed.
     */
    public void testRebalanceCompatibility() throws Exception {
        doTestNewSupplierOldDemander("2.3.0");
    }

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName)
                .setPeerClassLoadingEnabled(false)
                .setCacheConfiguration(
                    new CacheConfiguration(CACHE_NAME)
                        .setRebalanceMode(CacheRebalanceMode.SYNC)
                        .setBackups(0)
                        .setAffinity(new RendezvousAffinityFunction(false, 32))
                );

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /**
     * Test rebalancing with old node demander and new node supplier.
     *
     * @param ver Version of old node.
     * @throws Exception If failed.
     */
    private void doTestNewSupplierOldDemander(String ver) throws Exception {
        try {
            IgniteEx grid = startGrid(0);

            // Populate cache with data.
            final int entitiesCount = 10_000;
            try (IgniteDataStreamer streamer = grid.dataStreamer(CACHE_NAME)) {
                streamer.allowOverwrite(true);

                for (int k = 0; k < entitiesCount; k++) {
                    streamer.addData(k, k);
                }
            }

            startGrid(1, ver, cfg -> {
                cfg.setLocalHost("127.0.0.1");

                cfg.setPeerClassLoadingEnabled(false);

                TcpDiscoverySpi disco = new TcpDiscoverySpi();
                disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

                cfg.setDiscoverySpi(disco);

                cfg.setCacheConfiguration(
                        new CacheConfiguration(CACHE_NAME)
                                .setRebalanceMode(CacheRebalanceMode.SYNC)
                                .setBackups(0)
                                .setAffinity(new RendezvousAffinityFunction(false, 32))

                );
            }, ignite -> {
                // Wait for rebalance.
                ignite.cache(CACHE_NAME).rebalance().get();

                // Check no data loss.
                for (int k = 0; k < entitiesCount; k++)
                    Assert.assertEquals("Check failed for key " + k, k, (int) ignite.cache(CACHE_NAME).get(k));

                // Latch to synchronize checking process on new and old nodes.
                IgniteCountDownLatch latch = ignite.countDownLatch(LATCH_NAME, 2, false, true);
                latch.countDown();
                latch.await();
            });

            grid.cache(CACHE_NAME).rebalance().get();

            // Check no data loss.
            for (int k = 0; k < entitiesCount; k++)
                Assert.assertEquals("Check failed for key " + k, k, (int) grid.cache(CACHE_NAME).get(k));

            // Latch to synchronize checking process on new and old nodes.
            IgniteCountDownLatch latch = grid.countDownLatch(LATCH_NAME, 2, false, true);
            latch.countDown();
            try {
                latch.await(10000);
            }
            catch (IgniteException e) {
                throw new IgniteException("Unable to await check finished on old node", e);
            }
        } finally {
            stopAllGrids();
        }
    }
}

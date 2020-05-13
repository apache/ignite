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

package org.apache.ignite.cache.affinity;

import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CachePartitionExchangeWorkerTask;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_AFFINITY_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_REBALANCING_CANCELLATION_OPTIMIZATION;

/**
 * Test creates two exchange in same moment, in order to when first executing second would already in queue.
 */
@WithSystemProperty(key = IGNITE_DISABLE_REBALANCING_CANCELLATION_OPTIMIZATION, value = "false")
public class PendingExchangeTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setBackups(1));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Thats starts several caches in order to affinity history is exhausted.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_AFFINITY_HISTORY_SIZE, value = "2")
    public void testWithShortAfinityHistory() throws Exception {
        createClusterWithPendingExchnageDuringRebalance((ignite, exchangeManager) -> {
            GridCompoundFuture compFut = new GridCompoundFuture();

            for (int i = 0; i < 20; i++) {
                int finalNum = i;

                compFut.add(GridTestUtils.runAsync(() -> ignite.createCache(DEFAULT_CACHE_NAME + "_new" + finalNum)));
            }

            compFut.markInitialized();

            waitForExchnagesBegin(exchangeManager, 20);

            return compFut;
        });
    }

    /**
     * Thats starts one cache and several clients in one moment in order leading to pending client exchanges.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStartSeveralClients() throws Exception {
        createClusterWithPendingExchnageDuringRebalance((ignite, exchangeManager) -> {
            GridCompoundFuture compFut = new GridCompoundFuture();

            for (int i = 0; i < 5; i++) {
                int finalNum = i;

                compFut.add(GridTestUtils.runAsync(() -> startClientGrid("new_client" + finalNum)));
            }

            //Need to explicitly wait for laying of client exchanges on exchange queue, before cache start exchnage.
            waitForExchnagesBegin(exchangeManager, 5);

            compFut.add(GridTestUtils.runAsync(() -> ignite.createCache(DEFAULT_CACHE_NAME + "_new")));

            compFut.markInitialized();

            waitForExchnagesBegin(exchangeManager, 6);

            return compFut;
        });
    }

    /**
     * Test checks that pending exchange will lead to stable topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStartCachePending() throws Exception {
        createClusterWithPendingExchnageDuringRebalance((ignite, exchangeManager) ->
            GridTestUtils.runAsync(() -> ignite.createCache(DEFAULT_CACHE_NAME + "_new")));
    }

    /**
     * Start and stop cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStopStartCachePending() throws Exception {
        createClusterWithPendingExchnageDuringRebalance((ignite, exchangeManager) -> {
            GridCompoundFuture compFut = new GridCompoundFuture();

            compFut.add(GridTestUtils.runAsync(() -> ignite.createCache(DEFAULT_CACHE_NAME + "_new")));
            compFut.add(GridTestUtils.runAsync(() -> ignite.destroyCache(DEFAULT_CACHE_NAME + "_new")));

            compFut.markInitialized();

            waitForExchnagesBegin(exchangeManager, 1);

            return compFut;
        });
    }

    /**
     * Start several cache and stop their.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStopStartSeveralCachePending() throws Exception {
        createClusterWithPendingExchnageDuringRebalance((ignite, exchangeManager) -> {
            GridCompoundFuture compFut = new GridCompoundFuture();

            for (int i = 0; i < 5; i++) {
                int finalNum = i;

                compFut.add(GridTestUtils.runAsync(() -> ignite.createCache(DEFAULT_CACHE_NAME +
                    "_new" + finalNum)));
                compFut.add(GridTestUtils.runAsync(() -> ignite.destroyCache(DEFAULT_CACHE_NAME +
                    "_new" + finalNum)));
            }

            compFut.markInitialized();

            waitForExchnagesBegin(exchangeManager, 5);

            return compFut;
        });
    }

    /**
     * Start server and cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStartServerAndCache() throws Exception {
        createClusterWithPendingExchnageDuringRebalance((ignite, exchangeManager) -> {
            GridCompoundFuture compFut = new GridCompoundFuture();

            compFut.add(GridTestUtils.runAsync(() -> startGrid("new_srv")));
            compFut.add(GridTestUtils.runAsync(() -> ignite.createCache(DEFAULT_CACHE_NAME + "_new")));

            compFut.markInitialized();

            waitForExchnagesBegin(exchangeManager, 2);

            return compFut;
        });
    }

    /**
     * Start several servers, clients and caches.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStartServalServersWithClisntAndCache() throws Exception {
        createClusterWithPendingExchnageDuringRebalance((ignite, exchangeManager) -> {
            GridCompoundFuture compFut = new GridCompoundFuture();

            for (int i = 0; i < 3; i++) {
                int finalSrvNum = i;

                compFut.add(GridTestUtils.runAsync(() -> startGrid("new_srv" + finalSrvNum)));
                compFut.add(GridTestUtils.runAsync(() -> startClientGrid("new_client" + finalSrvNum)));
                compFut.add(GridTestUtils.runAsync(() -> ignite.createCache(DEFAULT_CACHE_NAME + "_new" + finalSrvNum)));
            }

            compFut.markInitialized();

            waitForExchnagesBegin(exchangeManager, 9);

            return compFut;
        });
    }

    /**
     * Start and stop several servers and clients.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStartStopServalServersWithClisnt() throws Exception {
        createClusterWithPendingExchnageDuringRebalance((ignite, exchangeManager) -> {
            GridCompoundFuture compFut = new GridCompoundFuture();

            for (int i = 0; i < 2; i++) {
                int finalSrvNum = i;

                compFut.add(GridTestUtils.runAsync(() -> startGrid("new_srv" + finalSrvNum)));
                compFut.add(GridTestUtils.runAsync(() -> startClientGrid("new_client" + finalSrvNum)));

                compFut.add(GridTestUtils.runAsync(() -> stopGrid("new_srv" + finalSrvNum)));
                compFut.add(GridTestUtils.runAsync(() -> stopGrid("new_client" + finalSrvNum)));
            }

            compFut.markInitialized();

            waitForExchnagesBegin(exchangeManager, 4);

            return compFut;
        });
    }

    /**
     * Waiting for exchanges beginning.
     *
     * @param ignite Ignite.
     */
    private void waitForExchnagesBegin(GridCachePartitionExchangeManager exchangeManager, int exchanges) {
        GridWorker exchWorker = U.field(exchangeManager, "exchWorker");
        Queue<CachePartitionExchangeWorkerTask> exchnageQueue = U.field(exchWorker, "futQ");

        try {
            assertTrue(GridTestUtils.waitForCondition(() -> {
                int exFuts = 0;

                for (CachePartitionExchangeWorkerTask task : exchnageQueue) {
                    if (task instanceof GridDhtPartitionsExchangeFuture)
                        exFuts++;
                }

                return exFuts >= exchanges;
            }, 30_000));
        }
        catch (IgniteInterruptedCheckedException e) {
            fail("Can’t wait for the exchnages beginning.");
        }
    }

    /**
     * @param clo Closure triggering exchange.
     * @throws Exception If failed.
     */
    private void createClusterWithPendingExchnageDuringRebalance(PendingExchangeTrigger clo) throws Exception {
        IgniteEx ignite0 = startGrids(3);

        try (IgniteDataStreamer streamer = ignite0.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 1000; i++)
                streamer.addData(i, i);
        }

        awaitPartitionMapExchange();

        GridCachePartitionExchangeManager exchangeManager1 = ignite(1).context().cache().context().exchange();

        CountDownLatch exchangeLatch = new CountDownLatch(1);

        AffinityTopologyVersion readyTop = exchangeManager1.readyAffinityVersion();

        exchangeManager1.registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onInitAfterTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                U.awaitQuiet(exchangeLatch);
            }
        });

        IgniteInternalFuture startNodeFut = GridTestUtils.runAsync(() -> stopGrid(2));

        assertTrue(GridTestUtils.waitForCondition(() ->
            exchangeManager1.lastTopologyFuture().initialVersion().after(readyTop), 10_000));

        IgniteInternalFuture exchangeTrigger = clo.trigger(ignite0, exchangeManager1);

        assertTrue(GridTestUtils.waitForCondition(exchangeManager1::hasPendingServerExchange, 10_000));

        exchangeLatch.countDown();

        startNodeFut.get(10_000);
        exchangeTrigger.get(10_000);

        awaitPartitionMapExchange();
    }

    /**
     * Trigger pending exchange closure interface.
     */
    private static interface PendingExchangeTrigger {

        /**
         * Invoke exchange.
         *
         * @param ignite Ignite.
         * @param exchangeManager Exchnage manager.
         * @return
         */
        IgniteInternalFuture trigger(Ignite ignite, GridCachePartitionExchangeManager exchangeManager);
    }
}

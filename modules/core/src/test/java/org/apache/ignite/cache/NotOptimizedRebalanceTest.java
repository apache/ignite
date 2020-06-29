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

package org.apache.ignite.cache;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_REBALANCING_CANCELLATION_OPTIMIZATION;

/**
 * Test checks rebalance behavior when several exchanges trigger sequence.
 */
@WithSystemProperty(key = IGNITE_DISABLE_REBALANCING_CANCELLATION_OPTIMIZATION, value = "true")
public class NotOptimizedRebalanceTest extends GridCommonAbstractTest {
    /** Start cluster nodes. */
    public static final int NODES_CNT = 3;

    /** Persistence enabled. */
    public boolean persistenceEnabled;

    /** Count of backup partitions. */
    public static final int BACKUPS = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setWalSegmentSize(4 * 1024 * 1024)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(100L * 1024 * 1024)
                    .setPersistenceEnabled(persistenceEnabled)))
            .setCacheConfiguration(
                new CacheConfiguration(DEFAULT_CACHE_NAME)
                    .setAffinity(new RendezvousAffinityFunction(false, 15))
                    .setBackups(BACKUPS));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Checks rebalance with persistence.
     *
     * @throws Exception
     */
    @Test
    public void testRebalanceWithPersistence() throws Exception {
        testRebalance(true, true);
    }

    /**
     * Checks rebalance without persistence.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceWithoutPersistence() throws Exception {
        testRebalance(false, true);
    }

    /**
     * Checks rebalance with persistence and client joining/lifting.
     *
     * @throws Exception
     */
    @Test
    public void testRebalanceWithPersistenceAndClient() throws Exception {
        testRebalance(true, false);
    }

    /**
     * Checks rebalance without persistence and client joining/lifting..
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceWithoutPersistenceAndClient() throws Exception {
        testRebalance(false, false);
    }

    /**
     * Trigger rebalance when node left topology.
     *
     * @param persistence Persistent flag.
     * @throws Exception If failed.
     */
    public void testRebalance(boolean persistence, boolean serverJoin) throws Exception {
        persistenceEnabled = persistence;

        IgniteEx ignite0 = startGrids(NODES_CNT);

        ignite0.cluster().active(true);

        ignite0.cluster().baselineAutoAdjustEnabled(false);

        IgniteEx newNode = serverJoin ? startGrid(NODES_CNT) : startClientGrid(NODES_CNT);

        grid(1).close();

        for (String cache : ignite0.cacheNames())
            loadData(ignite0, cache);

        awaitPartitionMapExchange();

        TestRecordingCommunicationSpi commSpi1 = startNodeWithBlockingRebalance(getTestIgniteInstanceName(1));

        commSpi1.waitForBlocked();

        Map<CacheGroupContext, IgniteInternalFuture<Boolean>> futs = getAllRebalanceFuturesByGroup(grid(1));

        checkAllFuturesProcessing(futs);

        for (int i = 0; i < 3; i++) {
            newNode.close();

            checkTopology(NODES_CNT);

            newNode = serverJoin ? startGrid(NODES_CNT) : startClientGrid(NODES_CNT);

            checkTopology(NODES_CNT + 1);
        }

        if (serverJoin)
            checkAllFuturesCancelled(futs);
        else
            checkAllFuturesProcessing(futs);

        commSpi1.stopBlock();

        awaitPartitionMapExchange();

        Map<CacheGroupContext, IgniteInternalFuture<Boolean>> newFuts = getAllRebalanceFuturesByGroup(grid(1));

        for (Map.Entry<CacheGroupContext, IgniteInternalFuture<Boolean>> grpFut : futs.entrySet()) {
            IgniteInternalFuture<Boolean> fut = grpFut.getValue();
            IgniteInternalFuture<Boolean> newFut = newFuts.get(grpFut.getKey());

            if (serverJoin)
                assertTrue(futureInfoString(fut), fut.isDone() && !fut.get());
            else
                assertSame(fut, newFut);

            assertTrue(futureInfoString(newFut), newFut.isDone() && newFut.get());
        }
    }

    /**
     * @param futs Matching group to rebalance's future.
     * @throws org.apache.ignite.IgniteCheckedException
     */
    public void checkAllFuturesCancelled(Map<CacheGroupContext, IgniteInternalFuture<Boolean>> futs)
        throws org.apache.ignite.IgniteCheckedException {
        for (IgniteInternalFuture<Boolean> fut : futs.values())
            assertTrue(futureInfoString(fut), fut.isDone() && !fut.get());
    }

    /**
     * @param futs Matching group to rebalance's future.
     */
    public void checkAllFuturesProcessing(Map<CacheGroupContext, IgniteInternalFuture<Boolean>> futs) {
        for (IgniteInternalFuture<Boolean> fut : futs.values())
            assertFalse(futureInfoString(fut), fut.isDone());
    }

    /**
     * Finds all existed rebalance future by all cache for Ignite's instance specified.
     *
     * @param ignite Ignite.
     * @return Array of rebelance futures.
     */
    private Map<CacheGroupContext, IgniteInternalFuture<Boolean>> getAllRebalanceFuturesByGroup(IgniteEx ignite) {
        HashMap<CacheGroupContext, IgniteInternalFuture<Boolean>> futs = new HashMap<>(ignite.cacheNames().size());

        for (String cache : ignite.cacheNames()) {
            IgniteInternalFuture<Boolean> fut = ignite.context().cache().cacheGroup(CU.cacheId(cache)).preloader().rebalanceFuture();

            futs.put(ignite.context().cache().cacheGroup(CU.cacheId(cache)), fut);
        }
        return futs;
    }

    /**
     * Prepares string representation of rebalance future.
     *
     * @param rebalanceFuture Rebalance future.
     * @return Information string about passed future.
     */
    private String futureInfoString(IgniteInternalFuture<Boolean> rebalanceFuture) {
        return "Fut: " + rebalanceFuture
            + " is done: " + rebalanceFuture.isDone()
            + " result: " + (rebalanceFuture.isDone() ? rebalanceFuture.result() : "None");
    }

    /**
     * Starts node with name <code>name</code> and blocks demand message for custom caches.
     *
     * @param name Node instance name.
     * @return Test communication SPI.
     * @throws Exception If failed.
     */
    private TestRecordingCommunicationSpi startNodeWithBlockingRebalance(String name) throws Exception {
        IgniteConfiguration cfg = optimize(getConfiguration(name));

        TestRecordingCommunicationSpi communicationSpi = (TestRecordingCommunicationSpi)cfg.getCommunicationSpi();

        communicationSpi.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage demandMessage = (GridDhtPartitionDemandMessage)msg;

                if (CU.cacheId(DEFAULT_CACHE_NAME) != demandMessage.groupId())
                    return false;

                info("Message was caught: " + msg.getClass().getSimpleName()
                    + " rebalanceId = " + U.field(demandMessage, "rebalanceId")
                    + " to: " + node.consistentId()
                    + " by cache id: " + demandMessage.groupId());

                return true;
            }

            return false;
        });

        startGrid(cfg);

        return communicationSpi;
    }

    /**
     * Loades several data entries to cache specified.
     *
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    private void loadData(Ignite ignite, String cacheName) {
        try (IgniteDataStreamer streamer = ignite.dataStreamer(cacheName)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < 100; i++)
                streamer.addData(i, System.nanoTime());
        }
    }
}

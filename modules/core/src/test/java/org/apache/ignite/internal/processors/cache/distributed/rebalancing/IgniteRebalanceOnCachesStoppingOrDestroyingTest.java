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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.lang.IgniteThrowableFunction;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

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

    /** Number of loaded keys in each cache. */
    private static final int KEYS_SIZE = 3000;

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
    @Test
    public void testStopCachesOnDeactivationFirstGroup() throws Exception {
        testStopCachesOnDeactivation(GROUP_1);
    }

    /**
     *
     */
    @Test
    public void testStopCachesOnDeactivationSecondGroup() throws Exception {
        testStopCachesOnDeactivation(GROUP_2);
    }

    /**
     * @param groupName Group name.
     * @throws Exception If failed.
     */
    private void testStopCachesOnDeactivation(String groupName) throws Exception {
        performTest(ig -> {
            ig.cluster().active(false);

            // Add to escape possible long waiting in awaitPartitionMapExchange due to {@link CacheAffinityChangeMessage}.
            ig.cluster().active(true);

            return null;
        }, groupName);
    }

    /**
     *
     */
    @Test
    public void testDestroySpecificCachesInDifferentCacheGroupsFirstGroup() throws Exception {
        testDestroySpecificCachesInDifferentCacheGroups(GROUP_1);
    }

    /**
     *
     */
    @Test
    public void testDestroySpecificCachesInDifferentCacheGroupsSecondGroup() throws Exception {
        testDestroySpecificCachesInDifferentCacheGroups(GROUP_2);
    }

    /**
     * @param groupName Group name.
     * @throws Exception If failed.
     */
    private void testDestroySpecificCachesInDifferentCacheGroups(String groupName) throws Exception {
        performTest(ig -> {
            ig.destroyCaches(Arrays.asList(CACHE_1, CACHE_3));

            return null;
        }, groupName);
    }

    /**
     *
     */
    @Test
    public void testDestroySpecificCacheAndCacheGroupFirstGroup() throws Exception {
        testDestroySpecificCacheAndCacheGroup(GROUP_1);
    }

    /**
     *
     */
    @Test
    public void testDestroySpecificCacheAndCacheGroupSecondGroup() throws Exception {
        testDestroySpecificCacheAndCacheGroup(GROUP_2);
    }

    /**
     * @param groupName Group name.
     * @throws Exception If failed.
     */
    private void testDestroySpecificCacheAndCacheGroup(String groupName) throws Exception {
        performTest(ig -> {
            ig.destroyCaches(Arrays.asList(CACHE_1, CACHE_3, CACHE_4));

            return null;
        }, groupName);
    }

    /**
     * @param testAction Action that trigger stop or destroy of caches.
     */
    private void performTest(IgniteThrowableFunction<Ignite, Void> testAction, String groupName) throws Exception {
        IgniteEx ig0 = (IgniteEx)startGrids(2);

        ig0.cluster().active(true);

        stopGrid(1);

        loadData(ig0);

        IgniteEx ig1 = startGrid(1);

        RebalanceBlockingSPI commSpi = (RebalanceBlockingSPI)ig1.configuration().getCommunicationSpi();

        // Complete all futures for groups that we don't need to wait.
        commSpi.suspendedMessages.forEach((k, v) -> {
            if (k != CU.cacheId(groupName))
                commSpi.resume(k);
        });

        CountDownLatch latch = commSpi.suspendRebalanceInMiddleLatch.get(CU.cacheId(groupName));

        assert latch != null;

        // Await some middle point rebalance for group.
        latch.await();

        testAction.apply(ig0);

        // Resume rebalance after action performed.
        commSpi.resume(CU.cacheId(groupName));

        awaitPartitionMapExchange(true, true, null, true);

        assertNull(grid(1).context().failure().failureContext());
    }

    /**
     * @param ig Ig.
     */
    private void loadData(Ignite ig) {
        List<CacheConfiguration> configs = Stream.of(
            F.t(CACHE_1, GROUP_1),
            F.t(CACHE_2, GROUP_1),
            F.t(CACHE_3, GROUP_2),
            F.t(CACHE_4, GROUP_2)
        ).map(names -> new CacheConfiguration<>(names.get1())
            .setGroupName(names.get2())
            .setRebalanceBatchSize(REBALANCE_BATCH_SIZE)
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, 64))
        ).collect(Collectors.toList());

        ig.getOrCreateCaches(configs);

        configs.forEach(cfg -> {
            try (IgniteDataStreamer<Object, Object> streamer = ig.dataStreamer(cfg.getName())) {
                for (int i = 0; i < KEYS_SIZE; i++)
                    streamer.addData(i, i);

                streamer.flush();
            }
        });
    }

    /**
     *
     */
    private static class RebalanceBlockingSPI extends TcpCommunicationSpi {
        /** */
        private final Map<Integer, Queue<T3<UUID, Message, IgniteRunnable>>> suspendedMessages = new ConcurrentHashMap<>();

        /** */
        private final Map<Integer, CountDownLatch> suspendRebalanceInMiddleLatch = new ConcurrentHashMap<>();

        /** */
        RebalanceBlockingSPI() {
            suspendedMessages.put(CU.cacheId(GROUP_1), new ConcurrentLinkedQueue<>());
            suspendedMessages.put(CU.cacheId(GROUP_2), new ConcurrentLinkedQueue<>());
            suspendRebalanceInMiddleLatch.put(CU.cacheId(GROUP_1), new CountDownLatch(3));
            suspendRebalanceInMiddleLatch.put(CU.cacheId(GROUP_2), new CountDownLatch(3));
        }

        /** {@inheritDoc} */
        @Override protected void notifyListener(UUID sndId, Message msg, IgniteRunnable msgC) {
            if (msg instanceof GridIoMessage &&
                ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                GridDhtPartitionSupplyMessage msg0 = (GridDhtPartitionSupplyMessage)((GridIoMessage)msg).message();

                CountDownLatch latch = suspendRebalanceInMiddleLatch.get(msg0.groupId());

                if (latch != null) {
                    if (latch.getCount() > 0)
                        latch.countDown();
                    else {
                        synchronized (this) {
                            // Order make sense!
                            Queue<T3<UUID, Message, IgniteRunnable>> q = suspendedMessages.get(msg0.groupId());

                            if (q != null) {
                                q.add(new T3<>(sndId, msg, msgC));

                                return;
                            }
                        }
                    }
                }
            }

            super.notifyListener(sndId, msg, msgC);
        }

        /**
         * @param cacheId Cache id.
         */
        public synchronized void resume(int cacheId) {
            for (T3<UUID, Message, IgniteRunnable> t : suspendedMessages.remove(cacheId))
                super.notifyListener(t.get1(), t.get2(), t.get3());
        }
    }
}

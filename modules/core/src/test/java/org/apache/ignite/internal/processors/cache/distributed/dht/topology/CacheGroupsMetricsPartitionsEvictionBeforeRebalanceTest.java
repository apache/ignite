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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.CacheGroupMetricsImpl.CACHE_GROUP_METRICS_PREFIX;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

public class CacheGroupsMetricsPartitionsEvictionBeforeRebalanceTest extends GridCommonAbstractTest {
    /** */
    private static final String GROUP = "group";

    /** */
    private static final int PARTITION_COUNT = 64;

    /** */
    private static final int KEYS_COUNT = 1_000;

    /** */
    private static final List<String> CACHE_NAMES = Arrays.asList("cache1", "cache2", "cache3");

    /** Eviction process can be started. */
    private CountDownLatch startEvict = new CountDownLatch(1);

    /** Last partition ready for eviction. */
    private CountDownLatch lastPart = new CountDownLatch(1);

    /** Node received supply message. */
    private CountDownLatch supplyMsg = new CountDownLatch(1);

    /** Test log. */
    private final ListeningTestLogger log = new ListeningTestLogger(false, GridAbstractTest.log);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TcpCommunicationSpi() {
                @Override protected void notifyListener(UUID sndId, Message msg, IgniteRunnable msgC) {
                    if (msg instanceof GridIoMessage &&
                        ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                        GridDhtPartitionSupplyMessage msg0 = (GridDhtPartitionSupplyMessage)((GridIoMessage)msg)
                            .message();
                        if (msg0.groupId() == CU.cacheId(DEFAULT_CACHE_NAME))
                            supplyMsg.countDown();
                    }

                    super.notifyListener(sndId, msg, msgC);
                }
            })
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                ))
            .setGridLogger(log);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void partitionsEvictionBeforeRebalanceTest() throws Exception {
        IgniteEx ig0 = startGrids(2);

        ig0.cluster().state(ClusterState.ACTIVE);

        stopGrid(1);

        loadData(ig0);

        IgniteEx ig1 = startGrid(1);

        U.await(supplyMsg);

        ig0.cluster().state(ClusterState.INACTIVE);

        blockEviction(ig1);

        ig0.cluster().state(ClusterState.ACTIVE);

        U.await(lastPart);

        LongMetric evictedPartitionsLeft = ig1.context().metric().registry(metricName(CACHE_GROUP_METRICS_PREFIX, GROUP))
            .findMetric("RebalancingEvictedPartitionsLeft");

        try {
            assertEquals("The number of partitions left to be evicted before rebalancing started must be equal " +
                "to total number of partitions in affinity function.", PARTITION_COUNT, evictedPartitionsLeft.value());
        } catch (Exception e) {
            startEvict.countDown();

            throw e;
        }

        LogListener evictionCompleted = LogListener
            .matches(s -> s.contains("Starting rebalance routine [" + GROUP)).build();

        log.registerListener(evictionCompleted);

        startEvict.countDown();

        assertTrue("Rebalance routine should be started.", evictionCompleted.check(3_000));

        assertEquals("After starting rebalance routine, the eviction must be finished.",0,
            evictedPartitionsLeft.value());
    }

    /**
     * @param node Node.
     */
    private void loadData(Ignite node) {
        List<CacheConfiguration> configs = CACHE_NAMES.stream()
            .map(name -> new CacheConfiguration<>(name)
            .setGroupName(GROUP)
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, PARTITION_COUNT))
        ).collect(Collectors.toList());

        configs.add(configs.get(0).setName(DEFAULT_CACHE_NAME).setGroupName(DEFAULT_CACHE_NAME));

        node.getOrCreateCaches(configs);

        configs.forEach(cfg -> {
            try (IgniteDataStreamer<Object, Object> streamer = node.dataStreamer(cfg.getName())) {
                for (int i = 0; i < KEYS_COUNT; i++)
                    streamer.addData(i, i);

                streamer.flush();
            }
        });
    }

    /**
     * Queue for blocking eviction, until the last partition is added to the queue.
     */
    private class TestQueue extends PriorityBlockingQueue<PartitionsEvictManager.PartitionEvictionTask> {

        /**
         *
         */
        public TestQueue() {
            super(1000, Comparator.comparingLong(p ->
                (U.<GridDhtLocalPartition>field(p, "part").fullSize())));
        }

        /**
         * @return null until the last partition is added to the queue.
         */
        @Override public PartitionsEvictManager.PartitionEvictionTask poll() {
            return startEvict.getCount() == 0 ? super.poll() : null;
        }

        /** {@inheritDoc} */
        @Override public boolean offer(PartitionsEvictManager.PartitionEvictionTask task) {
            GridDhtLocalPartition part = U.field(task, "part");

            if (part.group().name().equals(GROUP) && part.id() == PARTITION_COUNT - 1) {
                lastPart.countDown();

                U.awaitQuiet(startEvict);
            }

            return super.offer(task);
        }

        /**
         * @return zero until the last partition is added to the queue.
         */
        @Override public int size() {
            return startEvict.getCount() == 0 ? super.size() : 0;
        }
    }

    /**
     * @param node Node.
     */
    private void blockEviction(IgniteEx node) {
        Queue[] buckets = node.context().cache().context().evict().evictionQueue.buckets;

        for (int i = 0; i < buckets.length; i++)
            buckets[i] = new TestQueue();
    }
}

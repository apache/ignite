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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheGroupMetricsImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test scenarios with rebalancing, IGNITE_DISABLE_WAL_DURING_REBALANCING optimization and topology changes
 * such as client nodes join/leave, server nodes from BLT leave/join, server nodes out of BLT join/leave.
 */
public class IgnitePdsCacheWalDisabledOnRebalancingTest extends GridCommonAbstractTest {
    /** Block message predicate to set to Communication SPI in node configuration. */
    private IgniteBiPredicate<ClusterNode, Message> blockMessagePredicate;

    /** */
    private static final int CACHE1_PARTS_NUM = 8;

    /** */
    private static final int CACHE2_PARTS_NUM = 16;

    /** */
    private static final int CACHE3_PARTS_NUM = 32;

    /** */
    private static final int CACHE_SIZE = 2_000;

    /** */
    private static final String CACHE1_NAME = "cache1";

    /** */
    private static final String CACHE2_NAME = "cache2";

    /** */
    private static final String CACHE3_NAME = "cache3";

    /** Function to generate cache values. */
    private static final BiFunction<String, Integer, String> GENERATING_FUNC = (s, i) -> s + "_value_" + i;

    /** Flag to block rebalancing. */
    private static final AtomicBoolean blockRebalanceEnabled = new AtomicBoolean(false);

    /**  */
    private static final Semaphore fileIoBlockingSemaphore = new Semaphore(Integer.MAX_VALUE);

    /** */
    private boolean useBlockingFileIO;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        fileIoBlockingSemaphore.drainPermits();

        fileIoBlockingSemaphore.release(Integer.MAX_VALUE);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // This is required because some tests do full clearing of persistence folder losing BLT info on next join.
        cfg.setConsistentId(igniteInstanceName);

        CacheConfiguration ccfg1 = new CacheConfiguration(CACHE1_NAME)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.REPLICATED)
            .setAffinity(new RendezvousAffinityFunction(false, CACHE1_PARTS_NUM));

        CacheConfiguration ccfg2 = new CacheConfiguration(CACHE2_NAME)
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAffinity(new RendezvousAffinityFunction(false, CACHE2_PARTS_NUM));

        CacheConfiguration ccfg3 = new CacheConfiguration(CACHE3_NAME)
            .setBackups(2)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAffinity(new RendezvousAffinityFunction(false, CACHE3_PARTS_NUM));

        cfg.setCacheConfiguration(ccfg1, ccfg2, ccfg3);

        if (!"client".equals(igniteInstanceName)) {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration()
                .setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4)
                .setWalMode(WALMode.LOG_ONLY)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(256 * 1024 * 1024));

            if (useBlockingFileIO)
                dsCfg.setFileIOFactory(new BlockingCheckpointFileIOFactory());

            cfg.setDataStorageConfiguration(dsCfg);
        }

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();
        commSpi.blockMessages(blockMessagePredicate);

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /**
     * If client joins topology during rebalancing process, rebalancing finishes successfully,
     * all partitions are owned as expected when rebalancing finishes.
     */
    @Test
    public void testClientJoinsLeavesDuringRebalancing() throws Exception {
        Ignite ig0 = startGrids(2);

        ig0.active(true);

        for (int i = 1; i < 4; i++)
          fillCache(ig0.dataStreamer("cache" + i), CACHE_SIZE, GENERATING_FUNC);

        String ig1Name = grid(1).name();

        stopGrid(1);

        cleanPersistenceDir(ig1Name);

        int groupId = ((IgniteEx) ig0).cachex(CACHE3_NAME).context().groupId();

        blockMessagePredicate = (node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage)
                return ((GridDhtPartitionDemandMessage) msg).groupId() == groupId;

            return false;
        };

        IgniteEx ig1 = startGrid(1);

        startClientGrid("client");

        stopGrid("client");

        CacheGroupMetricsImpl metrics = ig1.cachex(CACHE3_NAME).context().group().metrics();

        assertTrue("Unexpected moving partitions count: " + metrics.getLocalNodeMovingPartitionsCount(),
            metrics.getLocalNodeMovingPartitionsCount() == CACHE3_PARTS_NUM);

        TestRecordingCommunicationSpi commSpi = (TestRecordingCommunicationSpi) ig1
            .configuration().getCommunicationSpi();

        commSpi.stopBlock();

        boolean waitResult = GridTestUtils.waitForCondition(
            () -> metrics.getLocalNodeMovingPartitionsCount() == 0,
            30_000);

        assertTrue("Failed to wait for owning all partitions, parts in moving state: "
            + metrics.getLocalNodeMovingPartitionsCount(), waitResult);
    }

    /**
     * If server nodes from BLT leave topology and then join again after additional keys were put to caches,
     * rebalance starts.
     *
     * Test verifies that all moving partitions get owned after rebalance finishes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testServerNodesFromBltLeavesAndJoinsDuringRebalancing() throws Exception {
        Ignite ig0 = startGridsMultiThreaded(4);

        fillCache(ig0.dataStreamer(CACHE3_NAME), CACHE_SIZE, GENERATING_FUNC);

        List<Integer> nonAffinityKeys1 = nearKeys(grid(1).cache(CACHE3_NAME), 100, CACHE_SIZE / 2);
        List<Integer> nonAffinityKeys2 = nearKeys(grid(2).cache(CACHE3_NAME), 100, CACHE_SIZE / 2);

        stopGrid(1);
        stopGrid(2);

        Set<Integer> nonAffinityKeysSet = new HashSet<>();

        nonAffinityKeysSet.addAll(nonAffinityKeys1);
        nonAffinityKeysSet.addAll(nonAffinityKeys2);

        fillCache(ig0.dataStreamer(CACHE3_NAME), nonAffinityKeysSet, GENERATING_FUNC);

        int groupId = ((IgniteEx) ig0).cachex(CACHE3_NAME).context().groupId();

        blockMessagePredicate = (node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage)
                return ((GridDhtPartitionDemandMessage) msg).groupId() == groupId;

            return false;
        };

        IgniteEx ig1 = startGrid(1);

        CacheGroupMetricsImpl metrics = ig1.cachex(CACHE3_NAME).context().group().metrics();

        TestRecordingCommunicationSpi commSpi = (TestRecordingCommunicationSpi) ig1
            .configuration().getCommunicationSpi();

        startGrid(2);

        commSpi.stopBlock();

        boolean allOwned = GridTestUtils.waitForCondition(
            () -> metrics.getLocalNodeMovingPartitionsCount() == 0, 30_000);

        assertTrue("Partitions were not owned, there are " + metrics.getLocalNodeMovingPartitionsCount() +
            " partitions in MOVING state", allOwned);
    }

    /**
     * Scenario: when rebalanced MOVING partitions are owning by checkpointer,
     * concurrent affinity change (caused by BLT change) may lead for additional partitions in MOVING state to appear.
     *
     * In such situation no partitions should be owned until new rebalancing process starts and finishes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalancedPartitionsOwningWithConcurrentAffinityChange() throws Exception {
        Ignite ig0 = startGridsMultiThreaded(4);

        ig0.cluster().baselineAutoAdjustEnabled(false);
        fillCache(ig0.dataStreamer(CACHE3_NAME), CACHE_SIZE, GENERATING_FUNC);

        // Stop idx=2 to prepare for baseline topology change later.
        stopGrid(2);

        // Stop idx=1 and cleanup LFS to trigger full rebalancing after it restart.
        String ig1Name = grid(1).name();
        stopGrid(1);
        cleanPersistenceDir(ig1Name);

        // Blocking fileIO and blockMessagePredicate to block checkpointer and rebalancing for node idx=1.
        useBlockingFileIO = true;
        int groupId = ((IgniteEx) ig0).cachex(CACHE3_NAME).context().groupId();
        blockMessagePredicate = (node, msg) -> {
            if (blockRebalanceEnabled.get() && msg instanceof GridDhtPartitionDemandMessage)
                return ((GridDhtPartitionDemandMessage) msg).groupId() == groupId;

            return false;
        };

        IgniteEx ig1;
        CacheGroupMetricsImpl metrics;
        int locMovingPartsNum;

        // Enable blocking checkpointer on node idx=1 (see BlockingCheckpointFileIOFactory).
        fileIoBlockingSemaphore.drainPermits();
        try {
            ig1 = startGrid(1);

            metrics = ig1.cachex(CACHE3_NAME).context().group().metrics();
            locMovingPartsNum = metrics.getLocalNodeMovingPartitionsCount();

            // Partitions remain in MOVING state even after PME and rebalancing when checkpointer is blocked.
            assertTrue("Expected non-zero value for local moving partitions count on node idx = 1: " +
                locMovingPartsNum, 0 < locMovingPartsNum && locMovingPartsNum < CACHE3_PARTS_NUM);

            blockRebalanceEnabled.set(true);

            // Change baseline topology and release checkpointer to verify
            // that no partitions will be owned after affinity change.
            ig0.cluster().setBaselineTopology(ig1.context().discovery().topologyVersion());
        }
        finally {
            fileIoBlockingSemaphore.release(Integer.MAX_VALUE);
        }

        locMovingPartsNum = metrics.getLocalNodeMovingPartitionsCount();
        assertTrue("Expected moving partitions count on node idx = 1 equals to all partitions of the cache " +
             CACHE3_NAME + ": " + locMovingPartsNum, locMovingPartsNum == CACHE3_PARTS_NUM);

        TestRecordingCommunicationSpi commSpi = (TestRecordingCommunicationSpi)ig1
            .configuration().getCommunicationSpi();

        // When we stop blocking demand message rebalancing should complete and all partitions should be owned.
        commSpi.stopBlock();

        boolean res = GridTestUtils.waitForCondition(
            () -> metrics.getLocalNodeMovingPartitionsCount() == 0, 15_000);

        assertTrue("All partitions on node idx = 1 are expected to be owned", res);

        verifyCache(ig1.cache(CACHE3_NAME), GENERATING_FUNC);
    }

    /**
     * Scenario: when rebalanced MOVING partitions are owning by checkpointer,
     * concurrent no-op exchange should not trigger partition clearing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalancedPartitionsOwningWithAffinitySwitch() throws Exception {
        Ignite ig0 = startGridsMultiThreaded(4);
        fillCache(ig0.dataStreamer(CACHE3_NAME), CACHE_SIZE, GENERATING_FUNC);

        // Stop idx=2 to prepare for baseline topology change later.
        stopGrid(2);

        // Stop idx=1 and cleanup LFS to trigger full rebalancing after it restart.
        String ig1Name = grid(1).name();
        stopGrid(1);
        cleanPersistenceDir(ig1Name);

        // Blocking fileIO and blockMessagePredicate to block checkpointer and rebalancing for node idx=1.
        useBlockingFileIO = true;

        // Enable blocking checkpointer on node idx=1 (see BlockingCheckpointFileIOFactory).
        fileIoBlockingSemaphore.drainPermits();

        // Wait for rebalance (all partitions will be in MOVING state until cp is finished).
        startGrid(1).cachex(CACHE3_NAME).context().group().preloader().rebalanceFuture().get();

        startClientGrid("client");

        assertFalse(grid(1).cache(CACHE2_NAME).lostPartitions().isEmpty());

        fileIoBlockingSemaphore.release(Integer.MAX_VALUE);

        ig0.resetLostPartitions(Collections.singleton(CACHE2_NAME));

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(grid(0), CACHE3_NAME));
    }

    /** FileIOFactory implementation that enables blocking of writes to disk so checkpoint can be blocked. */
    private static class BlockingCheckpointFileIOFactory implements FileIOFactory {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Delegate factory. */
        private final FileIOFactory delegateFactory = new RandomAccessFileIOFactory();

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = delegateFactory.create(file, modes);

            return new FileIODecorator(delegate) {
                @Override public int write(ByteBuffer srcBuf) throws IOException {
                    if (Thread.currentThread().getName().contains("checkpoint")) {
                        try {
                            fileIoBlockingSemaphore.acquire();
                        }
                        catch (InterruptedException ignored) {
                            // No-op.
                        }
                    }

                    return delegate.write(srcBuf);
                }

                @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
                    if (Thread.currentThread().getName().contains("checkpoint")) {
                        try {
                            fileIoBlockingSemaphore.acquire();
                        }
                        catch (InterruptedException ignored) {
                            // No-op.
                        }
                    }

                    return delegate.write(srcBuf, position);
                }

                @Override public int write(byte[] buf, int off, int len) throws IOException {
                    if (Thread.currentThread().getName().contains("checkpoint")) {
                        try {
                            fileIoBlockingSemaphore.acquire();
                        }
                        catch (InterruptedException ignored) {
                            // No-op.
                        }
                    }

                    return delegate.write(buf, off, len);
                }
            };
        }
    }

    /** */
    private void fillCache(
        IgniteDataStreamer streamer,
        int cacheSize,
        BiFunction<String, Integer, String> generatingFunc
    ) {
        String name = streamer.cacheName();

        for (int i = 0; i < cacheSize; i++)
            streamer.addData(i, generatingFunc.apply(name, i));
    }

    /** */
    private void fillCache(
        IgniteDataStreamer streamer,
        Collection<Integer> keys,
        BiFunction<String, Integer, String> generatingFunc
    ) {
        String cacheName = streamer.cacheName();

        for (Integer key : keys)
            streamer.addData(key, generatingFunc.apply(cacheName, key));
    }

    /** */
    private void verifyCache(IgniteCache cache, BiFunction<String, Integer, String> generatingFunc) {
        int size = cache.size(CachePeekMode.PRIMARY);

        String cacheName = cache.getName();

        for (int i = 0; i < size; i++) {
            String value = (String) cache.get(i);

            assertEquals(generatingFunc.apply(cacheName, i), value);
        }
    }
}

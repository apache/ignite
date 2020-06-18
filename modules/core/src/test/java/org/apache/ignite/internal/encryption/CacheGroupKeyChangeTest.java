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

package org.apache.ignite.internal.encryption;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.DiscoveryHook;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ACTIVE_KEY_ID_FOR_GROUP;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 *
 */
public class CacheGroupKeyChangeTest extends AbstractEncryptionTest {
    /** Timeout. */
    private static final long MAX_AWAIT_MILLIS = 15_000;

    /** */
    private static final String GRID_2 = "grid-2";

    /** Discovery hook for distributed process. */
    private InitMessageDiscoveryHook discoveryHook;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);
        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        if (discoveryHook != null)
            ((TestTcpDiscoverySpi)cfg.getDiscoverySpi()).discoveryHook(discoveryHook);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(100L * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setPageSize(4 * 1024)
            .setWalSegmentSize(1024 * 1024)
            .setWalSegments(4)
            .setMaxWalArchiveSize(10 * 1024 * 1024)
            .setCheckpointFrequency(30 * 1000L)
            .setWalMode(LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected <K, V> CacheConfiguration<K, V> cacheConfiguration(String name, String grp) {
        CacheConfiguration<K, V> cfg = super.cacheConfiguration(name, grp);

        return cfg.setAffinity(new RendezvousAffinityFunction(false, 8));
    }

    /** @throws Exception If failed. */
    @Test
    public void testRejectNodeJoinDuringRotation() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        createEncryptedCache(grids.get1(), grids.get2(), cacheName(), null);

        int grpId = CU.cacheId(cacheName());

        assertEquals(0, grids.get1().context().encryption().groupKey(grpId).id());

        TestRecordingCommunicationSpi commSpi = TestRecordingCommunicationSpi.spi(grids.get2());

        commSpi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        IgniteFuture<Void> fut = grids.get1().encryption().changeGroupKey(Collections.singleton(cacheName()));

        commSpi.waitForBlocked();

        assertThrowsWithCause(() -> startGrid(3), IgniteCheckedException.class);

        commSpi.stopBlock();

        fut.get();

        checkGroupKey(grpId, 1, MAX_AWAIT_MILLIS);

        checkEncryptedCaches(grids.get1(), grids.get2());
    }

    /** @throws Exception If failed. */
    @Test
    public void testRejectWhenNotAllBltNodesPresent() throws Exception {
        startTestGrids(true);

        createEncryptedCache(grid(GRID_0), grid(GRID_1), cacheName(), null);

        stopGrid(GRID_1);

        assertThrowsAnyCause(log, () -> {
            return grid(GRID_0).encryption().changeGroupKey(Collections.singleton(cacheName()));
        }, IgniteException.class, "Not all baseline nodes online [total=2, online=1]");
    }

    /** @throws Exception If failed. */
    @Test
    public void testNodeFailsBeforePrepare() throws Exception {
        checkNodeFailsDuringRotation(false, true, true);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCrdFailsBeforePrepare() throws Exception {
        checkNodeFailsDuringRotation(true, true, true);
    }

    /** @throws Exception If failed. */
    @Test
    public void testNodeFailsBeforePerform() throws Exception {
        checkNodeFailsDuringRotation(false, false, true);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCrdFailsBeforePerform() throws Exception {
        checkNodeFailsDuringRotation(true, false, true);
    }

    /** @throws Exception If failed. */
    @Test
    public void testNodeFailsAfterPrepare() throws Exception {
        checkNodeFailsDuringRotation(false, true, false);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCrdFailsAfterPrepare() throws Exception {
        checkNodeFailsDuringRotation(true, true, false);
    }

    /** @throws Exception If failed. */
    @Test
    public void testNodeFailsAfterPerform() throws Exception {
        checkNodeFailsDuringRotation(false, false, false);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCrdFailsAfterPerform() throws Exception {
        checkNodeFailsDuringRotation(true, false, false);
    }

    /**
     * @param stopCrd {@code True} to stop coordinator.
     * @param prepare {@code True} to stop on the prepare phase. {@code False} to stop on the perform phase.
     * @param discoBlock  {@code True} to block discovery, {@code False} to block communication SPI.
     */
    private void checkNodeFailsDuringRotation(boolean stopCrd, boolean prepare, boolean discoBlock) throws Exception {
        DistributedProcessType type = prepare ?
            DistributedProcessType.GROUP_KEY_CHANGE_PREPARE : DistributedProcessType.GROUP_KEY_CHANGE_FINISH;

        InitMessageDiscoveryHook locHook = new InitMessageDiscoveryHook(type);

        if (discoBlock && stopCrd)
            discoveryHook = locHook;

        IgniteEx grid0 = startGrid(GRID_0);

        if (discoBlock && !stopCrd)
            discoveryHook = locHook;

        IgniteEx grid1 = startGrid(GRID_1);

        grid0.cluster().active(true);

        createEncryptedCache(grid0, grid1, cacheName(), null);

        int grpId = CU.cacheId(cacheName());

        int keyId = 0;

        checkGroupKey(grpId, keyId, MAX_AWAIT_MILLIS);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid1);

        if (!discoBlock) {
            AtomicBoolean preparePhase = new AtomicBoolean(true);

            spi.blockMessages((node, msg) -> {
                if (msg instanceof SingleNodeMessage) {
                    boolean isPrepare = preparePhase.compareAndSet(true, false);

                    return prepare || !isPrepare;
                }

                return false;
            });
        }

        String alive = stopCrd ? GRID_1 : GRID_0;
        String stopped = stopCrd ? GRID_0 : GRID_1;

        IgniteFuture<Void> fut = grid(alive).encryption().changeGroupKey(Collections.singleton(cacheName()));

        IgniteInternalFuture stopFut = null;

        if (!discoBlock) {
            spi.waitForBlocked();

            stopFut = runAsync(() -> {
                if (stopCrd)
                    stopGrid(GRID_0, true);
                else
                    stopGrid(GRID_1, true);
            });
        }
        else {
            locHook.waitForBlocked(MAX_AWAIT_MILLIS);

            if (stopCrd)
                stopGrid(GRID_0, true);
            else
                stopGrid(GRID_1, true);

            locHook.stopBlock();
        }

        if (prepare)
            assertThrowsAnyCause(log, fut::get, IgniteCheckedException.class, null);
        else {
            keyId++;

            fut.get(MAX_AWAIT_MILLIS);
        }

        if (stopFut != null)
            stopFut.get();

        checkGroupKey(grpId, keyId, MAX_AWAIT_MILLIS);

        if (prepare) {
            IgniteEx stoppedNode = startGrid(stopped);

            stoppedNode.resetLostPartitions(Collections.singleton(ENCRYPTED_CACHE));

            awaitPartitionMapExchange();

            forceCheckpoint(stoppedNode);

            stoppedNode.encryption().changeGroupKey(Collections.singleton(cacheName())).get(MAX_AWAIT_MILLIS);

            checkGroupKey(grpId, keyId + 1, MAX_AWAIT_MILLIS);
        }
        else {
            System.setProperty(IGNITE_ACTIVE_KEY_ID_FOR_GROUP + grpId, String.valueOf(keyId));

            try {
                IgniteEx stoppedNode = startGrid(stopped);

                stoppedNode.resetLostPartitions(Collections.singleton(ENCRYPTED_CACHE));

                awaitPartitionMapExchange();

                forceCheckpoint(stoppedNode);

                stoppedNode.encryption().changeGroupKey(Collections.singleton(cacheName())).get(MAX_AWAIT_MILLIS);

                checkGroupKey(grpId, keyId + 1, MAX_AWAIT_MILLIS);
            } finally {
                System.clearProperty(IGNITE_ACTIVE_KEY_ID_FOR_GROUP + grpId);
            }
        }
    }

    /**
     * Ensures that we can rotate the key more than 255 times.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testKeyIdentifierOverflow() throws Exception {
        startTestGrids(true);

        IgniteEx node0 = grid(GRID_0);
        IgniteEx node1 = grid(GRID_1);

        createEncryptedCache(node0, node1, cacheName(), null);

        int grpId = CU.cacheId(cacheName());

        int maxItrs = 0xff * 2;

        for (int i = 0; i < maxItrs; i++) {
            node0.encryption().changeGroupKey(Collections.singleton(cacheName())).get();

            awaitEncryption(G.allGrids(), grpId, MAX_AWAIT_MILLIS);

            forceCheckpoint();

            assertEquals((byte)(i + 1), node0.context().encryption().groupKey(grpId).id());
        }

        checkGroupKey(grpId, (byte)maxItrs & 0xff, MAX_AWAIT_MILLIS);
    }

    /**
     * Ensures that after rotation, the node has correct key identifier.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeJoinAfterChange() throws Exception {
        startTestGrids(true);

        IgniteEx node0 = grid(GRID_0);
        IgniteEx node1 = grid(GRID_1);

        createEncryptedCache(node0, node1, cacheName(), null);

        node0.encryption().changeGroupKey(Collections.singleton(cacheName())).get();

        startGrid(GRID_2);

        resetBaselineTopology();

        awaitPartitionMapExchange();

        checkGroupKey(CU.cacheId(cacheName()), 1, MAX_AWAIT_MILLIS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheStartDuringRotation() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        createEncryptedCache(grids.get1(), grids.get2(), cacheName(), null);

        TestRecordingCommunicationSpi commSpi = TestRecordingCommunicationSpi.spi(grids.get2());

        commSpi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        IgniteFuture<Void> fut = grids.get1().encryption().changeGroupKey(Collections.singleton(cacheName()));

        commSpi.waitForBlocked();

        IgniteCache<Integer, Integer> cache = grids.get1().createCache(cacheConfiguration("cache1", null));

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        commSpi.stopBlock();

        fut.get();

        checkGroupKey(CU.cacheId(cacheName()), 1, MAX_AWAIT_MILLIS);

        checkGroupKey(CU.cacheId("cache1"), 0, MAX_AWAIT_MILLIS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheStartSameGroupDuringRotation() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        String grpName = "shared";

        createEncryptedCache(grids.get1(), grids.get2(), cacheName(), grpName);

        TestRecordingCommunicationSpi commSpi = TestRecordingCommunicationSpi.spi(grids.get2());

        commSpi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        IgniteFuture<Void> fut = grids.get1().encryption().changeGroupKey(Collections.singleton(grpName));

        commSpi.waitForBlocked();

        IgniteCache<Integer, Integer> cache =
            grids.get1().createCache(cacheConfiguration("cache1", grpName));

        commSpi.stopBlock();

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        fut.get();

        checkGroupKey(CU.cacheId(grpName), 1, MAX_AWAIT_MILLIS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testChangeKeyDuringRebalancing() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        IgniteEx node0 = grids.get1();
        IgniteEx node1 = grids.get2();

        createEncryptedCache(node0, node1, cacheName(), null);

        loadData(500_000);

        IgniteEx node2 = startGrid(GRID_2);

        resetBaselineTopology();

        int grpId = CU.cacheId(cacheName());

        IgniteFuture fut = node2.encryption().changeGroupKey(Collections.singleton(cacheName()));

        fut.get(MAX_AWAIT_MILLIS);

        stopAllGrids();

        startGrid(GRID_0);
        startGrid(GRID_1);
        startGrid(GRID_2);

        grid(GRID_0).cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        checkGroupKey(grpId, 1, MAX_AWAIT_MILLIS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBasicChange() throws Exception {
        startTestGrids(true);

        IgniteEx node1 = grid(GRID_0);
        IgniteEx node2 = grid(GRID_1);

        createEncryptedCache(node1, node2, cacheName(), null);

        forceCheckpoint();

        IgniteInternalCache<Object, Object> cache = node1.cachex(cacheName());

        int grpId = cache.context().groupId();

        node1.encryption().changeGroupKey(Collections.singleton(cacheName())).get();

        Map<Integer, Integer> keys1 = node1.context().encryption().groupKeysInfo(grpId);
        Map<Integer, Integer> keys2 = node2.context().encryption().groupKeysInfo(grpId);

        assertEquals(2, keys1.size());
        assertEquals(2, keys2.size());

        assertEquals(keys1, keys2);

        info("New key was set on all nodes [grpId=" + grpId + ", keys=" + keys1 + "]");

        checkGroupKey(grpId, 1, MAX_AWAIT_MILLIS);

        stopAllGrids();

        node1 = startGrid(GRID_0);
        node2 = startGrid(GRID_1);

        node1.cluster().state(ClusterState.ACTIVE);

        try (IgniteDataStreamer<Integer, String> streamer = node1.dataStreamer(cacheName())) {
            for (int i = 1_000; i < 500_000; i++) {
                streamer.addData(i, String.valueOf(i));

                if (i % 1_000 == 0 &&
                    node1.context().encryption().groupKeysInfo(grpId).size() == 1 &&
                    node2.context().encryption().groupKeysInfo(grpId).size() == 1)
                    break;
            }
        }

        assertEquals(1, node1.context().encryption().groupKeysInfo(grpId).size());
        assertEquals(1, node2.context().encryption().groupKeysInfo(grpId).size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBasicChangeWithConstantLoad() throws Exception {
        startTestGrids(true);

        IgniteEx node0 = grid(GRID_0);
        IgniteEx node1 = grid(GRID_1);

        createEncryptedCache(node0, node1, cacheName(), null);

        forceCheckpoint();

        IgniteInternalCache<Object, Object> cache = node0.cachex(cacheName());

        AtomicInteger cntr = new AtomicInteger(cache.size());

        CountDownLatch startLatch = new CountDownLatch(1);

        final Ignite somenode = node0;

        IgniteInternalFuture loadFut = GridTestUtils.runAsync(() -> {
            try (IgniteDataStreamer<Integer, String> streamer = somenode.dataStreamer(cacheName())) {
                while (!Thread.currentThread().isInterrupted()) {
                    int n = cntr.getAndIncrement();

                    streamer.addData(n, String.valueOf(n));

                    if (n == 5000)
                        startLatch.countDown();
                }
            }
        });

        startLatch.await(MAX_AWAIT_MILLIS, TimeUnit.MILLISECONDS);

        int grpId = cache.context().groupId();

        node0.encryption().changeGroupKey(Collections.singleton(cacheName())).get(MAX_AWAIT_MILLIS);

        Map<Integer, Integer> keys1 = node0.context().encryption().groupKeysInfo(grpId);
        Map<Integer, Integer> keys2 = node1.context().encryption().groupKeysInfo(grpId);

        assertEquals(2, keys1.size());
        assertEquals(2, keys2.size());

        assertEquals(keys1, keys2);

        awaitEncryption(G.allGrids(), grpId, MAX_AWAIT_MILLIS);

        forceCheckpoint();

        loadFut.cancel();

        // Ensure that data is encrypted with the new key.
        checkGroupKey(grpId, 1, MAX_AWAIT_MILLIS);

        stopAllGrids();

        node0 = startGrid(GRID_0);
        node1 = startGrid(GRID_1);

        node0.cluster().state(ClusterState.ACTIVE);

        // Wait for WAL segment remove..
        try (IgniteDataStreamer<Integer, String> streamer = node0.dataStreamer(cacheName())) {
            int start = cntr.get();

            for (; ; ) {
                int n = cntr.getAndIncrement();

                streamer.addData(n, String.valueOf(n));

                if (n % 1000 == 0 &&
                    node0.context().encryption().groupKeysInfo(grpId).size() == 1 &&
                    node1.context().encryption().groupKeysInfo(grpId).size() == 1)
                    break;

                if (n - start == 500_000)
                    break;
            }
        }

        assertEquals(1, node0.context().encryption().groupKeysInfo(grpId).size());
        assertEquals(1, node1.context().encryption().groupKeysInfo(grpId).size());
    }

    /**
     * Custom discovery hook to block distributed process.
     */
    private static class InitMessageDiscoveryHook extends DiscoveryHook {
        /**
         * Latch to sync execution.
         */
        private final CountDownLatch unlockLatch = new CountDownLatch(1);

        /**
         * Latch to sync execution.
         */
        private final CountDownLatch blockedLatch = new CountDownLatch(1);

        /**
         * Distributed process type.
         */
        private final DistributedProcessType type;

        /**
         * @param type Distributed process type.
         */
        private InitMessageDiscoveryHook(DistributedProcessType type) {
            this.type = type;
        }

        /** {@inheritDoc} */
        @Override public void beforeDiscovery(DiscoveryCustomMessage customMsg) {
            if (!(customMsg instanceof InitMessage))
                return;

            InitMessage msg = (InitMessage)customMsg;

            if (msg.type() != type.ordinal())
                return;

            try {
                blockedLatch.countDown();

                unlockLatch.await(MAX_AWAIT_MILLIS, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        }

        /**
         * @param timeout Timeout in milliseconds.
         * @throws InterruptedException If interrupted.
         */
        public void waitForBlocked(long timeout) throws InterruptedException {
            blockedLatch.await(timeout, TimeUnit.MILLISECONDS);
        }

        /** */
        public void stopBlock() {
            unlockLatch.countDown();
        }
    }
}

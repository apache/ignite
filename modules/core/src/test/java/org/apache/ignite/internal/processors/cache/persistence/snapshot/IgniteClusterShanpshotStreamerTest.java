/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerCacheUpdaters;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerRequest;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.stream.StreamReceiver;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests snapshot is consistent or warned under streaming load.
 */
public class IgniteClusterShanpshotStreamerTest extends AbstractSnapshotSelfTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    public static final String ERR_MSG = "Cache partitions differ for cache groups ";

    /** */
    private TcpDiscoverySpi discoverySpi;

    /** */
    private IgniteSnapshotManager grid0SnpMgr;

    /** */
    private volatile @Nullable CountDownLatch beginSecondStageLatch;

    /** */
    private volatile @Nullable CountDownLatch continueSecondStageLatch;

    /** */
    private IgniteEx notBlNode;

    /** */
    private IgniteEx client;

    /** {@inheritDoc} */
    @Override public void beforeTestSnapshot() throws Exception {
        super.beforeTestSnapshot();

        persistence = true;

        discoverySpi = waitingAtSecondStageDiscoSpi();

        startGrid(0);

        discoverySpi = null;

        // One more server node.
        startGrid(1);

        grid(0).cluster().state(ACTIVE);

        grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

        // Not-baseline server node.
        notBlNode = startGrid(G.allGrids().size());

        client = startClientGrid(G.allGrids().size());

        grid0SnpMgr = snp(grid(0));

        dfltCacheCfg = defaultCacheConfiguration();

        dfltCacheCfg
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getDataStorageConfiguration().setWalMode(WALMode.NONE);
        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setMaxSize(128L * 1024L * 1024L);

        if (discoverySpi != null)
            cfg.setDiscoverySpi(discoverySpi);

        assert cfg.getDiscoverySpi() instanceof TcpDiscoverySpi;

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new DataLoosingCommunicationSpi());

        return cfg;
    }

    /**
     * Tests snapshot consistency wnen streamer failed or canceled before snapshot. Default receiver.
     */
    @Test
    public void testDsFailsLongAgoDflt() throws Exception {
        for (Ignite ldr : Arrays.asList(notBlNode, client)) {
            prepareRunWithNewNode(ldr);

            doTestDsFailsBeforeSnp(ldr, true, null);
        }
    }

    /**
     * Tests snapshot consistency wnen streamer failed or canceled before snapshot. Batched receiver.
     */
    @Test
    public void testDsFailsLongAgoBatched() throws Exception {
        for (Ignite ldr : Arrays.asList(notBlNode, client)) {
            prepareRunWithNewNode(ldr);

            doTestDsFailsBeforeSnp(ldr, false, DataStreamerCacheUpdaters.batched());
        }
    }

    /**
     * Test snapshot consistency when streamer starts right after init snapshot process request. Default receiver.
     */
    @Test
    public void testDsBeginsAtStartStageDflt() throws Exception {
        for (Ignite ldr : testGrids()) {
            prepareRunWithNewNode(ldr);

            doTestDsBeginsAtStartStage(null, (IgniteEx)ldr);
        }
    }

    /**
     * Test snapshot consistency when streamer starts right after init snapshot process request. Batched receiver.
     */
    @Test
    public void testDsBeginsAtStartStageBatched() throws Exception {
        for (Ignite ldr : testGrids()) {
            prepareRunWithNewNode(ldr);

            doTestDsBeginsAtStartStage(DataStreamerCacheUpdaters.batched(), (IgniteEx)ldr);
        }
    }

    /**
     * Test snapshot consistency when streamer starts when snapshot process is not finished yet. Default receiver.
     */
    @Test
    public void testDsBeginsAtEndStageDflt() throws Exception {
        for (Ignite ldr : testGrids()) {
            prepareRunWithNewNode(ldr);

            doTestDsBeginsAtEndStage(null, ldr);
        }
    }

    /**
     * Test snapshot consistency when streamer starts when snapshot process is not finished yet. Batched receiver.
     */
    @Test
    public void testDsBeginsAtEndStageBatched() throws Exception {
        for (Ignite ldr : testGrids()) {
            prepareRunWithNewNode(ldr);

            doTestDsBeginsAtEndStage(DataStreamerCacheUpdaters.batched(), ldr);
        }
    }

    /**
     * Tests snapshot consistency wnen streamer starts before snapshot. Default receiver.
     */
    @Test
    public void testDsBeginsBeforeSnpDflt() throws Exception {
        for (Ignite ldr : testGrids()) {
            prepareRunWithNewNode(ldr);

            doTestDsBeginsBeforeSnp(ldr, true, null);
        }
    }

    /**
     * Tests snapshot consistency wnen streamer starts before snapshot. Batched receiver.
     */
    @Test
    public void testDsBeginsBeforeSnpBatched() throws Exception {
        for (Ignite ldr : testGrids()) {
            prepareRunWithNewNode(ldr);

            doTestDsBeginsBeforeSnp(ldr, false, DataStreamerCacheUpdaters.batched());
        }
    }

    /**
     * Tests snapshot consistecy when streamer starts before snapshot.
     *
     * @param ldrNode  Streaming node.
     * @param mustFail If {@code true}, checks snapshot process warns of inconsistency and ensures snapshot validation
     *                 finds errors. Otherwise, ensures snapshot validation is ok.
     * @param receiver Stream receiver. {@code Null} for default.
     */
    private void doTestDsBeginsBeforeSnp(Ignite ldrNode, boolean mustFail,
        @Nullable StreamReceiver<Integer, Object> receiver) throws Exception {
        int preLoadCnt = 5_000;

        CountDownLatch preload = new CountDownLatch(preLoadCnt);
        AtomicBoolean stopLoading = new AtomicBoolean(false);

        IgniteInternalFuture<?> loadFut = runLoad(ldrNode, receiver, preload, stopLoading, false, true);

        preload.await();

        try {
            if (mustFail)
                assertThrows(null, () -> grid0SnpMgr.createSnapshot(SNAPSHOT_NAME).get(),
                    IgniteException.class, ERR_MSG);
            else
                grid0SnpMgr.createSnapshot(SNAPSHOT_NAME).get();
        }
        finally {
            stopLoading.set(true);
            loadFut.get();
        }

        checkSnp(mustFail);
    }

    /**
     * Test snapshot consistency when streamer starts right after init snapshot process request.
     *
     * @param rcvr Stream receiver.
     * @param ldr Loader node.
     */
    private void doTestDsBeginsAtStartStage(StreamReceiver<Integer, Object> rcvr, IgniteEx ldr) throws Exception {
        fillCache(5_000, ldr);

        AtomicBoolean stopLoading = new AtomicBoolean();
        AtomicBoolean doLoadAtSnp = new AtomicBoolean(true);
        AtomicReference<IgniteInternalFuture<?>> loadDuringSnp = new AtomicReference<>();

        ldr.context().discovery().setCustomEventListener(InitMessage.class, (topVer, sender, msg) -> {
            SnapshotOperationRequest snpRq = extractSnpRequest(msg);

            if (snpRq == null || !doLoadAtSnp.compareAndSet(true, false))
                return;

            CountDownLatch loadLatch = new CountDownLatch(2_000);

            try {
                loadDuringSnp.set(runLoad(ldr, rcvr, loadLatch, stopLoading, false, true));

                loadLatch.await();
            }
            catch (InterruptedException ignored) {
                // No-op.
            }
        });

        try {
            grid0SnpMgr.createSnapshot(SNAPSHOT_NAME).get();
        }
        finally {
            stopLoading.set(true);
            assert loadDuringSnp.get() != null;
            loadDuringSnp.get().get();
        }

        checkSnp(false);
    }

    /**
     * Test snapshot consistency when streamer starts when snapshot process is not finished yet.
     *
     * @param receiver Stream receiver. {@code Null} for default.
     * @param ldr Loader node.
     */
    private void doTestDsBeginsAtEndStage(@Nullable StreamReceiver<Integer, Object> receiver, Ignite ldr)
        throws Exception {
        int preLoadCnt = 5_000;

        fillCache(preLoadCnt, ldr);

        // Streamer won't be launched before snapshot process initialized.
        beginSecondStageLatch = new CountDownLatch(1);
        // Snapshot process won't continue before streamer set.
        continueSecondStageLatch = new CountDownLatch(1);

        IgniteFuture<?> snpFut = grid0SnpMgr.createSnapshot(SNAPSHOT_NAME);

        // Wait for the snapshot initialization.
        beginSecondStageLatch.await();

        AtomicBoolean stopLoading = new AtomicBoolean();
        CountDownLatch dataLoadCounter = new CountDownLatch(preLoadCnt);

        IgniteInternalFuture<?> loadFut = runLoad(ldr, receiver, dataLoadCounter, stopLoading, false, true);

        continueSecondStageLatch.await();
        dataLoadCounter.await();

        try {
            snpFut.get();
        }
        finally {
            stopLoading.set(true);

            loadFut.get();
        }

        checkSnp(false);
    }

    /**
     * Tests snapshot consistency wnen streamer failed or canceled before snapshot.
     */
    private void doTestDsFailsBeforeSnp(Ignite ldr, boolean mustFail,
        @Nullable StreamReceiver<Integer, Object> receiver) throws Exception {
        CountDownLatch preloadLatch = new CountDownLatch(10_000);
        AtomicBoolean stopLoad = new AtomicBoolean();

        IgniteInternalFuture<?> loadFut = runLoad(ldr, receiver, preloadLatch, stopLoad, true, true);

        preloadLatch.await();
        stopLoad.set(true);
        loadFut.get();

        createAndCheckSnp(mustFail);
    }

    /** */
    private void createAndCheckSnp(boolean mustFail) throws IgniteCheckedException {
        if (mustFail)
            assertThrows(null, () -> grid0SnpMgr.createSnapshot(SNAPSHOT_NAME).get(),
                IgniteException.class, ERR_MSG);
        else
            grid0SnpMgr.createSnapshot(SNAPSHOT_NAME).get();

        checkSnp(mustFail);
    }

    /** */
    private void checkSnp(boolean mustFail) throws IgniteCheckedException {
        IdleVerifyResultV2 checkRes = grid0SnpMgr.checkSnapshot(SNAPSHOT_NAME, null).get();

        assertTrue(mustFail == checkRes.hasConflicts());
        assertTrue(checkRes.exceptions().isEmpty());
    }

    /**
     * Pre-laods cache.
     */
    private void fillCache(int preLoadCnt, Ignite ldrNode) throws Exception {
        CountDownLatch preload = new CountDownLatch(preLoadCnt);
        AtomicBoolean stopLoading = new AtomicBoolean(false);

        IgniteInternalFuture<?> loadFut = runLoad(ldrNode, null, preload, stopLoading, false, false);
        preload.await();
        stopLoading.set(true);
        loadFut.get();
    }

    /**
     * Loads or pre-loads cache.
     *
     * @param ldr Loader node.
     * @param receiver Stream receiver. {@code Null} for default.
     * @param dataSendCounter Data counter submitted to the streamer.
     * @param stop Stop load flag.
     * @param stop f {@code true}, cancels streaming without graceful flushing.
     * @param maleficent If {@code true}, erases batches to one of server node to ensure of snapshot inconsistency.
     */
    private IgniteInternalFuture<?> runLoad(Ignite ldr, @Nullable StreamReceiver<Integer, Object> receiver,
        CountDownLatch dataSendCounter, AtomicBoolean stop, boolean cancel, boolean maleficent) {

        return GridTestUtils.runMultiThreadedAsync(() -> {
            DataLoosingCommunicationSpi cm = (DataLoosingCommunicationSpi)ldr.configuration().getCommunicationSpi();

            boolean blockSet = false;

            if (maleficent) {
                Set<Object> bl = grid(0).cluster().currentBaselineTopology().stream().map(BaselineNode::consistentId)
                    .collect(Collectors.toSet());

                for (Ignite ig : G.allGrids()) {
                    if (ig.cluster().localNode().equals(ldr.cluster().localNode())
                        || !bl.contains(ig.cluster().localNode().consistentId()))
                        continue;

                    cm.block(ig.cluster().localNode().id());

                    blockSet = true;

                    break;
                }

                assert blockSet;
            }

            try (IgniteDataStreamer<Integer, Object> ds = ldr.dataStreamer(dfltCacheCfg.getName())) {
                if (receiver != null)
                    ds.receiver(receiver);

                // Makes predictable batches sequence.
                ds.perNodeBufferSize(256);
                ds.perThreadBufferSize(ds.perNodeBufferSize() * 4);
                ds.perNodeParallelOperations(3);

                int idx = 0;

                while (!stop.get()) {
                    ds.addData(idx, idx);

                    if (continueSecondStageLatch != null)
                        continueSecondStageLatch.countDown();

                    idx++;

                    if (dataSendCounter.getCount() > 0) {
                        dataSendCounter.countDown();

                        if (dataSendCounter.getCount() == 0)
                            ds.flush();
                    }
                }

                if (cancel)
                    ds.close(true);
            }
            finally {
                while (dataSendCounter.getCount() > 0)
                    dataSendCounter.countDown();

                if (blockSet)
                    cm.stopBlocking();
            }
        }, 1, "load-thread");
    }

    /**
     * @return DiscoverySpi able to wait for data streamer start in the middle of snapshot process.
     */
    private TcpDiscoverySpi waitingAtSecondStageDiscoSpi() {
        return new TcpDiscoverySpi() {
            /** {@inheritDoc} */
            @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
                SnapshotOperationRequest snpRq = extractSnpRequest(msg);

                if (snpRq != null) {
                    if (beginSecondStageLatch != null && snpRq.startStageEnded()) {
                        assert continueSecondStageLatch != null;

                        beginSecondStageLatch.countDown();

                        try {
                            continueSecondStageLatch.await();
                        }
                        catch (InterruptedException e) {
                            throw new IgniteException("Unable to wait for streamer started.", e);
                        }
                    }
                }

                super.sendCustomEvent(msg);
            }
        };
    }

    /** */
    private void prepareRunWithNewNode(Ignite node) throws InterruptedException {
        if (log.isInfoEnabled()) {
            log.info("Testing with node: " + node.cluster().localNode().id() +
                ", order: " + node.cluster().localNode().order() +
                ", client: " + node.cluster().localNode().isClient() +
                ", baseline: " + node.cluster().currentBaselineTopology().stream().map(BaselineNode::consistentId)
                .collect(Collectors.toSet()).contains(node.cluster().localNode().consistentId()));
        }

        U.delete(grid0SnpMgr.snapshotLocalDir(SNAPSHOT_NAME));

        grid(0).destroyCache(dfltCacheCfg.getName());

        awaitPartitionMapExchange();

        grid(0).createCache(dfltCacheCfg);

        awaitPartitionMapExchange();
    }

    /**
     * @param msg Message.
     * @return SnapshotOperationRequest if {@code msg} is snapshot operation message. {@code Null} otherwise.
     */
    private static SnapshotOperationRequest extractSnpRequest(DiscoverySpiCustomMessage msg) {
        if (!(msg instanceof CustomMessageWrapper))
            return null;

        CustomMessageWrapper cmw = (CustomMessageWrapper)msg;

        return cmw.delegate() instanceof InitMessage ? extractSnpRequest((InitMessage<?>)cmw.delegate()) : null;
    }

    /**
     * @param im Message.
     * @return SnapshotOperationRequest if {@code msg} is snapshot operation message. {@code Null} otherwise.
     */
    private static SnapshotOperationRequest extractSnpRequest(InitMessage<?> im) {
        return im.request() instanceof SnapshotOperationRequest ? (SnapshotOperationRequest)im.request() : null;
    }

    /**
     * @return Test grids to start streaming from.
     */
    private List<Ignite> testGrids() {
        List<Ignite> testNodes = G.allGrids();

        // No need to run from both server nodes.
        testNodes.removeIf(n -> n.cluster().localNode().order() == 1);

        assert testNodes.size() == G.allGrids().size() - 1;

        return testNodes;
    }

    /**
     * Drops streamer entries from the batches to certain node. Simulates late data income to the node. Makes data
     * inconsistent if data contains separates backup and primary records. {@link DataStreamerImpl.IsolatedUpdater}
     * does so. Doesn't affect consistency with normal cache loads with 'cache.put()' or 'cache.putAll()' witch are
     * used with stream receivers like {@link DataStreamerCacheUpdaters#batched()}. Doesn't drop messages to allow
     * Datastreamer work further.
     */
    private static class DataLoosingCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private final AtomicReference<UUID> block = new AtomicReference<>();
        
        /** */
        private final AtomicBoolean blocked = new AtomicBoolean();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage && (((GridIoMessage)msg).message() instanceof DataStreamerRequest)) {
                UUID toBlock = block.get();

                if (toBlock != null && toBlock.equals(node.id())) {
                    blocked.set(true);
                    
                    DataStreamerRequest dsrq = ((DataStreamerRequest)((GridIoMessage)msg).message());

                    AtomicInteger cnt = new AtomicInteger(dsrq.entries().size());

                    dsrq.entries().removeIf(e -> cnt.decrementAndGet() > 0);
                }
            }

            super.sendMessage(node, msg, ackC);
        }
        
        /** */
        public void block(UUID nodeId) {
            block.set(nodeId);

            blocked.set(false);
        }
        
        /** */
        public void stopBlocking() {
            block.set(null);

            assertTrue("No messages was blocked.", blocked.compareAndSet(true, false));
        }
    }
}

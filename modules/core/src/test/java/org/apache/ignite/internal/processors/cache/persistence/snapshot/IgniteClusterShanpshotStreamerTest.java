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

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
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
import org.apache.ignite.internal.processors.datastreamer.DataStreamerRequest;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.typedef.G;
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
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests snapshot is consistent under streaming load.
 */
@WithSystemProperty(key = IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK, value = "true")
public class IgniteClusterShanpshotStreamerTest extends AbstractSnapshotSelfTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String ERR_MSG = "Such updates may break data consistency until finished. Snapshot might " +
        "not be entirely restored";

    /** */
    private TcpDiscoverySpi discoverySpi;

    /** */
    private IgniteEx baselineServer;

    /** Excluded by cache filter. */
    private Ignite ignoredServer;

    /** */
    private IgniteEx notBaselineServer;

    /** */
    private IgniteEx client;

    /** */
    private IgniteSnapshotManager snpMngr;

    /** */
    private volatile @Nullable CountDownLatch launchDsLock;

    /** */
    private volatile @Nullable CountDownLatch dsRegisteredLock;

    /** {@inheritDoc} */
    @Override public void beforeTestSnapshot() throws Exception {
        super.beforeTestSnapshot();

        persistence = true;

        discoverySpi = waitingAtSnpProcSecondStageDiscoSpi();

        startGrid(0);

        discoverySpi = null;

        baselineServer = startGrid(1);

        ignoredServer = startGrid(2);

        grid(0).cluster().state(ACTIVE);

        grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

        notBaselineServer = startGrid(G.allGrids().size());

        client = startClientGrid(G.allGrids().size());

        snpMngr = snp(grid(0));

        dfltCacheCfg = defaultCacheConfiguration();

        dfltCacheCfg
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setNodeFilter(n -> !n.id().equals(ignoredServer.cluster().localNode().id()))
            .setBackups(1);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getDataStorageConfiguration().setWalMode(WALMode.NONE);

        if (discoverySpi != null)
            cfg.setDiscoverySpi(discoverySpi);

        assert cfg.getDiscoverySpi() instanceof TcpDiscoverySpi;

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new DataLoosingCommunicationSpi());

        return cfg;
    }

    /** @throws Exception If fails. */
    @Test
    public void testDsNextToStageDfltRcvrBlSrv() throws Exception {
        doTestDsNextToFirstStage(null, baselineServer);
    }

    /** @throws Exception If fails. */
    @Test
    public void testDsNextToStageDfltRcvrNotBlSrv() throws Exception {
        doTestDsNextToFirstStage(null, notBaselineServer);
    }

    /** @throws Exception If fails. */
    @Test
    public void testDsNextToStageDfltRcvrClient() throws Exception {
        doTestDsNextToFirstStage(null, client);
    }

    /** @throws Exception If fails. */
    @Test
    public void intestDsNextToStageDfltRcvrBlSrv() throws Exception {
        doTestDsNextToFirstStage(null, baselineServer);
    }

    /** @throws Exception If fails. */
    @Test
    public void testDsNextToStageBatchRcvrClient() throws Exception {
        doTestDsNextToFirstStage(DataStreamerCacheUpdaters.batched(), client);
    }

    /** @throws Exception If fails. */
    @Test
    public void testDsNextToStageBatchRcvrBlSrv() throws Exception {
        doTestDsNextToFirstStage(DataStreamerCacheUpdaters.batched(), baselineServer);
    }

    /** @throws Exception If fails. */
    @Test
    public void testDsNextToStageBatchRcvrNotBlSrv() throws Exception {
        doTestDsNextToFirstStage(DataStreamerCacheUpdaters.batched(), notBaselineServer);
    }

    /** @throws Exception If fails. */
    @Test
    public void testDsAtSecondStageBatchRcvrClient() throws Exception {
        doTestDsAtSecondStage(DataStreamerCacheUpdaters.batched(), client);
    }

    /** @throws Exception If fails. */
    @Test
    public void testDsAtSecondStageDftRcvrClient() throws Exception {
        doTestDsAtSecondStage(null, client);
    }

    /** @throws Exception If fails. */
    @Test
    public void testDsAtSecondStageDftRcvrBsSrv() throws Exception {
        doTestDsAtSecondStage( null, baselineServer);
    }

    /** @throws Exception If fails. */
    @Test
    public void testDsAtSecondStageDftRcvrNonBsSrv() throws Exception {
        doTestDsAtSecondStage( null, notBaselineServer);
    }

    /** @throws Exception If fails. */
    @Test
    public void testDsBeforeSnpDftRcvrClient() throws Exception {
        dotestDsBeforeSnp(client, true, null);
    }

    /** @throws Exception If fails. */
    @Test
    public void testDsBeforeSnpDftRcvrBsSrv() throws Exception {
        dotestDsBeforeSnp(baselineServer, true, null);
    }

    /** @throws Exception If fails. */
    @Test
    public void testDsBeforeSnpDftRcvrNotBsSrv() throws Exception {
        dotestDsBeforeSnp(notBaselineServer, true, null);
    }

    /** @throws Exception If fails. */
    @Test
    public void testDsBeforeSnpBatchRcvrClient() throws Exception {
        dotestDsBeforeSnp(client, false, DataStreamerCacheUpdaters.batched());
    }

    /** @throws Exception If fails. */
    @Test
    public void testDsBeforeSnpBatchRcvrBsSrv() throws Exception {
        dotestDsBeforeSnp(baselineServer, false, DataStreamerCacheUpdaters.batched());
    }

    /** @throws Exception If fails. */
    @Test
    public void testDsBeforeSnpBatchRcvrNotBsSrv() throws Exception {
        dotestDsBeforeSnp(notBaselineServer, false, DataStreamerCacheUpdaters.batched());
    }

    /**
     * Tests snapshot consistecy when snapshot process starts in the middle of streaming loading.
     *
     * @param ldrNode  Streaming node.
     * @param mustFail If {@code true}, checks snapshot prcess warns of inconsistency and ensures snapshot validation
     *                 finds errors. Otherwise, ensures snapshot validation is ok.
     * @param receiver Stream receiver. {@code Null} for default.
     */
    private void dotestDsBeforeSnp(Ignite ldrNode, boolean mustFail,
        @Nullable StreamReceiver<Integer, Object> receiver) throws Exception {
        int preLoadCnt = 5_000;

        grid(0).cache(dfltCacheCfg.getName()).clear();

        CountDownLatch preload = new CountDownLatch(preLoadCnt);
        AtomicBoolean stopLoading = new AtomicBoolean(false);

        IgniteInternalFuture<?> loadFut = runLoad(ldrNode, receiver, preload, stopLoading, true);

        preload.await();

        try {
            if (mustFail)
                assertThrows(null, () -> grid(0).snapshot().createSnapshot(SNAPSHOT_NAME).get(),
                    IgniteException.class, ERR_MSG);
            else
                grid(0).snapshot().createSnapshot(SNAPSHOT_NAME).get();
        }
        finally {
            stopLoading.set(true);
            loadFut.get();
        }

        IdleVerifyResultV2 checkRes = snpMngr.checkSnapshot(SNAPSHOT_NAME, null).get();

        assertTrue(checkRes.exceptions().isEmpty() && mustFail == checkRes.hasConflicts());
    }

    /**
     * Test snapshot consistency when streamer starts right after first-stage request.
     */
    private void doTestDsNextToFirstStage(StreamReceiver<Integer, Object> rcvr, IgniteEx ldr) throws Exception {
        fillCache(5_000, ldr);

        AtomicBoolean stopLoading = new AtomicBoolean();
        AtomicBoolean doLoadAtSnp = new AtomicBoolean(true);
        AtomicReference<IgniteInternalFuture<?>> loadDuringSnp = new AtomicReference<>();

        ldr.context().discovery().setCustomEventListener(InitMessage.class, (topVer, sender, msg) -> {
            SnapshotOperationRequest snpRq = extractSnpRequest(msg);

            if (snpRq == null || !doLoadAtSnp.compareAndSet(true, false))
                return;

            CountDownLatch loadLatch = new CountDownLatch(1_000);

            try {
                loadDuringSnp.set(runLoad(ldr, rcvr, loadLatch, stopLoading, true));

                loadLatch.await();
            }
            catch (InterruptedException ignored) {
                // No-op.
            }
        });

        try {
            snpMngr.createSnapshot(SNAPSHOT_NAME).get();
        }
        finally {
            stopLoading.set(true);
            assert loadDuringSnp.get() != null;
            loadDuringSnp.get().get();
        }

        IdleVerifyResultV2 checkRes = snpMngr.checkSnapshot(SNAPSHOT_NAME, null).get();

        assertTrue(!checkRes.hasConflicts() && checkRes.exceptions().isEmpty());
    }

    /**
     * Test snapshot consistency when streamer starts at the middle of snapshot process.
     */
    private void doTestDsAtSecondStage(@Nullable StreamReceiver<Integer, Object> receiver, Ignite dsNode) throws Exception {
        int preLoadCnt = 5_000;

        fillCache(preLoadCnt, dsNode);

        // Datastreamer won't be launched before snapshot process initialized.
        launchDsLock = new CountDownLatch(1);
        // Snapshot process won't continue before datastreamer set.
        dsRegisteredLock = new CountDownLatch(1);

        IgniteFuture<?> snpFut = grid(0).snapshot().createSnapshot(SNAPSHOT_NAME);

        // Wait for the snapshot initialization.
        launchDsLock.await();

        AtomicBoolean stopLoading = new AtomicBoolean();
        CountDownLatch dataLoadCounter = new CountDownLatch(preLoadCnt);

        IgniteInternalFuture<?> loadFut = runLoad(dsNode, receiver, dataLoadCounter, stopLoading, true);

        dsRegisteredLock.await();
        dataLoadCounter.await();

        try {
            snpFut.get();
        }
        finally {
            stopLoading.set(true);

            loadFut.get();
        }

        IdleVerifyResultV2 checkRes = snpMngr.checkSnapshot(SNAPSHOT_NAME, null).get();

        assertTrue(checkRes.hashConflicts().isEmpty() && checkRes.exceptions().isEmpty());
    }

    /**
     * Pre-laod cache for snapshot and close streamer.
     */
    private void fillCache(int preLoadCnt, Ignite loaderNode) throws Exception {
        CountDownLatch preload = new CountDownLatch(preLoadCnt);
        AtomicBoolean stopLoading = new AtomicBoolean(false);

        IgniteInternalFuture<?> loadFut = runLoad(loaderNode, null, preload, stopLoading, false);
        preload.await();
        stopLoading.set(true);
        loadFut.get();
    }

    /**
     * Loads or pre-loads cache.
     *
     * @param ldr             Loader node.
     * @param receiver        StreamReceiver. {@code Null} for default.
     * @param dataSendCounter Accumulator of submitted to streamer data.
     * @param stop            Stop load flag.
     * @param sabotageBatches If {@code true}, corrupts batches to one of server node to ensure snapshot inconsistency.
     */
    private IgniteInternalFuture<?> runLoad(Ignite ldr, @Nullable StreamReceiver<Integer, Object> receiver,
        CountDownLatch dataSendCounter, AtomicBoolean stop, boolean sabotageBatches) {

        return GridTestUtils.runMultiThreadedAsync(() -> {
            DataLoosingCommunicationSpi cm = (DataLoosingCommunicationSpi)ldr.configuration().getCommunicationSpi();

            if (sabotageBatches) {
                Set<Object> bl = grid(0).cluster().currentBaselineTopology().stream().map(BaselineNode::consistentId)
                    .collect(Collectors.toSet());

                boolean blocked = false;

                for (Ignite ig : G.allGrids()) {
                    if (ig.cluster().localNode().isClient()
                        || ig.equals(ignoredServer)
                        || ig.cluster().localNode().id().equals(ldr.cluster().localNode().id())
                        || !bl.contains(ig.cluster().localNode().consistentId()))
                        continue;

                    cm.block.set(ig.cluster().localNode());

                    blocked = true;

                    break;
                }

                assert blocked;
            }

            try (IgniteDataStreamer<Integer, Object> ds = ldr.dataStreamer(dfltCacheCfg.getName())) {
                if (receiver != null)
                    ds.receiver(receiver);

                ds.perNodeBufferSize(128);
                ds.perThreadBufferSize(ds.perNodeBufferSize() * 2);
                ds.perNodeParallelOperations(2);
                ds.autoFlushFrequency(1000);

                int idx = 0;

                while (!stop.get()) {
                    ds.addData(idx, idx);

                    if (dsRegisteredLock != null && dsRegisteredLock.getCount() > 0)
                        dsRegisteredLock.countDown();

                    idx++;

                    if (dataSendCounter.getCount() > 0) {
                        dataSendCounter.countDown();

                        if (dataSendCounter.getCount() == 0)
                            ds.flush();
                    }
                }
            }
            finally {
                cm.block.set(null);

                while (dataSendCounter.getCount() > 0)
                    dataSendCounter.countDown();
            }
        }, 1, "load-thread");
    }

    /**
     * @return DiscoverySpi able to wait for data streamer start in the middle of snapshot process.
     */
    private TcpDiscoverySpi waitingAtSnpProcSecondStageDiscoSpi() {
        return new TcpDiscoverySpi() {
            /** {@inheritDoc} */
            @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
                SnapshotOperationRequest snpRq = extractSnpRequest(msg);

                if (snpRq != null) {
                    if (launchDsLock != null && snpRq.startStageEnded()) {
                        assert dsRegisteredLock != null;

                        launchDsLock.countDown();

                        try {
                            dsRegisteredLock.await();
                        }
                        catch (InterruptedException e) {
                            throw new IgniteException("Unable to wait for datastreamer started.", e);
                        }
                    }
                }

                super.sendCustomEvent(msg);
            }
        };
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
     * Drops datastreamer entries from the batches to certain node. Simulates late data income to the node. Makes data
     * inconsistent if data contains backup records.
     */
    private static class DataLoosingCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private final AtomicReference<ClusterNode> block = new AtomicReference<>();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage && (((GridIoMessage)msg).message() instanceof DataStreamerRequest)) {
                ClusterNode toBlock = block.get();

                if (toBlock != null && toBlock.id().equals(node.id())) {
                    DataStreamerRequest dsrq = ((DataStreamerRequest)((GridIoMessage)msg).message());

                    AtomicInteger cnt = new AtomicInteger(dsrq.entries().size());

                    dsrq.entries().removeIf(e -> cnt.decrementAndGet() > 0);
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}

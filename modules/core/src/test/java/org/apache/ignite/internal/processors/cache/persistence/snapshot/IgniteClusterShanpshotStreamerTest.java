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

import java.util.Collections;
import java.util.List;
import java.util.Set;
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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
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
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests snapshot is consistent or warned under streaming load.
 */
public class IgniteClusterShanpshotStreamerTest extends AbstractSnapshotSelfTest {
    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final int CACHE2_LOAD_CNT = 10_000;

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private IgniteSnapshotManager snpMgr;

    /** {@inheritDoc} */
    @Override public void beforeTestSnapshot() throws Exception {
        super.beforeTestSnapshot();

        persistence = true;

        startGrid(0);

        // One more server node.
        startGrid(1);

        grid(0).cluster().state(ACTIVE);

        grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

        // Not-baseline server node.
        startGrid(G.allGrids().size());

        startClientGrid(G.allGrids().size());

        snpMgr = snp(grid(0));

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

        assert cfg.getDiscoverySpi() instanceof TcpDiscoverySpi;

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new DataLoosingCommunicationSpi());

        return cfg;
    }

    /**
     * Tests snapshot consistency wnen streamer starts before snapshot. Default receiver.
     */
    @Test
    public void testStreamerWhileSnpDefault() throws Exception {
        for (Ignite ldr : testGrids()) {
            prepareRunWithNewNode(ldr);

            doTestDsWhileSnp(ldr, true, false);
        }
    }

    /**
     * Tests snapshot consistency wnen streamer starts before snapshot. Overwriting receiver.
     */
    @Test
    public void testStreamerWhileSnpOverwriting() throws Exception {
        for (Ignite ldr : testGrids()) {
            prepareRunWithNewNode(ldr);

            doTestDsWhileSnp(ldr, false, true);
        }
    }

    /**
     * Tests snapshot consistency when streamer failed or canceled before snapshot. Default receiver.
     */
    @Test
    public void testDsFailsLongAgoDflt() throws Exception {
        for (Ignite ldr : testGrids()) {
            prepareRunWithNewNode(ldr);

            doTestDsFailsBeforeSnp(ldr, true, false);
        }
    }

    /**
     * Tests snapshot consistency when streamer failed or canceled before snapshot. Overwriting receiver.
     */
    @Test
    public void testDsFailsLongAgoOverwriting() throws Exception {
        for (Ignite ldr : testGrids()) {
            prepareRunWithNewNode(ldr);

            doTestDsFailsBeforeSnp(ldr, false, true);
        }
    }

    /**
     * Tests snapshot consistecy when streamer starts before snapshot.
     *
     * @param ldr Streaming node.
     * @param mustFail If {@code true}, checks snapshot process warns of inconsistency and ensures snapshot validation
     *                 finds errors. Otherwise, ensures snapshot validation is ok.
     * @param allowOverwrite 'allowOverwrite' setting.
     */
    private void doTestDsWhileSnp(Ignite ldr, boolean mustFail, boolean allowOverwrite) throws Exception {
        String expectedWrn = U.field(DataStreamerUpdatesHandler.class, "WRN_MSG");
        String notExpectedWrn = U.field(SnapshotPartitionsFastVerifyHandler.class, "WRN_MSG_BASE");

        int preLoadCnt = 10_000;

        CountDownLatch preload = new CountDownLatch(preLoadCnt);
        AtomicBoolean stopLoading = new AtomicBoolean(false);

        IgniteInternalFuture<?> loadFut = runLoad(ldr, allowOverwrite, preload, stopLoading);

        preload.await();

        try {
            if (mustFail) {
                Throwable e = assertThrows(null, () -> snpMgr.createSnapshot(SNAPSHOT_NAME).get(),
                    IgniteException.class, expectedWrn);

                assertFalse(e.getMessage().contains(notExpectedWrn));
            }
            else
                grid(0).snapshot().createSnapshot(SNAPSHOT_NAME).get();
        }
        finally {
            stopLoading.set(true);
            loadFut.get();
        }

        checkSnp(mustFail);
    }

    /**
     * Tests snapshot consistency when streamer failed or canceled before snapshot.
     *
     * @param ldr Streaming node.
     * @param mustFail If {@code true}, checks snapshot process warns of inconsistency and ensures snapshot validation
     *                 finds errors. Otherwise, ensures snapshot validation is ok.
     * @param allowOverwrite 'allowOverwrite' setting.
     */
    private void doTestDsFailsBeforeSnp(Ignite ldr, boolean mustFail, boolean allowOverwrite) throws Exception {
        String expectedWrn = U.field(SnapshotPartitionsFastVerifyHandler.class, "WRN_MSG_BASE");
        String notExpectedWrn = U.field(DataStreamerUpdatesHandler.class, "WRN_MSG");

        CountDownLatch preloadLatch = new CountDownLatch(10_000);
        AtomicBoolean stopLoad = new AtomicBoolean();

        IgniteInternalFuture<?> loadFut = runLoad(ldr, allowOverwrite, preloadLatch, stopLoad);

        preloadLatch.await();
        stopLoad.set(true);
        loadFut.get();

        if (mustFail) {
            Throwable e = assertThrows(null, () -> snpMgr.createSnapshot(SNAPSHOT_NAME).get(),
                IgniteException.class, expectedWrn);

            assertFalse(e.getMessage().contains(notExpectedWrn));
        }
        else
            snpMgr.createSnapshot(SNAPSHOT_NAME).get();

        checkSnp(mustFail);
    }

    /** */
    private void checkSnp(boolean mustFail) throws IgniteCheckedException, InterruptedException {
        IdleVerifyResultV2 checkRes = snpMgr.checkSnapshot(SNAPSHOT_NAME, null).get();
        checkRes = snpMgr.checkSnapshot(SNAPSHOT_NAME, null).get();

        assertTrue(mustFail == checkRes.hasConflicts());
        assertTrue(checkRes.exceptions().isEmpty());

        if (mustFail) {
            grid(0).destroyCache(CACHE2);

            awaitPartitionMapExchange();

            // CHeck the second cache is successfully restored.
            snpMgr.restoreSnapshot(SNAPSHOT_NAME, Collections.singletonList(CACHE2)).get();

            for (int i = 0; i < 10000; ++i)
                assertEquals(i, grid(0).cache(CACHE2).get(i));
        }
    }

    /**
     * Loads or pre-loads cache.
     *
     * @param ldr Loader node.
     * @param allowOverwrite 'allowOverwrite' setting.
     * @param dataSendCounter Data counter submitted to the streamer.
     * @param stop Stop load flag.
     */
    private IgniteInternalFuture<?> runLoad(Ignite ldr, boolean allowOverwrite, CountDownLatch dataSendCounter,
        AtomicBoolean stop) {

        return GridTestUtils.runMultiThreadedAsync(() -> {
            DataLoosingCommunicationSpi cm = (DataLoosingCommunicationSpi)ldr.configuration().getCommunicationSpi();

            boolean blockSet = false;

            Set<Object> bl = grid(0).cluster().currentBaselineTopology().stream().map(BaselineNode::consistentId)
                .collect(Collectors.toSet());

            for (Ignite ig : G.allGrids()) {
                if (ig.cluster().localNode().isClient()
                    || ig.cluster().localNode().id().equals(ldr.cluster().localNode().id())
                    || !bl.contains(ig.cluster().localNode().consistentId()))
                    continue;

                cm.block(ig.cluster().localNode());

                blockSet = true;

                break;
            }

            assert blockSet;

            try (IgniteDataStreamer<Integer, Object> ds = ldr.dataStreamer(dfltCacheCfg.getName())) {
                ds.allowOverwrite(allowOverwrite);

                int idx = 0;

                while (!stop.get()) {
                    ds.addData(idx, idx);

                    idx++;

                    if (dataSendCounter.getCount() > 0) {
                        dataSendCounter.countDown();

                        if (dataSendCounter.getCount() == 0)
                            ds.flush();
                    }
                }
            }
            finally {
                while (dataSendCounter.getCount() > 0)
                    dataSendCounter.countDown();

                if (blockSet)
                    cm.stopBlocking();
            }
        }, 1, "load-thread");
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

        U.delete(snpMgr.snapshotLocalDir(SNAPSHOT_NAME));

        grid(0).destroyCache(dfltCacheCfg.getName());
        grid(0).destroyCache(CACHE2);

        awaitPartitionMapExchange();

        grid(0).createCache(dfltCacheCfg);

        // Fill second cache. It must be restorable.
        CacheConfiguration<?, ?> c2cfg = new CacheConfiguration<>(dfltCacheCfg).setName(CACHE2);

        grid(0).createCache(c2cfg);

        try (IgniteDataStreamer<Integer, Integer> ds = grid(0).dataStreamer(CACHE2)) {
            for (int i = 0; i < CACHE2_LOAD_CNT; ++i)
                ds.addData(i, i);
        }

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
     * used with stream receivers like {@link DataStreamerCacheUpdaters#batched()}.
     */
    private static class DataLoosingCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private final AtomicReference<ClusterNode> block = new AtomicReference<>();

        /** */
        private final AtomicBoolean blocked = new AtomicBoolean();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage && (((GridIoMessage)msg).message() instanceof DataStreamerRequest)) {
                ClusterNode toBlock = block.get();

                if (toBlock != null && toBlock.id().equals(node.id())) {
                    blocked.set(true);

                    DataStreamerRequest dsrq = ((DataStreamerRequest)((GridIoMessage)msg).message());

                    AtomicInteger cnt = new AtomicInteger(dsrq.entries().size());

                    dsrq.entries().removeIf(e -> cnt.decrementAndGet() > 0);
                }
            }

            super.sendMessage(node, msg, ackC);
        }

        /** */
        public void block(ClusterNode n) {
            block.set(n);

            blocked.set(false);
        }

        /** */
        public void stopBlocking() {
            block.set(null);

            assertTrue("No messages was blocked.", blocked.compareAndSet(true, false));
        }
    }
}

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
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerCacheUpdaters;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerRequest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests snapshot is consistent or warned under streaming load.
 */
public class IgniteClusterShanpshotStreamerTest extends AbstractSnapshotSelfTest {
    /** */
    private static final String INMEM_DATA_REGION = "inMemDr";

    /** */
    private IgniteSnapshotManager snpMgr;

    /** */
    private IgniteEx client;

    /** {@inheritDoc} */
    @Override public void beforeTestSnapshot() throws Exception {
        super.beforeTestSnapshot();

        persistence = true;

        dfltCacheCfg.setBackups(2);

        startGrids(3);

        grid(0).cluster().state(ACTIVE);

        client = startClientGrid(G.allGrids().size());

        snpMgr = snp(grid(0));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // In-memory data region.
        DataRegionConfiguration inMemDr = new DataRegionConfiguration();
        inMemDr.setPersistenceEnabled(false);
        inMemDr.setMaxSize(100L * 1024L * 1024L);
        inMemDr.setInitialSize(inMemDr.getMaxSize());
        inMemDr.setName(INMEM_DATA_REGION);
        inMemDr.setPageEvictionMode(DataPageEvictionMode.RANDOM_2_LRU);
        cfg.getDataStorageConfiguration().setDataRegionConfigurations(inMemDr);

        cfg.setCommunicationSpi(new DataLoosingCommunicationSpi());

        return cfg;
    }

    /**
     * Tests snapshot consistency wnen streamer starts before snapshot. Default receiver.
     */
    @Test
    public void testStreamerWhileSnpDefault() throws Exception {
        doTestDsWhileSnp(true, false);
    }

    /**
     * Tests snapshot consistency wnen streamer starts before snapshot. Overwriting receiver.
     */
    @Test
    public void testStreamerWhileSnpOverwriting() throws Exception {
        doTestDsWhileSnp(false, true);
    }

    /**
     * Tests snapshot consistency when streamer failed or canceled before snapshot. Default receiver.
     */
    @Test
    public void testDsFailsLongAgoDefault() throws Exception {
        doTestDsFailsBeforeSnp(true, false);
    }

    /**
     * Tests snapshot consistency when streamer failed or canceled before snapshot. Overwriting receiver.
     */
    @Test
    public void testDsFailsLongAgoOverwriting() throws Exception {
        doTestDsFailsBeforeSnp(false, true);
    }

    /**
     * Tests other cache is restorable from snapshot.
     */
    @Test
    public void testOtherCacheRestores() throws InterruptedException, IgniteCheckedException {
        String cname = "cache2";
        String expectedWrn = U.field(DataStreamerUpdatesHandler.class, "WRN_MSG");

        grid(0).createCache(new CacheConfiguration<>(dfltCacheCfg).setName(cname));

        try (IgniteDataStreamer<Integer, Integer> ds = grid(0).dataStreamer(cname)) {
            for (int i = 0; i < 100; ++i)
                ds.addData(i, i);
        }

        CountDownLatch preloadLatch = new CountDownLatch(10_000);
        AtomicBoolean stopLoad = new AtomicBoolean();

        IgniteInternalFuture<?> loadFut = runLoad(grid(0), false, preloadLatch, stopLoad, true);
        preloadLatch.await();

        try {
            assertThrows(null, () -> snpMgr.createSnapshot(SNAPSHOT_NAME).get(), IgniteException.class, expectedWrn);
        }
        finally {
            stopLoad.set(true);
            loadFut.get();
        }

        checkSnp(true);

        grid(0).destroyCache(cname);
        grid(0).destroyCache(dfltCacheCfg.getName());

        snpMgr.restoreSnapshot(SNAPSHOT_NAME, Collections.singletonList(cname)).get();

        for (int i = 0; i < 100; ++i)
            assertEquals(i, grid(0).cache(cname).get(i));
    }

    /**
     * Tests streaming into in-memory cache doesn't affect snapshot.
     */
    @Test
    public void testInMemDoesntAffectSnp() throws InterruptedException, IgniteCheckedException {
        String cache2Name = "cache2";
        int loadCnt = 1000;

        grid(0).createCache(new CacheConfiguration<>(dfltCacheCfg).setName(cache2Name));

        try (IgniteDataStreamer<Object, Object> ds = grid(0).dataStreamer(cache2Name)) {
            for (int i = 0; i < loadCnt; ++i)
                ds.addData(i, i);
        }

        grid(0).destroyCache(dfltCacheCfg.getName());
        dfltCacheCfg.setDataRegionName(INMEM_DATA_REGION);
        dfltCacheCfg.setEncryptionEnabled(false);
        grid(0).createCache(dfltCacheCfg);

        AtomicBoolean stop = new AtomicBoolean();
        CountDownLatch preloadCnt = new CountDownLatch(loadCnt);

        IgniteInternalFuture<?> loadFut = runLoad(grid(2), false, preloadCnt, stop, false);

        preloadCnt.await();

        try {
            snpMgr.createSnapshot(SNAPSHOT_NAME).get();
        }
        finally {
            stop.set(true);
            loadFut.get();
        }

        grid(0).destroyCache(cache2Name);

        snpMgr.restoreSnapshot(SNAPSHOT_NAME, null).get();

        for (int i = 0; i < loadCnt; ++i)
            assertEquals(i, grid(0).cache(cache2Name).get(i));
    }

    /**
     * Tests snapshot consistecy when streamer starts before snapshot.
     *
     * @param mustFail If {@code true}, checks snapshot process warns of inconsistency and ensures snapshot validation
     *                 finds errors. Otherwise, ensures snapshot validation is ok.
     * @param allowOverwrite 'allowOverwrite' setting.
     */
    private void doTestDsWhileSnp(boolean mustFail, boolean allowOverwrite) throws Exception {
        String expectedWrn = U.field(DataStreamerUpdatesHandler.class, "WRN_MSG");
        String notExpectedWrn = U.field(SnapshotPartitionsFastVerifyHandler.class, "WRN_MSG_BASE");

        CountDownLatch preload = new CountDownLatch(10_000);
        AtomicBoolean stopLoading = new AtomicBoolean(false);

        IgniteInternalFuture<?> loadFut = runLoad(client, allowOverwrite, preload, stopLoading, true);

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
     * @param mustFail If {@code true}, checks snapshot process warns of inconsistency and ensures snapshot validation
     *                 finds errors. Otherwise, ensures snapshot validation is ok.
     * @param allowOverwrite 'allowOverwrite' setting.
     */
    private void doTestDsFailsBeforeSnp(boolean mustFail, boolean allowOverwrite) throws Exception {
        String expectedWrn = U.field(SnapshotPartitionsFastVerifyHandler.class, "WRN_MSG_BASE");
        String notExpectedWrn = U.field(DataStreamerUpdatesHandler.class, "WRN_MSG");

        CountDownLatch preloadLatch = new CountDownLatch(3_000);
        AtomicBoolean stopLoad = new AtomicBoolean();

        IgniteInternalFuture<?> loadFut = runLoad(client, allowOverwrite, preloadLatch, stopLoad, true);

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

        assertTrue(mustFail == checkRes.hasConflicts());
        assertTrue(checkRes.exceptions().isEmpty());
    }

    /**
     * Loads or pre-loads cache.
     *
     * @param ldr Loader node.
     * @param allowOverwrite 'allowOverwrite' setting.
     * @param dataSendCounter Data counter submitted to the streamer.
     * @param stop Stop load flag.
     * @param affect If {@code true}, simulates inconsistent updates.
     */
    private IgniteInternalFuture<?> runLoad(Ignite ldr, boolean allowOverwrite, CountDownLatch dataSendCounter,
        AtomicBoolean stop, boolean affect) {

        return GridTestUtils.runMultiThreadedAsync(() -> {
            DataLoosingCommunicationSpi cm = (DataLoosingCommunicationSpi)ldr.configuration().getCommunicationSpi();

            boolean blockSet = false;

            if (affect) {
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
            }

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

    /**
     * Drops streamer entries from the batches to certain node. Simulates inconsistent data contains separates backup
     * and primary records. {@link DataStreamerImpl.IsolatedUpdater} does so. Doesn't affect consistency with normal
     * cache loads with 'cache.put()' or 'cache.putAll()' witch are used with stream receivers like
     * {@link DataStreamerCacheUpdaters#batched()}. Doesn't drop or block messages allowing the streamer to keep
     * working because it awaits fore responses.
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

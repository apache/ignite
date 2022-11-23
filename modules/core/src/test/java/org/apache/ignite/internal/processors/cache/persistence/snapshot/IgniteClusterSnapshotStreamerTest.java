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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DATA_STREAMER_POOL_SIZE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests snapshot is consistent or snapshot process produces proper warning with concurrent streaming.
 */
public class IgniteClusterSnapshotStreamerTest extends AbstractSnapshotSelfTest {
    /** */
    private static final String INMEM_DATA_REGION = "inMemDr";

    /** */
    private IgniteEx client;

    /** {@inheritDoc} */
    @Override public void beforeTestSnapshot() throws Exception {
        super.beforeTestSnapshot();

        persistence = true;

        dfltCacheCfg.setBackups(1);

        startGrids(2);

        grid(0).cluster().state(ACTIVE);

        client = startClientGrid(G.allGrids().size());
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

        return cfg;
    }

    /**
     * Tests snapshot warning when streamer is working during snapshot creation. Default receiver.
     */
    @Test
    public void testStreamerWhileSnapshotDefault() throws Exception {
        for (IgniteEx snpHnd : snapshotCreators()) {
            doTestDataStreamerWhileSnapshot(snpHnd, false);

            cleanRun();
        }
    }

    /**
     * Tests snapshot warning when streamer is working during snapshot creation. Overwriting receiver.
     */
    @Test
    public void testStreamerWhileSnapshotOverwriting() throws Exception {
        for (IgniteEx snpHnd : snapshotCreators()) {
            doTestDataStreamerWhileSnapshot(snpHnd, true);

            cleanRun();
        }
    }

    /**
     * Tests snapshot warning when streamer failed or canceled before snapshot. Default receiver.
     */
    @Test
    public void testStreamerFailsLongAgoDefault() throws Exception {
        for (IgniteEx snpHnd : snapshotCreators()) {
            doTestDataStreamerFailedBeforeSnapshot(snpHnd, false);

            cleanRun();
        }
    }

    /**
     * Tests snapshot warning when streamer failed or canceled before snapshot. Overwriting receiver.
     */
    @Test
    public void testStreamerFailsLongAgoOverwriting() throws Exception {
        for (IgniteEx snpHnd : snapshotCreators()) {
            doTestDataStreamerFailedBeforeSnapshot(snpHnd, true);

            cleanRun();
        }
    }

    /**
     * Tests snapshot warning is restored from non-holding warning meta node.
     */
    @Test
    public void testMetaWarningRestoredByOnlyOneNode() throws Exception {
        doTestDataStreamerWhileSnapshot(client, false);

        // Check snapshot by only one node.
        stopGrid(0);

        createAndCheckSnapshot(client, false, DataStreamerUpdatesHandler.WRN_MSG);
    }

    /**
     * Tests not affected by streamer cache is restorable from snapshot.
     */
    @Test
    public void testOtherCacheRestores() throws Exception {
        for (IgniteEx snpHnd : snapshotCreators()) {
            String cname = "cache2";

            grid(0).createCache(new CacheConfiguration<>(dfltCacheCfg).setName(cname));

            try (IgniteDataStreamer<Integer, Integer> ds = grid(0).dataStreamer(cname)) {
                for (int i = 0; i < 100; ++i)
                    ds.addData(i, i);
            }

            AtomicBoolean stopLoad = new AtomicBoolean();

            IgniteInternalFuture<?> loadFut = runLoad(grid(0), false, stopLoad);

            try {
                assertThrows(null, () -> snp(snpHnd).createSnapshot(SNAPSHOT_NAME).get(), IgniteException.class,
                    DataStreamerUpdatesHandler.WRN_MSG);
            }
            finally {
                stopLoad.set(true);
                loadFut.get();
            }

            grid(0).destroyCache(cname);
            grid(0).destroyCache(dfltCacheCfg.getName());

            snp(snpHnd.cluster().localNode().isClient() ? grid(1) : snpHnd).restoreSnapshot(SNAPSHOT_NAME,
                Collections.singletonList(cname)).get();

            for (int i = 0; i < 100; ++i)
                assertEquals(i, grid(0).cache(cname).get(i));

            grid(0).destroyCache(cname);

            cleanRun();
        }
    }

    /**
     * Tests streaming into in-memory cache doesn't affect snapshot.
     */
    @Test
    public void testStreamingIntoInMemoryDoesntAffectSnapshot() throws Exception {
        for (IgniteEx snpHnd : snapshotCreators()) {
            String cache2Name = "cache2";
            int loadCnt = 1000;

            dfltCacheCfg.setEncryptionEnabled(encryption);
            dfltCacheCfg.setDataRegionName(null);
            grid(0).createCache(new CacheConfiguration<>(dfltCacheCfg).setName(cache2Name));

            try (IgniteDataStreamer<Object, Object> ds = grid(0).dataStreamer(cache2Name)) {
                for (int i = 0; i < loadCnt; ++i)
                    ds.addData(i, i);
            }

            grid(0).destroyCache(DEFAULT_CACHE_NAME);
            dfltCacheCfg.setEncryptionEnabled(false);
            dfltCacheCfg.setDataRegionName(INMEM_DATA_REGION);
            grid(0).createCache(dfltCacheCfg);

            AtomicBoolean stop = new AtomicBoolean();

            IgniteInternalFuture<?> loadFut = runLoad(client, false, stop);

            try {
                snp(snpHnd).createSnapshot(SNAPSHOT_NAME).get();
            }
            finally {
                stop.set(true);
                loadFut.get();
            }

            grid(0).destroyCache(cache2Name);

            snp(snpHnd.localNode().isClient() ? grid(1) : snpHnd).restoreSnapshot(SNAPSHOT_NAME, null).get();

            for (int i = 0; i < loadCnt; ++i)
                assertEquals(i, grid(0).cache(cache2Name).get(i));

            grid(0).destroyCache(cache2Name);

            cleanRun();
        }
    }

    /**
     * Tests snapshot warning when streamer is working during snapshot creation.
     *
     * @param allowOverwrite 'allowOverwrite' setting.
     */
    private void doTestDataStreamerWhileSnapshot(IgniteEx snpHnd, boolean allowOverwrite) throws Exception {
        AtomicBoolean stopLoading = new AtomicBoolean();

        TestRecordingCommunicationSpi cm = (TestRecordingCommunicationSpi)client.configuration().getCommunicationSpi();

        IgniteInternalFuture<?> loadFut = runLoad(client, allowOverwrite, stopLoading);

        cm.blockMessages(DataStreamerRequest.class, grid(0).name());

        cm.waitForBlocked(batchesPerNode(grid(0)));

        String expectedWrn = allowOverwrite ? null : DataStreamerUpdatesHandler.WRN_MSG;

        try {
            SnapshotPartitionsVerifyTaskResult checkRes = createAndCheckSnapshot(snpHnd, true, expectedWrn);

            if (expectedWrn != null) {
                Map<String, SnapshotMetadata> metaByNodes = checkRes.metas().values().stream().flatMap(List::stream)
                    .distinct().collect(Collectors.toMap(SnapshotMetadata::consistentId, Function.identity()));

                for (SnapshotMetadata m : metaByNodes.values()) {
                    // Check warnings are stored on coordinator only.
                    if (m.consistentId().equals(grid(0).cluster().localNode().consistentId().toString())) {
                        assertTrue(!F.isEmpty(m.warnings()) && m.warnings().size() == 1 &&
                            m.warnings().get(0).contains(expectedWrn));
                    }
                    else
                        assertTrue(F.isEmpty(m.warnings()));
                }
            }
        }
        finally {
            cm.stopBlock();
            stopLoading.set(true);
            loadFut.get();
        }
    }

    /**
     * Tests snapshot warning when streamer failed or canceled before snapshot.
     *
     * @param snpHnd Snapshot handler node.
     * @param allowOverwrite 'allowOverwrite' setting.
     */
    private void doTestDataStreamerFailedBeforeSnapshot(IgniteEx snpHnd, boolean allowOverwrite) throws Exception {
        IgniteEx newClient = startClientGrid();

        UUID newClientId = newClient.localNode().id();

        TestRecordingCommunicationSpi cm = (TestRecordingCommunicationSpi)newClient.configuration().getCommunicationSpi();

        CountDownLatch nodeGoneLatch = new CountDownLatch(1);

        grid(0).events().localListen(e -> {
            assert e instanceof DiscoveryEvent;

            if (((DiscoveryEvent)e).eventNode().id().equals(newClientId))
                nodeGoneLatch.countDown();

            return false;
        }, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);

        AtomicBoolean stopLoading = new AtomicBoolean();

        IgniteInternalFuture<?> loadFut = runLoad(newClient, allowOverwrite, stopLoading);

        cm.blockMessages(DataStreamerRequest.class, grid(0).name());

        cm.waitForBlocked(batchesPerNode(grid(0)));

        runAsync(() -> stopGrid(newClient.name(), true));

        nodeGoneLatch.await();

        stopLoading.set(true);
        loadFut.cancel();

        if (allowOverwrite)
            createAndCheckSnapshot(snpHnd, true, null);
        else
            createAndCheckSnapshot(snpHnd, true, SnapshotPartitionsQuickVerifyHandler.WRN_MSG);
    }

    /**
     * Runs DataStreamer asynchronously. Waits while streamer pre-loads some amount of data.
     *
     * @param ldr Loader node.
     * @param allowOverwrite 'allowOverwrite' setting.
     * @param stop Stop load flag.
     */
    private IgniteInternalFuture<?> runLoad(Ignite ldr, boolean allowOverwrite, AtomicBoolean stop)
        throws InterruptedException {
        CountDownLatch preload = new CountDownLatch(10_000);

        IgniteInternalFuture<?> res = runAsync(() -> {
            try (IgniteDataStreamer<Integer, Object> ds = ldr.dataStreamer(dfltCacheCfg.getName())) {
                ds.allowOverwrite(allowOverwrite);

                int idx = 0;

                while (!stop.get()) {
                    ds.addData(++idx, idx);

                    preload.countDown();
                }
            }
        }, "load-thread");

        preload.await();

        return res;
    }

    /** */
    private SnapshotPartitionsVerifyTaskResult createAndCheckSnapshot(IgniteEx snpHnd, boolean create, String expectedWrn)
        throws Exception {
        if (create) {
            if (expectedWrn == null)
                snp(snpHnd).createSnapshot(SNAPSHOT_NAME, null).get();
            else {
                ListeningTestLogger testLog = new ListeningTestLogger();

                LogListener logListener = LogListener.matches(expectedWrn).times(1).build();
                testLog.registerListener(logListener);

                assertThrows(
                    testLog,
                    () -> snp(snpHnd).createSnapshot(SNAPSHOT_NAME, null).get(),
                    IgniteException.class,
                    expectedWrn
                );

                // Ensure the warning message appears once.
                logListener.check();
            }
        }

        SnapshotPartitionsVerifyTaskResult checkRes = snp(snpHnd).checkSnapshot(SNAPSHOT_NAME, null).get();

        assertTrue(checkRes.exceptions().isEmpty());
        assertTrue((expectedWrn != null) == checkRes.idleVerifyResult().hasConflicts());

        if (expectedWrn != null) {
            ListeningTestLogger testLog = new ListeningTestLogger();

            LogListener lsnr = LogListener.matches(expectedWrn).times(1).build();

            testLog.registerListener(lsnr);

            checkRes.print(testLog::info);

            lsnr.check();
        }

        return checkRes;
    }

    /** */
    private int batchesPerNode(IgniteEx grid) {
        Integer poolSize = grid.localNode().attribute(ATTR_DATA_STREAMER_POOL_SIZE);

        return IgniteDataStreamer.DFLT_PARALLEL_OPS_MULTIPLIER * (poolSize != null ? poolSize :
            grid.localNode().metrics().getTotalCpus());
    }

    /** */
    private List<IgniteEx> snapshotCreators() {
        return G.allGrids().stream().map(g -> (IgniteEx)g).collect(Collectors.toList());
    }

    /**
     * Cleans current test run result and prepares next test run.
     */
    private void cleanRun() {
        grid(0).destroyCache(DEFAULT_CACHE_NAME);
        grid(0).createCache(dfltCacheCfg);
        U.delete(snp(grid(0)).snapshotLocalDir(SNAPSHOT_NAME));
    }
}

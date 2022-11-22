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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
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
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerRequest;
import org.apache.ignite.internal.util.typedef.G;
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

        return cfg;
    }

    /**
     * Tests snapshot warning when streamer is working during snapshot creation. Default receiver.
     */
    @Test
    public void testStreamerWhileSnapshotDefault() throws Exception {
        doTestDataStreamerWhileSnapshot(false);
    }

    /**
     * Tests snapshot warning when streamer is working during snapshot creation. Overwriting receiver.
     */
    @Test
    public void testStreamerWhileSnapshotOverwriting() throws Exception {
        doTestDataStreamerWhileSnapshot(true);
    }

    /**
     * Tests snapshot warning when streamer failed or canceled before snapshot. Default receiver.
     */
    @Test
    public void testStreamerFailsLongAgoDefault() throws Exception {
        doTestDataStreamerFailedBeforeSnapshot(false);
    }

    /**
     * Tests snapshot warning when streamer failed or canceled before snapshot. Overwriting receiver.
     */
    @Test
    public void testStreamerFailsLongAgoOverwriting() throws Exception {
        doTestDataStreamerFailedBeforeSnapshot(true);
    }

    /**
     * Tests not affected by streamer cache is restorable from snapshot.
     */
    @Test
    public void testOtherCacheRestores() throws Exception {
        String cname = "cache2";

        grid(0).createCache(new CacheConfiguration<>(dfltCacheCfg).setName(cname));

        try (IgniteDataStreamer<Integer, Integer> ds = grid(0).dataStreamer(cname)) {
            for (int i = 0; i < 100; ++i)
                ds.addData(i, i);
        }

        AtomicBoolean stopLoad = new AtomicBoolean();

        IgniteInternalFuture<?> loadFut = runLoad(grid(0), false, stopLoad);

        try {
            assertThrows(null, () -> snpMgr.createSnapshot(SNAPSHOT_NAME).get(), IgniteException.class,
                DataStreamerUpdatesHandler.WRN_MSG);
        }
        finally {
            stopLoad.set(true);
            loadFut.get();
        }

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
    public void testStreamingIntoInMememoryDoesntAffectSnapshot() throws Exception {
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

        IgniteInternalFuture<?> loadFut = runLoad(client, false, stop);

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
     * Tests snapshot warning when streamer is working during snapshot creation.
     *
     * @param allowOverwrite 'allowOverwrite' setting.
     */
    private void doTestDataStreamerWhileSnapshot(boolean allowOverwrite) throws Exception {
        AtomicBoolean stopLoading = new AtomicBoolean();

        TestRecordingCommunicationSpi cm = (TestRecordingCommunicationSpi)client.configuration().getCommunicationSpi();

        IgniteInternalFuture<?> loadFut = runLoad(client, allowOverwrite, stopLoading);

        cm.blockMessages(DataStreamerRequest.class, grid(0).name());

        cm.waitForBlocked(maxBatchesPerNode(grid(0)));

        String expectedWrn = allowOverwrite ? null : DataStreamerUpdatesHandler.WRN_MSG;

        try {
            SnapshotPartitionsVerifyTaskResult checkRes = createAndCheckSnapshot(true, expectedWrn);

            if (expectedWrn != null) {
                Map<String, SnapshotMetadata> metaByNodes = checkRes.metas().values().stream().flatMap(List::stream)
                    .distinct().collect(Collectors.toMap(SnapshotMetadata::consistentId, Function.identity()));

                for(SnapshotMetadata m : metaByNodes.values()){
                    // We take snapshot from grid(0)
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
            stopLoading.set(true);
            loadFut.get();
        }

        // Check snapshot by only one node.
        stopGrid(1);
        stopGrid(2);

        createAndCheckSnapshot(false, allowOverwrite ? null : DataStreamerUpdatesHandler.WRN_MSG);
    }

    /**
     * Tests snapshot warning when streamer failed or canceled before snapshot.
     *
     * @param allowOverwrite 'allowOverwrite' setting.
     */
    private void doTestDataStreamerFailedBeforeSnapshot(boolean allowOverwrite) throws Exception {
        TestRecordingCommunicationSpi clientCm =
            (TestRecordingCommunicationSpi)client.configuration().getCommunicationSpi();

        UUID clientId = client.localNode().id();

        CountDownLatch nodeGoneLatch = new CountDownLatch(1);

        grid(0).events().localListen(e -> {
            assert e instanceof DiscoveryEvent;

            if (((DiscoveryEvent)e).eventNode().id().equals(clientId))
                nodeGoneLatch.countDown();

            return false;
        }, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);

        AtomicBoolean stopLoading = new AtomicBoolean();

        IgniteInternalFuture<?> loadFut = runLoad(client, allowOverwrite, stopLoading);

        clientCm.blockMessages(DataStreamerRequest.class, grid(0).name());

        clientCm.waitForBlocked(batchesPerNode(grid(0)));

        runAsync(() -> stopGrid(client.name(), true));

        nodeGoneLatch.await();

        stopLoading.set(true);
        loadFut.cancel();

        if (allowOverwrite)
            createAndCheckSnapshot(null, null);
        else {
            createAndCheckSnapshot(SnapshotPartitionsQuickVerifyHandler.WRN_MSG,
                DataStreamerUpdatesHandler.WRN_MSG);
        }
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
    private SnapshotPartitionsVerifyTaskResult createAndCheckSnapshot(boolean create, String expectedWrn)
        throws Exception {
        if (create) {
            if (expectedWrn == null)
                snpMgr.createSnapshot(SNAPSHOT_NAME, null).get();
            else {
                ListeningTestLogger testLog = new ListeningTestLogger();

                LogListener logListener = LogListener.matches(expectedWrn).times(1).build();
                testLog.registerListener(logListener);

                assertThrows(
                    testLog,
                    () -> snpMgr.createSnapshot(SNAPSHOT_NAME, null).get(),
                    IgniteException.class,
                    expectedWrn
                );

                // Ensure the warning message appears once.
                logListener.check();
            }
        }

        SnapshotPartitionsVerifyTaskResult checkRes = snpMgr.checkSnapshot(SNAPSHOT_NAME, null).get();

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
    private int maxBatchesPerNode(IgniteEx grid) {
        Integer attrStreamerPoolSize = grid.localNode().attribute(ATTR_DATA_STREAMER_POOL_SIZE);

        return IgniteDataStreamer.DFLT_PARALLEL_OPS_MULTIPLIER * (attrStreamerPoolSize != null ? attrStreamerPoolSize :
            grid.localNode().metrics().getTotalCpus());
    }
}

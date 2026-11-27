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

package org.apache.ignite.util;

import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cdc.AbstractCdcTest.UserCdcConsumer;
import org.apache.ignite.cdc.CdcConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridJobExecuteRequest;
import org.apache.ignite.internal.GridJobExecuteResponse;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.cdc.CdcConsumerState;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.cdc.CdcManager;
import org.apache.ignite.internal.cdc.CdcMode;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.CdcDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.cdc.AbstractCdcTest.ChangeEventType.UPDATE;
import static org.apache.ignite.cdc.AbstractCdcTest.KEYS_CNT;
import static org.apache.ignite.cdc.CdcSelfTest.addData;
import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_ARCHIVED;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.stopThreads;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.util.GridCommandHandlerClusterByClassTest.CACHES;
import static org.apache.ignite.util.SystemViewCommandTest.NODE_ID;

/**
 * CDC command tests.
 */
public class CdcCommandTest extends GridCommandHandlerAbstractTest {
    /** */
    private static final String CDC_DISABLED_DATA_REGION = "cdc_disabled_data_region";

    /** */
    public static final String DELETE_LOST_SEGMENT_LINKS = "delete_lost_segment_links";

    /** */
    public static final String RESEND = "resend";

    /** */
    public static final String CLEAR = "clear";

    /** */
    public static final String RESET_MODE = "--reset-mode";

    /** */
    private IgniteEx srv0;

    /** */
    private IgniteEx srv1;

    /** */
    public static final String CDC = "--cdc";

    /** */
    private DistributedChangeableProperty<Serializable> cdcDisabled;

    /** */
    private volatile IgniteThrowableConsumer<WALRecord> onLogLsnr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setBackups(1));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalForceArchiveTimeout(1000)
            .setDataRegionConfigurations(new DataRegionConfiguration()
                .setName(CDC_DISABLED_DATA_REGION)
                .setCdcEnabled(false))
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setCdcEnabled(true)));

        cfg.setIncludeEventTypes(EVT_WAL_SEGMENT_ARCHIVED);

        cfg.setPluginProviders(new AbstractTestPluginProvider() {
            @Override public String name() {
                return "Test WAL provider";
            }

            @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
                if (!IgniteWriteAheadLogManager.class.equals(cls))
                    return null;

                return (T)new FileWriteAheadLogManager(((IgniteEx)ctx.grid()).context()) {
                    @Override public WALPointer log(WALRecord rec) throws IgniteCheckedException {
                        if (rec instanceof CdcDataRecord && onLogLsnr != null)
                            onLogLsnr.accept(rec);

                        return super.log(rec);
                    }
                };
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        srv0 = startGrid(0);
        srv1 = startGrid(1);

        awaitPartitionMapExchange();

        cdcDisabled = srv0.context().distributedConfiguration().property(FileWriteAheadLogManager.CDC_DISABLED);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopThreads(log);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testParseDeleteLostSegmentLinks() {
        injectTestSystemOut();

        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS,
                CDC, "unexpected_command"),
            "Command cdc can't be executed");

        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS,
                CDC, DELETE_LOST_SEGMENT_LINKS, NODE_ID),
            "Please specify a value for argument: --node-id");

        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS,
                CDC, DELETE_LOST_SEGMENT_LINKS, NODE_ID, "10"),
            "Failed to parse " + NODE_ID + " command argument.");
    }

    /** */
    @Test
    public void testDeleteLostSegmentLinksApplicationNotClosed() throws Exception {
        injectTestSystemOut();

        CountDownLatch appStarted = new CountDownLatch(1);

        CdcConfiguration cfg = new CdcConfiguration();

        cfg.setConsumer(new UserCdcConsumer() {
            @Override public void start(MetricRegistry mreg) {
                appStarted.countDown();
            }
        });

        CdcMain cdc = new CdcMain(getConfiguration(getTestIgniteInstanceName(0)), null, cfg);

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(cdc);

        appStarted.await(getTestTimeout(), TimeUnit.MILLISECONDS);

        String out = executeCommand(EXIT_CODE_UNEXPECTED_ERROR,
            CDC, DELETE_LOST_SEGMENT_LINKS, NODE_ID, srv0.localNode().id().toString());

        if (cliCommandHandler())
            assertContains(log, out, "Failed to delete lost segment CDC links. Unable to acquire lock to lock CDC folder.");

        assertFalse(fut.isDone());
    }

    /** */
    @Test
    public void testDeleteLostSegmentLinks() throws Exception {
        checkDeleteLostSegmentLinks(F.asList(0L, 2L), F.asList(2L), true);
    }

    /** */
    @Test
    public void testDeleteLostSegmentLinksOneNode() throws Exception {
        checkDeleteLostSegmentLinks(F.asList(0L, 2L), F.asList(2L), false);
    }

    /** */
    @Test
    public void testDeleteLostSegmentLinksMultipleGaps() throws Exception {
        checkDeleteLostSegmentLinks(F.asList(0L, 3L, 5L), F.asList(5L), true);
    }

    /** */
    private void checkDeleteLostSegmentLinks(List<Long> expBefore, List<Long> expAfter, boolean allNodes) throws Exception {
        archiveSegmentLinks(expBefore);

        checkLinks(srv0, expBefore);
        checkLinks(srv1, expBefore);

        String[] args = allNodes ? new String[] {CDC, DELETE_LOST_SEGMENT_LINKS} :
            new String[] {CDC, DELETE_LOST_SEGMENT_LINKS, NODE_ID, srv0.localNode().id().toString()};

        executeCommand(EXIT_CODE_OK, args);

        checkLinks(srv0, expAfter);
        checkLinks(srv1, allNodes ? expAfter : expBefore);
    }

    /** */
    private void checkLinks(IgniteEx srv, List<Long> expLinks) {
        NodeFileTree ft = srv.context().pdsFolderResolver().fileTree();

        File[] links = ft.walCdcSegments();

        assertEquals(expLinks.size(), links.length);
        Arrays.stream(links).map(File::toPath).map(ft::walSegmentIndex)
            .allMatch(expLinks::contains);
    }

    /** Archive given segments links with possible gaps. */
    private void archiveSegmentLinks(List<Long> idxs) throws Exception {
        for (long idx = 0; idx <= idxs.stream().mapToLong(v -> v).max().getAsLong(); idx++) {
            cdcDisabled.propagate(!idxs.contains(idx));

            archiveSegment();
        }
    }

    /** */
    private void archiveSegment() throws Exception {
        CountDownLatch latch = new CountDownLatch(G.allGrids().size());

        for (Ignite srv : G.allGrids()) {
            srv.events().localListen(evt -> {
                latch.countDown();

                return false;
            }, EVT_WAL_SEGMENT_ARCHIVED);
        }

        addData(srv1.cache(DEFAULT_CACHE_NAME), 0, KEYS_CNT);

        latch.await(getTestTimeout(), TimeUnit.MILLISECONDS);
    }

    /** */
    @Test
    public void testParseResend() {
        injectTestSystemOut();

        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS,
                CDC, "unexpected_command"),
            "Command cdc can't be executed");

        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS,
                CDC, RESEND),
            "Mandatory argument(s) missing: [--caches]");

        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS,
                CDC, RESEND, CACHES),
            "Please specify a value for argument: --caches");
    }

    /** */
    @Test
    public void testResendCacheData() throws Exception {
        UserCdcConsumer cnsmr0 = runCdc(srv0);
        UserCdcConsumer cnsmr1 = runCdc(srv1);

        addData(srv0.cache(DEFAULT_CACHE_NAME), 0, KEYS_CNT);

        waitForSize(cnsmr0, srv0.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY));
        waitForSize(cnsmr1, srv1.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY));

        cnsmr0.clear();
        cnsmr1.clear();

        executeCommand(EXIT_CODE_OK, CDC, RESEND, CACHES, DEFAULT_CACHE_NAME);

        waitForSize(cnsmr0, srv0.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY));
        waitForSize(cnsmr1, srv1.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY));
    }

    /** */
    @Test
    public void testResendCachesNotExist() {
        injectTestSystemOut();

        String out = executeCommand(EXIT_CODE_UNEXPECTED_ERROR, CDC, RESEND, CACHES, "unknown_cache");

        if (cliCommandHandler())
            assertContains(log, out, "Cache does not exist");

        String cdcDisabledCacheName = "cdcDisabledCache";

        srv0.getOrCreateCache(new CacheConfiguration<>()
            .setName(cdcDisabledCacheName)
            .setDataRegionName(CDC_DISABLED_DATA_REGION));

        out = executeCommand(EXIT_CODE_UNEXPECTED_ERROR, CDC, RESEND, CACHES, cdcDisabledCacheName);

        if (cliCommandHandler())
            assertContains(log, out, "CDC is not enabled for given cache");
    }

    /** */
    @Test
    public void testResendCancelOnNodeLeft() {
        injectTestSystemOut();

        addData(srv0.cache(DEFAULT_CACHE_NAME), 0, KEYS_CNT);

        for (Ignite srv : G.allGrids()) {
            TestRecordingCommunicationSpi.spi(srv).blockMessages((node, msg) -> {
                if (msg instanceof GridJobExecuteResponse) {
                    GridTestUtils.runAsync(srv::close);

                    return true;
                }

                return false;
            });
        }

        assertContains(log, executeCommand(EXIT_CODE_UNEXPECTED_ERROR,
                CDC, RESEND, CACHES, DEFAULT_CACHE_NAME),
            "CDC cache data resend cancelled. Failed to resend cache data on the node");
    }

    /** */
    @Test
    public void testResendCancelOnRebalanceInProgress() throws Exception {
        Assume.assumeTrue(cliCommandHandler());

        injectTestSystemOut();

        addData(srv0.cache(DEFAULT_CACHE_NAME), 0, KEYS_CNT);

        CountDownLatch rebalanceStarted = new CountDownLatch(1);

        for (Ignite srv : G.allGrids()) {
            TestRecordingCommunicationSpi.spi(srv).blockMessages((node, msg) -> {
                if (msg instanceof GridDhtPartitionSupplyMessage) {
                    rebalanceStarted.countDown();

                    return true;
                }

                return false;
            });
        }

        GridTestUtils.runAsync(() -> startGrid(3));

        rebalanceStarted.await();

        assertContains(log, executeCommand(EXIT_CODE_UNEXPECTED_ERROR,
                CDC, RESEND, CACHES, DEFAULT_CACHE_NAME),
            "CDC cache data resend cancelled. Rebalance sheduled");
    }

    /** */
    @Test
    public void testResendCancelOnTopologyChangeBeforeStart() throws Exception {
        injectTestSystemOut();

        addData(srv0.cache(DEFAULT_CACHE_NAME), 0, KEYS_CNT);

        CountDownLatch blocked = new CountDownLatch(1);

        for (Ignite srv : G.allGrids()) {
            TestRecordingCommunicationSpi.spi(srv).blockMessages((node, msg) -> {
                if (msg instanceof GridJobExecuteRequest) {
                    blocked.countDown();

                    return true;
                }

                return false;
            });
        }

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(() -> {
            String out = executeCommand(EXIT_CODE_UNEXPECTED_ERROR,
                CDC, RESEND, CACHES, DEFAULT_CACHE_NAME);

            if (cliCommandHandler())
                assertContains(log, out, "CDC cache data resend cancelled. Topology changed");
        });

        blocked.await();

        startGrid(3);
        awaitPartitionMapExchange();

        for (Ignite srv : G.allGrids())
            TestRecordingCommunicationSpi.spi(srv).stopBlock();

        fut.get();
    }

    /** */
    @Test
    public void testResendCancelOnTopologyChange() throws Exception {
        injectTestSystemOut();

        addData(srv0.cache(DEFAULT_CACHE_NAME), 0, KEYS_CNT);

        CountDownLatch preload = new CountDownLatch(1);
        CountDownLatch topChanged = new CountDownLatch(1);

        AtomicInteger cnt = new AtomicInteger();

        onLogLsnr = rec -> {
            if (cnt.incrementAndGet() < KEYS_CNT / 2)
                return;

            preload.countDown();

            U.await(topChanged);
        };

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(() -> {
            String out = executeCommand(EXIT_CODE_UNEXPECTED_ERROR,
                CDC, RESEND, CACHES, DEFAULT_CACHE_NAME);

            if (cliCommandHandler())
                assertContains(log, out, "CDC cache data resend cancelled. Topology changed");
        });

        preload.await();

        startGrid(3);

        topChanged.countDown();

        fut.get();
    }

    /** */
    @Test
    public void testResendOnClientJoin() throws Exception {
        UserCdcConsumer cnsmr0 = runCdc(srv0);
        UserCdcConsumer cnsmr1 = runCdc(srv1);

        addData(srv0.cache(DEFAULT_CACHE_NAME), 0, KEYS_CNT);

        waitForSize(cnsmr0, srv0.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY));
        waitForSize(cnsmr1, srv1.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY));

        cnsmr0.clear();
        cnsmr1.clear();

        CountDownLatch blocked = new CountDownLatch(1);

        for (Ignite srv : G.allGrids()) {
            TestRecordingCommunicationSpi.spi(srv).blockMessages((node, msg) -> {
                if (msg instanceof GridJobExecuteRequest) {
                    blocked.countDown();

                    return true;
                }

                return false;
            });
        }

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(() -> {
            executeCommand(EXIT_CODE_OK, CDC, RESEND, CACHES, DEFAULT_CACHE_NAME);
        });

        blocked.await();

        startClientGrid("client");

        for (Ignite srv : G.allGrids())
            TestRecordingCommunicationSpi.spi(srv).stopBlock();

        fut.get();

        waitForSize(cnsmr0, srv0.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY));
        waitForSize(cnsmr1, srv1.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY));
    }

    /** */
    @Test
    public void testCdcClearOnAllNodes() throws Exception {
        injectTestSystemOut();

        prepareForClear();

        // Temporary mode state can remain if the CDC application failed while saving it.
        File tmpModeState = fileTree(0).tmpCdcModeState().toFile();

        assertTrue(tmpModeState.createNewFile());

        File[] links0 = cdcLinks(0);
        File[] links1 = cdcLinks(1);

        assertContains(log, executeCommand(EXIT_CODE_OK, CDC, CLEAR), "CDC directories were successfully cleared.");

        checkCdcCleared(0, links0, true);
        checkCdcCleared(1, links1, true);
    }

    /** */
    @Test
    public void testCdcClearOnOneNode() throws Exception {
        injectTestSystemOut();

        prepareForClear();

        File[] links0 = cdcLinks(0);
        File[] links1 = cdcLinks(1);

        assertContains(log, executeCommand(EXIT_CODE_OK, CDC, CLEAR, NODE_ID, grid(1).localNode().id().toString()),
            "CDC directory was successfully cleared.");

        for (File link : links0)
            assertTrue("Segment link is deleted: " + link, link.exists());

        checkCdcNotCleared(0, 2);
        checkCdcCleared(1, links1, true);
    }

    /** */
    @Test
    public void testCdcClearOnClientNode() throws Exception {
        injectTestSystemOut();

        IgniteEx client = startClientGrid("client");

        assertContains(log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CDC, CLEAR, NODE_ID, client.localNode().id().toString()),
            "Client nodes do not support CDC clear command.");
    }

    /** */
    @Test
    public void testCdcClearWithResetMode() throws Exception {
        injectTestSystemOut();

        prepareForClear();

        File[] links0 = cdcLinks(0);
        File[] links1 = cdcLinks(1);

        assertContains(log, executeCommand(EXIT_CODE_OK, CDC, CLEAR, RESET_MODE),
            "CDC directories were successfully cleared.");

        checkCdcCleared(0, links0, false);
        checkCdcCleared(1, links1, false);
    }

    /** Checks that the mode is deleted after the rest of the state if clearing with mode reset fails. */
    @Test
    public void testCdcClearWithResetModePartialFailure() throws Exception {
        injectTestSystemOut();

        prepareForClear();

        NodeFileTree ft0 = fileTree(0);

        assertTrue(ft0.cdcCachesState().toFile().exists());

        // Deletion of a non-empty directory fails, so it replaces the mode state.
        File modeState = ft0.cdcModeState().toFile();
        File modeStateBlocker = new File(modeState, "blocker");

        assertTrue(modeState.delete());
        assertTrue(modeState.mkdir());
        assertTrue(modeStateBlocker.createNewFile());

        File[] links0 = cdcLinks(0);
        File[] links1 = cdcLinks(1);

        assertContains(log, executeCommand(EXIT_CODE_UNEXPECTED_ERROR, CDC, CLEAR, RESET_MODE),
            "Failed to clear CDC directory");

        for (File link : links0)
            assertFalse("Segment link is not deleted: " + link, link.exists());

        assertFalse(ft0.cdcWalState().toFile().exists());
        assertFalse(ft0.cdcTypesState().toFile().exists());
        assertFalse(ft0.cdcMappingsState().toFile().exists());
        assertFalse(ft0.cdcCachesState().toFile().exists());
        assertTrue(modeState.exists());

        checkCdcCleared(1, links1, false);

        // The command can be rerun after fixing the error.
        assertTrue(modeStateBlocker.delete());

        assertContains(log, executeCommand(EXIT_CODE_OK, CDC, CLEAR, RESET_MODE),
            "CDC directories were successfully cleared.");

        checkCdcCleared(0, links0, false);
    }

    /** */
    @Test
    public void testCdcClearWithLockAcquire() throws Exception {
        injectTestSystemOut();

        runCdc(grid(0));

        archiveSegment();

        waitForCdcState(0);

        File[] links1 = cdcLinks(1);

        assertTrue(links1.length > 0);

        assertContains(log, executeCommand(EXIT_CODE_UNEXPECTED_ERROR, CDC, CLEAR),
            "Unable to acquire lock to the CDC folder. Make sure a CDC app is shut down");

        checkCdcNotCleared(0, 1);
        checkCdcCleared(1, links1, true);

        stopThreads(log);

        File[] links0 = cdcLinks(0);

        links1 = cdcLinks(1);

        assertContains(log, executeCommand(EXIT_CODE_OK, CDC, CLEAR), "CDC directories were successfully cleared.");

        checkCdcCleared(0, links0, true);
        checkCdcCleared(1, links1, true);
    }

    /**
     * Checks that the CDC application captures changes after the CDC directory is cleared. The CDC application is not
     * started on the second node before clear, so its CDC mode is not saved.
     */
    @Test
    public void testCdcCapturesChangesAfterClear() throws Exception {
        injectTestSystemOut();

        prepareForClear(false);

        assertFalse(fileTree(1).cdcModeState().toFile().exists());

        assertContains(log, executeCommand(EXIT_CODE_OK, CDC, CLEAR), "CDC directories were successfully cleared.");

        UserCdcConsumer cnsmr0 = runCdc(grid(0));
        UserCdcConsumer cnsmr1 = runCdc(grid(1));

        addData(grid(0).cache(DEFAULT_CACHE_NAME), KEYS_CNT, 2 * KEYS_CNT);

        checkPrimaryKeysCaptured(grid(0), cnsmr0, KEYS_CNT, 2 * KEYS_CNT);
        checkPrimaryKeysCaptured(grid(1), cnsmr1, KEYS_CNT, 2 * KEYS_CNT);
    }

    /** Checks that the default CDC manager saves the CDC application mode even if the mode exists. */
    @Test
    public void testCdcClearOverwritesMode() throws Exception {
        injectTestSystemOut();

        prepareForClear();

        NodeFileTree ft0 = fileTree(0);

        File[] links0 = cdcLinks(0);

        new CdcConsumerState(log, ft0).saveCdcMode(CdcMode.IGNITE_NODE_ACTIVE);

        assertContains(log, executeCommand(EXIT_CODE_OK, CDC, CLEAR, NODE_ID, grid(0).localNode().id().toString()),
            "CDC directory was successfully cleared.");

        checkCdcCleared(0, links0, true);

        assertEquals(CdcMode.CDC_UTILITY_ACTIVE, new CdcConsumerState(log, ft0).loadCdcMode());
    }

    /**
     * Checks that the command fails and nothing is deleted on the node if CDC manager forbids clearing.
     * Other nodes are cleared.
     */
    @Test
    public void testCdcClearForbiddenByCdcManager() throws Exception {
        injectTestSystemOut();

        prepareForClear();

        File[] links0 = cdcLinks(0);
        File[] links1 = cdcLinks(1);

        replaceCdcManager(1, new TestCdcManager(fileTree(1), true));

        assertContains(log, executeCommand(EXIT_CODE_UNEXPECTED_ERROR, CDC, CLEAR),
            "CDC manager failed to prepare for clearing");

        for (File link : links1)
            assertTrue("Segment link is deleted: " + link, link.exists());

        checkCdcNotCleared(1, 2);

        // Clearing on other nodes doesn't depend on the failed node.
        checkCdcCleared(0, links0, true);
    }

    /** Checks that CDC manager is notified before and after clearing, and the mode is saved only by the manager. */
    @Test
    public void testCdcClearNotifiesCdcManager() throws Exception {
        injectTestSystemOut();

        prepareForClear();

        NodeFileTree ft0 = fileTree(0);

        File[] links0 = cdcLinks(0);

        new CdcConsumerState(log, ft0).saveCdcMode(CdcMode.IGNITE_NODE_ACTIVE);

        TestCdcManager cdcMgr = new TestCdcManager(ft0, false);

        replaceCdcManager(0, cdcMgr);

        String nodeId = grid(0).localNode().id().toString();

        assertContains(log, executeCommand(EXIT_CODE_OK, CDC, CLEAR, NODE_ID, nodeId),
            "CDC directory was successfully cleared.");

        assertTrue(cdcMgr.walStateExistsBeforeClear);
        assertFalse(cdcMgr.walStateExistsAfterClear);
        assertFalse(cdcMgr.resetMode);

        checkCdcCleared(0, links0, true);

        assertEquals(CdcMode.IGNITE_NODE_ACTIVE, new CdcConsumerState(log, ft0).loadCdcMode());

        assertContains(log, executeCommand(EXIT_CODE_OK, CDC, CLEAR, NODE_ID, nodeId, RESET_MODE),
            "CDC directory was successfully cleared.");

        assertTrue(cdcMgr.resetMode);

        assertFalse(ft0.cdcState().toFile().exists());
    }

    /**
     * Replaces CDC manager of the started node. The CDC clear job gets the manager of the node on each run.
     *
     * @param idx Node index.
     * @param cdcMgr CDC manager.
     */
    private void replaceCdcManager(int idx, CdcManager cdcMgr) {
        GridTestUtils.setFieldValue(grid(idx).context().cache().context(), "cdcMgr", cdcMgr);
    }

    /**
     * @param srv Server node.
     * @param cnsmr CDC consumer of the node.
     * @param from First key, inclusive.
     * @param to Last key, exclusive.
     */
    private void checkPrimaryKeysCaptured(IgniteEx srv, UserCdcConsumer cnsmr, int from, int to) throws Exception {
        List<Integer> expKeys = new ArrayList<>();

        for (int key = from; key < to; key++) {
            if (srv.affinity(DEFAULT_CACHE_NAME).isPrimary(srv.localNode(), key))
                expKeys.add(key);
        }

        assertFalse(expKeys.isEmpty());

        assertTrue(waitForCondition(() -> cnsmr.data(UPDATE, CU.cacheId(DEFAULT_CACHE_NAME)).containsAll(expKeys),
            getTestTimeout()));
    }

    /** */
    private void prepareForClear() throws Exception {
        prepareForClear(true);
    }

    /** @param runCdcOnSecondNode Whether to run the CDC application on the second node. */
    private void prepareForClear(boolean runCdcOnSecondNode) throws Exception {
        for (int i = 0; i < 2; i++) {
            NodeFileTree ft = fileTree(i);

            assertEquals(0, ft.walCdcSegments().length);
            assertFalse(ft.cdcState().toFile().exists());
        }

        runCdc(grid(0));

        if (runCdcOnSecondNode)
            runCdc(grid(1));

        archiveSegment();

        waitForCdcState(0);

        if (runCdcOnSecondNode)
            waitForCdcState(1);

        stopThreads(log);

        // Segments archived after CDC applications stop are not processed and not deleted.
        archiveSegment();
        archiveSegment();

        checkCdcNotCleared(0, 2);

        if (runCdcOnSecondNode)
            checkCdcNotCleared(1, 2);
    }

    /** @param idx Node index. */
    private void waitForCdcState(int idx) throws Exception {
        NodeFileTree ft = fileTree(idx);

        assertTrue(waitForCondition(() -> ft.cdcWalState().toFile().exists(), getTestTimeout()));
    }

    /**
     * @param idx Node index.
     * @param minSegCnt Minimal expected count of segment links.
     */
    private void checkCdcNotCleared(int idx, int minSegCnt) throws Exception {
        NodeFileTree ft = fileTree(idx);

        assertTrue(waitForCondition(() -> ft.walCdcSegments().length >= minSegCnt, getTestTimeout()));
        assertTrue(ft.cdcWalState().toFile().exists());
        assertTrue(ft.cdcModeState().toFile().exists());
    }

    /**
     * @param idx Node index.
     * @return CDC segment links of the node.
     */
    private File[] cdcLinks(int idx) {
        return fileTree(idx).walCdcSegments();
    }

    /**
     * @param idx Node index.
     * @return File tree of the node.
     */
    private NodeFileTree fileTree(int idx) {
        return grid(idx).context().pdsFolderResolver().fileTree();
    }

    /**
     * Checks that the CDC directory is cleared. Links created after clearing are kept, because CDC is enabled and
     * segments can be archived by timeout.
     *
     * @param idx Node index.
     * @param links Segment links existed before clearing.
     * @param modeExists Whether CDC mode state is expected to exist.
     */
    private void checkCdcCleared(int idx, File[] links, boolean modeExists) {
        NodeFileTree ft = fileTree(idx);

        for (File link : links)
            assertFalse("Segment link is not deleted: " + link, link.exists());

        assertEquals(modeExists, ft.cdcState().toFile().exists());

        if (modeExists) {
            File[] stateFiles = ft.cdcState().toFile().listFiles();

            assertEquals("Only CDC mode state is expected", 1, stateFiles.length);
            assertEquals("Unexpected state file", ft.cdcModeState().toFile(), stateFiles[0]);
        }
    }

    /** */
    public static UserCdcConsumer runCdc(Ignite ign) {
        UserCdcConsumer cnsmr = new UserCdcConsumer();

        CdcConfiguration cfg = new CdcConfiguration();

        cfg.setConsumer(cnsmr);
        cfg.setKeepBinary(false);

        CdcMain cdc = new CdcMain(ign.configuration(), null, cfg);

        GridTestUtils.runAsync(cdc);

        return cnsmr;
    }

    /** */
    public static void waitForSize(UserCdcConsumer cnsmr, int expSize) throws Exception {
        assertTrue(waitForCondition(() -> expSize == cnsmr.data(UPDATE, CU.cacheId(DEFAULT_CACHE_NAME)).size(),
            60_000));
    }

    /** Custom CDC manager that captures CDC events within the Ignite node. */
    private static class TestCdcManager extends GridCacheSharedManagerAdapter implements CdcManager {
        /** File tree of the node. */
        private final NodeFileTree ft;

        /** Whether to forbid clearing. */
        private final boolean forbidClear;

        /** Whether the WAL state exists on {@link #beforeClear()}. */
        private volatile boolean walStateExistsBeforeClear;

        /** Whether the WAL state exists on {@link #afterClear(boolean)}. */
        private volatile boolean walStateExistsAfterClear;

        /** Reset mode flag of the last {@link #afterClear(boolean)} call. */
        private volatile boolean resetMode;

        /**
         * @param ft File tree of the node.
         * @param forbidClear Whether to forbid clearing.
         */
        TestCdcManager(NodeFileTree ft, boolean forbidClear) {
            this.ft = ft;
            this.forbidClear = forbidClear;
        }

        /** {@inheritDoc} */
        @Override public boolean enabled() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public void collect(ByteBuffer dataBuf) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void beforeClear() throws IgniteCheckedException {
            if (forbidClear)
                throw new IgniteCheckedException("Test CDC manager forbids clearing.");

            walStateExistsBeforeClear = Files.exists(ft.cdcWalState());
        }

        /** {@inheritDoc} */
        @Override public void afterClear(boolean resetMode) {
            walStateExistsAfterClear = Files.exists(ft.cdcWalState());

            this.resetMode = resetMode;
        }
    }
}

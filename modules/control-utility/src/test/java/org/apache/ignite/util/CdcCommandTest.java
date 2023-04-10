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
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.commandline.cdc.CdcSubcommands;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.CdcDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cdc.AbstractCdcTest.ChangeEventType.UPDATE;
import static org.apache.ignite.cdc.AbstractCdcTest.KEYS_CNT;
import static org.apache.ignite.cdc.CdcSelfTest.addData;
import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_ARCHIVED;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.commandline.cdc.DeleteLostSegmentLinksCommand.DELETE_LOST_SEGMENT_LINKS;
import static org.apache.ignite.internal.commandline.cdc.DeleteLostSegmentLinksCommand.NODE_ID;
import static org.apache.ignite.internal.commandline.cdc.ResendCommand.CACHES;
import static org.apache.ignite.internal.commandline.cdc.ResendCommand.RESEND;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.stopThreads;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * CDC command tests.
 */
public class CdcCommandTest extends GridCommandHandlerAbstractTest {
    /** */
    private static final String CDC_DISABLED_DATA_REGION = "cdc_disabled_data_region";

    /** */
    private IgniteEx srv0;

    /** */
    private IgniteEx srv1;

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
                CommandList.CDC.text(), "unexpected_command"),
            "Invalid argument: unexpected_command. One of " + F.asList(CdcSubcommands.values()) + " is expected.");

        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS,
                CommandList.CDC.text(), DELETE_LOST_SEGMENT_LINKS, NODE_ID),
            "Failed to parse " + NODE_ID + " command argument.");

        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS,
                CommandList.CDC.text(), DELETE_LOST_SEGMENT_LINKS, NODE_ID, "10"),
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

        assertContains(log, executeCommand(EXIT_CODE_UNEXPECTED_ERROR,
                CommandList.CDC.text(), DELETE_LOST_SEGMENT_LINKS, NODE_ID, srv0.localNode().id().toString()),
            "Failed to delete lost segment CDC links. Unable to acquire lock to lock CDC folder.");

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

        String[] args = allNodes ? new String[] {CommandList.CDC.text(), DELETE_LOST_SEGMENT_LINKS} :
            new String[] {CommandList.CDC.text(), DELETE_LOST_SEGMENT_LINKS, NODE_ID, srv0.localNode().id().toString()};

        executeCommand(EXIT_CODE_OK, args);

        checkLinks(srv0, expAfter);
        checkLinks(srv1, allNodes ? expAfter : expBefore);
    }

    /** */
    private void checkLinks(IgniteEx srv, List<Long> expLinks) {
        FileWriteAheadLogManager wal0 = (FileWriteAheadLogManager)srv.context().cache().context().wal(true);

        File[] links = wal0.walCdcDirectory().listFiles(WAL_SEGMENT_FILE_FILTER);

        assertEquals(expLinks.size(), links.length);
        Arrays.stream(links).map(File::toPath).map(FileWriteAheadLogManager::segmentIndex)
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
                CommandList.CDC.text(), "unexpected_command"),
            "Invalid argument: unexpected_command. One of " + F.asList(CdcSubcommands.values()) + " is expected.");

        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS,
                CommandList.CDC.text(), RESEND),
            "At least one cache name should be specified.");

        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS,
                CommandList.CDC.text(), RESEND, CACHES),
            "At least one cache name should be specified.");
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

        executeCommand(EXIT_CODE_OK, CommandList.CDC.text(), RESEND, CACHES, DEFAULT_CACHE_NAME);

        waitForSize(cnsmr0, srv0.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY));
        waitForSize(cnsmr1, srv1.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY));
    }

    /** */
    @Test
    public void testResendCachesNotExist() {
        injectTestSystemOut();

        assertContains(log, executeCommand(EXIT_CODE_UNEXPECTED_ERROR,
                CommandList.CDC.text(), RESEND, CACHES, "unknown_cache"),
            "Cache does not exist");

        String cdcDisabledCacheName = "cdcDisabledCache";

        srv0.getOrCreateCache(new CacheConfiguration<>()
            .setName(cdcDisabledCacheName)
            .setDataRegionName(CDC_DISABLED_DATA_REGION));

        assertContains(log, executeCommand(EXIT_CODE_UNEXPECTED_ERROR,
                CommandList.CDC.text(), RESEND, CACHES, cdcDisabledCacheName),
            "CDC is not enabled for given cache");
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
                CommandList.CDC.text(), RESEND, CACHES, DEFAULT_CACHE_NAME),
            "CDC cache data resend cancelled. Failed to resend cache data on the node");
    }

    /** */
    @Test
    public void testResendCancelOnRebalanceInProgress() throws Exception {
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
                CommandList.CDC.text(), RESEND, CACHES, DEFAULT_CACHE_NAME),
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
            assertContains(log, executeCommand(EXIT_CODE_UNEXPECTED_ERROR,
                    CommandList.CDC.text(), RESEND, CACHES, DEFAULT_CACHE_NAME),
                "CDC cache data resend cancelled. Topology changed");
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
        CountDownLatch topologyChanged = new CountDownLatch(1);

        AtomicInteger cnt = new AtomicInteger();

        onLogLsnr = rec -> {
            if (cnt.incrementAndGet() < KEYS_CNT / 2)
                return;

            preload.countDown();

            U.await(topologyChanged);
        };

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(() -> {
            assertContains(log, executeCommand(EXIT_CODE_UNEXPECTED_ERROR,
                    CommandList.CDC.text(), RESEND, CACHES, DEFAULT_CACHE_NAME),
                "CDC cache data resend cancelled. Topology changed");
        });

        preload.await();

        startGrid(3);

        topologyChanged.countDown();

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
            executeCommand(EXIT_CODE_OK, CommandList.CDC.text(), RESEND, CACHES, DEFAULT_CACHE_NAME);
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
}

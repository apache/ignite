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

package org.apache.ignite.cdc;

import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cdc.CdcManager;
import org.apache.ignite.internal.cdc.CdcUtilityActiveCdcManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.RolloverType;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cdc.CdcManagerTest.TestCdcManager.cdcMgr;

/** */
@RunWith(Parameterized.class)
public class CdcManagerTest extends GridCommonAbstractTest {
    /** */
    private static final int WAL_SEG_SIZE = 64 * (int)U.MB;

    /** */
    private IgniteEx ign;

    /** */
    private TestCdcManager cdcMgr;

    /** */
    private FileWriteAheadLogManager walMgr;

    /** */
    private ListeningTestLogger lsnrLog;

    /** */
    private WALMode walMode;

    /** */
    private static volatile boolean failCollect;

    /** */
    @Parameterized.Parameter
    public boolean persistentEnabled;

    /** */
    @Parameterized.Parameters(name = "persistentEnabled={0}")
    public static Object[] params() {
        return new Object[] {false, true};
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(persistentEnabled)
                .setCdcEnabled(true))
            .setWalMode(walMode)
            .setWalSegmentSize(WAL_SEG_SIZE));

        cfg.setPluginProviders(new CdcManagerPluginProvider());

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        cfg.setGridLogger(lsnrLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        lsnrLog = new ListeningTestLogger(log);

        ign = startGrid(0);

        ign.cluster().state(ClusterState.ACTIVE);

        cdcMgr = cdcMgr(ign);
        walMgr = (FileWriteAheadLogManager)ign.context().cache().context().wal(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        failCollect = false;
    }

    /** */
    @Test
    public void testDisableByProperty() throws Exception {
        AtomicBoolean stopped = GridTestUtils.getFieldValue(cdcMgr(ign), "stop");

        assertFalse(stopped.get());

        DistributedChangeableProperty<Serializable> cdcDisableProp = ign.context().distributedConfiguration()
            .property(FileWriteAheadLogManager.CDC_DISABLED);

        cdcDisableProp.localUpdate(true);

        assertTrue(GridTestUtils.waitForCondition(stopped::get, 10_000, 10));
    }

    /** */
    @Test
    public void testSingleSegmentContent() throws Exception {
        for (int i = 0; i < 10_000; i++)
            ign.cache(DEFAULT_CACHE_NAME).put(i, i);

        assertEquals(0, walMgr.currentSegment());

        stopGrid(0);

        Path seg = Arrays.stream(ign.context().pdsFolderResolver().fileTree().wal().listFiles()).sorted().findFirst().get().toPath();

        ByteBuffer walBuf = ByteBuffer.wrap(Files.readAllBytes(seg));
        ByteBuffer cdcBuf = ByteBuffer.wrap(cdcMgr(ign).buf.array());
        cdcBuf.limit(WAL_SEG_SIZE);

        assertEquals(0, walBuf.compareTo(cdcBuf));
    }

    /** */
    @Test
    public void testMultipleSegmentContent() throws Exception {
        checkCdcContentWithRollover(() -> {
            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                ign.cache(DEFAULT_CACHE_NAME).put(i, i);

                if (walMgr.currentSegment() > 0)
                    break;
            }
        });
    }

    /** */
    @Test
    public void testMultipleSegmentContentWithForceNextSegmentRollover() throws Exception {
        checkCdcContentWithRollover(() -> rollSegment(RolloverType.NEXT_SEGMENT));
    }

    /** */
    @Test
    public void testMultipleSegmentContentWithForceCurrentSegmentRollover() throws Exception {
        checkCdcContentWithRollover(() -> rollSegment(RolloverType.CURRENT_SEGMENT));
    }

    /** */
    @Test
    public void testRestartNode() throws Exception {
        for (int i = 0; i < 100; i++)
            ign.cache(DEFAULT_CACHE_NAME).put(i, i);

        stopGrid(0);

        File seg = Arrays.stream(ign.context().pdsFolderResolver().fileTree().wal().listFiles()).sorted().findFirst().get();
        int len0 = writtenLength(seg);

        ByteBuffer buf0 = cdcMgr.buf;

        ign = startGrid(0);
        cdcMgr = cdcMgr(ign);

        for (int i = 0; i < 100; i++)
            ign.cache(DEFAULT_CACHE_NAME).put(i, i);

        stopGrid(0);

        int len1 = writtenLength(seg);

        // Check CDC buffer content on first start.
        ByteBuffer walBuf = ByteBuffer.wrap(Files.readAllBytes(seg.toPath()));
        walBuf.limit(len0);

        ByteBuffer cdcBuf = ByteBuffer.wrap(buf0.array());
        cdcBuf.limit(len0);

        assertEquals(0, walBuf.compareTo(cdcBuf));

        // Check CDC buffer content on second start.
        walBuf.limit(len1);
        walBuf.position(len0);

        cdcBuf = ByteBuffer.wrap(cdcMgr.buf.array());
        cdcBuf.limit(len1 - len0);

        assertEquals(0, walBuf.compareTo(cdcBuf));
    }

    /** */
    @Test
    public void testProhibitedWalModes() throws Exception {
        for (WALMode m: WALMode.values()) {
            stopGrid(0);
            cleanPersistenceDir();

            walMode = m;

            boolean cdcStart = m == WALMode.LOG_ONLY;

            LogListener cdcWarnLsnr = LogListener
                .matches("Custom CdcManager is only supported for WALMode.LOG_ONLY")
                .times(cdcStart ? 0 : 1)
                .build();

            lsnrLog.registerListener(cdcWarnLsnr);

            IgniteEx ign = startGrid(0);

            assertTrue(m.toString(), cdcWarnLsnr.check());

            if (cdcStart)
                assertTrue(cdcMgr(ign) instanceof TestCdcManager);
            else
                assertTrue(ign.context().cache().context().cdc() instanceof CdcUtilityActiveCdcManager);
        }
    }

    /** */
    @Test
    public void testCollectFailedIgnored() throws Exception {
        LogListener cdcErrLsnr = LogListener
            .matches("Error happened during CDC data collection.")
            .atLeast(1)
            .build();

        lsnrLog.registerListener(cdcErrLsnr);

        failCollect = true;

        ign.cache(DEFAULT_CACHE_NAME).put(0, 0);

        assertTrue(GridTestUtils.waitForCondition(cdcErrLsnr::check, 10_000, 10));

        assertEquals(0, ign.cache(DEFAULT_CACHE_NAME).get(0));
    }

    /** */
    @Test
    public void testCollectInvokedAfterRestore() throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName());

        AtomicBoolean restored = new AtomicBoolean();

        cfg.setPluginProviders(new AbstractTestPluginProvider() {
            @Override public String name() {
                return "CdcManagerPluginProvider";
            }

            @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
                if (CdcManager.class.equals(cls))
                    return (T)new DelayedCdcManager(restored);

                return null;
            }
        });

        startGrid(cfg);

        assertTrue(restored.get());
    }

    /** */
    public void checkCdcContentWithRollover(Runnable rollSegment) throws Exception {
        for (int i = 0; i < 10_000; i++)
            ign.cache(DEFAULT_CACHE_NAME).put(i, i);

        rollSegment.run();

        for (int i = 0; i < 10_000; i++)
            ign.cache(DEFAULT_CACHE_NAME).put(i, i);

        stopGrid(0);

        List<File> segs = Arrays.stream(ign.context().pdsFolderResolver().fileTree().wal().listFiles()).sorted()
            .limit(2)
            .collect(Collectors.toList());

        int firstSegActLen = writtenLength(segs.get(0));

        ByteBuffer walBuf = ByteBuffer.wrap(Files.readAllBytes(segs.get(0).toPath()));
        ByteBuffer cdcBuf = ByteBuffer.wrap(cdcMgr.buf.array());
        walBuf.limit(firstSegActLen);
        cdcBuf.limit(firstSegActLen);

        assertEquals(0, walBuf.compareTo(cdcBuf));

        walBuf = ByteBuffer.wrap(Files.readAllBytes(segs.get(1).toPath()));
        cdcBuf = ByteBuffer.wrap(cdcMgr.buf.array());
        cdcBuf.position(firstSegActLen + 1);
        cdcBuf.limit(firstSegActLen + 1 + WAL_SEG_SIZE);

        assertEquals(0, walBuf.compareTo(cdcBuf));
    }

    /** */
    private void rollSegment(RolloverType rollType) {
        if (persistentEnabled)
            dbMgr(ign).checkpointReadLock();

        try {
            walMgr.log(new CheckpointRecord(null), rollType);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (persistentEnabled)
                dbMgr(ign).checkpointReadUnlock();
        }
    }

    /** @return Length of the all written records in the specified segment. */
    private int writtenLength(File walSegment) throws Exception {
        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        IgniteWalIteratorFactory.IteratorParametersBuilder params = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .filesOrDirs(walSegment);

        try (WALIterator walIt = factory.iterator(params)) {
            while (walIt.hasNext())
                walIt.next();

            return walIt.lastRead().get().next().fileOffset();
        }
    }

    /** */
    private static class CdcManagerPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "CdcManagerPluginProvider";
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (CdcManager.class.equals(cls))
                return (T)new TestCdcManager();

            return null;
        }
    }

    /** Test {@link CdcManager} for testing WAL fsync guarantees. */
    protected static class TestCdcManager extends GridCacheSharedManagerAdapter implements CdcManager {
        /** Buffer to store collected data. */
        private final ByteBuffer buf;

        /** */
        TestCdcManager() {
            buf = ByteBuffer.allocate(2 * WAL_SEG_SIZE);

            Arrays.fill(buf.array(), (byte)0);

            buf.position(0);
        }

        /** {@inheritDoc} */
        @Override public void collect(ByteBuffer dataBuf) {
            if (failCollect)
                throw new RuntimeException();

            if (log.isDebugEnabled())
                log.debug("Collect data buffer [offset=" + dataBuf.position() + ", limit=" + dataBuf.limit() + ']');

            buf.put(dataBuf);
        }

        /** {@inheritDoc} */
        @Override public boolean enabled() {
            return true;
        }

        /** @return CdcManager for specified Ignite node. */
        static TestCdcManager cdcMgr(IgniteEx ign) {
            return (TestCdcManager)ign.context().cache().context().cdc();
        }
    }

    /** Test {@link CdcManager} order of calling methods. */
    protected static class DelayedCdcManager extends GridCacheSharedManagerAdapter implements CdcManager {
        /** Set to {@code true} after first {@link #collect(ByteBuffer)} call. */
        private volatile boolean collected;

        /** Set to {@code true} after binary memory restored. */
        private final AtomicBoolean restored;

        /** */
        DelayedCdcManager(AtomicBoolean restored) {
            this.restored = restored;
        }

        /** {@inheritDoc} */
        @Override public void collect(ByteBuffer dataBuf) {
            collected = true;
        }

        /** {@inheritDoc} */
        @Override public void afterBinaryMemoryRestore(
            IgniteCacheDatabaseSharedManager mgr,
            GridCacheDatabaseSharedManager.RestoreBinaryState restoreState
        ) {
            try {
                // Wait if any WALRecord is being written in background and collected.
                Thread.sleep(5_000);

                assert !collected;

                restored.set(true);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean enabled() {
            return true;
        }
    }
}

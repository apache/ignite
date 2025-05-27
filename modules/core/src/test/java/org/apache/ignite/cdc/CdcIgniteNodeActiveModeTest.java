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

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cdc.CdcConsumerState;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.cdc.CdcManager;
import org.apache.ignite.internal.cdc.CdcMode;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.CdcManagerRecord;
import org.apache.ignite.internal.pagemem.wal.record.CdcManagerStopRecord;
import org.apache.ignite.internal.pagemem.wal.record.RolloverType;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.wal.record.RecordUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.cdc.CdcMain.CDC_MODE;
import static org.apache.ignite.internal.cdc.CdcMain.COMMITTED_SEG_IDX;
import static org.apache.ignite.internal.cdc.CdcMain.CUR_SEG_IDX;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class CdcIgniteNodeActiveModeTest extends AbstractCdcTest {
    /** */
    private IgniteEx ign;

    /** */
    private CdcMain cdcMain;

    /** */
    private IgniteInternalFuture<?> cdcMainFut;

    /** */
    private UserCdcConsumer cnsmr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setCdcEnabled(true)));

        cfg.setPluginProviders(new NoOpCdcManagerPluginProvider());

        cfg.setCacheConfiguration(new CacheConfiguration<Integer, User>(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        ign = startGrid();

        ign.cluster().state(ACTIVE);

        cdcMain = createCdc((cnsmr = new UserCdcConsumer()), ign.configuration());

        cdcMainFut = runAsync(cdcMain);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cdcMainFut.cancel();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testCdcMetrics() throws Exception {
        awaitCdcModeValue(CdcMode.IGNITE_NODE_ACTIVE);

        IgniteCache<Integer, User> cache = ign.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 5_000; i++) {
            cache.put(i, createUser(i));

            if (i % 1_000 == 0) {
                writeCdcManagerRecord();
                rollSegment();
            }
        }

        MetricRegistry mreg = GridTestUtils.<StandaloneGridKernalContext>getFieldValue(cdcMain, "kctx")
            .metric().registry("cdc");

        assertTrue(GridTestUtils.waitForCondition(() ->
            mreg.<LongMetric>findMetric(CUR_SEG_IDX).value() > 1, 10_000, 100));

        assertTrue(GridTestUtils.waitForCondition(() ->
            mreg.<LongMetric>findMetric(COMMITTED_SEG_IDX).value() > 1, 10_000, 100));

        awaitCdcModeValue(CdcMode.IGNITE_NODE_ACTIVE);
    }

    /** */
    @Test
    public void testSwitchToCdcUtilityActiveMode() throws Exception {
        writeCdcManagerStopCdcRecord();

        rollSegment();

        awaitCdcModeValue(CdcMode.CDC_UTILITY_ACTIVE);
    }

    /** */
    @Test
    public void testSkipsNodeCommittedData() throws Exception {
        List<Integer> expUsers = new ArrayList<>();

        addData(0, 1, null);

        writeCdcManagerRecord();

        rollSegment();

        addData(2, 3, expUsers);

        rollSegment();

        addData(4, 5, expUsers);

        writeCdcManagerStopCdcRecord();

        rollSegment();

        awaitCdcModeValue(CdcMode.CDC_UTILITY_ACTIVE);

        checkConsumerData(expUsers);

        addData(6, 7, expUsers);

        rollSegment();

        checkConsumerData(expUsers);
    }

    /** */
    @Test
    public void testCleanWalsAfterCdcManagerRecord() throws Exception {
        checkCdcSegmentsExists(0, -1);  // No segments stored in CDC dir.

        rollSegment();

        checkCdcSegmentsExists(0, 0);  // Segment 0 is stored.

        rollSegment();

        checkCdcSegmentsExists(0, 1);  // Segment 1 is stored.

        writeCdcManagerRecord();
        rollSegment();

        checkCdcSegmentsExists(2, 2);  // Segments 0, 1 should be cleared after CdcManagerRecord.

        rollSegment();

        checkCdcSegmentsExists(2, 3);  // Segment 3 is stored.

        writeCdcManagerRecord();
        rollSegment();

        checkCdcSegmentsExists(4, 4);  // Segments 2, 3 should be cleared after CdcManagerRecord.

        writeCdcManagerStopCdcRecord();
        rollSegment();

        checkCdcSegmentsExists(5, 5); // Segment 4 is cleared after CdcMain consumes it.
    }

    /** */
    @Test
    public void testRestoreModeOnRestart() throws Exception {
        RunnableX restartUtil = () -> {
            cdcMainFut.cancel();
            cdcMain = createCdc(cnsmr, getConfiguration(getTestIgniteInstanceName()));
            cdcMainFut = runAsync(cdcMain);
        };

        List<Integer> expUsers = new ArrayList<>();

        addData(0, 1, null);

        rollSegment();

        addData(2, 3, null);

        restartUtil.run();

        rollSegment();

        writeCdcManagerRecord();
        writeCdcManagerStopCdcRecord();

        addData(4, 5, expUsers);

        rollSegment();

        restartUtil.run();

        awaitCdcModeValue(CdcMode.CDC_UTILITY_ACTIVE);

        checkConsumerData(expUsers);

        addData(4, 5, expUsers);

        rollSegment();

        checkConsumerData(expUsers);
    }

    /** */
    @Test
    public void testStoreCdcMode() throws Exception {
        writeCdcManagerStopCdcRecord();

        rollSegment();

        awaitCdcModeValue(CdcMode.CDC_UTILITY_ACTIVE);

        CdcConsumerState state = GridTestUtils.getFieldValue(cdcMain, "state");

        assertEquals(CdcMode.CDC_UTILITY_ACTIVE, state.loadCdcMode());
    }

    /** */
    private WALPointer rollSegment() throws IgniteCheckedException {
        return walMgr().log(RecordUtils.buildCheckpointRecord(), RolloverType.CURRENT_SEGMENT);
    }

    /** */
    private void writeCdcManagerStopCdcRecord() throws Exception {
        walMgr().log(new CdcManagerStopRecord());
    }

    /** */
    private void writeCdcManagerRecord() throws Exception {
        walMgr().log(new CdcManagerRecord(new T2<>(walMgr().log(RecordUtils.buildCheckpointRecord()), 0)));
    }

    /** */
    private IgniteWriteAheadLogManager walMgr() {
        return ign.context().cache().context().wal(true);
    }

    /**
     * @param expUsers Expected users read by CDC.
     */
    private void checkConsumerData(List<Integer> expUsers) throws IgniteInterruptedCheckedException {
        assertTrue(waitForCondition(() ->
            expUsers.equals(cnsmr.data(ChangeEventType.UPDATE, cacheId(DEFAULT_CACHE_NAME))
        ), 10_000, 10));
    }

    /**
     * @param from First index of user to add.
     * @param to Last index of user to add.
     * @param expCdcReadUsers Expected CDC read users.
     */
    private void addData(int from, int to, List<Integer> expCdcReadUsers) {
        IntStream.range(from, to).forEach(u -> ign.cache(DEFAULT_CACHE_NAME).put(u, createUser(u)));

        if (expCdcReadUsers != null)
            IntStream.range(from, to).forEach(expCdcReadUsers::add);
    }

    /** */
    private void awaitCdcModeValue(CdcMode expVal) throws IgniteInterruptedCheckedException {
        assertTrue(waitForCondition(() -> {
            try {
                ObjectMetric<String> m = GridTestUtils.<StandaloneGridKernalContext>getFieldValue(cdcMain, "kctx")
                    .metric().registry("cdc").findMetric(CDC_MODE);

                return m != null && expVal.name().equals(m.value());
            }
            catch (Exception e) {
                return false;
            }
        }, 30_000, 10));
    }

    /**
     * Checks that WAL CDC directory contains specified segments.
     *
     * @param from Start segment index to check.
     * @param to Inclusive end segment index.
     */
    private void checkCdcSegmentsExists(long from, long to) throws IgniteInterruptedCheckedException {
        List<Long> expected;

        if (from > to)
            expected = Collections.emptyList();
        else
            expected = LongStream.range(from, to + 1).boxed().collect(Collectors.toList());

        assertTrue(waitForCondition(() -> {
            try {
                NodeFileTree ft = ign.context().pdsFolderResolver().fileTree();

                List<Long> actual = Files.list(ft.walCdc().toPath())
                    .filter(p -> NodeFileTree.isWalFile(p.toFile()))
                    .map(ft::walSegmentIndex)
                    .sorted()
                    .collect(Collectors.toList());

                return expected.equals(actual);
            }
            catch (Exception e) {
                return false;
            }
        }, 10_000, 10));
    }

    /** */
    private static class NoOpCdcManagerPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "NoOpCdcManagerPluginProvider";
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (CdcManager.class.equals(cls))
                return (T)new NoOpCdcManager();

            return null;
        }
    }

    /** */
    protected static class NoOpCdcManager extends GridCacheSharedManagerAdapter implements CdcManager {
        /** {@inheritDoc} */
        @Override public boolean enabled() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public void collect(ByteBuffer dataBuf) {
            // No-op.
        }
    }
}

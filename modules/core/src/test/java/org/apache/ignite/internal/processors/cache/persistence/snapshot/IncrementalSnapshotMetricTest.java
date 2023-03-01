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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.RolloverType;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.AbstractIncrementalSnapshotTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.ObjectGauge;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_METRICS;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreProcess.SNAPSHOT_RESTORE_METRICS;

/** */
public class IncrementalSnapshotMetricTest extends AbstractIncrementalSnapshotTest {
    /** */
    private static CountDownLatch beforeFinRecLatch;

    /** */
    private static CountDownLatch logFinRecLatch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        if (getTestIgniteInstanceIndex(instanceName) == 0)
            cfg.setPluginProviders(new BlockingWALPluginProvider());

        return cfg;
    }

    /** @throws Exception If fails. */
    @Test
    public void testCreateIncrementalSnapshotMetrics() throws Exception {
        beforeFinRecLatch = new CountDownLatch(1);
        logFinRecLatch = new CountDownLatch(1);

        MetricRegistry mreg0 = grid(0).context().metric().registry(SNAPSHOT_METRICS);

        LongMetric startTime = mreg0.findMetric("LastIncrementalSnapshotStartTime");
        LongMetric endTime = mreg0.findMetric("LastIncrementalSnapshotEndTime");
        IntMetric incIdx = mreg0.findMetric("LastIncrementalSnapshotIndex");
        ObjectGauge<String> snpName = mreg0.findMetric("LastIncrementalSnapshotName");
        ObjectGauge<String> errMsg = mreg0.findMetric("LastIncrementalSnapshotErrorMessage");

        assertEquals(0, startTime.value());
        assertEquals(0, endTime.value());
        assertEquals(0, incIdx.value());
        assertTrue(snpName.value().isEmpty());
        assertTrue(errMsg.value().isEmpty());

        IgniteCache<Integer, Integer> cache = grid(0).cache(CACHE);

        for (int i = 0; i < 1024; i++)
            cache.put(i, ThreadLocalRandom.current().nextInt());

        IgniteFuture<Void> incSnpFut = grid(0).snapshot().createIncrementalSnapshot(SNP);

        assertTrue(GridTestUtils.waitForCondition(
            () -> snp(grid(0)).currentSnapshotTask(IncrementalSnapshotFutureTask.class) != null,
            5_000, 10));

        U.await(logFinRecLatch);

        assertTrue(startTime.value() > 0);
        assertEquals(0, endTime.value());
        assertEquals(1, incIdx.value());
        assertEquals(SNP, snpName.value());
        assertTrue(errMsg.value().isEmpty());

        beforeFinRecLatch.countDown();

        incSnpFut.get(getTestTimeout());

        assertTrue(startTime.value() > 0);
        assertTrue(endTime.value() > 0);
        assertEquals(1, incIdx.value());
        assertEquals(SNP, snpName.value());
        assertTrue(errMsg.value().isEmpty());

        stopGrid(1);

        GridTestUtils.assertThrows(log,
            () -> grid(0).snapshot().createIncrementalSnapshot(SNP).get(getTestTimeout()),
            IgniteException.class,
            null);

        assertFalse(errMsg.value().isEmpty());
    }

    /** @throws Exception If fails. */
    @Test
    public void testRestoreIncrementalSnapshotMetrics() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(CACHE);

        for (int i = 0; i < 1024; i++) {
            if (i == 512)
                rollWalSegment(grid(0));

            cache.put(i, ThreadLocalRandom.current().nextInt());
        }

        grid(0).snapshot().createIncrementalSnapshot(SNP).get(getTestTimeout());

        restartWithCleanPersistence(nodes(), Collections.singletonList(CACHE));

        MetricRegistry mreg0 = grid(0).context().metric().registry(SNAPSHOT_RESTORE_METRICS);

        IntMetric incIdx = mreg0.findMetric("incrementalIndex");
        IntMetric totalWalSeg = mreg0.findMetric("totalWalSegments");
        IntMetric procWalSeg = mreg0.findMetric("processedWalSegments");
        LongMetric procEntries = mreg0.findMetric("processedEntries");

        assertEquals(0, incIdx.value());
        assertEquals(-1, totalWalSeg.value());
        assertEquals(-1, procWalSeg.value());
        assertEquals(-1, procEntries.value());

        grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        assertEquals(1, incIdx.value());
        assertEquals(2, totalWalSeg.value());
        assertEquals(2, procWalSeg.value());
        assertEquals(1024L, procEntries.value());  // 1 primary and 2 backups.
    }

    /** */
    private static class BlockingWALPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "BlockingWALProvider";
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (IgniteWriteAheadLogManager.class.equals(cls))
                return (T)new BlockingWALManager(((IgniteEx)ctx.grid()).context());

            return null;
        }
    }

    /** Blocks writing to WAL {@link IncrementalSnapshotStartRecord}. */
    protected static class BlockingWALManager extends FileWriteAheadLogManager {
        /** */
        public BlockingWALManager(GridKernalContext ctx) {
            super(ctx);
        }

        /** {@inheritDoc} */
        @Override public WALPointer log(WALRecord record, RolloverType rolloverType) throws IgniteCheckedException, StorageException {
            if (record.type() == WALRecord.RecordType.INCREMENTAL_SNAPSHOT_FINISH_RECORD && logFinRecLatch != null) {
                logFinRecLatch.countDown();

                U.awaitQuiet(beforeFinRecLatch);
            }

            return super.log(record, rolloverType);
        }
    }

    /** {@inheritDoc} */
    @Override protected int nodes() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int backups() {
        return 2;
    }
}

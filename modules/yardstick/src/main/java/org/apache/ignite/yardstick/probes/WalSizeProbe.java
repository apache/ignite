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

package org.apache.ignite.yardstick.probes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.MemoryRecoveryRecord;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointWriteProgressSupplier;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkDriver;
import org.yardstickframework.BenchmarkProbePoint;
import org.yardstickframework.BenchmarkTotalsOnlyProbe;

/**
 * WAL size probe.
 */
public class WalSizeProbe implements BenchmarkTotalsOnlyProbe {
    /** WAL manager. */
    private volatile IgniteWriteAheadLogManager wal;

    /** Checkpoint process supplier. */
    private volatile CheckpointWriteProgressSupplier chpProc;

    /** WAL segment size. */
    private volatile int walSegmentSize;

    /** Previous WAL ptr. */
    private volatile FileWALPointer prevPtr;

    /** Current checkpoint written pages. */
    private volatile int chpPrevWrittenPages;

    /** Current checkpoint fsync pages. */
    private volatile int chpPrevSyncedPages;

    /** Current checkpoint total pages. */
    private volatile int chpPrevTotalPages;

    /** Collected points. */
    private Collection<BenchmarkProbePoint> collected = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public void start(BenchmarkDriver drv, BenchmarkConfiguration cfg) throws Exception {
        if (drv instanceof IgniteAbstractBenchmark) {
            IgniteEx ignite = (IgniteEx)((IgniteAbstractBenchmark)drv).ignite();

            wal = ignite.context().cache().context().wal();

            chpProc = (CheckpointWriteProgressSupplier)ignite.context().cache().context().database();

            if (wal != null) {
                walSegmentSize = ignite.configuration().getDataStorageConfiguration().getWalSegmentSize();

                prevPtr = (FileWALPointer)wal.log(new MemoryRecoveryRecord(0));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {

    }

    /** {@inheritDoc} */
    @Override public Collection<String> metaInfo() {
        return Arrays.asList("Time, sec", "WAL size, bytes/sec", "Checkpoint written pages, pages/sec"
            , "Checkpoint synced pages, pages/sec");
    }

    /** {@inheritDoc} */
    @Override public Collection<BenchmarkProbePoint> points() {
        Collection<BenchmarkProbePoint> ret = collected;

        collected = new ArrayList<>(ret.size() + 5);

        return ret;
    }

    /** {@inheritDoc} */
    @Override public void buildPoint(long time) {
        if (wal == null || chpProc == null)
            return;

        try {
            FileWALPointer ptr = (FileWALPointer)wal.log(new MemoryRecoveryRecord(time));

            double estimatedSize = (ptr.index() - prevPtr.index()) * walSegmentSize + ptr.fileOffset() - prevPtr.fileOffset();

            AtomicInteger writtenPagesCntr = chpProc.writtenPagesCounter();
            AtomicInteger syncedPagesCntr = chpProc.syncedPagesCounter();

            int chpCurWrittenPages = writtenPagesCntr == null ? 0 : writtenPagesCntr.get();
            int chpCurSyncedPages = syncedPagesCntr == null ? 0 : syncedPagesCntr.get();
            int chpCurTotalPages = chpProc.currentCheckpointPagesCount();

            int writtenPages = chpCurWrittenPages >= chpPrevWrittenPages ? chpCurWrittenPages - chpPrevWrittenPages :
                chpPrevTotalPages - chpPrevWrittenPages;

            int syncedPages = chpCurSyncedPages >= chpPrevSyncedPages ? chpCurSyncedPages - chpPrevSyncedPages :
                chpPrevTotalPages - chpPrevSyncedPages;

            chpPrevWrittenPages = chpCurWrittenPages;
            chpPrevSyncedPages = chpCurSyncedPages;
            chpPrevTotalPages = chpCurTotalPages;

            prevPtr = ptr;

            collected.add(new BenchmarkProbePoint(TimeUnit.MILLISECONDS.toSeconds(time),
                new double[] {estimatedSize, writtenPages, syncedPages}));
        }
        catch (IgniteCheckedException ignore) {
            // No-op.
        }
    }
}

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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.ClusterSnapshotRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CLUSTER_SNAPSHOT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD_V2;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.INCREMENTAL_SNAPSHOT_FINISH_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.INCREMENTAL_SNAPSHOT_START_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.TX_RECORD;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER;

/** Processes incremental snapshot: parse WAL segments and handles records. */
abstract class IncrementalSnapshotProcessor {
    /** Cache shared context. */
    private final GridCacheSharedContext<?, ?> cctx;

    /** Ignite logger. */
    private final IgniteLogger log;

    /** Snapshot file tree. */
    private final SnapshotFileTree sft;

    /** Incremental snapshot index. */
    private final int incIdx;

    /** Snapshot cache IDs. */
    private final Set<Integer> cacheIds;

    /** */
    IncrementalSnapshotProcessor(GridCacheSharedContext<?, ?> cctx, SnapshotFileTree sft, int incIdx, Set<Integer> cacheIds) {
        this.cctx = cctx;
        this.sft = sft;
        this.incIdx = incIdx;
        this.cacheIds = cacheIds;

        log = cctx.logger(getClass());
    }

    /**
     * Process incremental snapshot data.
     *
     * @param dataEntryHnd Handle data entries.
     * @param txHnd Handle transaction records.
     */
    void process(
        Consumer<DataEntry> dataEntryHnd,
        @Nullable Consumer<TxRecord> txHnd
    ) throws IgniteCheckedException, IOException {
        IncrementalSnapshotMetadata meta = cctx.snapshotMgr()
            .readIncrementalSnapshotMetadata(sft.incrementalSnapshotFileTree(incIdx).meta());

        File[] segments = walSegments();

        totalWalSegments(segments.length);

        UUID incSnpId = meta.requestId();

        File lastSeg = Arrays.stream(segments)
            .map(File::toPath)
            .max(Comparator.comparingLong(sft::walSegmentIndex))
            .orElseThrow(() -> new IgniteCheckedException("Last WAL segment wasn't found [snpName=" + sft.name() + ']'))
            .toFile();

        IncrementalSnapshotFinishRecord incSnpFinRec = readFinishRecord(lastSeg, incSnpId);

        if (incSnpFinRec == null) {
            throw new IgniteCheckedException("System WAL record for incremental snapshot wasn't found " +
                "[id=" + incSnpId + ", walSegFile=" + lastSeg + ']');
        }

        LongAdder applied = new LongAdder();

        initWalEntries(applied);
        processedWalSegments(0);

        Set<WALRecord.RecordType> recTypes = new HashSet<>(F.asList(
            CLUSTER_SNAPSHOT,
            INCREMENTAL_SNAPSHOT_START_RECORD,
            INCREMENTAL_SNAPSHOT_FINISH_RECORD,
            DATA_RECORD_V2));

        if (txHnd != null)
            recTypes.add(TX_RECORD);

        // Create a single WAL iterator for 2 steps: finding ClusterSnapshotRecord and applying incremental snapshots.
        // TODO: Fix it after resolving https://issues.apache.org/jira/browse/IGNITE-18718.
        try (WALIterator it = walIter(log, recTypes, segments)) {
            long startIdx = -1;

            // Step 1. Skips applying WAL until base snapshot record has been reached.
            while (it.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> walRec = it.next();

                WALRecord rec = walRec.getValue();

                if (rec.type() == CLUSTER_SNAPSHOT && ((ClusterSnapshotRecord)rec).clusterSnapshotName().equals(sft.name())) {
                    startIdx = walRec.getKey().index();

                    break;
                }
            }

            if (startIdx < 0) {
                throw new IgniteCheckedException("System WAL record for full snapshot wasn't found " +
                    "[snpName=" + sft.name() + ", walSegFile=" + segments[0] + ']');
            }

            UUID prevIncSnpId = incIdx > 1
                ? cctx.snapshotMgr().readIncrementalSnapshotMetadata(sft.incrementalSnapshotFileTree(incIdx - 1).meta()).requestId()
                : null;

            IgnitePredicate<GridCacheVersion> txVerFilter = prevIncSnpId != null
                ? txVer -> true : txVer -> !incSnpFinRec.excluded().contains(txVer);

            long lastProcessedIdx = 0;

            // Step 2. Apply incremental snapshots.
            while (it.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> walRec = it.next();

                long curIdx = walRec.getKey().index();

                if (curIdx != lastProcessedIdx) {
                    processedWalSegments((int)(curIdx - startIdx));

                    lastProcessedIdx = curIdx;
                }

                WALRecord rec = walRec.getValue();

                if (rec.type() == INCREMENTAL_SNAPSHOT_START_RECORD) {
                    IncrementalSnapshotStartRecord startRec = (IncrementalSnapshotStartRecord)rec;

                    if (startRec.id().equals(incSnpFinRec.id()))
                        txVerFilter = v -> incSnpFinRec.included().contains(v);
                }
                else if (rec.type() == INCREMENTAL_SNAPSHOT_FINISH_RECORD) {
                    IncrementalSnapshotFinishRecord finRec = (IncrementalSnapshotFinishRecord)rec;

                    if (finRec.id().equals(prevIncSnpId))
                        txVerFilter = txVer -> !incSnpFinRec.excluded().contains(txVer);
                }
                else if (rec.type() == DATA_RECORD_V2) {
                    DataRecord data = (DataRecord)rec;

                    for (DataEntry e: data.writeEntries()) {
                        // That is OK to restore only part of transaction related to a specified cache group,
                        // because a full snapshot restoring does the same.
                        if (!cacheIds.contains(e.cacheId()) || !txVerFilter.apply(e.nearXidVersion()))
                            continue;

                        dataEntryHnd.accept(e);

                        applied.increment();
                    }
                }
                else if (rec.type() == TX_RECORD) {
                    TxRecord tx = (TxRecord)rec;

                    if (!txVerFilter.apply(tx.nearXidVersion()))
                        continue;

                    txHnd.accept(tx);
                }
            }

            processedWalSegments(segments.length);
        }
    }

    /** @return WAL segments to restore for specified incremental index since the base snapshot. */
    private File[] walSegments() throws IgniteCheckedException {
        File[] segments = null;

        for (int i = 1; i <= incIdx; i++) {
            SnapshotFileTree.IncrementalSnapshotFileTree ift = sft.incrementalSnapshotFileTree(i);

            if (!ift.root().exists())
                throw new IgniteCheckedException("Incremental snapshot doesn't exists [dir=" + ift.root() + ']');

            if (!ift.wal().exists())
                throw new IgniteCheckedException("Incremental snapshot WAL directory doesn't exists [dir=" + ift.wal() + ']');

            File[] incSegs = ift.wal().listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER);

            if (incSegs == null)
                throw new IgniteCheckedException("Failed to list WAL segments from snapshot directory [dir=" + ift.root() + ']');

            if (segments == null)
                segments = incSegs;
            else {
                int segLen = segments.length;

                segments = Arrays.copyOf(segments, segLen + incSegs.length);

                System.arraycopy(incSegs, 0, segments, segLen, incSegs.length);
            }
        }

        if (F.isEmpty(segments)) {
            throw new IgniteCheckedException("No WAL segments found for incremental snapshot " +
                "[snpName=" + sft.name() + ", snpPath=" + sft.path() + ", incrementIndex=" + incIdx + ']');
        }

        return segments;
    }

    /** @return {@link IncrementalSnapshotFinishRecord} for specified snapshot, or {@code null} if not found. */
    private @Nullable IncrementalSnapshotFinishRecord readFinishRecord(File segment, UUID incSnpId) throws IgniteCheckedException {
        try (WALIterator it = walIter(log, Collections.singleton(INCREMENTAL_SNAPSHOT_FINISH_RECORD), segment)) {
            while (it.hasNext()) {
                IncrementalSnapshotFinishRecord finRec = (IncrementalSnapshotFinishRecord)it.next().getValue();

                if (finRec.id().equals(incSnpId))
                    return finRec;
            }
        }

        return null;
    }

    /**
     * @param log Ignite logger.
     * @param types WAL record types to read.
     * @param segments WAL segments.
     * @return Iterator over WAL segments.
     */
    private WALIterator walIter(IgniteLogger log, Set<WALRecord.RecordType> types, File... segments) throws IgniteCheckedException {
        return new IgniteWalIteratorFactory(log)
            .iterator(new IgniteWalIteratorFactory.IteratorParametersBuilder()
                .filter((recType, recPtr) -> types.contains(recType))
                .sharedContext(cctx)
                .filesOrDirs(segments));
    }

    /**
     * @param segCnt Total WAL segments in the incremental snapshot.
     */
    abstract void totalWalSegments(int segCnt);

    /**
     * @param segCnt Processed WAL segments for the incremental snapshot.
     */
    abstract void processedWalSegments(int segCnt);

    /**
     * @param entriesCnt Processed data entries for the incremental snapshot.
     */
    abstract void initWalEntries(LongAdder entriesCnt);
}

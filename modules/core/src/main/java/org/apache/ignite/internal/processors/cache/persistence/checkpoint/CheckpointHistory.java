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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE;

/**
 * Checkpoint history. Holds chronological ordered map with {@link CheckpointEntry CheckpointEntries}.
 * Data is loaded from corresponding checkpoint directory.
 * This directory holds files for checkpoint start and end.
 */
public class CheckpointHistory {
    /** Logger. */
    private final IgniteLogger log;

    /** Cache shared context. */
    private final GridCacheSharedContext<?, ?> cctx;

    /**
     * Maps checkpoint's timestamp (from CP file name) to CP entry.
     * Using TS provides historical order of CP entries in map ( first is oldest )
     */
    private final NavigableMap<Long, CheckpointEntry> histMap = new ConcurrentSkipListMap<>();

    /** The maximal number of checkpoints hold in memory. */
    private final int maxCpHistMemSize;

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public CheckpointHistory(GridKernalContext ctx) {
        cctx = ctx.cache().context();
        log = ctx.log(getClass());

        DataStorageConfiguration dsCfg = ctx.config().getDataStorageConfiguration();

        maxCpHistMemSize = Math.min(dsCfg.getWalHistorySize(),
            IgniteSystemProperties.getInteger(IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE, 100));
    }

    /**
     * @param checkpoints Checkpoints.
     */
    public void initialize(List<CheckpointEntry> checkpoints) {
        for (CheckpointEntry e : checkpoints)
            histMap.put(e.timestamp(), e);
    }

    /**
     * @param cpTs Checkpoint timestamp.
     * @return Initialized entry.
     * @throws IgniteCheckedException If failed to initialize entry.
     */
    private CheckpointEntry entry(Long cpTs) throws IgniteCheckedException {
        CheckpointEntry entry = histMap.get(cpTs);

        if (entry == null)
            throw new IgniteCheckedException("Checkpoint entry was removed: " + cpTs);

        return entry;
    }

    /**
     * @return First checkpoint entry if exists. Otherwise {@code null}.
     */
    public CheckpointEntry firstCheckpoint() {
        Map.Entry<Long,CheckpointEntry> entry = histMap.firstEntry();

        return entry != null ? entry.getValue() : null;
    }

    /**
     * @return Last checkpoint entry if exists. Otherwise {@code null}.
     */
    public CheckpointEntry lastCheckpoint() {
        Map.Entry<Long,CheckpointEntry> entry = histMap.lastEntry();

        return entry != null ? entry.getValue() : null;
    }

    /**
     * @return First checkpoint WAL pointer if exists. Otherwise {@code null}.
     */
    public WALPointer firstCheckpointPointer() {
        CheckpointEntry entry = firstCheckpoint();

        return entry != null ? entry.checkpointMark() : null;
    }

    /**
     * @return Collection of checkpoint timestamps.
     */
    public Collection<Long> checkpoints(boolean descending) {
        if (descending)
            return histMap.descendingKeySet();

        return histMap.keySet();
    }

    /**
     *
     */
    public Collection<Long> checkpoints() {
        return checkpoints(false);
    }

    /**
     * Adds checkpoint entry after the corresponding WAL record has been written to WAL. The checkpoint itself
     * is not finished yet.
     *
     * @param entry Entry to add.
     */
    public void addCheckpoint(CheckpointEntry entry) {
        histMap.put(entry.timestamp(), entry);
    }

    /**
     * @return {@code true} if there is space for next checkpoint.
     */
    public boolean hasSpace() {
        return histMap.size() + 1 <= maxCpHistMemSize;
    }

    /**
     * Clears checkpoint history after WAL truncation.
     *
     * @return List of checkpoint entries removed from history.
     */
    public List<CheckpointEntry> onWalTruncated(WALPointer ptr) {
        List<CheckpointEntry> removed = new ArrayList<>();

        FileWALPointer highBound = (FileWALPointer)ptr;

        for (CheckpointEntry cpEntry : histMap.values()) {
            FileWALPointer cpPnt = (FileWALPointer)cpEntry.checkpointMark();

            if (highBound.compareTo(cpPnt) <= 0)
                break;

            if (cctx.wal().reserved(cpEntry.checkpointMark())) {
                U.warn(log, "Could not clear historyMap due to WAL reservation on cp: " + cpEntry +
                    ", history map size is " + histMap.size());

                break;
            }

            histMap.remove(cpEntry.timestamp());

            removed.add(cpEntry);
        }

        return removed;
    }

    /**
     * Logs and clears checkpoint history after checkpoint finish.
     *
     * @return List of checkpoints removed from history.
     */
    public List<CheckpointEntry> onCheckpointFinished(GridCacheDatabaseSharedManager.Checkpoint chp, boolean truncateWal) {
        List<CheckpointEntry> rmv = new ArrayList<>();

        chp.walSegsCoveredRange(calculateWalSegmentsCovered());

        int deleted = 0;

        while (histMap.size() > maxCpHistMemSize) {
            Map.Entry<Long, CheckpointEntry> entry = histMap.firstEntry();

            CheckpointEntry cpEntry = entry.getValue();

            if (cctx.wal().reserved(cpEntry.checkpointMark())) {
                U.warn(log, "Could not clear historyMap due to WAL reservation on cpEntry " + cpEntry.checkpointId() +
                    ", history map size is " + histMap.size());

                break;
            }

            if (truncateWal)
                deleted += cctx.wal().truncate(null, cpEntry.checkpointMark());

            histMap.remove(entry.getKey());

            rmv.add(cpEntry);
        }

        chp.walFilesDeleted(deleted);

        return rmv;
    }

    /**
     * Calculates indexes of WAL segments covered by last checkpoint.
     *
     * @return list of indexes or empty list if there are no checkpoints.
     */
    private IgniteBiTuple<Long, Long> calculateWalSegmentsCovered() {
        IgniteBiTuple<Long, Long> tup = new IgniteBiTuple<>(-1L, -1L);

        Map.Entry<Long, CheckpointEntry> lastEntry = histMap.lastEntry();

        if (lastEntry == null)
            return tup;

        Map.Entry<Long, CheckpointEntry> previousEntry = histMap.lowerEntry(lastEntry.getKey());

        WALPointer lastWALPointer = lastEntry.getValue().checkpointMark();

        long lastIdx = 0;

        long prevIdx = 0;

        if (lastWALPointer instanceof FileWALPointer) {
            lastIdx = ((FileWALPointer)lastWALPointer).index();

            if (previousEntry != null)
                prevIdx = ((FileWALPointer)previousEntry.getValue().checkpointMark()).index();
        }

        tup.set1(prevIdx);
        tup.set2(lastIdx - 1);

        return tup;
    }

    /**
     * Tries to search for a WAL pointer for the given partition counter start.
     *
     * @param grpId Cache group ID.
     * @param part Partition ID.
     * @param partCntrSince Partition counter or {@code null} to search for minimal counter.
     * @return Checkpoint entry or {@code null} if failed to search.
     */
    @Nullable public WALPointer searchPartitionCounter(int grpId, int part, long partCntrSince) {
        CheckpointEntry entry = searchCheckpointEntry(grpId, part, partCntrSince);

        if (entry == null)
            return null;

        return entry.checkpointMark();
    }

    /**
     * Tries to search for a WAL pointer for the given partition counter start.
     *
     * @param grpId Cache group ID.
     * @param part Partition ID.
     * @param partCntrSince Partition counter or {@code null} to search for minimal counter.
     * @return Checkpoint entry or {@code null} if failed to search.
     */
    @Nullable public CheckpointEntry searchCheckpointEntry(int grpId, int part, long partCntrSince) {
        for (Long cpTs : checkpoints(true)) {
            try {
                CheckpointEntry entry = entry(cpTs);

                Long foundCntr = entry.partitionCounter(cctx, grpId, part);

                if (foundCntr != null && foundCntr <= partCntrSince)
                    return entry;
            }
            catch (IgniteCheckedException ignore) {
                break;
            }
        }

        return null;
    }

    /**
     * Finds and reserves earliest valid checkpoint for each of given groups and partitions.
     *
     * @param groupsAndPartitions Groups and partitions to find and reserve earliest valid checkpoint.
     *
     * @return Map (groupId, Map (partitionId, earliest valid checkpoint to history search)).
     */
    public Map<Integer, Map<Integer, CheckpointEntry>> searchAndReserveCheckpoints(
        final Map<Integer, Set<Integer>> groupsAndPartitions
    ) {
        if (F.isEmpty(groupsAndPartitions))
            return Collections.emptyMap();

        final Map<Integer, Map<Integer, CheckpointEntry>> res = new HashMap<>();

        CheckpointEntry prevReserved = null;

        // Iterate over all possible checkpoints starting from latest and moving to earliest.
        for (Long cpTs : checkpoints(true)) {
            CheckpointEntry chpEntry = null;

            try {
                chpEntry = entry(cpTs);

                boolean reserved = cctx.wal().reserve(chpEntry.checkpointMark());

                // If checkpoint WAL history can't be reserved, stop searching.
                if (!reserved)
                    break;

                for (Integer grpId : new HashSet<>(groupsAndPartitions.keySet()))
                    if (!isCheckpointApplicableForGroup(grpId, chpEntry))
                        groupsAndPartitions.remove(grpId);

                for (Map.Entry<Integer, CheckpointEntry.GroupState> state : chpEntry.groupState(cctx).entrySet()) {
                    int grpId = state.getKey();
                    CheckpointEntry.GroupState cpGrpState = state.getValue();

                    Set<Integer> applicablePartitions = groupsAndPartitions.get(grpId);

                    if (F.isEmpty(applicablePartitions))
                        continue;

                    Set<Integer> inapplicablePartitions = null;

                    for (Integer partId : applicablePartitions) {
                        int pIdx = cpGrpState.indexByPartition(partId);

                        if (pIdx >= 0)
                            res.computeIfAbsent(grpId, k -> new HashMap<>()).put(partId, chpEntry);
                        else {
                            if (inapplicablePartitions == null)
                                inapplicablePartitions = new HashSet<>();

                            // Partition is no more applicable for history search, exclude partition from searching.
                            inapplicablePartitions.add(partId);
                        }
                    }

                    if (!F.isEmpty(inapplicablePartitions))
                        for (Integer partId : inapplicablePartitions)
                            applicablePartitions.remove(partId);
                }

                // Remove groups from search with empty set of applicable partitions.
                for (Map.Entry<Integer, Set<Integer>> e : new HashSet<>(groupsAndPartitions.entrySet()))
                    if (e.getValue().isEmpty())
                        groupsAndPartitions.remove(e.getKey());

                // All groups are no more applicable, release history and stop searching.
                if (groupsAndPartitions.isEmpty()) {
                    cctx.wal().release(chpEntry.checkpointMark());

                    break;
                }
                else {
                    // Release previous checkpoint marker.
                    if (prevReserved != null)
                        cctx.wal().release(prevReserved.checkpointMark());

                    prevReserved = chpEntry;
                }
            }
            catch (IgniteCheckedException ex) {
                U.error(log, "Failed to process checkpoint: " + (chpEntry != null ? chpEntry : "none"), ex);
            }
        }

        return res;
    }

    /**
     * Checkpoint is not applicable when:
     * 1) WAL was disabled somewhere after given checkpoint.
     * 2) Checkpoint doesn't contain specified {@code grpId}.
     *
     * @param grpId Group ID.
     * @param cp Checkpoint.
     */
    private boolean isCheckpointApplicableForGroup(int grpId, CheckpointEntry cp) throws IgniteCheckedException {
        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager) cctx.database();

        if (dbMgr.isCheckpointInapplicableForWalRebalance(cp.timestamp(), grpId))
            return false;

        if (!cp.groupState(cctx).containsKey(grpId))
            return false;

        return true;
    }
}

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE;

/**
 * Checkpoint history. Holds chronological ordered map with {@link CheckpointEntry
 * CheckpointEntries}. Data is loaded from corresponding checkpoint directory. This directory holds files for
 * checkpoint start and end.
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
                U.warn(log, "Could not clear historyMap due to WAL reservation on cpEntry " + cpEntry.checkpointId() +
                    ", history map size is " + histMap.size());

                break;
            }

            histMap.remove(cpEntry.timestamp());

            removed.add(cpEntry);
        }

        return removed;
    }

    /**
     * Clears checkpoint history after checkpoint finish.
     *
     * @return List of checkpoints removed from history.
     */
    public List<CheckpointEntry> onCheckpointFinished(GridCacheDatabaseSharedManager.Checkpoint chp, boolean truncateWal) {
        List<CheckpointEntry> removed = new ArrayList<>();

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

            removed.add(cpEntry);
        }

        chp.walFilesDeleted(deleted);

        if (!chp.hasDelta())
            cctx.wal().allowCompressionUntil(chp.checkpointEntry().checkpointMark());

        return removed;
    }

    /**
     * Tries to search for a WAL pointer for the given partition counter start.
     *
     * @param grpId Cache group ID.
     * @param part Partition ID.
     * @param partCntrSince Partition counter or {@code null} to search for minimal counter.
     * @return Checkpoint entry or {@code null} if failed to search.
     */
    @Nullable public WALPointer searchPartitionCounter(int grpId, int part, @Nullable Long partCntrSince) {
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
    @Nullable public CheckpointEntry searchCheckpointEntry(int grpId, int part, @Nullable Long partCntrSince) {
        boolean hasGap = false;
        CheckpointEntry first = null;

        for (Long cpTs : checkpoints()) {
            try {
                CheckpointEntry entry = entry(cpTs);

                Long foundCntr = entry.partitionCounter(cctx, grpId, part);

                if (foundCntr != null) {
                    if (partCntrSince == null) {
                        if (hasGap) {
                            first = entry;

                            hasGap = false;
                        }

                        if (first == null)
                            first = entry;
                    }
                    else if (foundCntr <= partCntrSince) {
                        first = entry;

                        hasGap = false;
                    }
                    else
                        return hasGap ? null : first;
                }
                else
                    hasGap = true;
            }
            catch (IgniteCheckedException ignore) {
                // Treat exception the same way as a gap.
                hasGap = true;
            }
        }

        return hasGap ? null : first;
    }

    /**
     * Finds and reserves earliest valid checkpoint for each given groups and partitions.
     *
     * @param applicableGroupsAndPartitions Groups and partitions to find and reserve earliest valid checkpoint.
     *
     * @return Map (groupId, Map (partitionId, earliest valid checkpoint to history search)).
     */
    public Map<Integer, Map<Integer, CheckpointEntry>> searchAndReserveCheckpoints(
        final Map<Integer, Set<Integer>> applicableGroupsAndPartitions
    ) {
        if (F.isEmpty(applicableGroupsAndPartitions))
            return Collections.emptyMap();

        final Map<Integer, Map<Integer, CheckpointEntry>> res = new HashMap<>();

        // Iterate over all possible checkpoints starting from latest and moving to earliest.
        for (Long cpTs : checkpoints(true)) {
            CheckpointEntry chpEntry = null;

            try {
                chpEntry = entry(cpTs);

                boolean reserved = cctx.wal().reserve(chpEntry.checkpointMark());

                // If checkpoint WAL history can't be reserved, stop searching.
                if (!reserved)
                    break;

                // If WAL was disabled locally or globally after this checkpoint exclude such group from search.
                for (Integer grpId : applicableGroupsAndPartitions.keySet())
                    if (walWasDisabledForCp(grpId, chpEntry))
                        applicableGroupsAndPartitions.remove(grpId);

                Map<Integer, CheckpointEntry.GroupState> cpGroupStates = chpEntry.groupState(cctx);

                // If group state wasn't persisted, exclude such group from search.
                for (Integer grpId : applicableGroupsAndPartitions.keySet())
                    if (!cpGroupStates.containsKey(grpId))
                        applicableGroupsAndPartitions.remove(grpId);

                // No more applicable groups left, release history and stop searching.
                if (applicableGroupsAndPartitions.isEmpty()) {
                    cctx.wal().release(chpEntry.checkpointMark());

                    break;
                }

                for (Map.Entry<Integer, CheckpointEntry.GroupState> state : cpGroupStates.entrySet()) {
                    int grpId = state.getKey();
                    CheckpointEntry.GroupState cpGroupState = state.getValue();

                    Set<Integer> applicablePartitions = applicableGroupsAndPartitions.get(grpId);

                    if (F.isEmpty(applicablePartitions))
                        continue;

                    for (Integer partId : applicableGroupsAndPartitions.get(grpId)) {
                        int pIdx = cpGroupState.indexByPartition(partId);

                        if (pIdx >= 0)
                            res.computeIfAbsent(grpId, k -> new HashMap<>()).put(partId, chpEntry);
                        else // Partition is no more applicable for history search, exclude
                            applicablePartitions.remove(partId);
                    }
                }
            }
            catch (IgniteCheckedException ex) {
                U.error(log, "Failed to read checkpoint entry: " + (chpEntry != null ? chpEntry : "none"), ex);
            }
        }

        return res;
    }

    public boolean walWasDisabledForCp(int grpId, CheckpointEntry cp) {
        // metastorage read...
        return false;
    }
}

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

import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.CacheState;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_GRP_STATE_LAZY_STORE;

/**
 * Class represents checkpoint state.
 */
public class CheckpointEntry {
    /** Checkpoint timestamp. */
    private final long cpTs;

    /** Checkpoint end mark. */
    private final WALPointer cpMark;

    /** Checkpoint ID. */
    private final UUID cpId;

    /** State of groups and partitions snapshotted at the checkpoint begin. */
    private volatile SoftReference<GroupStateLazyStore> grpStateLazyStore;

    /**
     * Checkpoint entry constructor.
     *
     * If {@code grpStates} is null then it will be inited lazy from wal pointer.
     *
     * @param cpTs Checkpoint timestamp.
     * @param cpMark Checkpoint mark pointer.
     * @param cpId Checkpoint ID.
     * @param cacheGrpStates Cache groups states.
     */
    CheckpointEntry(
        long cpTs,
        WALPointer cpMark,
        UUID cpId,
        @Nullable Map<Integer, CacheState> cacheGrpStates
    ) {
        this.cpTs = cpTs;
        this.cpMark = cpMark;
        this.cpId = cpId;

        if (cacheGrpStates == null)
            this.grpStateLazyStore = new SoftReference<>(new GroupStateLazyStore(null));
        else
            this.grpStateLazyStore = new SoftReference<>(new GroupStateLazyStore(GroupStateLazyStore.remap(cacheGrpStates)));
    }

    /**
     * @return Checkpoint timestamp.
     */
    public long timestamp() {
        return cpTs;
    }

    /**
     * @return Checkpoint ID.
     */
    public UUID checkpointId() {
        return cpId;
    }

    /**
     * @return Checkpoint mark.
     */
    public WALPointer checkpointMark() {
        return cpMark;
    }

    /**
     * @param wal Write ahead log manager.
     * @return Group id -> group state map.
     */
    public Map<Integer, GroupState> groupState(
        IgniteWriteAheadLogManager wal
    ) throws IgniteCheckedException {
        GroupStateLazyStore store = initIfNeeded(wal);

        return store.grpStates;
    }

    /**
     * @param wal Write ahead log manager.
     * @return Group lazy store.
     */
    private GroupStateLazyStore initIfNeeded(IgniteWriteAheadLogManager wal) throws IgniteCheckedException {
        GroupStateLazyStore store = grpStateLazyStore.get();

        if (store == null || IgniteSystemProperties.getBoolean(IGNITE_DISABLE_GRP_STATE_LAZY_STORE, false)) {
            store = new GroupStateLazyStore();

            grpStateLazyStore = new SoftReference<>(store);
        }

        store.initIfNeeded(wal, cpMark);

        return store;
    }

    /**
     * @return Checkpoint entry's group states.
     */
    @Nullable Map<Integer, GroupState> groupStates() {
        GroupStateLazyStore store = grpStateLazyStore.get();

        return store != null ? store.groupStates() : null;
    }

    /**
     * Sets up {@link #grpStateLazyStore} from group states map.
     *
     * @param groupStates Checkpoint entry's group states map.
     */
    void fillStore(Map<Integer, GroupState> groupStates) {
        grpStateLazyStore = new SoftReference<>(new GroupStateLazyStore(groupStates));
    }

    /**
     * @param wal Write ahead log manager.
     * @param grpId Cache group ID.
     * @param part Partition ID.
     * @return Partition counter or partition pending counter. {@code Null} if not found.
     * @throws IgniteCheckedException If something is wrong when loading the counter from WAL history.
     */
    public @Nullable T2<Long, Long> partitionCounter(IgniteWriteAheadLogManager wal, int grpId, int part)
        throws IgniteCheckedException {
        GroupStateLazyStore store = initIfNeeded(wal);

        return store.partitionCounter(grpId, part);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CheckpointEntry [id=" + cpId + ", timestamp=" + cpTs + ", ptr=" + cpMark + "]";
    }

    /**
     *
     */
    public static class GroupState {
        /** Partition ids. */
        private final int[] parts;

        /** Partition counters which corresponds to partition ids. */
        private final long[] cnts;

        /** Pending updates partition counters which corresponds to partition ids. */
        private final long[] pendingCnts;

        /** Next index to insert to parts and cnts. */
        private int idx;

        /**
         * @param partsCnt Partitions count.
         */
        private GroupState(int partsCnt) {
            parts = new int[partsCnt];
            cnts = new long[partsCnt];
            pendingCnts = new long[partsCnt];
        }

        /**
         * @param parts Partitions' ids.
         * @param cnts Partitions' counters.
         * @param pendingCnts Pending updates partitions' counters.
         * @param size Partitions count.
         */
        GroupState(int parts[], long[] cnts, long[] pendingCnts, int size) {
            this.parts = parts;
            this.cnts = cnts;
            this.pendingCnts = pendingCnts;
            this.idx = size;
        }

        /**
         * @return Partitions' ids.
         */
        int[] partitionIds() {
            return parts;
        }

        /**
         * @return Partitions' counters.
         */
        long[] partitionCounters() {
            return cnts;
        }

        /**
         * @return Partitions' pending counters.
         */
        long[] partitionPendingCounters() {
            return pendingCnts;
        }

        /**
         * @param partId Partition ID to add.
         * @param cntr Partition counter.
         * @param pendingCntr Pending updates partition counter.
         */
        public void addPartitionCounter(int partId, long cntr, long pendingCntr) {
            if (idx == parts.length)
                throw new IllegalStateException("Failed to add new partition to the partitions state " +
                    "(no enough space reserved) [partId=" + partId + ", reserved=" + parts.length + ']');

            if (idx > 0) {
                if (parts[idx - 1] >= partId)
                    throw new IllegalStateException("Adding partition in a wrong order [prev=" + parts[idx - 1] +
                        ", cur=" + partId + ']');
            }

            assert pendingCntr >= cntr;

            parts[idx] = partId;

            cnts[idx] = cntr;

            pendingCnts[idx] = pendingCntr;

            idx++;
        }

        /**
         * Gets partition counter by partition ID.
         *
         * @param partId Partition ID.
         * @return Partition update counter (will return {@code -1} if partition is not present in the record).
         * {@code Null} if record not found.
         */
        public @Nullable T2<Long, Long> counterByPartition(int partId) {
            int idx = indexByPartition(partId);

            return idx >= 0 ? new T2<>(cnts[idx], pendingCnts[idx]) : null;
        }

        /**
         * Gets partition pending counter by partition ID.
         *
         * @param partId Partition ID.
         * @return Partition update counter (will return {@code -1} if partition is not present in the record).
         */
        public long pendingCounterByPartition(int partId) {
            int idx = indexByPartition(partId);

            return idx >= 0 ? pendingCnts[idx] : -1;
        }

        /**
         * Return a partition id by an index of this group state. Index was passed through parameter have to be less
         * than size.
         *
         * @param idx Partition index.
         * @return Patition id.
         */
        public int getPartitionByIndex(int idx) {
            return parts[idx];
        }

        /**
         * Return size of this group state.
         *
         * @return Size of an internal indexes array fro this group state.
         */
        public int size() {
            return idx;
        }

        /**
         * @param partId Partition ID to search.
         * @return Non-negative index of partition if found or negative value if not found.
         */
        public int indexByPartition(int partId) {
            return Arrays.binarySearch(parts, 0, idx, partId);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "GroupState [cap=" + parts.length + ", size=" + idx + ']';
        }
    }

    /**
     * Group state lazy store.
     */
    public static class GroupStateLazyStore {
        /** */
        private static final AtomicIntegerFieldUpdater<GroupStateLazyStore> initGuardUpdater =
            AtomicIntegerFieldUpdater.newUpdater(GroupStateLazyStore.class, "initGuard");

        /** Cache states. Initialized lazily. */
        @Nullable private volatile Map<Integer, GroupState> grpStates;

        /** */
        private final CountDownLatch latch;

        /** */
        @SuppressWarnings("unused")
        private volatile int initGuard;

        /** Initialization exception. */
        private IgniteCheckedException initEx;

        /**
         * Default constructor.
         */
        private GroupStateLazyStore() {
            this(null);
        }

        /**
         * Constructor with group state map.
         *
         * @param stateMap Group state map.
         */
        GroupStateLazyStore(@Nullable Map<Integer, GroupState> stateMap) {
            if (stateMap != null) {
                initGuard = 1;

                latch = new CountDownLatch(0);
            }
            else
                latch = new CountDownLatch(1);

            this.grpStates = stateMap;
        }

        /**
         * @param stateRec Cache group state.
         */
        private static Map<Integer, GroupState> remap(@NotNull Map<Integer, CacheState> stateRec) {
            if (stateRec.isEmpty())
                return Collections.emptyMap();

            Map<Integer, GroupState> grpStates = U.newHashMap(stateRec.size());

            for (Integer grpId : stateRec.keySet()) {
                CacheState recState = stateRec.get(grpId);

                GroupState grpState = new GroupState(recState.size());

                for (int i = 0; i < recState.size(); i++) {
                    byte partState = recState.stateByIndex(i);

                    if (GridDhtPartitionState.fromOrdinal(partState) != GridDhtPartitionState.OWNING)
                        continue;

                    grpState.addPartitionCounter(
                        recState.partitionByIndex(i),
                        recState.partitionCounterByIndex(i),
                        recState.partitionPendingCounterByIndex(i)
                    );
                }

                grpStates.put(grpId, grpState);
            }

            return grpStates;
        }

        /**
         * @return Partitions' states by group id.
         */
        @Nullable Map<Integer, GroupState> groupStates() {
            return grpStates;
        }

        /**
         * @param grpId Group id.
         * @param part Partition id.
         * @return Partition counter or partition pending counter. {@code Null} if node found.
         */
        private @Nullable T2<Long, Long> partitionCounter(int grpId, int part) {
            assert initGuard != 0 : initGuard;

            if (initEx != null || grpStates == null)
                return null;

            GroupState state = grpStates.get(grpId);

            if (state != null) {
                T2<Long, Long> cntr = state.counterByPartition(part);

                return cntr == null ? null : cntr;
            }

            return null;
        }

        /**
         * @param wal Write ahead log manager.
         * @param ptr Checkpoint wal pointer.
         * @throws IgniteCheckedException If failed to read WAL entry.
         */
        private void initIfNeeded(
            IgniteWriteAheadLogManager wal,
            WALPointer ptr
        ) throws IgniteCheckedException {
            if (initGuardUpdater.compareAndSet(this, 0, 1)) {
                try (WALIterator it = wal.replay(ptr)) {
                    if (it.hasNextX()) {
                        IgniteBiTuple<WALPointer, WALRecord> tup = it.nextX();

                        CheckpointRecord rec = (CheckpointRecord)tup.get2();

                        Map<Integer, CacheState> stateRec = rec.cacheGroupStates();

                        grpStates = remap(stateRec);
                    }
                    else {
                        throw new IgniteCheckedException(
                            "Failed to find checkpoint record at the given WAL pointer: " + ptr);
                    }
                }
                catch (IgniteCheckedException e) {
                    initEx = e;

                    throw e;
                }
                finally {
                    latch.countDown();
                }
            }
            else {
                U.await(latch);

                if (initEx != null)
                    throw initEx;
            }
        }
    }
}

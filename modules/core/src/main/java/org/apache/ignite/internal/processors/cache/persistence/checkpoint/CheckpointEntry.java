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
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CacheState;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

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
    public CheckpointEntry(
        long cpTs,
        WALPointer cpMark,
        UUID cpId,
        @Nullable Map<Integer, CacheState> cacheGrpStates
    ) {
        this.cpTs = cpTs;
        this.cpMark = cpMark;
        this.cpId = cpId;
        this.grpStateLazyStore = new SoftReference<>(new GroupStateLazyStore(cacheGrpStates));
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
     * @param cctx Cache shred context.
     */
    public Map<Integer, GroupState> groupState(
        GridCacheSharedContext cctx
    ) throws IgniteCheckedException {
        GroupStateLazyStore store = initIfNeeded(cctx);

        return store.grpStates;
    }

    /**
     * @param cctx Cache shred context.
     * @return Group lazy store.
     */
    private GroupStateLazyStore initIfNeeded(GridCacheSharedContext cctx) throws IgniteCheckedException {
        GroupStateLazyStore store = grpStateLazyStore.get();

        if (store == null) {
            store = new GroupStateLazyStore();

            grpStateLazyStore = new SoftReference<>(store);
        }

        store.initIfNeeded(cctx, cpMark);

        return store;
    }

    /**
     * @param cctx Cache shared context.
     * @param grpId Cache group ID.
     * @param part Partition ID.
     * @return Partition counter or {@code null} if not found.
     */
    public Long partitionCounter(GridCacheSharedContext cctx, int grpId, int part) {
        GroupStateLazyStore store;

        try {
            store = initIfNeeded(cctx);
        }
        catch (IgniteCheckedException e) {
            return null;
        }

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
        /** */
        private int[] parts;

        /** */
        private long[] cnts;

        /** */
        private int idx;

        /**
         * @param partsCnt Partitions count.
         */
        private GroupState(int partsCnt) {
            parts = new int[partsCnt];
            cnts = new long[partsCnt];
        }

        /**
         * @param partId Partition ID to add.
         * @param cntr Partition counter.
         */
        public void addPartitionCounter(int partId, long cntr) {
            if (idx == parts.length)
                throw new IllegalStateException("Failed to add new partition to the partitions state " +
                    "(no enough space reserved) [partId=" + partId + ", reserved=" + parts.length + ']');

            if (idx > 0) {
                if (parts[idx - 1] >= partId)
                    throw new IllegalStateException("Adding partition in a wrong order [prev=" + parts[idx - 1] +
                        ", cur=" + partId + ']');
            }

            parts[idx] = partId;

            cnts[idx] = cntr;

            idx++;
        }

        /**
         * Gets partition counter by partition ID.
         *
         * @param partId Partition ID.
         * @return Partition update counter (will return {@code -1} if partition is not present in the record).
         */
        public long counterByPartition(int partId) {
            int idx = indexByPartition(partId);

            return idx >= 0 ? cnts[idx] : -1;
        }

        /**
         * Return a partition id by an index of this group state.
         * Index was passed through parameter have to be less than size.
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
     *  Group state lazy store.
     */
    public static class GroupStateLazyStore {
        /** */
        private static final AtomicIntegerFieldUpdater<GroupStateLazyStore> initGuardUpdater =
            AtomicIntegerFieldUpdater.newUpdater(GroupStateLazyStore.class, "initGuard");

        /** Cache states. Initialized lazily. */
        private volatile Map<Integer, GroupState> grpStates;

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
         * @param cacheGrpStates Cache group state.
         */
        private GroupStateLazyStore(Map<Integer, CacheState> cacheGrpStates) {
            if (cacheGrpStates != null) {
                initGuard = 1;

                latch = new CountDownLatch(0);
            }
            else
                latch = new CountDownLatch(1);

            grpStates = remap(cacheGrpStates);
        }

        /**
         * @param stateRec Cache group state.
         */
        private Map<Integer, GroupState> remap(Map<Integer, CacheState> stateRec) {
            if (stateRec == null)
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
                        recState.partitionCounterByIndex(i)
                    );
                }

                grpStates.put(grpId, grpState);
            }

            return grpStates;
        }

        /**
         * @param grpId Group id.
         * @param part Partition id.
         * @return Partition counter.
         */
        private Long partitionCounter(int grpId, int part) {
            assert initGuard != 0 : initGuard;

            if (initEx != null || grpStates == null)
                return null;

            GroupState state = grpStates.get(grpId);

            if (state != null) {
                long cntr = state.counterByPartition(part);

                return cntr < 0 ? null : cntr;
            }

            return null;
        }

        /**
         * @param cctx Cache shared context.
         * @param ptr Checkpoint wal pointer.
         * @throws IgniteCheckedException If failed to read WAL entry.
         */
        private void initIfNeeded(
            GridCacheSharedContext cctx,
            WALPointer ptr
        ) throws IgniteCheckedException {
            if (initGuardUpdater.compareAndSet(this, 0, 1)) {
                try (WALIterator it = cctx.wal().replay(ptr)) {
                    if (it.hasNextX()) {
                        IgniteBiTuple<WALPointer, WALRecord> tup = it.nextX();

                        CheckpointRecord rec = (CheckpointRecord)tup.get2();

                        Map<Integer, CacheState> stateRec = rec.cacheGroupStates();

                        grpStates = remap(stateRec);
                    }
                    else
                        initEx = new IgniteCheckedException(
                            "Failed to find checkpoint record at the given WAL pointer: " + ptr);
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

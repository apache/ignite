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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObjectInput;
import org.apache.ignite.internal.dto.IgniteDataTransferObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Earliest checkpoint map snapshot.
 * Speeds up construction of the earliestCp map in the {@link CheckpointHistory}.
 *
 * <b>The snapshot is stored on disk ({@link CheckpointMarkersStorage#EARLIEST_CP_SNAPSHOT_FILE}),
 * so its binary format must stay readable across versions.</b>
 */
public class EarliestCheckpointMapSnapshot implements Externalizable {
    /** Serial version UUID. */
    private static final long serialVersionUID = 0L;

    /** Header magic number {@code IgniteDataTransferObject#MAGIC}. */
    private static final int MAGIC = 0x42BEEF00;

    /** Last snapshot's checkpoint timestamp. */
    private Map</*Checkpoint id */ UUID, Map</* Group id */ Integer, GroupStateSnapshot>> data = new HashMap<>();

    /** Ids of checkpoints present at the time of the snapshot capture. */
    private Set<UUID> checkpointIds;

    /** Constructor. */
    public EarliestCheckpointMapSnapshot(
        Set<UUID> checkpointIds,
        Map<UUID, Map<Integer, GroupStateSnapshot>> earliestCp
    ) {
        this.checkpointIds = checkpointIds;
        this.data = earliestCp;
    }

    /** Default constructor. */
    public EarliestCheckpointMapSnapshot() {
        checkpointIds = new HashSet<>();
    }

    /**
     * Gets a group state by a checkpoint id.
     *
     * @param checkpointId Checkpoint id.
     * @return Group state.
     */
    @Nullable public Map<Integer, CheckpointEntry.GroupState> groupState(UUID checkpointId) {
        Map<Integer, GroupStateSnapshot> grpStateSnapshotMap = data.get(checkpointId);

        Map<Integer, CheckpointEntry.GroupState> grpStateMap = null;

        if (grpStateSnapshotMap != null) {
            grpStateMap = new HashMap<>();

            for (Map.Entry<Integer, GroupStateSnapshot> e : grpStateSnapshotMap.entrySet()) {
                Integer k = e.getKey();
                GroupStateSnapshot v = e.getValue();

                grpStateMap.put(k, new CheckpointEntry.GroupState(
                    v.partitionIds(),
                    v.partitionCounters(),
                    v.size()
                ));
            }

        }
        return grpStateMap;
    }

    /**
     * Returns {@code true} if a checkpoint was present during the snapshot capture, {@code false} otherwise.
     *
     * @param checkpointId Checkpoint id.
     * @return {@code true} if checkpoint was present, {@code false} otherwise.
     */
    public boolean checkpointWasPresent(UUID checkpointId) {
        return checkpointIds.contains(checkpointId);
    }

    /**
     * <b>The snapshot is stored on disk ({@link CheckpointMarkersStorage#EARLIEST_CP_SNAPSHOT_FILE}),
     * so its binary format must stay readable across versions.</b>
     */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(MAGIC);

        try (IgniteDataTransferObjectOutput dtout = new IgniteDataTransferObjectOutput(out)) {
            U.writeMap(dtout, data);
            U.writeCollection(dtout, checkpointIds);
        }
    }

    /**
     * <b>The snapshot is stored on disk ({@link CheckpointMarkersStorage#EARLIEST_CP_SNAPSHOT_FILE}),
     * so its binary format must stay readable across versions.</b>
     */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        readHeader(in);

        try (IgniteDataTransferObjectInput dtin = new IgniteDataTransferObjectInput(in)) {
            data = U.readMap(dtin);
            checkpointIds = U.readSet(dtin);
        }
    }

    /** */
    private static void readHeader(ObjectInput in) throws IOException {
        int hdr = in.readInt();

        if ((hdr & MAGIC) != MAGIC) {
            throw new IOException("Unexpected earliest checkpoint map snapshot header " +
                "[actual=" + Integer.toHexString(hdr) + ", expected=" + Integer.toHexString(MAGIC) + "]");
        }
    }

    /** {@link CheckpointEntry.GroupState} snapshot. */
    static class GroupStateSnapshot implements Externalizable {
        /** Serial version UUID. */
        private static final long serialVersionUID = 0L;

        /** Partition ids. */
        private int[] parts;

        /** Partition counters which corresponds to partition ids. */
        private long[] cnts;

        /** Partitions count. */
        private int size;

        /**
         * @param parts Partitions' ids.
         * @param cnts Partitions' counters.
         * @param size Partitions count.
         */
        GroupStateSnapshot(int[] parts, long[] cnts, int size) {
            this.parts = parts;
            this.cnts = cnts;
            this.size = size;
        }

        /**
         * Constructor for serialization.
         */
        public GroupStateSnapshot() {
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
         * @return Partitions count.
         */
        public int size() {
            return size;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(MAGIC);

            try (IgniteDataTransferObjectOutput dtout = new IgniteDataTransferObjectOutput(out)) {
                U.writeIntArray(dtout, parts);
                U.writeLongArray(dtout, cnts);
                dtout.writeInt(size);
            }
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            readHeader(in);

            try (IgniteDataTransferObjectInput dtin = new IgniteDataTransferObjectInput(in)) {
                parts = U.readIntArray(dtin);
                cnts = U.readLongArray(dtin);
                size = dtin.readInt();
            }
        }
    }
}

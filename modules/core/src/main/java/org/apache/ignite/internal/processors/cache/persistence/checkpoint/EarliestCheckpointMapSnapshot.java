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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.jetbrains.annotations.Nullable;

/**
 * Earliest checkpoint map snapshot.
 * Speeds up construction of the earliestCp map in the {@link CheckpointHistory}.
 */
public class EarliestCheckpointMapSnapshot extends IgniteDataTransferObject {
    /** Serial version UUID. */
    private static final long serialVersionUID = 0L;

    /** Last snapshot's checkpoint timestamp. */
    @Order(0)
    Map</*Checkpoint id */ UUID, Map</* Group id */ Integer, GroupStateSnapshot>> data = new HashMap<>();

    /** Ids of checkpoints present at the time of the snapshot capture. */
    @Order(1)
    Set<UUID> checkpointIds;

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

    /** {@link CheckpointEntry.GroupState} snapshot. */
    public static class GroupStateSnapshot extends IgniteDataTransferObject {
        /** Serial version UUID. */
        private static final long serialVersionUID = 0L;

        /** Partition ids. */
        @Order(0)
        int[] parts;

        /** Partition counters which corresponds to partition ids. */
        @Order(1)
        long[] cnts;

        /** Partitions count. */
        @Order(2)
        int size;

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
    }
}

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

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.jetbrains.annotations.Nullable;

/** Per-node result of the snapshot lists command. Contains information of the snapshots found on current node. */
public final class SnapshotListJobResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Local node's snapshot descriptions. */
    @Order(0)
    SnapshotInfo[] snapshots;

    /** Default constructor for serialization purposes. */
    public SnapshotListJobResult() {
        // No-op.
    }

    /** */
    public SnapshotListJobResult(SnapshotInfo[] snapshots) {
        this.snapshots = snapshots;
    }

    /** @return The snapshots main information (names, sizes, etc.). */
    public SnapshotInfo[] snapshots() {
        return snapshots;
    }

    /** Hold combined snapshot data information: name, size, creation/modification time, incremental parts, external storages. */
    public static class SnapshotInfo extends IgniteDataTransferObject {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Snapshot name. Is {@code null} for its external storages or incremental parts. */
        @Order(0)
        @Nullable String name;

        /** Total size of a snapshot. Or size of its external storages or incremental parts. */
        @Order(1)
        long size;

        /**
         * Creation or modification date of snapshot or of its incremental parts.
         * Is {@code null} for snapshot external storages' data.
         */
        @Order(2)
        @Nullable Long date;

        /** Snapshot external storages' information. Is {@code null} for not the main snapshot data. */
        @Order(3)
        @Nullable SnapshotInfo extStors;

        /** Snapshot incremental parts information. Is {@code null} for not the main snapshot data. */
        @Order(4)
        @Nullable SnapshotInfo incs;

        /** Number of snapshot external storages or incremental parts. Is {@code null} for the main snapshot data. */
        @Order(5)
        @Nullable Integer cnt;

        /** Empty constructor for serialization purposes. */
        public SnapshotInfo() {
            // No-op.
        }

        /** Creates snapshot main information. */
        public SnapshotInfo(
            String name,
            long size,
            long date,
            @Nullable SnapshotInfo extStors,
            @Nullable SnapshotInfo incs
        ) {
            this.name = name;
            this.size = size + (extStors == null ? 0L : extStors.size());
            this.date = date;
            this.extStors = extStors;
            this.incs = incs;
        }

        /** Creates external storages' information. */
        public SnapshotInfo(int cnt, long size) {
            this.cnt = cnt;
            this.size = size;
        }

        /** Creates incremental parts information. */
        public SnapshotInfo(int cnt, long size, long date) {
            this.cnt = cnt;
            this.size = size;
            this.date = date;
        }

        /** @return Snapshot name. Or {@code null} for its external storages or incremental parts. */
        public @Nullable String name() {
            return name;
        }

        /** @return Total size of a snapshot. Or size of its external storages or incremental parts. */
        public long size() {
            return size;
        }

        /**
         * @return Creation date of a snapshot or of its incremental parts. Or {@code null} for snapshot
         * external storages' data.
         */
        public @Nullable Long date() {
            return date;
        }

        /** @return Snapshot external storages' information. Is {@code null} for not the main snapshot information. */
        public @Nullable SnapshotInfo externalStorages() {
            return extStors;
        }

        /** @return Snapshot incremental parts information. Is {@code null} for not the main snapshot information. */
        public @Nullable SnapshotInfo incrementals() {
            return incs;
        }

        /** @return Number of snapshot external storages or incremental parts. Or {@code null} for the main snapshot data.*/
        public @Nullable Integer number() {
            return cnt;
        }
    }
}

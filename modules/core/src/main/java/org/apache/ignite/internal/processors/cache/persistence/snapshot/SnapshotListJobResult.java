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

import java.util.Map;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.jetbrains.annotations.Nullable;

/** Per-node result of the snapshot list task. Contains information about the snapshots found on the node. */
public final class SnapshotListJobResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Local node's snapshot descriptions. */
    @Order(0)
    Map<String, SnapshotInfo> snapshots;

    /** Default constructor for serialization purposes. */
    public SnapshotListJobResult() {
        // No-op.
    }

    /** */
    public SnapshotListJobResult(Map<String, SnapshotInfo> snapshots) {
        this.snapshots = snapshots;
    }

    /** @return Snapshot descriptions by snapshot name. */
    public Map<String, SnapshotInfo> snapshots() {
        return snapshots;
    }

    /** Holds information about a snapshot or its parts: size, creation time, number of incremental parts or external storages. */
    public static class SnapshotInfo extends IgniteDataTransferObject {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Total size of a snapshot. Or size of its external storages or incremental parts. */
        @Order(0)
        long size;

        /** Creation date of the snapshot or of its incremental parts. */
        @Order(1)
        @Nullable Long date;

        /** Snapshot external storages description if it exists. */
        @Order(2)
        @Nullable SnapshotInfo extStors;

        /** Snapshot incremental parts description if it exists. */
        @Order(3)
        @Nullable SnapshotInfo incs;

        /** Number of snapshot external storages or incremental parts. */
        @Order(4)
        @Nullable Integer cnt;

        /** Empty constructor for serialization purposes. */
        public SnapshotInfo() {
            // No-op.
        }

        /** Creates the snapshot main description. */
        public SnapshotInfo(
            long mainSize,
            long date,
            @Nullable SnapshotInfo extStors,
            @Nullable SnapshotInfo incs
        ) {
            // Incremental snapshots are inside the main snapshot directory, so their size is already counted in the main size.
            size = mainSize + (extStors == null ? 0L : extStors.size());
            this.date = date;
            this.extStors = extStors;
            this.incs = incs;
        }

        /** Creates the description of snapshot external storages. */
        public SnapshotInfo(int cnt, long size) {
            this.cnt = cnt;
            this.size = size;
        }

        /** Creates the description of incremental snapshot parts. */
        public SnapshotInfo(int cnt, long size, long date) {
            this(cnt, size);

            this.date = date;
        }

        /** @return Total size of a snapshot. Or size of its external storages or incremental parts. */
        public long size() {
            return size;
        }

        /**
         * @return Creation date of a snapshot or of its incremental parts. {@code Null} for snapshot
         * external storages description.
         */
        public @Nullable Long date() {
            return date;
        }

        /**
         * @return Snapshot external storages description if it exists. Is always {@code null} for anything other than
         * the main snapshot description.
         */
        public @Nullable SnapshotInfo externalStorages() {
            return extStors;
        }

        /**
         * @return Snapshot incremental parts description if it exists. Is always {@code null} for anything other than
         * the main snapshot description.
         */
        public @Nullable SnapshotInfo incrementals() {
            return incs;
        }

        /** @return Number of snapshot external storages or incremental parts. Or {@code null} for the main snapshot data. */
        public @Nullable Integer number() {
            return cnt;
        }
    }
}

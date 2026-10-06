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

    /** Snapshot main information (names, sizes, etc.). */
    @Order(0)
    SnapshotInfo[] snapshots;

    /** Optional information of external snapshots storages. */
    @Order(1)
    SnapshotInfo[] extStorages;

    /** Optional information of incrementals snapshots. */
    @Order(2)
    SnapshotInfo[] incrementalSnps;

    /** Default constructor for serialization purposes. */
    public SnapshotListJobResult() {
        // No-op.
    }

    /** */
    public SnapshotListJobResult(
        SnapshotInfo[] snapshots,
        @Nullable SnapshotInfo[] extStorages,
        @Nullable SnapshotInfo[] incrementalSnps
    ) {
        this.snapshots = snapshots;
        this.extStorages = extStorages;
        this.incrementalSnps = incrementalSnps;
    }

    /** @return The snapshots main information (names, sizes, etc.). */
    public @Nullable SnapshotInfo[] snapshots() {
        return snapshots;
    }

    /** @return Optional information of external snapshots storages. */
    public @Nullable SnapshotInfo[] externalStorages() {
        return extStorages;
    }

    /** @return Optional information of incrementals snapshots. */
    public @Nullable SnapshotInfo[] incrementalSnapshots() {
        return incrementalSnps;
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

        /** Number of snapshot external storages or incremental parts. Is {@code null} for the main snapshot data. */
        @Order(2)
        @Nullable Integer cnt;

        /**
         * Creation or modification date of snapshot or of its incremental parts.
         * Is {@code null} for snapshot external storages' data.
         */
        @Order(3)
        @Nullable Long date;

        /** Empty constructor for serialization purposes. */
        public SnapshotInfo() {
            // No-op.
        }

        /** Creates snapshot main information. */
        public SnapshotInfo(String name, long size, long date) {
            this.name = name;
            this.size = size;
            this.date = date;
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
         * @return Creation or modification date of a snapshot or of its incremental parts. Or {@code null} for snapshot
         * external storages' data.
         */
        public @Nullable Long date() {
            return date;
        }

        /** @return Number of snapshot external storages or incremental parts. Or {@code null} for the main snapshot data.*/
        public @Nullable Integer number() {
            return cnt;
        }
    }
}

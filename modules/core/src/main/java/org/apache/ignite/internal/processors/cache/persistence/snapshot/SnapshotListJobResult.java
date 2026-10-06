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
import org.apache.ignite.internal.management.snapshot.SnapshotListTask;
import org.jetbrains.annotations.Nullable;

/** Accumulated result of {@link SnapshotListTask}. */
public final class SnapshotListJobResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Snapshot names. */
    @Order(0)
    String[] snpName;

    /** Snapshot sizes. */
    @Order(1)
    long[] size;

    /** Creation times. */
    @Order(2)
    long[] creationTime;

    /** Numbers of related incremental snapshots. */
    @Order(3)
    int[] incCnt;

    /** Total sizes of incremental snapshots. */
    @Order(4)
    long[] incSize;

    /** Last modified times. Actual if {@link #incCnt}[n] > 0. */
    @Order(5)
    long[] editTime;

    /** Total sizes of external-storage snapshots. */
    @Order(6)
    ExtraRecordsData[] extStorages;

    /** Default constructor for serialization purposes. */
    public SnapshotListJobResult() {
        // No-op.
    }

    /** */
    public SnapshotListJobResult(
        String[] name,
        long[] size,
        long[] createTime,
        @Nullable int[] incCnt,
        @Nullable long[] incSize,
        @Nullable long[] editTime,
        @Nullable ExtraRecordsData[] extStorages
    ) {
        snpName = name;
        this.size = size;
        creationTime = createTime;

        this.incCnt = incCnt;
        this.incSize = incSize;
        this.editTime = editTime;

        this.extStorages = extStorages;
    }

    /** */
    public String[] snapshotNames() {
        return snpName;
    }

    /** */
    public long[] sizes() {
        return size;
    }

    /** */
    public long[] creationTimes() {
        return creationTime;
    }

    /** */
    public @Nullable int[] incrementalsCount() {
        return incCnt;
    }

    /** */
    public @Nullable long[] incrementalsSizes() {
        return incSize;
    }

    /** */
    public @Nullable long[] editTimes() {
        return editTime;
    }

    /** */
    public @Nullable ExtraRecordsData[] externalStorages() {
        return extStorages;
    }

    /** Additional, optional snapshot data like about incremental snapshots or external storages. */
    public static class ExtraRecordsData extends IgniteDataTransferObject {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Number of related records or storages. */
        @Order(0)
        int cnt;

        /** Total size of related records or storages. */
        @Order(1)
        long size;

        /** Optional creation date of related records or storages. */
        @Order(2)
        @Nullable Long date;

        /** Empty constructor for serialization purposes. */
        public ExtraRecordsData() {
            // No-op.
        }

        /** */
        private ExtraRecordsData(int cnt, long size, @Nullable Long date) {
            this.cnt = cnt;
            this.size = size;
            this.date = date;
        }

        /** */
        public ExtraRecordsData(int cnt, long size) {
            this(cnt, size, null);
        }

        /** @return Number of related records or storages. */
        public int number() {
            return cnt;
        }

        /** @return Total size of related records or storages. */
        public long size() {
            return size;
        }

        /** @return Optional creation date of related records or storages. */
        public @Nullable Long date() {
            return date;
        }
    }
}

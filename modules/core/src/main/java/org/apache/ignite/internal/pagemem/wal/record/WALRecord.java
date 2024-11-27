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

package org.apache.ignite.internal.pagemem.wal.record;

import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointStatus;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordPurpose.CUSTOM;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordPurpose.INTERNAL;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordPurpose.LOGICAL;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordPurpose.MIXED;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordPurpose.PHYSICAL;

/**
 * Log entry abstract class.
 */
public abstract class WALRecord {
    /**
     * Record type. Ordinal of this record will be written to file. <br>
     * <b>Note:</b> Do not change order of elements <br>
     */
    public enum RecordType {
        /** */
        TX_RECORD(0, LOGICAL),

        /** */
        PAGE_RECORD(1, PHYSICAL),

        /** @deprecated Use {@link #DATA_RECORD_V2} instead. */
        @Deprecated
        DATA_RECORD(2, LOGICAL),

        /** Checkpoint (begin) record */
        CHECKPOINT_RECORD(3, PHYSICAL),

        /** WAL segment header record. */
        HEADER_RECORD(4, INTERNAL),

        // Delta records.

        /** */
        INIT_NEW_PAGE_RECORD(5, PHYSICAL),

        /** */
        DATA_PAGE_INSERT_RECORD(6, PHYSICAL),

        /** */
        DATA_PAGE_INSERT_FRAGMENT_RECORD(7, PHYSICAL),

        /** */
        DATA_PAGE_REMOVE_RECORD(8, PHYSICAL),

        /** */
        DATA_PAGE_SET_FREE_LIST_PAGE(9, PHYSICAL),

        /** */
        BTREE_META_PAGE_INIT_ROOT(10, PHYSICAL),

        /** */
        BTREE_META_PAGE_ADD_ROOT(11, PHYSICAL),

        /** */
        BTREE_META_PAGE_CUT_ROOT(12, PHYSICAL),

        /** */
        BTREE_INIT_NEW_ROOT(13, PHYSICAL),

        /** */
        BTREE_PAGE_RECYCLE(14, PHYSICAL),

        /** */
        BTREE_PAGE_INSERT(15, PHYSICAL),

        /** */
        BTREE_FIX_LEFTMOST_CHILD(16, PHYSICAL),

        /** */
        BTREE_FIX_COUNT(17, PHYSICAL),

        /** */
        BTREE_PAGE_REPLACE(18, PHYSICAL),

        /** */
        BTREE_PAGE_REMOVE(19, PHYSICAL),

        /** */
        BTREE_PAGE_INNER_REPLACE(20, PHYSICAL),

        /** */
        BTREE_FIX_REMOVE_ID(21, PHYSICAL),

        /** */
        BTREE_FORWARD_PAGE_SPLIT(22, PHYSICAL),

        /** */
        BTREE_EXISTING_PAGE_SPLIT(23, PHYSICAL),

        /** */
        BTREE_PAGE_MERGE(24, PHYSICAL),

        /** */
        PAGES_LIST_SET_NEXT(25, PHYSICAL),

        /** */
        PAGES_LIST_SET_PREVIOUS(26, PHYSICAL),

        /** */
        PAGES_LIST_INIT_NEW_PAGE(27, PHYSICAL),

        /** */
        PAGES_LIST_ADD_PAGE(28, PHYSICAL),

        /** */
        PAGES_LIST_REMOVE_PAGE(29, PHYSICAL),

        /** */
        META_PAGE_INIT(30, PHYSICAL),

        /** */
        PARTITION_META_PAGE_UPDATE_COUNTERS(31, PHYSICAL),

        /** Memory recovering start marker */
        MEMORY_RECOVERY(32),

        /** */
        TRACKING_PAGE_DELTA(33, PHYSICAL),

        /** Meta page update last successful snapshot id. */
        META_PAGE_UPDATE_LAST_SUCCESSFUL_SNAPSHOT_ID(34, MIXED),

        /** Meta page update last successful full snapshot id. */
        META_PAGE_UPDATE_LAST_SUCCESSFUL_FULL_SNAPSHOT_ID(35, MIXED),

        /** Meta page update next snapshot id. */
        META_PAGE_UPDATE_NEXT_SNAPSHOT_ID(36, MIXED),

        /** Meta page update last allocated index. */
        META_PAGE_UPDATE_LAST_ALLOCATED_INDEX(37, MIXED),

        /** Partition meta update state. */
        PART_META_UPDATE_STATE(38, MIXED),

        /** Page list meta reset count record. */
        PAGE_LIST_META_RESET_COUNT_RECORD(39, PHYSICAL),

        /** Switch segment record.
         *  Marker record for indicate end of segment.
         *  If the next one record is written down exactly at the end of segment,
         *  SWITCH_SEGMENT_RECORD will not be written, if not then it means that we have more
         *  that one byte in the end,then we write SWITCH_SEGMENT_RECORD as marker end of segment.
         *  No need write CRC or WAL pointer for this record. It is byte marker record.
         *  */
        SWITCH_SEGMENT_RECORD(40, INTERNAL),

        /** */
        DATA_PAGE_UPDATE_RECORD(41, PHYSICAL),

        /** init */
        BTREE_META_PAGE_INIT_ROOT2(42, PHYSICAL),

        /** Partition destroy. */
        PARTITION_DESTROY(43, PHYSICAL),

        /** Snapshot record. */
        SNAPSHOT(44),

        /** Metastore data record. */
        METASTORE_DATA_RECORD(45, LOGICAL),

        /** Exchange record. */
        EXCHANGE(46),

        /** Reserved for future record. */
        RESERVED(47),

        /** Rotated id part record. */
        ROTATED_ID_PART_RECORD(48, PHYSICAL),

        /** */
        @Deprecated
        MVCC_DATA_PAGE_MARK_UPDATED_RECORD(49, PHYSICAL),

        /** */
        @Deprecated
        MVCC_DATA_PAGE_TX_STATE_HINT_UPDATED_RECORD(50, PHYSICAL),

        /** */
        @Deprecated
        MVCC_DATA_PAGE_NEW_TX_STATE_HINT_UPDATED_RECORD(51, PHYSICAL),

        /** Encrypted WAL-record. */
        ENCRYPTED_RECORD(52, MIXED),

        /**
         * Ecnrypted data record.
         * @deprecated Use {@link #ENCRYPTED_DATA_RECORD_V3} instead.
         */
        @Deprecated
        ENCRYPTED_DATA_RECORD(53, LOGICAL),

        /** Mvcc data record. */
        @Deprecated
        MVCC_DATA_RECORD(54, LOGICAL),

        /** Mvcc Tx state change record. */
        @Deprecated
        MVCC_TX_RECORD(55, LOGICAL),

        /** Consistent cut record. */
        CONSISTENT_CUT(56),

        /** Rollback tx record. */
        ROLLBACK_TX_RECORD(57, LOGICAL),

        /** Partition meta page containing update counter gaps. */
        PARTITION_META_PAGE_UPDATE_COUNTERS_V2(58, PHYSICAL),

        /** Init root meta page (with flags and created version) */
        BTREE_META_PAGE_INIT_ROOT_V3(59, PHYSICAL),

        /** Master key change record. */
        MASTER_KEY_CHANGE_RECORD(60, LOGICAL),

        /** Record that indicates that "corrupted" flag should be removed from tracking page. */
        TRACKING_PAGE_REPAIR_DELTA(61, PHYSICAL),

        /** Out-of-order update which is used by atomic caches on backup nodes. (Placeholder) */
        OUT_OF_ORDER_UPDATE(62, LOGICAL),

        /** Encrypted WAL-record. */
        ENCRYPTED_RECORD_V2(63, MIXED),

        /**
         * Ecnrypted data record.
         * @deprecated Use {@link #ENCRYPTED_DATA_RECORD_V3} instead.
         */
        @Deprecated
        ENCRYPTED_DATA_RECORD_V2(64, LOGICAL),

        /** Master key change record containing multiple keys for single cache group. */
        MASTER_KEY_CHANGE_RECORD_V2(65, LOGICAL),

        /** Logical record to restart reencryption with the latest encryption key. */
        REENCRYPTION_START_RECORD(66, LOGICAL),

        /** Partition meta page delta record includes encryption status data. */
        PARTITION_META_PAGE_DELTA_RECORD_V3(67, PHYSICAL),

        /** Index meta page delta record includes encryption status data. */
        INDEX_META_PAGE_DELTA_RECORD(68, PHYSICAL),

        /** IGNITE-11704 placeholder: Partition meta page delta record includes tombstones count. */
        PARTITION_META_PAGE_DELTA_RECORD_V4(69, PHYSICAL),

        /** Data record V2. */
        DATA_RECORD_V2(70, LOGICAL),

        /** Ecnrypted data record. */
        ENCRYPTED_DATA_RECORD_V3(71, LOGICAL),

        /** Record for renaming the index root pages. */
        INDEX_ROOT_PAGE_RENAME_RECORD(72, LOGICAL),

        /** Partition clearing start. */
        PARTITION_CLEARING_START_RECORD(73, LOGICAL),

        /** Ecnrypted out-of-order update which is used by atomic caches on backup nodes. (Placeholder) */
        ENCRYPTED_OUT_OF_ORDER_UPDATE(74, LOGICAL),

        /** ClusterSnapshot start. */
        CLUSTER_SNAPSHOT(75, LOGICAL),

        /** Incremental snapshot start record. */
        INCREMENTAL_SNAPSHOT_START_RECORD(76, LOGICAL),

        /** Incremental snapshot finish record. */
        INCREMENTAL_SNAPSHOT_FINISH_RECORD(77, LOGICAL),

        /** CDC data record. */
        CDC_DATA_RECORD(78, CUSTOM),

        /** CDC manager record. */
        CDC_MANAGER_RECORD(79, CUSTOM),

        /** CDC manager record. */
        CDC_MANAGER_STOP_RECORD(80, CUSTOM),

        /** Physical WAL record that represents a fragment of an entry update. (Placeholder) */
        DATA_PAGE_FRAGMENTED_UPDATE_RECORD(81, PHYSICAL),

        /** Reserved for further improvements. */
        RESERVED_IDX2(82);

        /** Index for serialization. Should be consistent throughout all versions. */
        private final int idx;

        /**
         * When you're adding a new record don't forget to choose record purpose explicitly
         * if record is needed for physical or logical recovery.
         * By default the purpose of record is {@link RecordPurpose#CUSTOM} and this record will not be used in recovery process.
         * For more information read description of {@link RecordPurpose}.
         */
        private final RecordPurpose purpose;

        /**
         * @param idx Index for serialization.
         * @param purpose Purpose.
         */
        RecordType(int idx, RecordPurpose purpose) {
            assert idx >= 0 : idx;

            this.idx = idx;
            this.purpose = purpose;
        }

        /**
         * @param idx Index for serialization.
         */
        RecordType(int idx) {
            this(idx, CUSTOM);
        }

        /**
         * @return Index for serialization.
         */
        public int index() {
            return idx;
        }

        /**
         * @return Purpose of record.
         */
        public RecordPurpose purpose() {
            return purpose;
        }

        /** */
        private static final RecordType[] VALS;

        static {
            RecordType[] recordTypes = RecordType.values();

            int maxIdx = 0;
            for (RecordType recordType : recordTypes)
                maxIdx = Math.max(maxIdx, recordType.idx);

            VALS = new RecordType[maxIdx + 1];

            for (RecordType recordType : recordTypes)
                VALS[recordType.idx] = recordType;
        }

        /** */
        public static RecordType fromIndex(int idx) {
            return idx < 0 || idx >= VALS.length ? null : VALS[idx];
        }

        /**
         * Fake record type, causes stop iterating and indicates segment EOF
         * <b>Note:</b> regular record type is incremented by 1 and minimal value written to file is also 1
         * For {@link WALMode#FSYNC} this value is at least came from padding
         */
        public static final int STOP_ITERATION_RECORD_TYPE = 0;
    }

    /**
     * Record purposes set.
     */
    public enum RecordPurpose {
        /**
         * Internal records are needed for correct iterating over WAL structure.
         * These records will never be returned to user during WAL iteration.
         */
        INTERNAL,
        /**
         * Physical records are needed for correct recovering physical state of {@link org.apache.ignite.internal.pagemem.PageMemory}.
         * {@link org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager#restoreBinaryMemory(
         * org.apache.ignite.lang.IgnitePredicate, org.apache.ignite.lang.IgniteBiPredicate)}.
         */
        PHYSICAL,
        /**
         * Logical records are needed to replay logical updates since last checkpoint.
         * {@link GridCacheDatabaseSharedManager#applyLogicalUpdates(CheckpointStatus, org.apache.ignite.lang.IgnitePredicate,
         * org.apache.ignite.lang.IgniteBiPredicate, boolean)}
         */
        LOGICAL,
        /**
         * Physical-logical records are used both for physical and logical recovery.
         * Usually these records contain meta-information about partitions.
         * NOTE: Not recommend to use this type without strong reason.
         */
        MIXED,
        /**
         * Custom records are needed for any custom iterations over WAL in various components.
         */
        CUSTOM
    }

    /** */
    private int size;

    /** */
    private int chainSize;

    /** */
    @GridToStringExclude
    private WALRecord prev;

    /** */
    private WALPointer pos;

    /**
     * @param chainSize Chain size in bytes.
     */
    public void chainSize(int chainSize) {
        this.chainSize = chainSize;
    }

    /**
     * @return Get chain size in bytes.
     */
    public int chainSize() {
        return chainSize;
    }

    /**
     * @return Previous record in chain.
     */
    public WALRecord previous() {
        return prev;
    }

    /**
     * @param prev Previous record in chain.
     */
    public void previous(WALRecord prev) {
        this.prev = prev;
    }

    /**
     * @return Position in file.
     */
    public WALPointer position() {
        return pos;
    }

    /**
     * @param pos Position in file.
     */
    public void position(WALPointer pos) {
        assert pos != null;

        this.pos = pos;
    }

    /**
     * @return Size of this record in bytes.
     */
    public int size() {
        return size;
    }

    /**
     * @param size Size of this record in bytes.
     */
    public void size(int size) {
        assert size >= 0 : size;

        this.size = size;
    }

    /**
     * @return Entry type.
     */
    public abstract RecordType type();

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WALRecord.class, this, "type", type());
    }
}

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
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordPurpose.*;

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
        TX_RECORD (LOGICAL),

        /** */
        PAGE_RECORD (PHYSICAL),

        /** */
        DATA_RECORD (LOGICAL),

        /** Checkpoint (begin) record */
        CHECKPOINT_RECORD (PHYSICAL),

        /** WAL segment header record. */
        HEADER_RECORD (INTERNAL),

        // Delta records.

        /** */
        INIT_NEW_PAGE_RECORD (PHYSICAL),

        /** */
        DATA_PAGE_INSERT_RECORD (PHYSICAL),

        /** */
        DATA_PAGE_INSERT_FRAGMENT_RECORD (PHYSICAL),

        /** */
        DATA_PAGE_REMOVE_RECORD (PHYSICAL),

        /** */
        DATA_PAGE_SET_FREE_LIST_PAGE (PHYSICAL),

        /** */
        BTREE_META_PAGE_INIT_ROOT (PHYSICAL),

        /** */
        BTREE_META_PAGE_ADD_ROOT (PHYSICAL),

        /** */
        BTREE_META_PAGE_CUT_ROOT (PHYSICAL),

        /** */
        BTREE_INIT_NEW_ROOT (PHYSICAL),

        /** */
        BTREE_PAGE_RECYCLE (PHYSICAL),

        /** */
        BTREE_PAGE_INSERT (PHYSICAL),

        /** */
        BTREE_FIX_LEFTMOST_CHILD (PHYSICAL),

        /** */
        BTREE_FIX_COUNT (PHYSICAL),

        /** */
        BTREE_PAGE_REPLACE (PHYSICAL),

        /** */
        BTREE_PAGE_REMOVE (PHYSICAL),

        /** */
        BTREE_PAGE_INNER_REPLACE (PHYSICAL),

        /** */
        BTREE_FIX_REMOVE_ID (PHYSICAL),

        /** */
        BTREE_FORWARD_PAGE_SPLIT (PHYSICAL),

        /** */
        BTREE_EXISTING_PAGE_SPLIT (PHYSICAL),

        /** */
        BTREE_PAGE_MERGE (PHYSICAL),

        /** */
        PAGES_LIST_SET_NEXT (PHYSICAL),

        /** */
        PAGES_LIST_SET_PREVIOUS (PHYSICAL),

        /** */
        PAGES_LIST_INIT_NEW_PAGE (PHYSICAL),

        /** */
        PAGES_LIST_ADD_PAGE (PHYSICAL),

        /** */
        PAGES_LIST_REMOVE_PAGE (PHYSICAL),

        /** */
        META_PAGE_INIT (PHYSICAL),

        /** */
        PARTITION_META_PAGE_UPDATE_COUNTERS (PHYSICAL),

        /** Memory recovering start marker */
        MEMORY_RECOVERY,

        /** */
        TRACKING_PAGE_DELTA (PHYSICAL),

        /** Meta page update last successful snapshot id. */
        META_PAGE_UPDATE_LAST_SUCCESSFUL_SNAPSHOT_ID (MIXED),

        /** Meta page update last successful full snapshot id. */
        META_PAGE_UPDATE_LAST_SUCCESSFUL_FULL_SNAPSHOT_ID (MIXED),

        /** Meta page update next snapshot id. */
        META_PAGE_UPDATE_NEXT_SNAPSHOT_ID (MIXED),

        /** Meta page update last allocated index. */
        META_PAGE_UPDATE_LAST_ALLOCATED_INDEX (MIXED),

        /** Partition meta update state. */
        PART_META_UPDATE_STATE (MIXED),

        /** Page list meta reset count record. */
        PAGE_LIST_META_RESET_COUNT_RECORD (PHYSICAL),

        /** Switch segment record.
         *  Marker record for indicate end of segment.
         *  If the next one record is written down exactly at the end of segment,
         *  SWITCH_SEGMENT_RECORD will not be written, if not then it means that we have more
         *  that one byte in the end,then we write SWITCH_SEGMENT_RECORD as marker end of segment.
         *  No need write CRC or WAL pointer for this record. It is byte marker record.
         *  */
        SWITCH_SEGMENT_RECORD (INTERNAL),

        /** */
        DATA_PAGE_UPDATE_RECORD (PHYSICAL),

        /** init */
        BTREE_META_PAGE_INIT_ROOT2 (PHYSICAL),

        /** Partition destroy. */
        PARTITION_DESTROY (PHYSICAL),

        /** Snapshot record. */
        SNAPSHOT,

        /** Metastore data record. */
        METASTORE_DATA_RECORD (LOGICAL),

        /** Exchange record. */
        EXCHANGE,

        /** Reserved for future record. */
        RESERVED,

        /** Rotated id part record. */
        ROTATED_ID_PART_RECORD (PHYSICAL),

        /** */
        MVCC_DATA_PAGE_MARK_UPDATED_RECORD (PHYSICAL),

        /** */
        MVCC_DATA_PAGE_TX_STATE_HINT_UPDATED_RECORD (PHYSICAL),

        /** */
        MVCC_DATA_PAGE_NEW_TX_STATE_HINT_UPDATED_RECORD (PHYSICAL),

        /** Encrypted WAL-record. */
        ENCRYPTED_RECORD (PHYSICAL),

        /** Ecnrypted data record. */
        ENCRYPTED_DATA_RECORD (LOGICAL),

        /** Mvcc data record. */
        MVCC_DATA_RECORD (LOGICAL),

        /** Mvcc Tx state change record. */
        MVCC_TX_RECORD (LOGICAL),

        /** Rollback tx record. */
        ROLLBACK_TX_RECORD (LOGICAL),

        /** */
        PARTITION_META_PAGE_UPDATE_COUNTERS_V2 (PHYSICAL);

        /**
         * When you're adding a new record don't forget to choose record purpose explicitly
         * if record is needed for physical or logical recovery.
         * By default the purpose of record is {@link RecordPurpose#CUSTOM} and this record will not be used in recovery process.
         * For more information read description of {@link RecordPurpose}.
         */
        private final RecordPurpose purpose;

        /**
         * @param purpose Purpose.
         */
        RecordType(RecordPurpose purpose) {
            this.purpose = purpose;
        }

        /**
         * Default constructor.
         */
        RecordType() {
            this(CUSTOM);
        }

        /**
         * @return Purpose of record.
         */
        public RecordPurpose purpose() {
            return purpose;
        }

        /** */
        private static final RecordType[] VALS = RecordType.values();

        /** */
        public static RecordType fromOrdinal(int ord) {
            return ord < 0 || ord >= VALS.length ? null : VALS[ord];
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
         * {@link org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager#restoreBinaryMemory(org.apache.ignite.lang.IgnitePredicate, org.apache.ignite.lang.IgniteBiPredicate)}.
         */
        PHYSICAL,
        /**
         * Logical records are needed to replay logical updates since last checkpoint.
         * {@link GridCacheDatabaseSharedManager#applyLogicalUpdates(org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.CheckpointStatus, org.apache.ignite.lang.IgnitePredicate, org.apache.ignite.lang.IgniteBiPredicate, boolean)}
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
        assert size >= 0: size;

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

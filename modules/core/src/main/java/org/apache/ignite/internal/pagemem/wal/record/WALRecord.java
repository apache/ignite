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
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

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
        TX_RECORD (0),

        /** */
        PAGE_RECORD (1),

        /** */
        DATA_RECORD (2),

        /** Checkpoint (begin) record */
        CHECKPOINT_RECORD (3),

        /** WAL segment header record. */
        HEADER_RECORD (4),

        // Delta records.

        /** */
        INIT_NEW_PAGE_RECORD (5),

        /** */
        DATA_PAGE_INSERT_RECORD (6),

        /** */
        DATA_PAGE_INSERT_FRAGMENT_RECORD (7),

        /** */
        DATA_PAGE_REMOVE_RECORD (8),

        /** */
        DATA_PAGE_SET_FREE_LIST_PAGE (9),

        /** */
        BTREE_META_PAGE_INIT_ROOT (10),

        /** */
        BTREE_META_PAGE_ADD_ROOT (11),

        /** */
        BTREE_META_PAGE_CUT_ROOT (12),

        /** */
        BTREE_INIT_NEW_ROOT (13),

        /** */
        BTREE_PAGE_RECYCLE (14),

        /** */
        BTREE_PAGE_INSERT (15),

        /** */
        BTREE_FIX_LEFTMOST_CHILD (16),

        /** */
        BTREE_FIX_COUNT (17),

        /** */
        BTREE_PAGE_REPLACE (18),

        /** */
        BTREE_PAGE_REMOVE (19),

        /** */
        BTREE_PAGE_INNER_REPLACE (20),

        /** */
        BTREE_FIX_REMOVE_ID (21),

        /** */
        BTREE_FORWARD_PAGE_SPLIT (22),

        /** */
        BTREE_EXISTING_PAGE_SPLIT (23),

        /** */
        BTREE_PAGE_MERGE (24),

        /** */
        PAGES_LIST_SET_NEXT (25),

        /** */
        PAGES_LIST_SET_PREVIOUS (26),

        /** */
        PAGES_LIST_INIT_NEW_PAGE (27),

        /** */
        PAGES_LIST_ADD_PAGE (28),

        /** */
        PAGES_LIST_REMOVE_PAGE (29),

        /** */
        META_PAGE_INIT (30),

        /** */
        PARTITION_META_PAGE_UPDATE_COUNTERS (31),

        /** Memory recovering start marker */
        MEMORY_RECOVERY (32),

        /** */
        TRACKING_PAGE_DELTA (33),

        /** Meta page update last successful snapshot id. */
        META_PAGE_UPDATE_LAST_SUCCESSFUL_SNAPSHOT_ID (34),

        /** Meta page update last successful full snapshot id. */
        META_PAGE_UPDATE_LAST_SUCCESSFUL_FULL_SNAPSHOT_ID (35),

        /** Meta page update next snapshot id. */
        META_PAGE_UPDATE_NEXT_SNAPSHOT_ID (36),

        /** Meta page update last allocated index. */
        META_PAGE_UPDATE_LAST_ALLOCATED_INDEX (37),

        /** Partition meta update state. */
        PART_META_UPDATE_STATE (38),

        /** Page list meta reset count record. */
        PAGE_LIST_META_RESET_COUNT_RECORD (39),

        /** Switch segment record.
         *  Marker record for indicate end of segment.
         *  If the next one record is written down exactly at the end of segment,
         *  SWITCH_SEGMENT_RECORD will not be written, if not then it means that we have more
         *  that one byte in the end,then we write SWITCH_SEGMENT_RECORD as marker end of segment.
         *  No need write CRC or WAL pointer for this record. It is byte marker record.
         *  */
        SWITCH_SEGMENT_RECORD (40),

        /** */
        DATA_PAGE_UPDATE_RECORD (41),

        /** init */
        BTREE_META_PAGE_INIT_ROOT2 (42),

        /** Partition destroy. */
        PARTITION_DESTROY (43),

        /** Snapshot record. */
        SNAPSHOT (44),

        /** Metastore data record. */
        METASTORE_DATA_RECORD (45),

        /** Exchange record. */
        EXCHANGE (46),

        /** Reserved for future record. */
        RESERVED (47),

        /** Rollback tx record. */
        ROLLBACK_TX_RECORD (57),

        /** */
        PARTITION_META_PAGE_UPDATE_COUNTERS_V2 (58),

        /** Init root meta page (with flags and created version) */
        BTREE_META_PAGE_INIT_ROOT_V3 (59),

        /** Record that indicates that "corrupted" flag should be removed from tracking page. */
        TRACKING_PAGE_REPAIR_DELTA (61);

        /** Index for serialization. Should be consistent throughout all versions. */
        private final int idx;

        /**
         * @param idx Index for serialization.
         */
        RecordType(int idx) {
            this.idx = idx;
        }

        /**
         * @return Index for serialization.
         */
        public int index() {
            return idx;
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
     * @return Need wal rollOver.
     */
    public boolean rollOver(){
        return false;
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

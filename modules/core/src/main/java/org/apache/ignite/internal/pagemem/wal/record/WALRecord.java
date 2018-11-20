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
        TX_RECORD,

        /** */
        PAGE_RECORD,

        /** */
        DATA_RECORD,

        /** Checkpoint (begin) record */
        CHECKPOINT_RECORD,

        /** WAL segment header record. */
        HEADER_RECORD,

        // Delta records.

        /** */
        INIT_NEW_PAGE_RECORD,

        /** */
        DATA_PAGE_INSERT_RECORD,

        /** */
        DATA_PAGE_INSERT_FRAGMENT_RECORD,

        /** */
        DATA_PAGE_REMOVE_RECORD,

        /** */
        DATA_PAGE_SET_FREE_LIST_PAGE,

        /** */
        BTREE_META_PAGE_INIT_ROOT,

        /** */
        BTREE_META_PAGE_ADD_ROOT,

        /** */
        BTREE_META_PAGE_CUT_ROOT,

        /** */
        BTREE_INIT_NEW_ROOT,

        /** */
        BTREE_PAGE_RECYCLE,

        /** */
        BTREE_PAGE_INSERT,

        /** */
        BTREE_FIX_LEFTMOST_CHILD,

        /** */
        BTREE_FIX_COUNT,

        /** */
        BTREE_PAGE_REPLACE,

        /** */
        BTREE_PAGE_REMOVE,

        /** */
        BTREE_PAGE_INNER_REPLACE,

        /** */
        BTREE_FIX_REMOVE_ID,

        /** */
        BTREE_FORWARD_PAGE_SPLIT,

        /** */
        BTREE_EXISTING_PAGE_SPLIT,

        /** */
        BTREE_PAGE_MERGE,

        /** */
        PAGES_LIST_SET_NEXT,

        /** */
        PAGES_LIST_SET_PREVIOUS,

        /** */
        PAGES_LIST_INIT_NEW_PAGE,

        /** */
        PAGES_LIST_ADD_PAGE,

        /** */
        PAGES_LIST_REMOVE_PAGE,

        /** */
        META_PAGE_INIT,

        /** */
        PARTITION_META_PAGE_UPDATE_COUNTERS,

        /** Memory recovering start marker */
        MEMORY_RECOVERY,

        /** */
        TRACKING_PAGE_DELTA,

        /** Meta page update last successful snapshot id. */
        META_PAGE_UPDATE_LAST_SUCCESSFUL_SNAPSHOT_ID,

        /** Meta page update last successful full snapshot id. */
        META_PAGE_UPDATE_LAST_SUCCESSFUL_FULL_SNAPSHOT_ID,

        /** Meta page update next snapshot id. */
        META_PAGE_UPDATE_NEXT_SNAPSHOT_ID,

        /** Meta page update last allocated index. */
        META_PAGE_UPDATE_LAST_ALLOCATED_INDEX,

        /** Partition meta update state. */
        PART_META_UPDATE_STATE,

        /** Page list meta reset count record. */
        PAGE_LIST_META_RESET_COUNT_RECORD,

        /** Switch segment record.
         *  Marker record for indicate end of segment.
         *  If the next one record is written down exactly at the end of segment,
         *  SWITCH_SEGMENT_RECORD will not be written, if not then it means that we have more
         *  that one byte in the end,then we write SWITCH_SEGMENT_RECORD as marker end of segment.
         *  No need write CRC or WAL pointer for this record. It is byte marker record.
         *  */
        SWITCH_SEGMENT_RECORD,

        /** */
        DATA_PAGE_UPDATE_RECORD,

        /** init */
        BTREE_META_PAGE_INIT_ROOT2,

        /** Partition destroy. */
        PARTITION_DESTROY,

        /** Snapshot record. */
        SNAPSHOT,

        /** Metastore data record. */
        METASTORE_DATA_RECORD,

        /** Exchange record. */
        EXCHANGE,

        /** Reserved for future record. */
        RESERVED,

        /** Rotated id part record. */
        ROTATED_ID_PART_RECORD,

        /** */
        MVCC_DATA_PAGE_MARK_UPDATED_RECORD,

        /** */
        MVCC_DATA_PAGE_TX_STATE_HINT_UPDATED_RECORD,

        /** */
        MVCC_DATA_PAGE_NEW_TX_STATE_HINT_UPDATED_RECORD;

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

/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal.record;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;

/**
 * Utility class for handling WAL record types.
 */
public final class RecordTypes {
    /** */
    public static final Set<WALRecord.RecordType> DELTA_TYPE_SET = new HashSet<>();

    static {
        DELTA_TYPE_SET.add(WALRecord.RecordType.PAGE_RECORD);
        DELTA_TYPE_SET.add(WALRecord.RecordType.INIT_NEW_PAGE_RECORD);
        DELTA_TYPE_SET.add(WALRecord.RecordType.DATA_PAGE_INSERT_RECORD);
        DELTA_TYPE_SET.add(WALRecord.RecordType.DATA_PAGE_INSERT_FRAGMENT_RECORD);
        DELTA_TYPE_SET.add(WALRecord.RecordType.DATA_PAGE_REMOVE_RECORD);
        DELTA_TYPE_SET.add(WALRecord.RecordType.DATA_PAGE_SET_FREE_LIST_PAGE);
        DELTA_TYPE_SET.add(WALRecord.RecordType.MVCC_DATA_PAGE_MARK_UPDATED_RECORD);
        DELTA_TYPE_SET.add(WALRecord.RecordType.MVCC_DATA_PAGE_TX_STATE_HINT_UPDATED_RECORD);
        DELTA_TYPE_SET.add(WALRecord.RecordType.MVCC_DATA_PAGE_NEW_TX_STATE_HINT_UPDATED_RECORD);
        DELTA_TYPE_SET.add(WALRecord.RecordType.BTREE_META_PAGE_INIT_ROOT);
        DELTA_TYPE_SET.add(WALRecord.RecordType.BTREE_META_PAGE_ADD_ROOT);
        DELTA_TYPE_SET.add(WALRecord.RecordType.BTREE_META_PAGE_CUT_ROOT);
        DELTA_TYPE_SET.add(WALRecord.RecordType.BTREE_INIT_NEW_ROOT);
        DELTA_TYPE_SET.add(WALRecord.RecordType.BTREE_PAGE_RECYCLE);
        DELTA_TYPE_SET.add(WALRecord.RecordType.BTREE_PAGE_INSERT);
        DELTA_TYPE_SET.add(WALRecord.RecordType.BTREE_FIX_LEFTMOST_CHILD);
        DELTA_TYPE_SET.add(WALRecord.RecordType.BTREE_FIX_COUNT);
        DELTA_TYPE_SET.add(WALRecord.RecordType.BTREE_PAGE_REPLACE);
        DELTA_TYPE_SET.add(WALRecord.RecordType.BTREE_PAGE_REMOVE);
        DELTA_TYPE_SET.add(WALRecord.RecordType.BTREE_PAGE_INNER_REPLACE);
        DELTA_TYPE_SET.add(WALRecord.RecordType.BTREE_FIX_REMOVE_ID);
        DELTA_TYPE_SET.add(WALRecord.RecordType.BTREE_FORWARD_PAGE_SPLIT);
        DELTA_TYPE_SET.add(WALRecord.RecordType.BTREE_EXISTING_PAGE_SPLIT);
        DELTA_TYPE_SET.add(WALRecord.RecordType.BTREE_PAGE_MERGE);
        DELTA_TYPE_SET.add(WALRecord.RecordType.PAGES_LIST_SET_NEXT);
        DELTA_TYPE_SET.add(WALRecord.RecordType.PAGES_LIST_SET_PREVIOUS);
        DELTA_TYPE_SET.add(WALRecord.RecordType.PAGES_LIST_INIT_NEW_PAGE);
        DELTA_TYPE_SET.add(WALRecord.RecordType.PAGES_LIST_ADD_PAGE);
        DELTA_TYPE_SET.add(WALRecord.RecordType.PAGES_LIST_REMOVE_PAGE);
        DELTA_TYPE_SET.add(WALRecord.RecordType.META_PAGE_INIT);
        DELTA_TYPE_SET.add(WALRecord.RecordType.PARTITION_META_PAGE_UPDATE_COUNTERS);
        DELTA_TYPE_SET.add(WALRecord.RecordType.TRACKING_PAGE_DELTA);
        DELTA_TYPE_SET.add(WALRecord.RecordType.META_PAGE_UPDATE_LAST_SUCCESSFUL_SNAPSHOT_ID);
        DELTA_TYPE_SET.add(WALRecord.RecordType.META_PAGE_UPDATE_LAST_SUCCESSFUL_FULL_SNAPSHOT_ID);
        DELTA_TYPE_SET.add(WALRecord.RecordType.META_PAGE_UPDATE_NEXT_SNAPSHOT_ID);
        DELTA_TYPE_SET.add(WALRecord.RecordType.META_PAGE_UPDATE_LAST_ALLOCATED_INDEX);
        DELTA_TYPE_SET.add(WALRecord.RecordType.PAGE_LIST_META_RESET_COUNT_RECORD);
        DELTA_TYPE_SET.add(WALRecord.RecordType.DATA_PAGE_UPDATE_RECORD);
        DELTA_TYPE_SET.add(WALRecord.RecordType.BTREE_META_PAGE_INIT_ROOT2);
        DELTA_TYPE_SET.add(WALRecord.RecordType.ROTATED_ID_PART_RECORD);
    }
}

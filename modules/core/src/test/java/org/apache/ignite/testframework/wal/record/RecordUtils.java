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

package org.apache.ignite.testframework.wal.record;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.wal.record.CdcManagerRecord;
import org.apache.ignite.internal.pagemem.wal.record.CdcManagerStopRecord;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.ExchangeRecord;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.IndexRenameRootPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.MasterKeyChangeRecordV2;
import org.apache.ignite.internal.pagemem.wal.record.MemoryRecoveryRecord;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.PartitionClearingStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.ReencryptionStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.RollbackRecord;
import org.apache.ignite.internal.pagemem.wal.record.SnapshotRecord;
import org.apache.ignite.internal.pagemem.wal.record.SwitchSegmentRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.ClusterSnapshotRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertFragmentRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageRemoveRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageSetFreeListPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageUpdateRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.FixCountRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.FixLeftmostChildRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.FixRemoveId;
import org.apache.ignite.internal.pagemem.wal.record.delta.InitNewPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.InsertRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageAddRootRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageCutRootRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageInitRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageInitRootInlineRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageInitRootRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdateIndexDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdateLastAllocatedIndex;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdateLastSuccessfulFullSnapshotId;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdateLastSuccessfulSnapshotId;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdateNextSnapshotId;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdatePartitionDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdatePartitionDataRecordV2;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdatePartitionDataRecordV3;
import org.apache.ignite.internal.pagemem.wal.record.delta.NewRootInitRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageListMetaResetCountRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListAddPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListInitNewPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListRemovePageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListSetNextRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListSetPreviousRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PartitionDestroyRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PartitionMetaStateRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.RecycleRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.RemoveRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.ReplaceRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.RotatedIdPartRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.SplitExistingPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.TrackingPageDeltaRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.TrackingPageRepairDeltaRecord;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.tree.DataInnerIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.transactions.TransactionState;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_EXISTING_PAGE_SPLIT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_FIX_COUNT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_FIX_LEFTMOST_CHILD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_FIX_REMOVE_ID;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_FORWARD_PAGE_SPLIT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_INIT_NEW_ROOT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_META_PAGE_ADD_ROOT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_META_PAGE_CUT_ROOT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_META_PAGE_INIT_ROOT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_META_PAGE_INIT_ROOT2;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_META_PAGE_INIT_ROOT_V3;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_PAGE_INNER_REPLACE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_PAGE_INSERT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_PAGE_MERGE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_PAGE_RECYCLE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_PAGE_REMOVE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_PAGE_REPLACE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CDC_DATA_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CDC_MANAGER_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CDC_MANAGER_STOP_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CHECKPOINT_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CLUSTER_SNAPSHOT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CONSISTENT_CUT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_PAGE_FRAGMENTED_UPDATE_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_PAGE_INSERT_FRAGMENT_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_PAGE_INSERT_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_PAGE_REMOVE_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_PAGE_SET_FREE_LIST_PAGE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_PAGE_UPDATE_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD_V2;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.ENCRYPTED_DATA_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.ENCRYPTED_DATA_RECORD_V2;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.ENCRYPTED_DATA_RECORD_V3;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.ENCRYPTED_OUT_OF_ORDER_UPDATE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.ENCRYPTED_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.ENCRYPTED_RECORD_V2;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.EXCHANGE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.HEADER_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.INCREMENTAL_SNAPSHOT_FINISH_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.INCREMENTAL_SNAPSHOT_START_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.INDEX_META_PAGE_DELTA_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.INDEX_ROOT_PAGE_RENAME_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.INIT_NEW_PAGE_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.MASTER_KEY_CHANGE_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.MASTER_KEY_CHANGE_RECORD_V2;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.MEMORY_RECOVERY;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.METASTORE_DATA_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.META_PAGE_INIT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.META_PAGE_UPDATE_LAST_ALLOCATED_INDEX;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.META_PAGE_UPDATE_LAST_SUCCESSFUL_FULL_SNAPSHOT_ID;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.META_PAGE_UPDATE_LAST_SUCCESSFUL_SNAPSHOT_ID;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.META_PAGE_UPDATE_NEXT_SNAPSHOT_ID;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.MVCC_DATA_PAGE_MARK_UPDATED_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.MVCC_DATA_PAGE_NEW_TX_STATE_HINT_UPDATED_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.MVCC_DATA_PAGE_TX_STATE_HINT_UPDATED_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.MVCC_DATA_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.MVCC_TX_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.OUT_OF_ORDER_UPDATE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.PAGES_LIST_ADD_PAGE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.PAGES_LIST_INIT_NEW_PAGE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.PAGES_LIST_REMOVE_PAGE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.PAGES_LIST_SET_NEXT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.PAGES_LIST_SET_PREVIOUS;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.PAGE_LIST_META_RESET_COUNT_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.PAGE_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.PARTITION_CLEARING_START_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.PARTITION_DESTROY;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.PARTITION_META_PAGE_DELTA_RECORD_V3;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.PARTITION_META_PAGE_DELTA_RECORD_V4;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.PARTITION_META_PAGE_UPDATE_COUNTERS;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.PARTITION_META_PAGE_UPDATE_COUNTERS_V2;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.PART_META_UPDATE_STATE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.REENCRYPTION_START_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.RESERVED;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.RESERVED_IDX2;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.ROLLBACK_TX_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.ROTATED_ID_PART_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.SNAPSHOT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.SWITCH_SEGMENT_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.TRACKING_PAGE_DELTA;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.TRACKING_PAGE_REPAIR_DELTA;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.TX_RECORD;
import static org.apache.ignite.internal.processors.cache.tree.DataInnerIO.VERSIONS;

/**
 * Class contains builder methods for at least one record of each type
 * {@link org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType}.
 * NOTE!!: It is better to rewrite these builder methods to the builder of each record for flexible use where it is
 * required.
 */
public class RecordUtils {
    /** */
    private static final Map<WALRecord.RecordType, Supplier<WALRecord>> TEST_WAL_RECORD_SUPPLIER =
        new EnumMap<>(WALRecord.RecordType.class);

    static {
        put(TX_RECORD, RecordUtils::buildTxRecord);
        put(PAGE_RECORD, RecordUtils::buildPageSnapshot);
        put(DATA_RECORD, RecordUtils::buildDataRecord);
        put(DATA_RECORD_V2, RecordUtils::buildDataRecord);
        put(CDC_DATA_RECORD, RecordUtils::buildDataRecord);
        put(CHECKPOINT_RECORD, RecordUtils::buildCheckpointRecord);
        put(HEADER_RECORD, buildUnsupportedWalRecord(HEADER_RECORD));
        put(INIT_NEW_PAGE_RECORD, RecordUtils::buildInitNewPageRecord);
        put(DATA_PAGE_INSERT_RECORD, RecordUtils::buildDataPageInsertRecord);
        put(DATA_PAGE_INSERT_FRAGMENT_RECORD, RecordUtils::buildDataPageInsertFragmentRecord);
        put(DATA_PAGE_REMOVE_RECORD, RecordUtils::buildDataPageRemoveRecord);
        put(DATA_PAGE_SET_FREE_LIST_PAGE, RecordUtils::buildDataPageSetFreeListPageRecord);
        put(BTREE_META_PAGE_INIT_ROOT, RecordUtils::buildMetaPageInitRootRecord);
        put(BTREE_META_PAGE_ADD_ROOT, RecordUtils::buildMetaPageAddRootRecord);
        put(BTREE_META_PAGE_CUT_ROOT, RecordUtils::buildMetaPageCutRootRecord);
        put(BTREE_INIT_NEW_ROOT, RecordUtils::buildNewRootInitRecord);
        put(BTREE_PAGE_RECYCLE, RecordUtils::buildRecycleRecord);
        put(BTREE_PAGE_INSERT, RecordUtils::buildInsertRecord);
        put(BTREE_FIX_LEFTMOST_CHILD, RecordUtils::buildFixLeftmostChildRecord);
        put(BTREE_FIX_COUNT, RecordUtils::buildFixCountRecord);
        put(BTREE_PAGE_REPLACE, RecordUtils::buildReplaceRecord);
        put(BTREE_PAGE_REMOVE, RecordUtils::buildRemoveRecord);
        put(BTREE_PAGE_INNER_REPLACE, buildUnsupportedWalRecord(BTREE_PAGE_INNER_REPLACE));
        put(BTREE_FIX_REMOVE_ID, RecordUtils::buildFixRemoveId);
        put(BTREE_FORWARD_PAGE_SPLIT, buildUnsupportedWalRecord(BTREE_FORWARD_PAGE_SPLIT));
        put(BTREE_EXISTING_PAGE_SPLIT, RecordUtils::buildSplitExistingPageRecord);
        put(BTREE_PAGE_MERGE, buildUnsupportedWalRecord(BTREE_PAGE_MERGE));
        put(PAGES_LIST_SET_NEXT, RecordUtils::buildPagesListSetNextRecord);
        put(PAGES_LIST_SET_PREVIOUS, RecordUtils::buildPagesListSetPreviousRecord);
        put(PAGES_LIST_INIT_NEW_PAGE, RecordUtils::buildPagesListInitNewPageRecord);
        put(PAGES_LIST_ADD_PAGE, RecordUtils::buildPagesListAddPageRecord);
        put(PAGES_LIST_REMOVE_PAGE, RecordUtils::buildPagesListRemovePageRecord);
        put(META_PAGE_INIT, RecordUtils::buildMetaPageInitRecord);
        put(INDEX_META_PAGE_DELTA_RECORD, RecordUtils::buildMetaPageIndexDeltaRecord);
        put(PARTITION_META_PAGE_UPDATE_COUNTERS, RecordUtils::buildMetaPageUpdatePartitionDataRecord);
        put(MEMORY_RECOVERY, RecordUtils::buildMemoryRecoveryRecord);
        put(TRACKING_PAGE_DELTA, RecordUtils::buildTrackingPageDeltaRecord);
        put(TRACKING_PAGE_REPAIR_DELTA, RecordUtils::buildTrackingPageRepairDeltaRecord);
        put(META_PAGE_UPDATE_LAST_SUCCESSFUL_SNAPSHOT_ID, RecordUtils::buildMetaPageUpdateLastSuccessfulSnapshotId);
        put(META_PAGE_UPDATE_LAST_SUCCESSFUL_FULL_SNAPSHOT_ID, RecordUtils::buildMetaPageUpdateLastSuccessfulFullSnapshotId);
        put(META_PAGE_UPDATE_NEXT_SNAPSHOT_ID, RecordUtils::buildMetaPageUpdateNextSnapshotId);
        put(META_PAGE_UPDATE_LAST_ALLOCATED_INDEX, RecordUtils::buildMetaPageUpdateLastAllocatedIndex);
        put(PART_META_UPDATE_STATE, RecordUtils::buildPartitionMetaStateRecord);
        put(PAGE_LIST_META_RESET_COUNT_RECORD, RecordUtils::buildPageListMetaResetCountRecord);
        put(SWITCH_SEGMENT_RECORD, RecordUtils::buildSwitchSegmentRecord);
        put(DATA_PAGE_UPDATE_RECORD, RecordUtils::buildDataPageUpdateRecord);
        put(BTREE_META_PAGE_INIT_ROOT2, RecordUtils::buildMetaPageInitRootInlineRecord);
        put(PARTITION_DESTROY, RecordUtils::buildPartitionDestroyRecord);
        put(SNAPSHOT, RecordUtils::buildSnapshotRecord);
        put(METASTORE_DATA_RECORD, RecordUtils::buildMetastoreDataRecord);
        put(EXCHANGE, RecordUtils::buildExchangeRecord);
        put(RESERVED, buildUnsupportedWalRecord(RESERVED));
        put(ROLLBACK_TX_RECORD, RecordUtils::buildRollbackRecord);
        put(PARTITION_META_PAGE_UPDATE_COUNTERS_V2, RecordUtils::buildMetaPageUpdatePartitionDataRecordV2);
        put(PARTITION_META_PAGE_DELTA_RECORD_V3, RecordUtils::buildMetaPageUpdatePartitionDataRecordV3);
        put(PARTITION_META_PAGE_DELTA_RECORD_V4, buildUnsupportedWalRecord(PARTITION_META_PAGE_DELTA_RECORD_V4));
        put(MASTER_KEY_CHANGE_RECORD, buildUnsupportedWalRecord(MASTER_KEY_CHANGE_RECORD));
        put(MASTER_KEY_CHANGE_RECORD_V2, RecordUtils::buildMasterKeyChangeRecordV2);
        put(REENCRYPTION_START_RECORD, RecordUtils::buildEncryptionStatusRecord);
        put(ROTATED_ID_PART_RECORD, RecordUtils::buildRotatedIdPartRecord);
        put(ENCRYPTED_RECORD, buildUnsupportedWalRecord(ENCRYPTED_RECORD));
        put(ENCRYPTED_DATA_RECORD, buildUnsupportedWalRecord(ENCRYPTED_DATA_RECORD));
        put(ENCRYPTED_RECORD_V2, buildUnsupportedWalRecord(ENCRYPTED_RECORD_V2));
        put(ENCRYPTED_DATA_RECORD_V2, buildUnsupportedWalRecord(ENCRYPTED_DATA_RECORD_V2));
        put(ENCRYPTED_DATA_RECORD_V3, buildUnsupportedWalRecord(ENCRYPTED_DATA_RECORD_V3));
        put(CONSISTENT_CUT, buildUnsupportedWalRecord(CONSISTENT_CUT));
        put(BTREE_META_PAGE_INIT_ROOT_V3, buildUnsupportedWalRecord(BTREE_META_PAGE_INIT_ROOT_V3));
        put(OUT_OF_ORDER_UPDATE, buildUnsupportedWalRecord(OUT_OF_ORDER_UPDATE));
        put(INDEX_ROOT_PAGE_RENAME_RECORD, RecordUtils::buildIndexRenameRootPageRecord);
        put(PARTITION_CLEARING_START_RECORD, RecordUtils::buildPartitionClearingStartedRecord);
        put(ENCRYPTED_OUT_OF_ORDER_UPDATE, buildUnsupportedWalRecord(ENCRYPTED_OUT_OF_ORDER_UPDATE));
        put(CLUSTER_SNAPSHOT, RecordUtils::buildClusterSnapshotRecord);
        put(INCREMENTAL_SNAPSHOT_START_RECORD, RecordUtils::buildIncrementalSnapshotStartRecord);
        put(INCREMENTAL_SNAPSHOT_FINISH_RECORD, RecordUtils::buildIncrementalSnapshotFinishRecord);
        put(CDC_MANAGER_RECORD, RecordUtils::buildCdcManagerStopRecord);
        put(CDC_MANAGER_STOP_RECORD, RecordUtils::buildCdcManagerStopRecord);
        put(DATA_PAGE_FRAGMENTED_UPDATE_RECORD, buildUnsupportedWalRecord(DATA_PAGE_FRAGMENTED_UPDATE_RECORD));

        put(MVCC_DATA_RECORD, buildUnsupportedWalRecord(MVCC_DATA_RECORD));
        put(MVCC_TX_RECORD, buildUnsupportedWalRecord(MVCC_TX_RECORD));
        put(MVCC_DATA_PAGE_MARK_UPDATED_RECORD, buildUnsupportedWalRecord(MVCC_DATA_PAGE_MARK_UPDATED_RECORD));
        put(MVCC_DATA_PAGE_TX_STATE_HINT_UPDATED_RECORD,
            buildUnsupportedWalRecord(MVCC_DATA_PAGE_TX_STATE_HINT_UPDATED_RECORD));
        put(MVCC_DATA_PAGE_NEW_TX_STATE_HINT_UPDATED_RECORD,
            buildUnsupportedWalRecord(MVCC_DATA_PAGE_NEW_TX_STATE_HINT_UPDATED_RECORD));

        put(RESERVED_IDX2, buildUnsupportedWalRecord(RESERVED_IDX2));
    }

    /** */
    private static void put(WALRecord.RecordType type, Supplier<WALRecord> supplier) {
        TEST_WAL_RECORD_SUPPLIER.put(type, supplier);
    }

    /** **/
    public static WALRecord buildWalRecord(WALRecord.RecordType recordType) {
        Supplier<WALRecord> supplier = TEST_WAL_RECORD_SUPPLIER.get(recordType);

        return supplier == null ? null : supplier.get();
    }

    /** **/
    public static TxRecord buildTxRecord() {
        return new TxRecord(
            TransactionState.PREPARED,
            new GridCacheVersion(),
            new GridCacheVersion(),
            Collections.singletonMap((short)1, Collections.singletonList((short)1))
        );
    }

    /** **/
    public static PageSnapshot buildPageSnapshot() {
        //Deserialization doesn't check size from header but it is always expected page size as length of pageData.
        byte[] random = new byte[4096];
        //Real page size should be equal to page size.
        return new PageSnapshot(new FullPageId(1L, 1), random, 4096);
    }

    /** **/
    public static DataRecord buildDataRecord() {
        return new DataRecord(Collections.emptyList());
    }

    /** **/
    public static CheckpointRecord buildCheckpointRecord() {
        CheckpointRecord record = new CheckpointRecord(new WALPointer(1, 1, 1));
        record.cacheGroupStates(new HashMap<>());
        return record;
    }

    /** **/
    public static InitNewPageRecord buildInitNewPageRecord() {
        return new InitNewPageRecord(1, 1L, 1, 1, 1L);
    }

    /** **/
    public static DataPageInsertRecord buildDataPageInsertRecord() {
        byte[] random = {1, 3, 5};
        return new DataPageInsertRecord(1, 1L, random);
    }

    /** **/
    public static DataPageInsertFragmentRecord buildDataPageInsertFragmentRecord() {
        byte[] random = {1, 3, 5};
        return new DataPageInsertFragmentRecord(1, 1L, random, 1L);
    }

    /** **/
    public static DataPageRemoveRecord buildDataPageRemoveRecord() {
        return new DataPageRemoveRecord(1, 1, 1);
    }

    /** **/
    public static DataPageSetFreeListPageRecord buildDataPageSetFreeListPageRecord() {
        return new DataPageSetFreeListPageRecord(1, 1, 1);
    }

    /** **/
    public static MetaPageInitRootRecord buildMetaPageInitRootRecord() {
        return new MetaPageInitRootRecord(1, 1, 2);
    }

    /** **/
    public static MetaPageAddRootRecord buildMetaPageAddRootRecord() {
        return new MetaPageAddRootRecord(1, 1, 1);
    }

    /** **/
    public static MetaPageCutRootRecord buildMetaPageCutRootRecord() {
        return new MetaPageCutRootRecord(1, 1L);
    }

    /** **/
    public static NewRootInitRecord buildNewRootInitRecord() {
        DataInnerIO latest = VERSIONS.latest();
        //Deserialization doesn't check size from header but it is always expected io.getItemSize as length of rowBytes.
        byte[] rowBytes = new byte[latest.getItemSize()];

        return new NewRootInitRecord(1, 1L, 1, latest, 1, rowBytes, 1L);
    }

    /** **/
    public static RecycleRecord buildRecycleRecord() {
        return new RecycleRecord(1, 1, 1);
    }

    /** **/
    public static InsertRecord buildInsertRecord() {
        DataInnerIO latest = VERSIONS.latest();
        //Deserialization doesn't check size from header but it is always expected io.getItemSize as length of rowBytes.
        byte[] rowBytes = new byte[latest.getItemSize()];

        return new InsertRecord(1, 1, latest, 1, rowBytes, 1);
    }

    /** **/
    public static FixLeftmostChildRecord buildFixLeftmostChildRecord() {
        return new FixLeftmostChildRecord(1, 1, 1);
    }

    /** **/
    public static FixCountRecord buildFixCountRecord() {
        return new FixCountRecord(1, 1, 1);
    }

    /** **/
    public static ReplaceRecord buildReplaceRecord() {
        DataInnerIO latest = VERSIONS.latest();
        //Deserialization doesn't check size from header but it is always expected io.getItemSize as length of rowBytes.
        byte[] rowBytes = new byte[latest.getItemSize()];

        return new ReplaceRecord(1, 1, latest, rowBytes, 1);
    }

    /** **/
    public static RemoveRecord buildRemoveRecord() {
        return new RemoveRecord(1, 1, 1, 1);
    }

    /** **/
    public static FixRemoveId buildFixRemoveId() {
        return new FixRemoveId(1, 1, 1);
    }

    /** **/
    public static SplitExistingPageRecord buildSplitExistingPageRecord() {
        return new SplitExistingPageRecord(1, 1, 1, 1);
    }

    /** **/
    public static PagesListSetNextRecord buildPagesListSetNextRecord() {
        return new PagesListSetNextRecord(1, 1, 1);
    }

    /** **/
    public static PagesListSetPreviousRecord buildPagesListSetPreviousRecord() {
        return new PagesListSetPreviousRecord(1, 1, 1);
    }

    /** **/
    public static PagesListInitNewPageRecord buildPagesListInitNewPageRecord() {
        return new PagesListInitNewPageRecord(1, 1, 1, 1, 1, 1, 1);
    }

    /** **/
    public static PagesListAddPageRecord buildPagesListAddPageRecord() {
        return new PagesListAddPageRecord(1, 1, 1);
    }

    /** **/
    public static PagesListRemovePageRecord buildPagesListRemovePageRecord() {
        return new PagesListRemovePageRecord(1, 1, 1);
    }

    /** **/
    public static MetaPageInitRecord buildMetaPageInitRecord() {
        return new MetaPageInitRecord(1, 1, 1, 1, 1, 1);
    }

    /** **/
    public static MetaPageUpdateIndexDataRecord buildMetaPageIndexDeltaRecord() {
        return new MetaPageUpdateIndexDataRecord(1, 1, 0, 0);
    }

    /** **/
    public static MetaPageUpdatePartitionDataRecord buildMetaPageUpdatePartitionDataRecord() {
        return new MetaPageUpdatePartitionDataRecord(1, 1, 1, 1, 1, 1, (byte)1, 1);
    }

    /** **/
    public static MemoryRecoveryRecord buildMemoryRecoveryRecord() {
        return new MemoryRecoveryRecord(1);
    }

    /** **/
    public static TrackingPageDeltaRecord buildTrackingPageDeltaRecord() {
        return new TrackingPageDeltaRecord(1, 1, 1, 1, 1);
    }

    /** **/
    public static TrackingPageRepairDeltaRecord buildTrackingPageRepairDeltaRecord() {
        return new TrackingPageRepairDeltaRecord(1, 1);
    }

    /** **/
    public static MetaPageUpdateLastSuccessfulSnapshotId buildMetaPageUpdateLastSuccessfulSnapshotId() {
        return new MetaPageUpdateLastSuccessfulSnapshotId(1, 1, 1, 1);
    }

    /** **/
    public static MetaPageUpdateLastSuccessfulFullSnapshotId buildMetaPageUpdateLastSuccessfulFullSnapshotId() {
        return new MetaPageUpdateLastSuccessfulFullSnapshotId(1, 1, 1);
    }

    /** **/
    public static MetaPageUpdateNextSnapshotId buildMetaPageUpdateNextSnapshotId() {
        return new MetaPageUpdateNextSnapshotId(1, 1, 1);
    }

    /** **/
    public static MetaPageUpdateLastAllocatedIndex buildMetaPageUpdateLastAllocatedIndex() {
        return new MetaPageUpdateLastAllocatedIndex(1, 1, 1);
    }

    /** **/
    public static PartitionMetaStateRecord buildPartitionMetaStateRecord() {
        return new PartitionMetaStateRecord(1, 1, GridDhtPartitionState.OWNING, 1);
    }

    /** **/
    public static PageListMetaResetCountRecord buildPageListMetaResetCountRecord() {
        return new PageListMetaResetCountRecord(1, 1);
    }

    /** **/
    public static SwitchSegmentRecord buildSwitchSegmentRecord() {
        return new SwitchSegmentRecord();
    }

    /** **/
    public static DataPageUpdateRecord buildDataPageUpdateRecord() {
        byte[] random = {1, 3, 5};

        return new DataPageUpdateRecord(1, 1, 1, random);
    }

    /** **/
    public static MetaPageInitRootInlineRecord buildMetaPageInitRootInlineRecord() {
        return new MetaPageInitRootInlineRecord(1, 1, 2, 1);
    }

    /** **/
    public static PartitionDestroyRecord buildPartitionDestroyRecord() {
        return new PartitionDestroyRecord(1, 1);
    }

    /** **/
    public static SnapshotRecord buildSnapshotRecord() {
        return new SnapshotRecord(1, true);
    }

    /** **/
    public static MetastoreDataRecord buildMetastoreDataRecord() {
        byte[] val = {1, 3, 5};

        return new MetastoreDataRecord("key", val);
    }

    /** **/
    public static ExchangeRecord buildExchangeRecord() {
        return new ExchangeRecord((short)1, ExchangeRecord.Type.LEFT, 1);
    }

    /** **/
    public static RollbackRecord buildRollbackRecord() {
        return new RollbackRecord(1, 1, 1, 1);
    }

    /** **/
    public static MetaPageUpdatePartitionDataRecordV2 buildMetaPageUpdatePartitionDataRecordV2() {
        return new MetaPageUpdatePartitionDataRecordV2(1, 1, 1, 1, 1, 1, (byte)1, 1, 1);
    }

    /** **/
    public static MetaPageUpdatePartitionDataRecordV3 buildMetaPageUpdatePartitionDataRecordV3() {
        return new MetaPageUpdatePartitionDataRecordV3(1, 1, 1, 1, 1, 1, (byte)1, 1, 1, 0, 0);
    }

    /** **/
    public static MasterKeyChangeRecordV2 buildMasterKeyChangeRecordV2() {
        return new MasterKeyChangeRecordV2("", Collections.emptyList());
    }

    /** **/
    public static ReencryptionStartRecord buildEncryptionStatusRecord() {
        return new ReencryptionStartRecord(Collections.emptyMap());
    }

    /** **/
    public static RotatedIdPartRecord buildRotatedIdPartRecord() {
        return new RotatedIdPartRecord(1, 1, 2);
    }

    /**
     * Return {@code true} if include to write-ahead log.
     *
     * @param walRecord Instance of {@link WALRecord}.
     * @return {@code True} if include to write-ahead log.
     */
    public static boolean isIncludeIntoLog(WALRecord walRecord) {
        return !UnsupportedWalRecord.class.isInstance(walRecord) && !SwitchSegmentRecord.class.isInstance(walRecord);
    }

    /**
     * Build a fake {@link IndexRenameRootPageRecord}.
     *
     * @return New instance.
     */
    public static IndexRenameRootPageRecord buildIndexRenameRootPageRecord() {
        return new IndexRenameRootPageRecord(
            CU.cacheId("test-cache"),
            "oldTreeName",
            "newTreeName",
            666
        );
    }

    /** **/
    public static PartitionClearingStartRecord buildPartitionClearingStartedRecord() {
        return new PartitionClearingStartRecord(12, 345, 123456789);
    }

    /** **/
    public static ClusterSnapshotRecord buildClusterSnapshotRecord() {
        return new ClusterSnapshotRecord("snp-1234567890");
    }

    /**
     * Creates a new supplier that always return UnsupportedWalRecord for the given {@code type}.
     *
     * @param type WAL record type.
     * @return Supplier that always return UnsupportedWalRecord.
     */
    private static Supplier<WALRecord> buildUnsupportedWalRecord(WALRecord.RecordType type) {
        return () -> new UnsupportedWalRecord(type);
    }

    /** **/
    public static IncrementalSnapshotStartRecord buildIncrementalSnapshotStartRecord() {
        return new IncrementalSnapshotStartRecord(UUID.randomUUID());
    }

    /** **/
    public static IncrementalSnapshotFinishRecord buildIncrementalSnapshotFinishRecord() {
        return new IncrementalSnapshotFinishRecord(
            UUID.randomUUID(), F.asSet(new GridCacheVersion()), F.asSet(new GridCacheVersion()));
    }

    /** **/
    public static CdcManagerRecord buildCdcManagerRecord() {
        return new CdcManagerRecord(new T2<>(new WALPointer(0, 0, 0), 0));
    }

    /** **/
    public static CdcManagerStopRecord buildCdcManagerStopRecord() {
        return new CdcManagerStopRecord();
    }
}

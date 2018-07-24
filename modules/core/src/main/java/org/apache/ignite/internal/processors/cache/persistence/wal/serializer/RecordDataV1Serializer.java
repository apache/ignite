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

package org.apache.ignite.internal.processors.cache.persistence.wal.serializer;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.wal.record.CacheState;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.LazyDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.MemoryRecoveryRecord;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertFragmentRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageRemoveRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageSetFreeListPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageUpdateRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.FixCountRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.FixLeftmostChildRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.FixRemoveId;
import org.apache.ignite.internal.pagemem.wal.record.delta.InitNewPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.InnerReplaceRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.InsertRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MergeRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageAddRootRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageCutRootRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageInitRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageInitRootInlineRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageInitRootRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdateLastAllocatedIndex;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdateLastSuccessfulFullSnapshotId;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdateLastSuccessfulSnapshotId;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdateNextSnapshotId;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdatePartitionDataRecord;
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
import org.apache.ignite.internal.pagemem.wal.record.delta.SplitForwardPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.TrackingPageDeltaRecord;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.record.HeaderRecord;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Record data V1 serializer.
 */
public class RecordDataV1Serializer implements RecordDataSerializer {
    /** Length of HEADER record data. */
    static final int HEADER_RECORD_DATA_SIZE = /*Magic*/8 + /*Version*/4;

    /** Cache shared context */
    private final GridCacheSharedContext cctx;

    /** Size of page used for PageMemory regions */
    private final int pageSize;

    /** Cache object processor to reading {@link DataEntry DataEntries} */
    private final IgniteCacheObjectProcessor co;

    /** Serializer of {@link TxRecord} records. */
    private TxRecordSerializer txRecordSerializer;

    /**
     * @param cctx Cache shared context.
     */
    public RecordDataV1Serializer(GridCacheSharedContext cctx) {
        this.cctx = cctx;
        this.txRecordSerializer = new TxRecordSerializer();
        this.co = cctx.kernalContext().cacheObjects();
        this.pageSize = cctx.database().pageSize();
    }

    /** {@inheritDoc} */
    @Override public int size(WALRecord record) throws IgniteCheckedException {
        switch (record.type()) {
            case PAGE_RECORD:
                assert record instanceof PageSnapshot;

                PageSnapshot pageRec = (PageSnapshot)record;

                return pageRec.pageData().length + 12;

            case CHECKPOINT_RECORD:
                CheckpointRecord cpRec = (CheckpointRecord)record;

                assert cpRec.checkpointMark() == null || cpRec.checkpointMark() instanceof FileWALPointer :
                        "Invalid WAL record: " + cpRec;

                int cacheStatesSize = cacheStatesSize(cpRec.cacheGroupStates());

                FileWALPointer walPtr = (FileWALPointer)cpRec.checkpointMark();

                return 18 + cacheStatesSize + (walPtr == null ? 0 : 16);

            case META_PAGE_INIT:
                return /*cache ID*/4 + /*page ID*/8 + /*ioType*/2  + /*ioVer*/2 +  /*tree root*/8 + /*reuse root*/8;

            case PARTITION_META_PAGE_UPDATE_COUNTERS:
                return /*cache ID*/4 + /*page ID*/8 + /*upd cntr*/8 + /*rmv id*/8 + /*part size*/4 + /*counters page id*/8 + /*state*/ 1
                        + /*allocatedIdxCandidate*/ 4;

            case MEMORY_RECOVERY:
                return 8;

            case PARTITION_DESTROY:
                return /*cacheId*/4 + /*partId*/4;

            case DATA_RECORD:
                DataRecord dataRec = (DataRecord)record;

                return 4 + dataSize(dataRec);

            case METASTORE_DATA_RECORD:
                MetastoreDataRecord metastoreDataRec = (MetastoreDataRecord)record;

                return  4 + metastoreDataRec.key().getBytes().length + 4 +
                    (metastoreDataRec.value() != null ? metastoreDataRec.value().length : 0);

            case HEADER_RECORD:
                return HEADER_RECORD_DATA_SIZE;

            case DATA_PAGE_INSERT_RECORD:
                DataPageInsertRecord diRec = (DataPageInsertRecord)record;

                return 4 + 8 + 2 + diRec.payload().length;

            case DATA_PAGE_UPDATE_RECORD:
                DataPageUpdateRecord uRec = (DataPageUpdateRecord)record;

                return 4 + 8 + 2 + 4 +
                        uRec.payload().length;

            case DATA_PAGE_INSERT_FRAGMENT_RECORD:
                final DataPageInsertFragmentRecord difRec = (DataPageInsertFragmentRecord)record;

                return 4 + 8 + 8 + 4 + difRec.payloadSize();

            case DATA_PAGE_REMOVE_RECORD:
                return 4 + 8 + 1;

            case DATA_PAGE_SET_FREE_LIST_PAGE:
                return 4 + 8 + 8;

            case INIT_NEW_PAGE_RECORD:
                return 4 + 8 + 2 + 2 + 8;

            case BTREE_META_PAGE_INIT_ROOT:
                return 4 + 8 + 8;

            case BTREE_META_PAGE_INIT_ROOT2:
                return 4 + 8 + 8 + 2;

            case BTREE_META_PAGE_ADD_ROOT:
                return 4 + 8 + 8;

            case BTREE_META_PAGE_CUT_ROOT:
                return 4 + 8;

            case BTREE_INIT_NEW_ROOT:
                NewRootInitRecord<?> riRec = (NewRootInitRecord<?>)record;

                return 4 + 8 + 8 + 2 + 2 + 8 + 8 + riRec.io().getItemSize();

            case BTREE_PAGE_RECYCLE:
                return 4 + 8 + 8;

            case BTREE_PAGE_INSERT:
                InsertRecord<?> inRec = (InsertRecord<?>)record;

                return 4 + 8 + 2 + 2 + 2 + 8 + inRec.io().getItemSize();

            case BTREE_FIX_LEFTMOST_CHILD:
                return 4 + 8 + 8;

            case BTREE_FIX_COUNT:
                return 4 + 8 + 2;

            case BTREE_PAGE_REPLACE:
                ReplaceRecord<?> rRec = (ReplaceRecord<?>)record;

                return 4 + 8 + 2 + 2 + 2 + rRec.io().getItemSize();

            case BTREE_PAGE_REMOVE:
                return 4 + 8 + 2 + 2;

            case BTREE_PAGE_INNER_REPLACE:
                return 4 + 8 + 2 + 8 + 2 + 8;

            case BTREE_FORWARD_PAGE_SPLIT:
                return 4 + 8 + 8 + 2 + 2 + 8 + 2 + 2;

            case BTREE_EXISTING_PAGE_SPLIT:
                return 4 + 8 + 2 + 8;

            case BTREE_PAGE_MERGE:
                return 4 + 8 + 8 + 2 + 8 + 1;

            case BTREE_FIX_REMOVE_ID:
                return 4 + 8 + 8;

            case PAGES_LIST_SET_NEXT:
                return 4 + 8 + 8;

            case PAGES_LIST_SET_PREVIOUS:
                return 4 + 8 + 8;

            case PAGES_LIST_INIT_NEW_PAGE:
                return 4 + 8 + 4 + 4 + 8 + 8 + 8;

            case PAGES_LIST_ADD_PAGE:
                return 4 + 8 + 8;

            case PAGES_LIST_REMOVE_PAGE:
                return 4 + 8 + 8;

            case TRACKING_PAGE_DELTA:
                return 4 + 8 + 8 + 8 + 8;

            case META_PAGE_UPDATE_LAST_SUCCESSFUL_SNAPSHOT_ID:
                return 4 + 8 + 8 + 8;

            case META_PAGE_UPDATE_LAST_SUCCESSFUL_FULL_SNAPSHOT_ID:
                return 4 + 8 + 8;

            case META_PAGE_UPDATE_NEXT_SNAPSHOT_ID:
                return 4 + 8 + 8;

            case META_PAGE_UPDATE_LAST_ALLOCATED_INDEX:
                return 4 + 8 + 4;

            case PART_META_UPDATE_STATE:
                return /*cacheId*/ 4 + /*partId*/ 4 + /*State*/1 + /*Update Counter*/ 8;

            case PAGE_LIST_META_RESET_COUNT_RECORD:
                return /*cacheId*/ 4 + /*pageId*/ 8;

            case ROTATED_ID_PART_RECORD:
                return 4 + 8 + 1;

            case SWITCH_SEGMENT_RECORD:
                return 0;

            case TX_RECORD:
                return txRecordSerializer.size((TxRecord)record);

            default:
                throw new UnsupportedOperationException("Type: " + record.type());
        }
    }

    /** {@inheritDoc} */
    @Override public WALRecord readRecord(WALRecord.RecordType type, ByteBufferBackedDataInput in) throws IOException, IgniteCheckedException {
        WALRecord res;

        switch (type) {
            case PAGE_RECORD:
                byte[] arr = new byte[pageSize];

                int cacheId = in.readInt();
                long pageId = in.readLong();

                in.readFully(arr);

                res = new PageSnapshot(new FullPageId(pageId, cacheId), arr);

                break;

            case CHECKPOINT_RECORD:
                long msb = in.readLong();
                long lsb = in.readLong();
                boolean hasPtr = in.readByte() != 0;
                int idx = hasPtr ? in.readInt() : 0;
                int off = hasPtr ? in.readInt() : 0;
                int len = hasPtr ? in.readInt() : 0;

                Map<Integer, CacheState> states = readPartitionStates(in);

                boolean end = in.readByte() != 0;

                FileWALPointer walPtr = hasPtr ? new FileWALPointer(idx, off, len) : null;

                CheckpointRecord cpRec = new CheckpointRecord(new UUID(msb, lsb), walPtr, end);

                cpRec.cacheGroupStates(states);

                res = cpRec;

                break;

            case META_PAGE_INIT:
                cacheId = in.readInt();
                pageId = in.readLong();

                int ioType = in.readUnsignedShort();
                int ioVer = in.readUnsignedShort();
                long treeRoot = in.readLong();
                long reuseListRoot = in.readLong();

                res = new MetaPageInitRecord(cacheId, pageId, ioType, ioVer, treeRoot, reuseListRoot);

                break;

            case PARTITION_META_PAGE_UPDATE_COUNTERS:
                cacheId = in.readInt();
                pageId = in.readLong();

                long updCntr = in.readLong();
                long rmvId = in.readLong();
                int partSize = in.readInt();
                long countersPageId = in.readLong();
                byte state = in.readByte();
                int allocatedIdxCandidate = in.readInt();

                res = new MetaPageUpdatePartitionDataRecord(cacheId, pageId, updCntr, rmvId, partSize, countersPageId, state, allocatedIdxCandidate);

                break;

            case MEMORY_RECOVERY:
                long ts = in.readLong();

                res = new MemoryRecoveryRecord(ts);

                break;

            case PARTITION_DESTROY:
                cacheId = in.readInt();
                int partId = in.readInt();

                res = new PartitionDestroyRecord(cacheId, partId);

                break;

            case DATA_RECORD:
                int entryCnt = in.readInt();

                List<DataEntry> entries = new ArrayList<>(entryCnt);

                for (int i = 0; i < entryCnt; i++)
                    entries.add(readDataEntry(in));

                res = new DataRecord(entries, 0L);

                break;

            case METASTORE_DATA_RECORD:
                int strLen = in.readInt();

                byte[] strBytes = new byte[strLen];

                in.readFully(strBytes);

                String key = new String(strBytes);

                int valLen = in.readInt();

                assert valLen >= 0;

                byte[] val;

                if (valLen > 0) {
                    val = new byte[valLen];

                    in.readFully(val);
                }
                else
                    val = null;

                return new MetastoreDataRecord(key, val);

            case HEADER_RECORD:
                long magic = in.readLong();

                if (magic != HeaderRecord.REGULAR_MAGIC && magic != HeaderRecord.COMPACTED_MAGIC)
                    throw new EOFException("Magic is corrupted [actual=" + U.hexLong(magic) + ']');

                int ver = in.readInt();

                res = new HeaderRecord(ver);

                break;

            case DATA_PAGE_INSERT_RECORD: {
                cacheId = in.readInt();
                pageId = in.readLong();

                int size = in.readUnsignedShort();

                in.ensure(size);

                byte[] payload = new byte[size];

                in.readFully(payload);

                res = new DataPageInsertRecord(cacheId, pageId, payload);

                break;
            }

            case DATA_PAGE_UPDATE_RECORD: {
                cacheId = in.readInt();
                pageId = in.readLong();

                int itemId = in.readInt();

                int size = in.readUnsignedShort();

                in.ensure(size);

                byte[] payload = new byte[size];

                in.readFully(payload);

                res = new DataPageUpdateRecord(cacheId, pageId, itemId, payload);

                break;
            }

            case DATA_PAGE_INSERT_FRAGMENT_RECORD: {
                cacheId = in.readInt();
                pageId = in.readLong();

                final long lastLink = in.readLong();
                final int payloadSize = in.readInt();

                final byte[] payload = new byte[payloadSize];

                in.readFully(payload);

                res = new DataPageInsertFragmentRecord(cacheId, pageId, payload, lastLink);

                break;
            }

            case DATA_PAGE_REMOVE_RECORD:
                cacheId = in.readInt();
                pageId = in.readLong();

                int itemId = in.readUnsignedByte();

                res = new DataPageRemoveRecord(cacheId, pageId, itemId);

                break;

            case DATA_PAGE_SET_FREE_LIST_PAGE:
                cacheId = in.readInt();
                pageId = in.readLong();

                long freeListPage = in.readLong();

                res = new DataPageSetFreeListPageRecord(cacheId, pageId, freeListPage);

                break;

            case INIT_NEW_PAGE_RECORD:
                cacheId = in.readInt();
                pageId = in.readLong();

                ioType = in.readUnsignedShort();
                ioVer = in.readUnsignedShort();
                long virtualPageId = in.readLong();

                res = new InitNewPageRecord(cacheId, pageId, ioType, ioVer, virtualPageId);

                break;

            case BTREE_META_PAGE_INIT_ROOT:
                cacheId = in.readInt();
                pageId = in.readLong();

                long rootId = in.readLong();

                res = new MetaPageInitRootRecord(cacheId, pageId, rootId);

                break;

            case BTREE_META_PAGE_INIT_ROOT2:
                cacheId = in.readInt();
                pageId = in.readLong();

                long rootId2 = in.readLong();
                int inlineSize = in.readShort();

                res = new MetaPageInitRootInlineRecord(cacheId, pageId, rootId2, inlineSize);

                break;

            case BTREE_META_PAGE_ADD_ROOT:
                cacheId = in.readInt();
                pageId = in.readLong();

                rootId = in.readLong();

                res = new MetaPageAddRootRecord(cacheId, pageId, rootId);

                break;

            case BTREE_META_PAGE_CUT_ROOT:
                cacheId = in.readInt();
                pageId = in.readLong();

                res = new MetaPageCutRootRecord(cacheId, pageId);

                break;

            case BTREE_INIT_NEW_ROOT:
                cacheId = in.readInt();
                pageId = in.readLong();

                rootId = in.readLong();
                ioType = in.readUnsignedShort();
                ioVer = in.readUnsignedShort();
                long leftId = in.readLong();
                long rightId = in.readLong();

                BPlusIO<?> io = BPlusIO.getBPlusIO(ioType, ioVer);

                byte[] rowBytes = new byte[io.getItemSize()];

                in.readFully(rowBytes);

                res = new NewRootInitRecord<>(cacheId, pageId, rootId, (BPlusInnerIO<?>)io, leftId, rowBytes, rightId);

                break;

            case BTREE_PAGE_RECYCLE:
                cacheId = in.readInt();
                pageId = in.readLong();

                long newPageId = in.readLong();

                res = new RecycleRecord(cacheId, pageId, newPageId);

                break;

            case BTREE_PAGE_INSERT:
                cacheId = in.readInt();
                pageId = in.readLong();

                ioType = in.readUnsignedShort();
                ioVer = in.readUnsignedShort();
                int itemIdx = in.readUnsignedShort();
                rightId = in.readLong();

                io = BPlusIO.getBPlusIO(ioType, ioVer);

                rowBytes = new byte[io.getItemSize()];

                in.readFully(rowBytes);

                res = new InsertRecord<>(cacheId, pageId, io, itemIdx, rowBytes, rightId);

                break;

            case BTREE_FIX_LEFTMOST_CHILD:
                cacheId = in.readInt();
                pageId = in.readLong();

                rightId = in.readLong();

                res = new FixLeftmostChildRecord(cacheId, pageId, rightId);

                break;

            case BTREE_FIX_COUNT:
                cacheId = in.readInt();
                pageId = in.readLong();

                int cnt = in.readUnsignedShort();

                res = new FixCountRecord(cacheId, pageId, cnt);

                break;

            case BTREE_PAGE_REPLACE:
                cacheId = in.readInt();
                pageId = in.readLong();

                ioType = in.readUnsignedShort();
                ioVer = in.readUnsignedShort();
                itemIdx = in.readUnsignedShort();

                io = BPlusIO.getBPlusIO(ioType, ioVer);

                rowBytes = new byte[io.getItemSize()];

                in.readFully(rowBytes);

                res = new ReplaceRecord<>(cacheId, pageId, io, rowBytes, itemIdx);

                break;

            case BTREE_PAGE_REMOVE:
                cacheId = in.readInt();
                pageId = in.readLong();

                itemIdx = in.readUnsignedShort();
                cnt = in.readUnsignedShort();

                res = new RemoveRecord(cacheId, pageId, itemIdx, cnt);

                break;

            case BTREE_PAGE_INNER_REPLACE:
                cacheId = in.readInt();
                pageId = in.readLong();

                int dstIdx = in.readUnsignedShort();
                long srcPageId = in.readLong();
                int srcIdx = in.readUnsignedShort();
                rmvId = in.readLong();

                res = new InnerReplaceRecord<>(cacheId, pageId, dstIdx, srcPageId, srcIdx, rmvId);

                break;

            case BTREE_FORWARD_PAGE_SPLIT:
                cacheId = in.readInt();
                pageId = in.readLong();

                long fwdId = in.readLong();
                ioType = in.readUnsignedShort();
                ioVer = in.readUnsignedShort();
                srcPageId = in.readLong();
                int mid = in.readUnsignedShort();
                cnt = in.readUnsignedShort();

                res = new SplitForwardPageRecord(cacheId, pageId, fwdId, ioType, ioVer, srcPageId, mid, cnt);

                break;

            case BTREE_EXISTING_PAGE_SPLIT:
                cacheId = in.readInt();
                pageId = in.readLong();

                mid = in.readUnsignedShort();
                fwdId = in.readLong();

                res = new SplitExistingPageRecord(cacheId, pageId, mid, fwdId);

                break;

            case BTREE_PAGE_MERGE:
                cacheId = in.readInt();
                pageId = in.readLong();

                long prntId = in.readLong();
                int prntIdx = in.readUnsignedShort();
                rightId = in.readLong();
                boolean emptyBranch = in.readBoolean();

                res = new MergeRecord<>(cacheId, pageId, prntId, prntIdx, rightId, emptyBranch);

                break;

            case BTREE_FIX_REMOVE_ID:
                cacheId = in.readInt();
                pageId = in.readLong();

                rmvId = in.readLong();

                res = new FixRemoveId(cacheId, pageId, rmvId);

                break;

            case PAGES_LIST_SET_NEXT:
                cacheId = in.readInt();
                pageId = in.readLong();
                long nextPageId = in.readLong();

                res = new PagesListSetNextRecord(cacheId, pageId, nextPageId);

                break;

            case PAGES_LIST_SET_PREVIOUS:
                cacheId = in.readInt();
                pageId = in.readLong();
                long prevPageId = in.readLong();

                res = new PagesListSetPreviousRecord(cacheId, pageId, prevPageId);

                break;

            case PAGES_LIST_INIT_NEW_PAGE:
                cacheId = in.readInt();
                pageId = in.readLong();
                ioType = in.readInt();
                ioVer = in.readInt();
                newPageId = in.readLong();
                prevPageId = in.readLong();
                long addDataPageId = in.readLong();

                res = new PagesListInitNewPageRecord(cacheId, pageId, ioType, ioVer, newPageId, prevPageId, addDataPageId);

                break;

            case PAGES_LIST_ADD_PAGE:
                cacheId = in.readInt();
                pageId = in.readLong();
                long dataPageId = in.readLong();

                res = new PagesListAddPageRecord(cacheId, pageId, dataPageId);

                break;

            case PAGES_LIST_REMOVE_PAGE:
                cacheId = in.readInt();
                pageId = in.readLong();
                long rmvdPageId = in.readLong();

                res = new PagesListRemovePageRecord(cacheId, pageId, rmvdPageId);

                break;

            case TRACKING_PAGE_DELTA:
                cacheId = in.readInt();
                pageId = in.readLong();

                long pageIdToMark = in.readLong();
                long nextSnapshotId0 = in.readLong();
                long lastSuccessfulSnapshotId0 = in.readLong();

                res = new TrackingPageDeltaRecord(cacheId, pageId, pageIdToMark, nextSnapshotId0, lastSuccessfulSnapshotId0);

                break;

            case META_PAGE_UPDATE_NEXT_SNAPSHOT_ID:
                cacheId = in.readInt();
                pageId = in.readLong();

                long nextSnapshotId = in.readLong();

                res = new MetaPageUpdateNextSnapshotId(cacheId, pageId, nextSnapshotId);

                break;

            case META_PAGE_UPDATE_LAST_SUCCESSFUL_FULL_SNAPSHOT_ID:
                cacheId = in.readInt();
                pageId = in.readLong();

                long lastSuccessfulFullSnapshotId = in.readLong();

                res = new MetaPageUpdateLastSuccessfulFullSnapshotId(cacheId, pageId, lastSuccessfulFullSnapshotId);

                break;

            case META_PAGE_UPDATE_LAST_SUCCESSFUL_SNAPSHOT_ID:
                cacheId = in.readInt();
                pageId = in.readLong();

                long lastSuccessfulSnapshotId = in.readLong();
                long lastSuccessfulSnapshotTag = in.readLong();

                res = new MetaPageUpdateLastSuccessfulSnapshotId(cacheId, pageId, lastSuccessfulSnapshotId, lastSuccessfulSnapshotTag);

                break;

            case META_PAGE_UPDATE_LAST_ALLOCATED_INDEX:
                cacheId = in.readInt();
                pageId = in.readLong();

                int lastAllocatedIdx = in.readInt();

                res = new MetaPageUpdateLastAllocatedIndex(cacheId, pageId, lastAllocatedIdx);

                break;

            case PART_META_UPDATE_STATE:
                cacheId = in.readInt();

                partId = in.readInt();

                state = in.readByte();

                long updateCntr = in.readLong();

                GridDhtPartitionState partState = GridDhtPartitionState.fromOrdinal(state);

                res = new PartitionMetaStateRecord(cacheId, partId, partState, updateCntr);

                break;

            case PAGE_LIST_META_RESET_COUNT_RECORD:
                cacheId = in.readInt();
                pageId = in.readLong();

                res = new PageListMetaResetCountRecord(cacheId, pageId);
                break;

            case ROTATED_ID_PART_RECORD:
                cacheId = in.readInt();
                pageId = in.readLong();

                byte rotatedIdPart = in.readByte();

                res = new RotatedIdPartRecord(cacheId, pageId, rotatedIdPart);

                break;

            case SWITCH_SEGMENT_RECORD:
                throw new EOFException("END OF SEGMENT");

            case TX_RECORD:
                res = txRecordSerializer.read(in);

                break;

            default:
                throw new UnsupportedOperationException("Type: " + type);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void writeRecord(WALRecord rec, ByteBuffer buf) throws IgniteCheckedException {
        switch (rec.type()) {
            case PAGE_RECORD:
                PageSnapshot snap = (PageSnapshot)rec;

                buf.putInt(snap.fullPageId().groupId());
                buf.putLong(snap.fullPageId().pageId());
                buf.put(snap.pageData());

                break;

            case MEMORY_RECOVERY:
                MemoryRecoveryRecord memoryRecoveryRecord = (MemoryRecoveryRecord)rec;

                buf.putLong(memoryRecoveryRecord.time());

                break;

            case PARTITION_DESTROY:
                PartitionDestroyRecord partDestroy = (PartitionDestroyRecord)rec;

                buf.putInt(partDestroy.groupId());
                buf.putInt(partDestroy.partitionId());

                break;

            case META_PAGE_INIT:
                MetaPageInitRecord updRootsRec = (MetaPageInitRecord)rec;

                buf.putInt(updRootsRec.groupId());
                buf.putLong(updRootsRec.pageId());

                buf.putShort((short)updRootsRec.ioType());
                buf.putShort((short)updRootsRec.ioVersion());
                buf.putLong(updRootsRec.treeRoot());
                buf.putLong(updRootsRec.reuseListRoot());

                break;

            case PARTITION_META_PAGE_UPDATE_COUNTERS:
                MetaPageUpdatePartitionDataRecord partDataRec = (MetaPageUpdatePartitionDataRecord)rec;

                buf.putInt(partDataRec.groupId());
                buf.putLong(partDataRec.pageId());

                buf.putLong(partDataRec.updateCounter());
                buf.putLong(partDataRec.globalRemoveId());
                buf.putInt(partDataRec.partitionSize());
                buf.putLong(partDataRec.countersPageId());
                buf.put(partDataRec.state());
                buf.putInt(partDataRec.allocatedIndexCandidate());

                break;

            case CHECKPOINT_RECORD:
                CheckpointRecord cpRec = (CheckpointRecord)rec;

                assert cpRec.checkpointMark() == null || cpRec.checkpointMark() instanceof FileWALPointer :
                        "Invalid WAL record: " + cpRec;

                FileWALPointer walPtr = (FileWALPointer)cpRec.checkpointMark();
                UUID cpId = cpRec.checkpointId();

                buf.putLong(cpId.getMostSignificantBits());
                buf.putLong(cpId.getLeastSignificantBits());

                buf.put(walPtr == null ? (byte)0 : 1);

                if (walPtr != null) {
                    buf.putLong(walPtr.index());
                    buf.putInt(walPtr.fileOffset());
                    buf.putInt(walPtr.length());
                }

                putCacheStates(buf, cpRec.cacheGroupStates());

                buf.put(cpRec.end() ? (byte)1 : 0);

                break;

            case DATA_RECORD:
                DataRecord dataRec = (DataRecord)rec;

                buf.putInt(dataRec.writeEntries().size());

                for (DataEntry dataEntry : dataRec.writeEntries())
                    putDataEntry(buf, dataEntry);

                break;

            case METASTORE_DATA_RECORD:
                MetastoreDataRecord metastoreDataRecord = (MetastoreDataRecord)rec;

                byte[] strBytes = metastoreDataRecord.key().getBytes();

                buf.putInt(strBytes.length);
                buf.put(strBytes);
                if (metastoreDataRecord.value() != null) {
                    buf.putInt(metastoreDataRecord.value().length);
                    buf.put(metastoreDataRecord.value());
                }
                else
                    buf.putInt(0);

                break;

            case HEADER_RECORD:
                buf.putLong(HeaderRecord.REGULAR_MAGIC);

                buf.putInt(((HeaderRecord)rec).version());

                break;

            case DATA_PAGE_INSERT_RECORD:
                DataPageInsertRecord diRec = (DataPageInsertRecord)rec;

                buf.putInt(diRec.groupId());
                buf.putLong(diRec.pageId());

                buf.putShort((short)diRec.payload().length);

                buf.put(diRec.payload());

                break;

            case DATA_PAGE_UPDATE_RECORD:
                DataPageUpdateRecord uRec = (DataPageUpdateRecord)rec;

                buf.putInt(uRec.groupId());
                buf.putLong(uRec.pageId());
                buf.putInt(uRec.itemId());

                buf.putShort((short)uRec.payload().length);

                buf.put(uRec.payload());

                break;

            case DATA_PAGE_INSERT_FRAGMENT_RECORD:
                final DataPageInsertFragmentRecord difRec = (DataPageInsertFragmentRecord)rec;

                buf.putInt(difRec.groupId());
                buf.putLong(difRec.pageId());

                buf.putLong(difRec.lastLink());
                buf.putInt(difRec.payloadSize());
                buf.put(difRec.payload());

                break;

            case DATA_PAGE_REMOVE_RECORD:
                DataPageRemoveRecord drRec = (DataPageRemoveRecord)rec;

                buf.putInt(drRec.groupId());
                buf.putLong(drRec.pageId());

                buf.put((byte)drRec.itemId());

                break;

            case DATA_PAGE_SET_FREE_LIST_PAGE:
                DataPageSetFreeListPageRecord freeListRec = (DataPageSetFreeListPageRecord)rec;

                buf.putInt(freeListRec.groupId());
                buf.putLong(freeListRec.pageId());

                buf.putLong(freeListRec.freeListPage());

                break;

            case INIT_NEW_PAGE_RECORD:
                InitNewPageRecord inpRec = (InitNewPageRecord)rec;

                buf.putInt(inpRec.groupId());
                buf.putLong(inpRec.pageId());

                buf.putShort((short)inpRec.ioType());
                buf.putShort((short)inpRec.ioVersion());
                buf.putLong(inpRec.newPageId());

                break;

            case BTREE_META_PAGE_INIT_ROOT:
                MetaPageInitRootRecord imRec = (MetaPageInitRootRecord)rec;

                buf.putInt(imRec.groupId());
                buf.putLong(imRec.pageId());

                buf.putLong(imRec.rootId());

                break;

            case BTREE_META_PAGE_INIT_ROOT2:
                MetaPageInitRootInlineRecord imRec2 = (MetaPageInitRootInlineRecord)rec;

                buf.putInt(imRec2.groupId());
                buf.putLong(imRec2.pageId());

                buf.putLong(imRec2.rootId());

                buf.putShort((short)imRec2.inlineSize());
                break;

            case BTREE_META_PAGE_ADD_ROOT:
                MetaPageAddRootRecord arRec = (MetaPageAddRootRecord)rec;

                buf.putInt(arRec.groupId());
                buf.putLong(arRec.pageId());

                buf.putLong(arRec.rootId());

                break;

            case BTREE_META_PAGE_CUT_ROOT:
                MetaPageCutRootRecord crRec = (MetaPageCutRootRecord)rec;

                buf.putInt(crRec.groupId());
                buf.putLong(crRec.pageId());

                break;

            case BTREE_INIT_NEW_ROOT:
                NewRootInitRecord<?> riRec = (NewRootInitRecord<?>)rec;

                buf.putInt(riRec.groupId());
                buf.putLong(riRec.pageId());

                buf.putLong(riRec.rootId());
                buf.putShort((short)riRec.io().getType());
                buf.putShort((short)riRec.io().getVersion());
                buf.putLong(riRec.leftId());
                buf.putLong(riRec.rightId());

                putRow(buf, riRec.rowBytes());

                break;

            case BTREE_PAGE_RECYCLE:
                RecycleRecord recRec = (RecycleRecord)rec;

                buf.putInt(recRec.groupId());
                buf.putLong(recRec.pageId());

                buf.putLong(recRec.newPageId());

                break;

            case BTREE_PAGE_INSERT:
                InsertRecord<?> inRec = (InsertRecord<?>)rec;

                buf.putInt(inRec.groupId());
                buf.putLong(inRec.pageId());

                buf.putShort((short)inRec.io().getType());
                buf.putShort((short)inRec.io().getVersion());
                buf.putShort((short)inRec.index());
                buf.putLong(inRec.rightId());

                putRow(buf, inRec.rowBytes());

                break;

            case BTREE_FIX_LEFTMOST_CHILD:
                FixLeftmostChildRecord flRec = (FixLeftmostChildRecord)rec;

                buf.putInt(flRec.groupId());
                buf.putLong(flRec.pageId());

                buf.putLong(flRec.rightId());

                break;

            case BTREE_FIX_COUNT:
                FixCountRecord fcRec = (FixCountRecord)rec;

                buf.putInt(fcRec.groupId());
                buf.putLong(fcRec.pageId());

                buf.putShort((short)fcRec.count());

                break;

            case BTREE_PAGE_REPLACE:
                ReplaceRecord<?> rRec = (ReplaceRecord<?>)rec;

                buf.putInt(rRec.groupId());
                buf.putLong(rRec.pageId());

                buf.putShort((short)rRec.io().getType());
                buf.putShort((short)rRec.io().getVersion());
                buf.putShort((short)rRec.index());

                putRow(buf, rRec.rowBytes());

                break;

            case BTREE_PAGE_REMOVE:
                RemoveRecord rmRec = (RemoveRecord)rec;

                buf.putInt(rmRec.groupId());
                buf.putLong(rmRec.pageId());

                buf.putShort((short)rmRec.index());
                buf.putShort((short)rmRec.count());

                break;

            case BTREE_PAGE_INNER_REPLACE:
                InnerReplaceRecord<?> irRec = (InnerReplaceRecord<?>)rec;

                buf.putInt(irRec.groupId());
                buf.putLong(irRec.pageId());

                buf.putShort((short)irRec.destinationIndex());
                buf.putLong(irRec.sourcePageId());
                buf.putShort((short)irRec.sourceIndex());
                buf.putLong(irRec.removeId());

                break;

            case BTREE_FORWARD_PAGE_SPLIT:
                SplitForwardPageRecord sfRec = (SplitForwardPageRecord)rec;

                buf.putInt(sfRec.groupId());
                buf.putLong(sfRec.pageId());

                buf.putLong(sfRec.forwardId());
                buf.putShort((short)sfRec.ioType());
                buf.putShort((short)sfRec.ioVersion());
                buf.putLong(sfRec.sourcePageId());
                buf.putShort((short)sfRec.middleIndex());
                buf.putShort((short)sfRec.count());

                break;

            case BTREE_EXISTING_PAGE_SPLIT:
                SplitExistingPageRecord seRec = (SplitExistingPageRecord)rec;

                buf.putInt(seRec.groupId());
                buf.putLong(seRec.pageId());

                buf.putShort((short)seRec.middleIndex());
                buf.putLong(seRec.forwardId());

                break;

            case BTREE_PAGE_MERGE:
                MergeRecord<?> mRec = (MergeRecord<?>)rec;

                buf.putInt(mRec.groupId());
                buf.putLong(mRec.pageId());

                buf.putLong(mRec.parentId());
                buf.putShort((short)mRec.parentIndex());
                buf.putLong(mRec.rightId());
                buf.put((byte)(mRec.isEmptyBranch() ? 1 : 0));

                break;

            case PAGES_LIST_SET_NEXT:
                PagesListSetNextRecord plNextRec = (PagesListSetNextRecord)rec;

                buf.putInt(plNextRec.groupId());
                buf.putLong(plNextRec.pageId());

                buf.putLong(plNextRec.nextPageId());

                break;

            case PAGES_LIST_SET_PREVIOUS:
                PagesListSetPreviousRecord plPrevRec = (PagesListSetPreviousRecord)rec;

                buf.putInt(plPrevRec.groupId());
                buf.putLong(plPrevRec.pageId());

                buf.putLong(plPrevRec.previousPageId());

                break;

            case PAGES_LIST_INIT_NEW_PAGE:
                PagesListInitNewPageRecord plNewRec = (PagesListInitNewPageRecord)rec;

                buf.putInt(plNewRec.groupId());
                buf.putLong(plNewRec.pageId());
                buf.putInt(plNewRec.ioType());
                buf.putInt(plNewRec.ioVersion());
                buf.putLong(plNewRec.newPageId());

                buf.putLong(plNewRec.previousPageId());
                buf.putLong(plNewRec.dataPageId());

                break;

            case PAGES_LIST_ADD_PAGE:
                PagesListAddPageRecord plAddRec = (PagesListAddPageRecord)rec;

                buf.putInt(plAddRec.groupId());
                buf.putLong(plAddRec.pageId());

                buf.putLong(plAddRec.dataPageId());

                break;

            case PAGES_LIST_REMOVE_PAGE:
                PagesListRemovePageRecord plRmvRec = (PagesListRemovePageRecord)rec;

                buf.putInt(plRmvRec.groupId());
                buf.putLong(plRmvRec.pageId());

                buf.putLong(plRmvRec.removedPageId());

                break;

            case BTREE_FIX_REMOVE_ID:
                FixRemoveId frRec = (FixRemoveId)rec;

                buf.putInt(frRec.groupId());
                buf.putLong(frRec.pageId());

                buf.putLong(frRec.removeId());

                break;

            case TRACKING_PAGE_DELTA:
                TrackingPageDeltaRecord tpDelta = (TrackingPageDeltaRecord)rec;

                buf.putInt(tpDelta.groupId());
                buf.putLong(tpDelta.pageId());

                buf.putLong(tpDelta.pageIdToMark());
                buf.putLong(tpDelta.nextSnapshotId());
                buf.putLong(tpDelta.lastSuccessfulSnapshotId());

                break;

            case META_PAGE_UPDATE_NEXT_SNAPSHOT_ID:
                MetaPageUpdateNextSnapshotId mpUpdateNextSnapshotId = (MetaPageUpdateNextSnapshotId)rec;

                buf.putInt(mpUpdateNextSnapshotId.groupId());
                buf.putLong(mpUpdateNextSnapshotId.pageId());

                buf.putLong(mpUpdateNextSnapshotId.nextSnapshotId());

                break;

            case META_PAGE_UPDATE_LAST_SUCCESSFUL_FULL_SNAPSHOT_ID:
                MetaPageUpdateLastSuccessfulFullSnapshotId mpUpdateLastSuccFullSnapshotId =
                        (MetaPageUpdateLastSuccessfulFullSnapshotId)rec;

                buf.putInt(mpUpdateLastSuccFullSnapshotId.groupId());
                buf.putLong(mpUpdateLastSuccFullSnapshotId.pageId());

                buf.putLong(mpUpdateLastSuccFullSnapshotId.lastSuccessfulFullSnapshotId());

                break;

            case META_PAGE_UPDATE_LAST_SUCCESSFUL_SNAPSHOT_ID:
                MetaPageUpdateLastSuccessfulSnapshotId mpUpdateLastSuccSnapshotId =
                        (MetaPageUpdateLastSuccessfulSnapshotId)rec;

                buf.putInt(mpUpdateLastSuccSnapshotId.groupId());
                buf.putLong(mpUpdateLastSuccSnapshotId.pageId());

                buf.putLong(mpUpdateLastSuccSnapshotId.lastSuccessfulSnapshotId());
                buf.putLong(mpUpdateLastSuccSnapshotId.lastSuccessfulSnapshotTag());

                break;

            case META_PAGE_UPDATE_LAST_ALLOCATED_INDEX:
                MetaPageUpdateLastAllocatedIndex mpUpdateLastAllocatedIdx =
                        (MetaPageUpdateLastAllocatedIndex) rec;

                buf.putInt(mpUpdateLastAllocatedIdx.groupId());
                buf.putLong(mpUpdateLastAllocatedIdx.pageId());

                buf.putInt(mpUpdateLastAllocatedIdx.lastAllocatedIndex());

                break;

            case PART_META_UPDATE_STATE:
                PartitionMetaStateRecord partMetaStateRecord = (PartitionMetaStateRecord) rec;

                buf.putInt(partMetaStateRecord.groupId());

                buf.putInt(partMetaStateRecord.partitionId());

                buf.put(partMetaStateRecord.state());

                buf.putLong(partMetaStateRecord.updateCounter());

                break;

            case PAGE_LIST_META_RESET_COUNT_RECORD:
                PageListMetaResetCountRecord pageListMetaResetCntRecord = (PageListMetaResetCountRecord) rec;

                buf.putInt(pageListMetaResetCntRecord.groupId());
                buf.putLong(pageListMetaResetCntRecord.pageId());

                break;

            case ROTATED_ID_PART_RECORD:
                RotatedIdPartRecord rotatedIdPartRecord = (RotatedIdPartRecord) rec;

                buf.putInt(rotatedIdPartRecord.groupId());
                buf.putLong(rotatedIdPartRecord.pageId());

                buf.put(rotatedIdPartRecord.rotatedIdPart());

                break;

            case TX_RECORD:
                txRecordSerializer.write((TxRecord)rec, buf);

                break;

            case SWITCH_SEGMENT_RECORD:
                break;

            default:
                throw new UnsupportedOperationException("Type: " + rec.type());
        }
    }

    /**
     * Return shared cache context.
     *
     * @return Shared cache context.
     */
    public GridCacheSharedContext cctx() {
        return cctx;
    }

    /**
     * @param buf Buffer to write to.
     * @param entry Data entry.
     */
    static void putDataEntry(ByteBuffer buf, DataEntry entry) throws IgniteCheckedException {
        buf.putInt(entry.cacheId());

        if (!entry.key().putValue(buf))
            throw new AssertionError();

        if (entry.value() == null)
            buf.putInt(-1);
        else if (!entry.value().putValue(buf))
            throw new AssertionError();

        buf.put((byte)entry.op().ordinal());

        putVersion(buf, entry.nearXidVersion(), true);
        putVersion(buf, entry.writeVersion(), false);

        buf.putInt(entry.partitionId());
        buf.putLong(entry.partitionCounter());
        buf.putLong(entry.expireTime());
    }

    /**
     * @param states Cache states.
     */
    private static void putCacheStates(ByteBuffer buf, Map<Integer, CacheState> states) {
        buf.putShort((short)states.size());

        for (Map.Entry<Integer, CacheState> entry : states.entrySet()) {
            buf.putInt(entry.getKey());

            CacheState state = entry.getValue();

            // Need 2 bytes for the number of partitions.
            buf.putShort((short)state.size());

            for (int i = 0; i < state.size(); i++) {
                buf.putShort((short)state.partitionByIndex(i));

                buf.putLong(state.partitionSizeByIndex(i));
                buf.putLong(state.partitionCounterByIndex(i));
            }
        }
    }

    /**
     * @param buf Buffer.
     * @param ver Version to write.
     * @param allowNull Is {@code null}version allowed.
     */
    private static void putVersion(ByteBuffer buf, GridCacheVersion ver, boolean allowNull) {
        CacheVersionIO.write(buf, ver, allowNull);
    }

    /**
     * @param buf Buffer.
     * @param rowBytes Row bytes.
     */
    @SuppressWarnings("unchecked")
    private static void putRow(ByteBuffer buf, byte[] rowBytes) {
        assert rowBytes.length > 0;

        buf.put(rowBytes);
    }

    /**
     * @param in Input to read from.
     * @return Read entry.
     */
    DataEntry readDataEntry(ByteBufferBackedDataInput in) throws IOException, IgniteCheckedException {
        int cacheId = in.readInt();

        int keySize = in.readInt();
        byte keyType = in.readByte();
        byte[] keyBytes = new byte[keySize];
        in.readFully(keyBytes);

        int valSize = in.readInt();

        byte valType = 0;
        byte[] valBytes = null;

        if (valSize >= 0) {
            valType = in.readByte();
            valBytes = new byte[valSize];
            in.readFully(valBytes);
        }

        byte ord = in.readByte();

        GridCacheOperation op = GridCacheOperation.fromOrdinal(ord & 0xFF);

        GridCacheVersion nearXidVer = readVersion(in, true);
        GridCacheVersion writeVer = readVersion(in, false);

        int partId = in.readInt();
        long partCntr = in.readLong();
        long expireTime = in.readLong();

        GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

        if (cacheCtx != null) {
            CacheObjectContext coCtx = cacheCtx.cacheObjectContext();

            KeyCacheObject key = co.toKeyCacheObject(coCtx, keyType, keyBytes);

            if (key.partition() == -1)
                key.partition(partId);

            CacheObject val = valBytes != null ? co.toCacheObject(coCtx, valType, valBytes) : null;

            return new DataEntry(
                    cacheId,
                    key,
                    val,
                    op,
                    nearXidVer,
                    writeVer,
                    expireTime,
                    partId,
                    partCntr
            );
        }
        else
            return new LazyDataEntry(
                    cctx,
                    cacheId,
                    keyType,
                    keyBytes,
                    valType,
                    valBytes,
                    op,
                    nearXidVer,
                    writeVer,
                    expireTime,
                    partId,
                    partCntr);
    }

    /**
     * @param buf Buffer to read from.
     * @return Read map.
     */
    private Map<Integer, CacheState> readPartitionStates(DataInput buf) throws IOException {
        int caches = buf.readShort() & 0xFFFF;

        if (caches == 0)
            return Collections.emptyMap();

        Map<Integer, CacheState> states = new HashMap<>(caches, 1.0f);

        for (int i = 0; i < caches; i++) {
            int cacheId = buf.readInt();

            int parts = buf.readShort() & 0xFFFF;

            CacheState state = new CacheState(parts);

            for (int p = 0; p < parts; p++) {
                int partId = buf.readShort() & 0xFFFF;
                long size = buf.readLong();
                long partCntr = buf.readLong();

                state.addPartitionState(partId, size, partCntr);
            }

            states.put(cacheId, state);
        }

        return states;
    }

    /**
     * Changes the buffer position by the number of read bytes.
     *
     * @param in Data input to read from.
     * @param allowNull Is {@code null}version allowed.
     * @return Read cache version.
     */
    private GridCacheVersion readVersion(ByteBufferBackedDataInput in, boolean allowNull) throws IOException {
        // To be able to read serialization protocol version.
        in.ensure(1);

        try {
            int size = CacheVersionIO.readSize(in.buffer(), allowNull);

            in.ensure(size);

            return CacheVersionIO.read(in.buffer(), allowNull);
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /**
     * @param dataRec Data record to serialize.
     * @return Full data record size.
     * @throws IgniteCheckedException If failed to obtain the length of one of the entries.
     */
    private int dataSize(DataRecord dataRec) throws IgniteCheckedException {
        int sz = 0;

        for (DataEntry entry : dataRec.writeEntries())
            sz += entrySize(entry);

        return sz;
    }

    /**
     * @param entry Entry to get size for.
     * @return Entry size.
     * @throws IgniteCheckedException If failed to get key or value bytes length.
     */
    private int entrySize(DataEntry entry) throws IgniteCheckedException {
        GridCacheContext cctx = this.cctx.cacheContext(entry.cacheId());
        CacheObjectContext coCtx = cctx.cacheObjectContext();

        return
            /*cache ID*/4 +
            /*key*/entry.key().valueBytesLength(coCtx) +
            /*value*/(entry.value() == null ? 4 : entry.value().valueBytesLength(coCtx)) +
            /*op*/1 +
            /*near xid ver*/CacheVersionIO.size(entry.nearXidVersion(), true) +
            /*write ver*/CacheVersionIO.size(entry.writeVersion(), false) +
            /*part ID*/4 +
            /*expire Time*/8 +
            /*part cnt*/8;
    }

    /**
     * @param states Partition states.
     * @return Size required to write partition states.
     */
    private int cacheStatesSize(Map<Integer, CacheState> states) {
        // Need 4 bytes for the number of caches.
        int size = 2;

        for (Map.Entry<Integer, CacheState> entry : states.entrySet()) {
            // Cache ID.
            size += 4;

            // Need 2 bytes for the number of partitions.
            size += 2;

            CacheState state = entry.getValue();

            // 2 bytes partition ID, size and counter per partition.
            size += 18 * state.size();
        }

        return size;
    }
}

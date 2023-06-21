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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.managers.encryption.GroupKey;
import org.apache.ignite.internal.managers.encryption.GroupKeyEncrypted;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.wal.record.CacheState;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.EncryptedRecord;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.IndexRenameRootPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.LazyDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.MasterKeyChangeRecordV2;
import org.apache.ignite.internal.pagemem.wal.record.MemoryRecoveryRecord;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MvccDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.PartitionClearingStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.ReencryptionStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType;
import org.apache.ignite.internal.pagemem.wal.record.WalRecordCacheGroupAware;
import org.apache.ignite.internal.pagemem.wal.record.delta.ClusterSnapshotRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertFragmentRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageMvccMarkUpdatedRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageMvccUpdateNewTxStateHintRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageMvccUpdateTxStateHintRecord;
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
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageInitRootInlineFlagsCreatedVersionRecord;
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
import org.apache.ignite.internal.pagemem.wal.record.delta.SplitForwardPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.TrackingPageDeltaRecord;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.record.HeaderRecord;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.apache.ignite.spi.encryption.noop.NoopEncryptionSpi;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CDC_DATA_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD_V2;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.ENCRYPTED_DATA_RECORD_V2;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.ENCRYPTED_DATA_RECORD_V3;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.ENCRYPTED_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.ENCRYPTED_RECORD_V2;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.MASTER_KEY_CHANGE_RECORD_V2;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.REC_TYPE_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.putRecordType;

/**
 * Record data V1 serializer.
 */
public class RecordDataV1Serializer implements RecordDataSerializer {
    /** Length of HEADER record data. */
    static final int HEADER_RECORD_DATA_SIZE = /*Magic*/8 + /*Version*/4;

    /** Cache shared context. */
    protected final GridCacheSharedContext cctx;

    /** Size of page used for PageMemory regions. */
    protected final int pageSize;

    /** Size of page without encryption overhead. */
    protected final int realPageSize;

    /** Cache object processor to reading {@link DataEntry DataEntries}. */
    protected final IgniteCacheObjectProcessor co;

    /** Logger. */
    private final IgniteLogger log;

    /** Serializer of {@link TxRecord} records. */
    private TxRecordSerializer txRecordSerializer;

    /** Encryption SPI instance. */
    private final EncryptionSpi encSpi;

    /** Encryption manager. */
    private final GridEncryptionManager encMgr;

    /** */
    private final boolean encryptionDisabled;

    /** */
    private static final byte ENCRYPTED = 1;

    /** */
    private static final byte PLAIN = 0;

    /**
     * @param cctx Cache shared context.
     */
    public RecordDataV1Serializer(GridCacheSharedContext cctx) {
        this.cctx = cctx;
        this.txRecordSerializer = new TxRecordSerializer();
        this.co = cctx == null ? null : cctx.kernalContext().cacheObjects();
        this.pageSize = cctx == null ? -1 : cctx.database().pageSize();
        this.encSpi = cctx == null ? null : cctx.gridConfig().getEncryptionSpi();
        this.encMgr = cctx == null ? null : cctx.kernalContext().encryption();

        encryptionDisabled = encSpi instanceof NoopEncryptionSpi;

        //This happen on offline WAL iteration(we don't have encryption keys available).
        if (encSpi != null)
            this.realPageSize = CU.encryptedPageSize(pageSize, encSpi);
        else
            this.realPageSize = pageSize;

        log = cctx == null ? null : cctx.logger(getClass());
    }

    /** {@inheritDoc} */
    @Override public int size(WALRecord record) throws IgniteCheckedException {
        int clSz = plainSize(record);

        if (needEncryption(record))
            return encSpi.encryptedSize(clSz) + 4 /* groupId */ + 4 /* data size */ + 1 /* key ID */ + REC_TYPE_SIZE;

        return clSz;
    }

    /** {@inheritDoc} */
    @Override public WALRecord readRecord(RecordType type, ByteBufferBackedDataInput in, int size)
        throws IOException, IgniteCheckedException {
        if (type == ENCRYPTED_RECORD || type == ENCRYPTED_RECORD_V2) {
            DecryptionResult decryptionResult = readEncryptedData(in, true, type == ENCRYPTED_RECORD_V2);

            if (decryptionResult.isDecryptedSuccessfully()) {
                ByteBufferBackedDataInput data = decryptionResult.decryptedData();

                return readPlainRecord(decryptionResult.recordType(), data, true, data.buffer().capacity());
            }
            else
                return new EncryptedRecord(decryptionResult.grpId(), decryptionResult.recordType());
        }

        return readPlainRecord(type, in, false, size);
    }

    /** {@inheritDoc} */
    @Override public void writeRecord(WALRecord rec, ByteBuffer buf) throws IgniteCheckedException {
        if (needEncryption(rec)) {
            int clSz = plainSize(rec);

            ByteBuffer clData = ByteBuffer.allocate(clSz);

            writePlainRecord(rec, clData);

            clData.rewind();

            writeEncryptedData(((WalRecordCacheGroupAware)rec).groupId(), rec.type(), clData, buf);

            return;
        }

        writePlainRecord(rec, buf);
    }

    /**
     * @param rec Record to check.
     * @return {@code True} if this record should be encrypted.
     */
    private boolean needEncryption(WALRecord rec) {
        if (encryptionDisabled)
            return false;

        if (!(rec instanceof WalRecordCacheGroupAware))
            return false;

        return needEncryption(((WalRecordCacheGroupAware)rec).groupId());
    }

    /**
     * @param grpId Group id.
     * @return {@code True} if this record should be encrypted.
     */
    private boolean needEncryption(int grpId) {
        if (encryptionDisabled)
            return false;

        GridEncryptionManager encMgr = cctx.kernalContext().encryption();

        return encMgr != null && encMgr.getActiveKey(grpId) != null;
    }

    /**
     * Reads and decrypt data from {@code in} stream.
     *
     * @param in Input stream.
     * @param readType If {@code true} plain record type will be read from {@code in}.
     * @param readKeyId If {@code true} encryption key identifier will be read from {@code in}.
     * @return Plain data stream, group id, plain record type,
     * @throws IOException If failed.
     * @throws IgniteCheckedException If failed.
     */
    private DecryptionResult readEncryptedData(
        ByteBufferBackedDataInput in,
        boolean readType,
        boolean readKeyId
    ) throws IOException, IgniteCheckedException {
        int grpId = in.readInt();
        int encRecSz = in.readInt();

        RecordType plainRecType = readType ? RecordV1Serializer.readRecordType(in) : null;

        int keyId = readKeyId ? in.readUnsignedByte() : GridEncryptionManager.INITIAL_KEY_ID;

        // Encryption Manager can be null during offline WAL iteration
        if (encMgr == null || encSpi == null) {
            int skipped = in.skipBytes(encRecSz);

            assert skipped == encRecSz;

            return new DecryptionResult(null, plainRecType, grpId);
        }

        GroupKey grpKey = encMgr.groupKey(grpId, keyId);

        // Encryption key is not available when restoring the MetaStorage
        if (grpKey == null) {
            int skipped = in.skipBytes(encRecSz);

            assert skipped == encRecSz;

            return new DecryptionResult(null, plainRecType, grpId);
        }

        byte[] encData = new byte[encRecSz];

        in.readFully(encData);

        byte[] clData = encSpi.decrypt(encData, grpKey.key());

        return new DecryptionResult(ByteBuffer.wrap(clData), plainRecType, grpId);
    }

    /**
     * Writes encrypted {@code clData} to {@code dst} stream.
     *
     * @param grpId Group id;
     * @param plainRecType Plain record type
     * @param clData Plain data.
     * @param dst Destination buffer.
     */
    private void writeEncryptedData(int grpId, @Nullable RecordType plainRecType, ByteBuffer clData, ByteBuffer dst) {
        int dtSz = encSpi.encryptedSize(clData.capacity());

        dst.putInt(grpId);
        dst.putInt(dtSz);

        if (plainRecType != null)
            putRecordType(dst, plainRecType);

        GroupKey grpKey = encMgr.getActiveKey(grpId);

        dst.put(grpKey.id());

        encSpi.encrypt(clData, grpKey.key(), dst);
    }

    /**
     * @param record Record to measure.
     * @return Plain(without encryption) size of serialized rec in bytes.
     * @throws IgniteCheckedException If failed.
     */
    int plainSize(WALRecord record) throws IgniteCheckedException {
        switch (record.type()) {
            case PAGE_RECORD:
                assert record instanceof PageSnapshot;

                PageSnapshot pageRec = (PageSnapshot)record;

                return pageRec.pageDataSize() + 12;

            case CHECKPOINT_RECORD:
                CheckpointRecord cpRec = (CheckpointRecord)record;

                int cacheStatesSize = cacheStatesSize(cpRec.cacheGroupStates());

                WALPointer walPtr = cpRec.checkpointMark();

                return 18 + cacheStatesSize + (walPtr == null ? 0 : 16);

            case META_PAGE_INIT:
                return /*cache ID*/4 + /*page ID*/8 + /*ioType*/2 + /*ioVer*/2 +  /*tree root*/8 + /*reuse root*/8;

            case INDEX_META_PAGE_DELTA_RECORD:
                return /*cache ID*/4 + /*page ID*/8 + /*encrypt page index*/ 4 + /*encrypt pages count*/4;

            case PARTITION_META_PAGE_UPDATE_COUNTERS:
                return /*cache ID*/4 + /*page ID*/8 + /*upd cntr*/8 + /*rmv id*/8 + /*part size*/4 + /*counters page id*/8 + /*state*/ 1
                        + /*allocatedIdxCandidate*/ 4;

            case PARTITION_META_PAGE_UPDATE_COUNTERS_V2:
                return /*cache ID*/4 + /*page ID*/8 + /*upd cntr*/8 + /*rmv id*/8 + /*part size*/4 + /*counters page id*/8 + /*state*/ 1
                    + /*allocatedIdxCandidate*/ 4 + /*link*/ 8;

            case PARTITION_META_PAGE_DELTA_RECORD_V3:
                return /*cache ID*/4 + /*page ID*/8 + /*upd cntr*/8 + /*rmv id*/8 + /*part size*/4 + /*counters page id*/8 + /*state*/ 1
                    + /*allocatedIdxCandidate*/ 4 + /*link*/ 8 + /*encrypt page index*/ 4 + /*encrypt pages count*/4;

            case MEMORY_RECOVERY:
                return 8;

            case PARTITION_DESTROY:
                return /*cacheId*/4 + /*partId*/4;

            case DATA_RECORD_V2:
                DataRecord dataRec = (DataRecord)record;

                return 4 + dataSize(dataRec);

            case METASTORE_DATA_RECORD:
                MetastoreDataRecord metastoreDataRec = (MetastoreDataRecord)record;

                return 4 + metastoreDataRec.key().getBytes().length + 4 +
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

            case MVCC_DATA_PAGE_MARK_UPDATED_RECORD:
                return 4 + 8 + 4 + 8 + 8 + 4;

            case MVCC_DATA_PAGE_TX_STATE_HINT_UPDATED_RECORD:
                return 4 + 8 + 4 + 1;

            case MVCC_DATA_PAGE_NEW_TX_STATE_HINT_UPDATED_RECORD:
                return 4 + 8 + 4 + 1;

            case INIT_NEW_PAGE_RECORD:
                return 4 + 8 + 2 + 2 + 8;

            case BTREE_META_PAGE_INIT_ROOT:
                return 4 + 8 + 8;

            case BTREE_META_PAGE_INIT_ROOT2:
                return 4 + 8 + 8 + 2;

            case BTREE_META_PAGE_INIT_ROOT_V3:
                return 4 + 8 + 8 + 2 + 8 + IgniteProductVersion.SIZE_IN_BYTES;

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

            case MASTER_KEY_CHANGE_RECORD_V2:
                return ((MasterKeyChangeRecordV2)record).dataSize();

            case REENCRYPTION_START_RECORD:
                return ((ReencryptionStartRecord)record).dataSize();

            case INDEX_ROOT_PAGE_RENAME_RECORD:
                return ((IndexRenameRootPageRecord)record).dataSize();

            case PARTITION_CLEARING_START_RECORD:
                return 4 + 4 + 8;

            case CLUSTER_SNAPSHOT:
                return 4 + ((ClusterSnapshotRecord)record).clusterSnapshotName().getBytes().length;

            case INCREMENTAL_SNAPSHOT_START_RECORD:
                return 16;

            case INCREMENTAL_SNAPSHOT_FINISH_RECORD:
                return ((IncrementalSnapshotFinishRecord)record).dataSize();

            default:
                throw new UnsupportedOperationException("Type: " + record.type());
        }
    }

    /**
     * Reads {@code WalRecord} of {@code type} from input.
     * Input should be plain(not encrypted).
     *
     * @param type Record type.
     * @param in Input
     * @param encrypted Record was encrypted.
     * @param recordSize Record size.
     * @return Deserialized record.
     * @throws IOException If failed.
     * @throws IgniteCheckedException If failed.
     */
    WALRecord readPlainRecord(RecordType type, ByteBufferBackedDataInput in,
        boolean encrypted, int recordSize) throws IOException, IgniteCheckedException {
        WALRecord res;

        switch (type) {
            case PAGE_RECORD:
                byte[] arr = new byte[pageSize];

                int cacheId = in.readInt();
                long pageId = in.readLong();

                in.readFully(arr);

                res = new PageSnapshot(new FullPageId(pageId, cacheId), arr, encrypted ? realPageSize : pageSize);

                break;

            case CHECKPOINT_RECORD:
                long msb = in.readLong();
                long lsb = in.readLong();
                boolean hasPtr = in.readByte() != 0;
                long idx = hasPtr ? in.readLong() : 0;
                int off = hasPtr ? in.readInt() : 0;
                int len = hasPtr ? in.readInt() : 0;

                Map<Integer, CacheState> states = readPartitionStates(in);

                boolean end = in.readByte() != 0;

                WALPointer walPtr = hasPtr ? new WALPointer(idx, off, len) : null;

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

                res = new MetaPageInitRecord(cacheId, pageId, ioType, ioVer, treeRoot, reuseListRoot, log);

                break;

            case INDEX_META_PAGE_DELTA_RECORD:
                res = new MetaPageUpdateIndexDataRecord(in);

                break;

            case PARTITION_META_PAGE_UPDATE_COUNTERS:
                res = new MetaPageUpdatePartitionDataRecord(in);

                break;

            case PARTITION_META_PAGE_UPDATE_COUNTERS_V2:
                res = new MetaPageUpdatePartitionDataRecordV2(in);

                break;

            case PARTITION_META_PAGE_DELTA_RECORD_V3:
                res = new MetaPageUpdatePartitionDataRecordV3(in);

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
            case DATA_RECORD_V2:
                int entryCnt = in.readInt();

                if (entryCnt == 1)
                    res = new DataRecord(readPlainDataEntry(in, type), 0L);
                else {
                    List<DataEntry> entries = new ArrayList<>(entryCnt);

                    for (int i = 0; i < entryCnt; i++)
                        entries.add(readPlainDataEntry(in, type));

                    res = new DataRecord(entries, 0L);
                }

                break;

            case ENCRYPTED_DATA_RECORD:
            case ENCRYPTED_DATA_RECORD_V2:
            case ENCRYPTED_DATA_RECORD_V3:
                entryCnt = in.readInt();

                if (entryCnt == 1)
                    res = new DataRecord(readEncryptedDataEntry(in, type), 0L);
                else {
                    List<DataEntry> entries = new ArrayList<>(entryCnt);

                    for (int i = 0; i < entryCnt; i++)
                        entries.add(readEncryptedDataEntry(in, type));

                    res = new DataRecord(entries, 0L);
                }

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

            case MVCC_DATA_PAGE_MARK_UPDATED_RECORD:
                cacheId = in.readInt();
                pageId = in.readLong();

                itemId = in.readInt();
                long newMvccCrd = in.readLong();
                long newMvccCntr = in.readLong();
                int newMvccOpCntr = in.readInt();

                res = new DataPageMvccMarkUpdatedRecord(cacheId, pageId, itemId, newMvccCrd, newMvccCntr, newMvccOpCntr);

                break;

            case MVCC_DATA_PAGE_TX_STATE_HINT_UPDATED_RECORD:
                cacheId = in.readInt();
                pageId = in.readLong();

                itemId = in.readInt();
                byte txState = in.readByte();

                res = new DataPageMvccUpdateTxStateHintRecord(cacheId, pageId, itemId, txState);

                break;

            case MVCC_DATA_PAGE_NEW_TX_STATE_HINT_UPDATED_RECORD:
                cacheId = in.readInt();
                pageId = in.readLong();

                itemId = in.readInt();
                byte newTxState = in.readByte();

                res = new DataPageMvccUpdateNewTxStateHintRecord(cacheId, pageId, itemId, newTxState);

                break;

            case INIT_NEW_PAGE_RECORD:
                cacheId = in.readInt();
                pageId = in.readLong();

                ioType = in.readUnsignedShort();
                ioVer = in.readUnsignedShort();
                long virtualPageId = in.readLong();

                res = new InitNewPageRecord(cacheId, pageId, ioType, ioVer, virtualPageId, log);

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

            case BTREE_META_PAGE_INIT_ROOT_V3:
                cacheId = in.readInt();
                pageId = in.readLong();

                long rootId3 = in.readLong();
                int inlineSize3 = in.readShort();

                long flags = in.readLong();

                byte[] revHash = new byte[IgniteProductVersion.REV_HASH_SIZE];
                byte maj = in.readByte();
                byte min = in.readByte();
                byte maint = in.readByte();
                long verTs = in.readLong();
                in.readFully(revHash);

                IgniteProductVersion createdVer = new IgniteProductVersion(
                    maj,
                    min,
                    maint,
                    verTs,
                    revHash);

                res = new MetaPageInitRootInlineFlagsCreatedVersionRecord(cacheId, pageId, rootId3,
                    inlineSize3, flags, createdVer);

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
                long rmvId = in.readLong();

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

                res = new PagesListInitNewPageRecord(cacheId, pageId, ioType, ioVer, newPageId, prevPageId, addDataPageId, log);

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

                byte state = in.readByte();

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
                res = txRecordSerializer.readTx(in);

                break;

            case MASTER_KEY_CHANGE_RECORD:
            case MASTER_KEY_CHANGE_RECORD_V2:
                int keyNameLen = in.readInt();

                byte[] keyNameBytes = new byte[keyNameLen];

                in.readFully(keyNameBytes);

                String masterKeyName = new String(keyNameBytes);

                int keysCnt = in.readInt();

                List<T2<Integer, GroupKeyEncrypted>> grpKeys = new ArrayList<>(keysCnt);

                boolean readKeyId = type == MASTER_KEY_CHANGE_RECORD_V2;

                for (int i = 0; i < keysCnt; i++) {
                    int grpId = in.readInt();
                    int keyId = readKeyId ? in.readByte() & 0xff : 0;

                    int grpKeySize = in.readInt();
                    byte[] grpKey = new byte[grpKeySize];

                    in.readFully(grpKey);

                    grpKeys.add(new T2<>(grpId, new GroupKeyEncrypted(keyId, grpKey)));
                }

                res = new MasterKeyChangeRecordV2(masterKeyName, grpKeys);

                break;

            case REENCRYPTION_START_RECORD:
                int grpsCnt = in.readInt();

                Map<Integer, Byte> map = U.newHashMap(grpsCnt);

                for (int i = 0; i < grpsCnt; i++) {
                    int grpId = in.readInt();
                    byte keyId = in.readByte();

                    map.put(grpId, keyId);
                }

                res = new ReencryptionStartRecord(map);

                break;

            case INDEX_ROOT_PAGE_RENAME_RECORD:
                res = new IndexRenameRootPageRecord(in);

                break;

            case PARTITION_CLEARING_START_RECORD:
                int partId0 = in.readInt();
                int grpId = in.readInt();
                long clearVer = in.readLong();

                res = new PartitionClearingStartRecord(partId0, grpId, clearVer);

                break;

            case CLUSTER_SNAPSHOT:
                int snpNameLen = in.readInt();

                byte[] snpName = new byte[snpNameLen];

                in.readFully(snpName);

                res = new ClusterSnapshotRecord(new String(snpName));

                break;

            case INCREMENTAL_SNAPSHOT_START_RECORD:
                long mst = in.readLong();
                long lst = in.readLong();

                res = new IncrementalSnapshotStartRecord(new UUID(mst, lst));

                break;

            case INCREMENTAL_SNAPSHOT_FINISH_RECORD:
                long mstSignBits = in.readLong();
                long lstSignBits = in.readLong();

                Set<GridCacheVersion> included = readVersions(in);
                Set<GridCacheVersion> excluded = readVersions(in);

                res = new IncrementalSnapshotFinishRecord(new UUID(mstSignBits, lstSignBits), included, excluded);

                break;

            default:
                throw new UnsupportedOperationException("Type: " + type);
        }

        return res;
    }

    /**
     * Write {@code rec} to {@code buf} without encryption.
     *
     * @param rec Record to serialize.
     * @param buf Output buffer.
     * @throws IgniteCheckedException If failed.
     */
    void writePlainRecord(WALRecord rec, ByteBuffer buf) throws IgniteCheckedException {
        switch (rec.type()) {
            case PAGE_RECORD:
                PageSnapshot snap = (PageSnapshot)rec;

                buf.putInt(snap.fullPageId().groupId());
                buf.putLong(snap.fullPageId().pageId());
                buf.put(snap.pageDataBuffer());

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

            case INDEX_META_PAGE_DELTA_RECORD:
                ((MetaPageUpdateIndexDataRecord)rec).toBytes(buf);

                break;

            case PARTITION_META_PAGE_UPDATE_COUNTERS:
            case PARTITION_META_PAGE_UPDATE_COUNTERS_V2:
            case PARTITION_META_PAGE_DELTA_RECORD_V3:
                ((MetaPageUpdatePartitionDataRecord)rec).toBytes(buf);

                break;

            case CHECKPOINT_RECORD:
                CheckpointRecord cpRec = (CheckpointRecord)rec;
                WALPointer walPtr = cpRec.checkpointMark();
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

            case DATA_RECORD_V2:
                DataRecord dataRec = (DataRecord)rec;

                int entryCnt = dataRec.entryCount();

                buf.putInt(entryCnt);

                boolean encrypted = isDataRecordEncrypted(dataRec);

                for (int i = 0; i < entryCnt; i++) {
                    if (encrypted)
                        putEncryptedDataEntry(buf, dataRec.get(i));
                    else
                        putPlainDataEntry(buf, dataRec.get(i));
                }

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

            case MVCC_DATA_PAGE_MARK_UPDATED_RECORD:
                DataPageMvccMarkUpdatedRecord rmvRec = (DataPageMvccMarkUpdatedRecord)rec;

                buf.putInt(rmvRec.groupId());
                buf.putLong(rmvRec.pageId());

                buf.putInt(rmvRec.itemId());
                buf.putLong(rmvRec.newMvccCrd());
                buf.putLong(rmvRec.newMvccCntr());
                buf.putInt(rmvRec.newMvccOpCntr());

                break;

            case MVCC_DATA_PAGE_TX_STATE_HINT_UPDATED_RECORD:
                DataPageMvccUpdateTxStateHintRecord txStRec = (DataPageMvccUpdateTxStateHintRecord)rec;

                buf.putInt(txStRec.groupId());
                buf.putLong(txStRec.pageId());

                buf.putInt(txStRec.itemId());
                buf.put(txStRec.txState());

                break;

            case MVCC_DATA_PAGE_NEW_TX_STATE_HINT_UPDATED_RECORD:
                DataPageMvccUpdateNewTxStateHintRecord newTxStRec = (DataPageMvccUpdateNewTxStateHintRecord)rec;

                buf.putInt(newTxStRec.groupId());
                buf.putLong(newTxStRec.pageId());

                buf.putInt(newTxStRec.itemId());
                buf.put(newTxStRec.txState());

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

            case BTREE_META_PAGE_INIT_ROOT_V3:
                MetaPageInitRootInlineFlagsCreatedVersionRecord imRec3 =
                    (MetaPageInitRootInlineFlagsCreatedVersionRecord)rec;

                buf.putInt(imRec3.groupId());
                buf.putLong(imRec3.pageId());

                buf.putLong(imRec3.rootId());

                buf.putShort((short)imRec3.inlineSize());

                buf.putLong(imRec3.flags());

                // Write created version.
                IgniteProductVersion createdVer = imRec3.createdVersion();
                buf.put(createdVer.major());
                buf.put(createdVer.minor());
                buf.put(createdVer.maintenance());
                buf.putLong(createdVer.revisionTimestamp());
                buf.put(createdVer.revisionHash());

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
                buf.putLong(tpDelta.nextSnapshotTag());
                buf.putLong(tpDelta.lastSuccessfulSnapshotTag());

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
                        (MetaPageUpdateLastAllocatedIndex)rec;

                buf.putInt(mpUpdateLastAllocatedIdx.groupId());
                buf.putLong(mpUpdateLastAllocatedIdx.pageId());

                buf.putInt(mpUpdateLastAllocatedIdx.lastAllocatedIndex());

                break;

            case PART_META_UPDATE_STATE:
                PartitionMetaStateRecord partMetaStateRecord = (PartitionMetaStateRecord)rec;

                buf.putInt(partMetaStateRecord.groupId());

                buf.putInt(partMetaStateRecord.partitionId());

                buf.put(partMetaStateRecord.state());

                buf.putLong(0);

                break;

            case PAGE_LIST_META_RESET_COUNT_RECORD:
                PageListMetaResetCountRecord pageListMetaResetCntRecord = (PageListMetaResetCountRecord)rec;

                buf.putInt(pageListMetaResetCntRecord.groupId());
                buf.putLong(pageListMetaResetCntRecord.pageId());

                break;

            case ROTATED_ID_PART_RECORD:
                RotatedIdPartRecord rotatedIdPartRecord = (RotatedIdPartRecord)rec;

                buf.putInt(rotatedIdPartRecord.groupId());
                buf.putLong(rotatedIdPartRecord.pageId());

                buf.put(rotatedIdPartRecord.rotatedIdPart());

                break;

            case TX_RECORD:
                txRecordSerializer.write((TxRecord)rec, buf);

                break;

            case SWITCH_SEGMENT_RECORD:
                break;

            case MASTER_KEY_CHANGE_RECORD_V2:
                MasterKeyChangeRecordV2 mkChangeRec = (MasterKeyChangeRecordV2)rec;

                byte[] keyIdBytes = mkChangeRec.getMasterKeyName().getBytes();

                buf.putInt(keyIdBytes.length);
                buf.put(keyIdBytes);

                List<T2<Integer, GroupKeyEncrypted>> grpKeys = mkChangeRec.getGrpKeys();

                buf.putInt(grpKeys.size());

                for (T2<Integer, GroupKeyEncrypted> entry : grpKeys) {
                    GroupKeyEncrypted grpKey = entry.get2();

                    buf.putInt(entry.get1());
                    buf.put((byte)grpKey.id());

                    buf.putInt(grpKey.key().length);
                    buf.put(grpKey.key());
                }

                break;

            case REENCRYPTION_START_RECORD:
                ReencryptionStartRecord statusRecord = (ReencryptionStartRecord)rec;

                Map<Integer, Byte> grps = statusRecord.groups();

                buf.putInt(grps.size());

                for (Map.Entry<Integer, Byte> e : grps.entrySet()) {
                    buf.putInt(e.getKey());
                    buf.put(e.getValue());
                }

                break;

            case INDEX_ROOT_PAGE_RENAME_RECORD:
                ((IndexRenameRootPageRecord)rec).writeRecord(buf);

                break;

            case PARTITION_CLEARING_START_RECORD:
                PartitionClearingStartRecord partitionClearingStartRecord = (PartitionClearingStartRecord)rec;

                buf.putInt(partitionClearingStartRecord.partitionId());

                buf.putInt(partitionClearingStartRecord.groupId());

                buf.putLong(partitionClearingStartRecord.clearVersion());

                break;

            case INCREMENTAL_SNAPSHOT_START_RECORD:
                IncrementalSnapshotStartRecord startRec = (IncrementalSnapshotStartRecord)rec;

                buf.putLong(startRec.id().getMostSignificantBits());
                buf.putLong(startRec.id().getLeastSignificantBits());

                break;

            case INCREMENTAL_SNAPSHOT_FINISH_RECORD:
                IncrementalSnapshotFinishRecord incSnpFinRec = (IncrementalSnapshotFinishRecord)rec;

                buf.putLong(incSnpFinRec.id().getMostSignificantBits());
                buf.putLong(incSnpFinRec.id().getLeastSignificantBits());

                buf.putInt(incSnpFinRec.included().size());

                for (GridCacheVersion v: incSnpFinRec.included())
                    putVersion(buf, v, false);

                buf.putInt(incSnpFinRec.excluded().size());

                for (GridCacheVersion v: incSnpFinRec.excluded())
                    putVersion(buf, v, false);

                break;

            case CLUSTER_SNAPSHOT:
                byte[] snpName = ((ClusterSnapshotRecord)rec).clusterSnapshotName().getBytes();

                buf.putInt(snpName.length);
                buf.put(snpName);

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
     * @throws IgniteCheckedException If failed.
     */
    void putEncryptedDataEntry(ByteBuffer buf, DataEntry entry) throws IgniteCheckedException {
        DynamicCacheDescriptor desc = cctx.cache().cacheDescriptor(entry.cacheId());

        if (desc != null && needEncryption(desc.groupId())) {
            int clSz = entrySize(entry);

            ByteBuffer clData = ByteBuffer.allocate(clSz);

            putPlainDataEntry(clData, entry);

            clData.rewind();

            buf.put(ENCRYPTED);

            writeEncryptedData(desc.groupId(), null, clData, buf);
        }
        else {
            buf.put(PLAIN);

            putPlainDataEntry(buf, entry);
        }
    }

    /**
     * @param buf Buffer to write to.
     * @param entry Data entry.
     */
    void putPlainDataEntry(ByteBuffer buf, DataEntry entry) throws IgniteCheckedException {
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

        if (!(entry instanceof MvccDataEntry))
            buf.put(entry.flags());
    }

    /**
     * @param states Cache states.
     */
    private static void putCacheStates(ByteBuffer buf, Map<Integer, CacheState> states) {
        buf.putShort((short)states.size());

        for (Entry<Integer, CacheState> entry : states.entrySet()) {
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
    static void putVersion(ByteBuffer buf, GridCacheVersion ver, boolean allowNull) {
        CacheVersionIO.write(buf, ver, allowNull);
    }

    /**
     * @param buf Buffer.
     * @param rowBytes Row bytes.
     */
    private static void putRow(ByteBuffer buf, byte[] rowBytes) {
        assert rowBytes.length > 0;

        buf.put(rowBytes);
    }

    /**
     * @param in Input to read from.
     * @param recType Record type.
     * @return Read entry.
     * @throws IOException If failed.
     * @throws IgniteCheckedException If failed.
     */
    DataEntry readEncryptedDataEntry(ByteBufferBackedDataInput in, RecordType recType) throws IOException, IgniteCheckedException {
        boolean needDecryption = in.readByte() == ENCRYPTED;

        RecordType dataRecordType = recType == ENCRYPTED_DATA_RECORD_V3 ? DATA_RECORD_V2 : DATA_RECORD;

        if (needDecryption) {
            boolean readKeyId = recType == ENCRYPTED_DATA_RECORD_V2 || recType == ENCRYPTED_DATA_RECORD_V3;

            DecryptionResult decryptionResult = readEncryptedData(in, false, readKeyId);

            return decryptionResult.isDecryptedSuccessfully()
                ? readPlainDataEntry(decryptionResult.decryptedData(), dataRecordType)
                : new EncryptedDataEntry();
        }

        return readPlainDataEntry(in, dataRecordType);
    }

    /**
     * @param in Input to read from.
     * @return Read entry.
     */
    DataEntry readPlainDataEntry(ByteBufferBackedDataInput in, RecordType type) throws IOException, IgniteCheckedException {
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
        byte flags = type == DATA_RECORD_V2 || type == CDC_DATA_RECORD ? in.readByte() : (byte)0;

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
                    partCntr,
                    flags
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
                    partCntr,
                    flags
            );
    }

    /**
     * @param rec Record.
     * @return Real record type.
     */
    RecordType recordType(WALRecord rec) {
        if (encryptionDisabled)
            return rec.type();

        if (needEncryption(rec))
            return ENCRYPTED_RECORD_V2;

        if (rec.type() != DATA_RECORD && rec.type() != DATA_RECORD_V2)
            return rec.type();

        return isDataRecordEncrypted((DataRecord)rec) ? ENCRYPTED_DATA_RECORD_V3 : rec.type();
    }

    /**
     * @param rec Data record.
     * @return {@code True} if this data record should be encrypted.
     */
    boolean isDataRecordEncrypted(DataRecord rec) {
        if (encryptionDisabled)
            return false;

        int entryCnt = rec.entryCount();

        for (int i = 0; i < entryCnt; i++) {
            DataEntry e = rec.get(i);

            if (cctx.cacheContext(e.cacheId()) != null && needEncryption(cctx.cacheContext(e.cacheId()).groupId()))
                return true;
        }

        return false;
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
    GridCacheVersion readVersion(ByteBufferBackedDataInput in, boolean allowNull) throws IOException {
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
     * Read set of versions.
     *
     * @param in Data input to read from.
     * @return Read set of cache versions.
     */
    private Set<GridCacheVersion> readVersions(ByteBufferBackedDataInput in) throws IOException {
        int txsSize = in.readInt();

        Set<GridCacheVersion> txs = new HashSet<>();

        for (int i = 0; i < txsSize; i++) {
            GridCacheVersion v = readVersion(in, false);

            txs.add(v);
        }

        return txs;
    }

    /**
     * @param dataRec Data record to serialize.
     * @return Full data record size.
     * @throws IgniteCheckedException If failed to obtain the length of one of the entries.
     */
    protected int dataSize(DataRecord dataRec) throws IgniteCheckedException {
        boolean encrypted = isDataRecordEncrypted(dataRec);

        int sz = 0;
        int entryCnt = dataRec.entryCount();

        for (int i = 0; i < entryCnt; i++) {
            DataEntry entry = dataRec.get(i);

            int clSz = entrySize(entry);

            if (!encryptionDisabled && needEncryption(cctx.cacheContext(entry.cacheId()).groupId()))
                sz += encSpi.encryptedSize(clSz) + 1 /*encrypted flag*/ + 4 /*groupId*/ + 4 /*data size*/ + 1 /*key ID*/;
            else {
                sz += clSz;

                if (encrypted)
                    sz += 1 /* encrypted flag */;
            }
        }

        return sz;
    }

    /**
     * @param entry Entry to get size for.
     * @return Entry size.
     * @throws IgniteCheckedException If failed to get key or value bytes length.
     */
    protected int entrySize(DataEntry entry) throws IgniteCheckedException {
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
            /*part cnt*/8 +
            /*flags*/(entry instanceof MvccDataEntry ? 0 : 1);
    }

    /**
     * @param states Partition states.
     * @return Size required to write partition states.
     */
    private int cacheStatesSize(Map<Integer, CacheState> states) {
        // Need 4 bytes for the number of caches.
        int size = 2;

        for (Entry<Integer, CacheState> entry : states.entrySet()) {
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

    /**
     * Represents encrypted Data Entry ({@link #key}, {@link #val value}) pair.
     */
    public static class EncryptedDataEntry extends DataEntry {
        /** Constructor. */
        EncryptedDataEntry() {
            super(0, null, null, READ, null, null, 0, 0, 0, EMPTY_FLAGS);
        }
    }
}

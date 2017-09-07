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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertFragmentRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageUpdateRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.RecordDataSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.record.HeaderRecord;

/**
 * Record data V2 serializer.
 */
public class RecordDataV2Serializer implements RecordDataSerializer {
    /** V1 data serializer delegate. */
    private final RecordDataV1Serializer delegateSerializer;

    /** Serializer of {@link TxRecord} records. */
    private final TxRecordSerializer txRecordSerializer;

    /**
     * Create an instance of V2 data serializer.
     *
     * @param delegateSerializer V1 data serializer.
     */
    public RecordDataV2Serializer(RecordDataV1Serializer delegateSerializer, GridCacheSharedContext cctx) {
        this.delegateSerializer = delegateSerializer;
        this.txRecordSerializer = new TxRecordSerializer(cctx);
    }

    /** {@inheritDoc} */
    @Override public int size(WALRecord record) throws IgniteCheckedException {
        if (record instanceof HeaderRecord)
            throw new UnsupportedOperationException(
                "Getting size of header records is forbidden since version 2 of serializer");

        switch (record.type()) {
            case TX_RECORD:
                return txRecordSerializer.sizeOfTxRecord((TxRecord)record);

            case DATA_PAGE_INSERT_RECORD:
                DataPageInsertRecord diRec = (DataPageInsertRecord)record;

                return 4 + 8 + 4 +
                        ((FileWALPointer) diRec.reference()).size();

            case DATA_PAGE_UPDATE_RECORD:
                DataPageUpdateRecord uRec = (DataPageUpdateRecord)record;

                return 4 + 8 + 4 + 4 +
                        ((FileWALPointer) uRec.reference()).size();

            case DATA_PAGE_INSERT_FRAGMENT_RECORD:
                final DataPageInsertFragmentRecord difRec = (DataPageInsertFragmentRecord)record;

                return 4 + 8 + 8 + 4 + 4 +
                        ((FileWALPointer) difRec.reference()).size();

            default:
                return delegateSerializer.size(record);
        }
    }

    /** {@inheritDoc} */
    @Override public WALRecord readRecord(
        WALRecord.RecordType type,
        ByteBufferBackedDataInput in
    ) throws IOException, IgniteCheckedException {
        switch (type) {
            case TX_RECORD:
                return txRecordSerializer.readTxRecord(in);

            case DATA_PAGE_INSERT_RECORD: {
                int cacheId = in.readInt();
                long pageId = in.readLong();

                int payloadSize = in.readInt();

                WALPointer reference = FileWALPointer.read(in);

                return new DataPageInsertRecord(cacheId, pageId, payloadSize, reference);

            }

            case DATA_PAGE_UPDATE_RECORD: {
                int cacheId = in.readInt();
                long pageId = in.readLong();

                int itemId = in.readInt();

                int payloadSize = in.readInt();

                WALPointer reference = FileWALPointer.read(in);

                return new DataPageUpdateRecord(cacheId, pageId, itemId, payloadSize, reference);
            }

            case DATA_PAGE_INSERT_FRAGMENT_RECORD: {
                int cacheId = in.readInt();
                long pageId = in.readLong();

                long lastLink = in.readLong();
                int payloadSize = in.readInt();
                int recordOffset = in.readInt();
                WALPointer reference = FileWALPointer.read(in);

                return new DataPageInsertFragmentRecord(cacheId, pageId, payloadSize, recordOffset, lastLink, reference);
            }

            default:
                return delegateSerializer.readRecord(type, in);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeRecord(WALRecord record, ByteBuffer buf) throws IgniteCheckedException {
        if (record instanceof HeaderRecord)
            throw new UnsupportedOperationException("Writing header records is forbidden since version 2 of serializer");

        switch (record.type()) {
            case TX_RECORD:
                txRecordSerializer.writeTxRecord((TxRecord)record, buf);

                break;

            case DATA_PAGE_INSERT_RECORD:
                DataPageInsertRecord diRec = (DataPageInsertRecord)record;

                buf.putInt(diRec.groupId());
                buf.putLong(diRec.pageId());
                buf.putInt(diRec.payloadSize());

                ((FileWALPointer) diRec.reference()).put(buf);

                break;

            case DATA_PAGE_UPDATE_RECORD:
                DataPageUpdateRecord uRec = (DataPageUpdateRecord)record;

                buf.putInt(uRec.groupId());
                buf.putLong(uRec.pageId());
                buf.putInt(uRec.itemId());
                buf.putInt(uRec.payloadSize());

                ((FileWALPointer) uRec.reference()).put(buf);

                break;

            case DATA_PAGE_INSERT_FRAGMENT_RECORD:
                final DataPageInsertFragmentRecord difRec = (DataPageInsertFragmentRecord)record;

                buf.putInt(difRec.groupId());
                buf.putLong(difRec.pageId());

                buf.putLong(difRec.lastLink());
                buf.putInt(difRec.payloadSize());
                buf.putInt(difRec.offset());

                ((FileWALPointer) difRec.reference()).put(buf);

                break;

            default:
               delegateSerializer.writeRecord(record, buf);
        }
    }
}

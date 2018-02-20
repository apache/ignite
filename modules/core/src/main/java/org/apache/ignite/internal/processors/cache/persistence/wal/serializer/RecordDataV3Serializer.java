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
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertFragmentRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertFragmentReferencedRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertReferencedRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageUpdateRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageUpdateReferencedRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;

public class RecordDataV3Serializer implements RecordDataSerializer {

    private final RecordDataV2Serializer delegateSerializer;

    public RecordDataV3Serializer(RecordDataV2Serializer delegateSerializer) {
        this.delegateSerializer = delegateSerializer;
    }

    @Override public int size(WALRecord record) throws IgniteCheckedException {
        switch (record.type()) {
            case DATA_PAGE_INSERT_REF_RECORD:
                return 4 + 8 + FileWALPointer.size();

            case DATA_PAGE_UPDATE_REF_RECORD:
                return 4 + 8 + 4 + FileWALPointer.size();

            case DATA_PAGE_INSERT_FRAGMENT_REF_RECORD:
                return 4 + 8 + 8 + 4 + FileWALPointer.size();

            default:
                return delegateSerializer.size(record);
        }
    }

    @Override public WALRecord readRecord(WALRecord.RecordType type, ByteBufferBackedDataInput in) throws IOException, IgniteCheckedException {
        switch (type) {
            case DATA_PAGE_INSERT_REF_RECORD: {
                int cacheId = in.readInt();
                long pageId = in.readLong();

                WALPointer reference = FileWALPointer.read(in);

                return new DataPageInsertReferencedRecord(cacheId, pageId, reference);
            }

            case DATA_PAGE_UPDATE_REF_RECORD: {
                int cacheId = in.readInt();
                long pageId = in.readLong();

                int itemId = in.readInt();

                WALPointer reference = FileWALPointer.read(in);

                return new DataPageUpdateReferencedRecord(cacheId, pageId, itemId, reference);
            }

            case DATA_PAGE_INSERT_FRAGMENT_REF_RECORD: {
                int cacheId = in.readInt();
                long pageId = in.readLong();

                long lastLink = in.readLong();

                int offset = in.readInt();
                WALPointer reference = FileWALPointer.read(in);

                return new DataPageInsertFragmentReferencedRecord(cacheId, pageId, offset, lastLink, reference);
            }

            default:
                return delegateSerializer.readRecord(type, in);
        }
    }

    @Override public void writeRecord(WALRecord record, ByteBuffer buf) throws IgniteCheckedException {
        switch (record.type()) {
            case DATA_PAGE_INSERT_REF_RECORD:
                DataPageInsertReferencedRecord diRec = (DataPageInsertReferencedRecord) record;

                buf.putInt(diRec.groupId());
                buf.putLong(diRec.pageId());

                ((FileWALPointer) diRec.reference()).put(buf);

                break;

            case DATA_PAGE_UPDATE_REF_RECORD:
                DataPageUpdateReferencedRecord uRec = (DataPageUpdateReferencedRecord) record;

                buf.putInt(uRec.groupId());
                buf.putLong(uRec.pageId());

                buf.putInt(uRec.itemId());

                ((FileWALPointer) uRec.reference()).put(buf);

                break;

            case DATA_PAGE_INSERT_FRAGMENT_REF_RECORD:
                final DataPageInsertFragmentReferencedRecord difRec = (DataPageInsertFragmentReferencedRecord) record;

                buf.putInt(difRec.groupId());
                buf.putLong(difRec.pageId());

                buf.putLong(difRec.lastLink());
                buf.putInt(difRec.offset());

                ((FileWALPointer) difRec.reference()).put(buf);

                break;

            default:
                delegateSerializer.writeRecord(record, buf);
        }
    }
}

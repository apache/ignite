/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.HEADER_RECORD;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.HEADER_RECORD_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.readPosition;

/** Byte Buffer WAL Iterator */
public class ByteBufferWalIterator extends AbstractWalRecordsIteratorAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** WAL records serializer. */
    private final RecordSerializer serializer;

    /** Buffer input wrapper. */
    private final ByteBufferBackedDataInputImpl dataInput;

    /** Last read pointer. */
    private @Nullable WALPointer lastReadPtr;

    /** */
    public ByteBufferWalIterator(
        ByteBuffer byteBuf,
        RecordSerializer serializer,
        @Nullable WALPointer lastWrittenPtr
    ) throws IgniteCheckedException {
        this.serializer = serializer;

        lastReadPtr = lastWrittenPtr;

        dataInput = new ByteBufferBackedDataInputImpl();

        dataInput.buffer(byteBuf);

        advance();
    }

    /** */
    private IgniteBiTuple<WALPointer, WALRecord> advanceRecord() throws IgniteCheckedException {
        if (!dataInput.buffer().hasRemaining())
            return null;

        IgniteBiTuple<WALPointer, WALRecord> result;

        try {
            if (curRec == null)
                skipHeader();

            WALPointer nextPtr = new WALPointer(lastReadPtr.index(), lastReadPtr.fileOffset() + lastReadPtr.length(), 0);

            WALRecord rec = serializer.readRecord(dataInput, nextPtr);

            // TODO: concurrency between FileredRecord#size in different WAL consumers? compaction thread vs cdc thread?
            nextPtr.length(rec.size());

            result = new IgniteBiTuple<>(nextPtr, rec);

            lastReadPtr = nextPtr;
        }
        catch (SegmentEofException e) {
            return null;
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }

        return result;
    }

    /** */
    private void skipHeader() throws IOException {
        int position = dataInput.buffer().position();

        int type = dataInput.readUnsignedByte();

        WALRecord.RecordType recType = WALRecord.RecordType.fromIndex(type - 1);

        if (recType == HEADER_RECORD) {
            long segIdx = readPosition(dataInput).index();

            dataInput.buffer().position(position + HEADER_RECORD_SIZE);

            lastReadPtr = new WALPointer(segIdx, 0, HEADER_RECORD_SIZE);
        }
        else {
            assert lastReadPtr != null;

            dataInput.buffer().position(position);
        }
    }

    /** {@inheritDoc} */
    @Override protected void advance() throws IgniteCheckedException {
        do
            curRec = advanceRecord();
        while (curRec != null && curRec.get2().type() == null);
    }

    /** {@inheritDoc} */
    @Override public Optional<WALPointer> lastRead() {
        return Optional.of(lastReadPtr);
    }
}

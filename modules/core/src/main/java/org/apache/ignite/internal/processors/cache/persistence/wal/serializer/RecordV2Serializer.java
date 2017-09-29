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
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentEofException;
import org.apache.ignite.internal.processors.cache.persistence.wal.WalSegmentTailReachedException;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.io.RecordIO;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.CRC_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.REC_TYPE_SIZE;

/**
 * Record V2 serializer.
 * Stores records in following format:
 * <ul>
 * <li>Record type from {@link WALRecord.RecordType#ordinal()} incremented by 1</li>
 * <li>WAL pointer to double check consistency</li>
 * <li>Record length</li>
 * <li>Data</li>
 * <li>CRC or zero padding</li>
 * </ul>
 */
public class RecordV2Serializer implements RecordSerializer {
    /** Length of WAL Pointer: Index (8) + File offset (4) + Record length (4) */
    public static final int FILE_WAL_POINTER_SIZE = 8 + 4 + 4;

    /** V2 data serializer. */
    private final RecordDataV2Serializer dataSerializer;

    /** Write pointer. */
    private final boolean writePointer;

    /** Record read/write functional interface. */
    private final RecordIO recordIO = new RecordIO() {

        /** {@inheritDoc} */
        @Override public int sizeWithHeaders(WALRecord record) throws IgniteCheckedException {
            return dataSerializer.size(record) + REC_TYPE_SIZE + FILE_WAL_POINTER_SIZE + CRC_SIZE;
        }

        /** {@inheritDoc} */
        @Override public WALRecord readWithHeaders(
            ByteBufferBackedDataInput in,
            WALPointer expPtr
        ) throws IOException, IgniteCheckedException {
            WALRecord.RecordType recType = RecordV1Serializer.readRecordType(in);

            if (recType == WALRecord.RecordType.SWITCH_SEGMENT_RECORD)
                throw new SegmentEofException("Reached end of segment", null);

            FileWALPointer ptr = readPositionAndCheckPoint(in, expPtr);

            return dataSerializer.readRecord(recType, in);
        }

        /** {@inheritDoc} */
        @Override public void writeWithHeaders(
            WALRecord record,
            ByteBuffer buf
        ) throws IgniteCheckedException {
            // Write record type.
            RecordV1Serializer.putRecordType(buf, record);

            // Write record file position.
            putPositionOfRecord(buf, record);

            // Write record data.
            dataSerializer.writeRecord(record, buf);
        }
    };

    /**
     * Create an instance of Record V2 serializer.
     *
     * @param dataSerializer V2 data serializer.
     */
    public RecordV2Serializer(RecordDataV2Serializer dataSerializer, boolean writePointer) {
        this.dataSerializer = dataSerializer;
        this.writePointer = writePointer;
    }

    /** {@inheritDoc} */
    @Override public int version() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public boolean writePointer() {
        return writePointer;
    }

    /** {@inheritDoc} */
    @Override public int size(WALRecord record) throws IgniteCheckedException {
        return recordIO.sizeWithHeaders(record);
    }

    /** {@inheritDoc} */
    @Override public void writeRecord(WALRecord record, ByteBuffer buf) throws IgniteCheckedException {
        RecordV1Serializer.writeWithCrc(record, buf, recordIO);
    }

    /** {@inheritDoc} */
    @Override public WALRecord readRecord(FileInput in, WALPointer expPtr) throws IOException, IgniteCheckedException {
        return RecordV1Serializer.readWithCrc(in, expPtr, recordIO);
    }

    /**
     * @param in Data input to read pointer from.
     * @return Read file WAL pointer.
     * @throws IOException If failed to write.
     */
    public static FileWALPointer readPositionAndCheckPoint(
        DataInput in,
        WALPointer expPtr
    ) throws IgniteCheckedException, IOException {
        long idx = in.readLong();
        int fileOffset = in.readInt();
        int length = in.readInt();

        FileWALPointer p = (FileWALPointer)expPtr;

        if (!F.eq(idx, p.index()) || !F.eq(fileOffset, p.fileOffset()))
            throw new WalSegmentTailReachedException(
                "WAL segment tail is reached. [ " +
                        "Expected next state: {Index=" + p.index() + ",Offset=" + p.fileOffset() + "}, " +
                        "Actual state : {Index=" + idx + ",Offset=" + fileOffset + "} ]", null);

        return new FileWALPointer(idx, fileOffset, length);
    }

    /**
     * Writes record file position to given {@code buf}.
     *
     * @param buf Buffer to write record file position.
     * @param record WAL record.
     */
    public static void putPositionOfRecord(ByteBuffer buf, WALRecord record) {
        FileWALPointer p = (FileWALPointer)record.position();

        buf.putLong(p.index());
        buf.putInt(p.fileOffset());
        buf.putInt(record.size());
    }
}

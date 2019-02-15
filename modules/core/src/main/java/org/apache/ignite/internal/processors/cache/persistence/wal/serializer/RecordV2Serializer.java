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

package org.apache.ignite.internal.processors.cache.persistence.wal.serializer;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.FilteredRecord;
import org.apache.ignite.internal.pagemem.wal.record.MarshalledRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.FileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentEofException;
import org.apache.ignite.internal.processors.cache.persistence.wal.WalSegmentTailReachedException;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.io.RecordIO;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.SWITCH_SEGMENT_RECORD;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.CRC_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.REC_TYPE_SIZE;

/**
 * Record V2 serializer.
 * Stores records in following format:
 * <ul>
 * <li>Record type from {@link RecordType#ordinal()} incremented by 1</li>
 * <li>WAL pointer to double check consistency</li>
 * <li>Record length</li>
 * <li>Data</li>
 * <li>CRC or zero padding</li>
 * </ul>
 */
public class RecordV2Serializer implements RecordSerializer {
    /** Length of WAL Pointer: Index (8) + File offset (4) + Record length (4) */
    private static final int FILE_WAL_POINTER_SIZE = 8 + 4 + 4;

    /** V2 data serializer. */
    private final RecordDataV2Serializer dataSerializer;

    /** Write pointer. */
    private final boolean writePointer;

    /**
     * Marshalled mode.
     * Records are not deserialized in this mode, {@link MarshalledRecord} with binary representation are read instead.
     */
    private final boolean marshalledMode;

    /** Skip position check flag. Should be set for reading compacted wal file with skipped physical records. */
    private final boolean skipPositionCheck;

    /** Thread-local heap byte buffer. */
    private final ThreadLocal<ByteBuffer> heapTlb = new ThreadLocal<ByteBuffer>() {
        @Override protected ByteBuffer initialValue() {
            ByteBuffer buf = ByteBuffer.allocate(4096);

            buf.order(GridUnsafe.NATIVE_BYTE_ORDER);

            return buf;
        }
    };

    /**
     * Record type filter.
     * {@link FilteredRecord} is deserialized instead of original record if type doesn't match filter.
     */
    private final IgniteBiPredicate<RecordType, WALPointer> recordFilter;

    /** Record read/write functional interface. */
    private final RecordIO recordIO = new RecordIO() {

        /** {@inheritDoc} */
        @Override public int sizeWithHeaders(WALRecord record) throws IgniteCheckedException {
            int recordSize = dataSerializer.size(record);

            int recordSizeWithType = recordSize + REC_TYPE_SIZE;

            // Why this condition here, see SWITCH_SEGMENT_RECORD doc.
            return record.type() != SWITCH_SEGMENT_RECORD ?
                recordSizeWithType + FILE_WAL_POINTER_SIZE + CRC_SIZE : recordSizeWithType;
        }

        /** {@inheritDoc} */
        @Override public WALRecord readWithHeaders(
            ByteBufferBackedDataInput in,
            WALPointer expPtr
        ) throws IOException, IgniteCheckedException {
            WALRecord.RecordType recType = RecordV1Serializer.readRecordType(in);

            if (recType == SWITCH_SEGMENT_RECORD)
                throw new SegmentEofException("Reached end of segment", null);

            FileWALPointer ptr = readPositionAndCheckPoint(in, expPtr, skipPositionCheck, recType);

            if (recType == null) {
                FileWALPointer exp = (FileWALPointer)expPtr;

                throw new IOException("Unknown record type: " + recType +
                    ", expected pointer [idx=" + exp.index() + ", offset=" + exp.fileOffset() + "]");
            }

            if (recType.purpose() != WALRecord.RecordPurpose.INTERNAL
                && recordFilter != null && !recordFilter.apply(recType, ptr)) {
                int toSkip = ptr.length() - REC_TYPE_SIZE - FILE_WAL_POINTER_SIZE - CRC_SIZE;

                assert toSkip >= 0 : "Too small saved record length: " + ptr;

                if (in.skipBytes(toSkip) < toSkip)
                    throw new EOFException("Reached end of file while reading record: " + ptr);

                return FilteredRecord.INSTANCE;
            }
            else if (marshalledMode) {
                ByteBuffer buf = heapTlb.get();

                if (buf.capacity() < ptr.length())
                    heapTlb.set(buf = ByteBuffer.allocate(ptr.length() * 3 / 2).order(ByteOrder.nativeOrder()));
                else
                    buf.clear();

                buf.put((byte)(recType.ordinal() + 1));

                buf.putLong(ptr.index());
                buf.putInt(ptr.fileOffset());
                buf.putInt(ptr.length());

                in.readFully(buf.array(), buf.position(), ptr.length() - buf.position());
                buf.position(ptr.length());

                // Unwind reading CRC.
                in.buffer().position(in.buffer().position() - CRC_SIZE);

                buf.flip();

                assert buf.remaining() == ptr.length();

                return new MarshalledRecord(recType, ptr, buf);
            }
            else {
                WALRecord rec = dataSerializer.readRecord(recType, in);

                rec.position(ptr);

                return rec;
            }

        }

        /** {@inheritDoc} */
        @Override public void writeWithHeaders(
            WALRecord record,
            ByteBuffer buf
        ) throws IgniteCheckedException {
            // Write record type.
            RecordV1Serializer.putRecordType(buf, dataSerializer.recordType(record));

            // SWITCH_SEGMENT_RECORD should have only type, no need to write pointer.
            if (record.type() == SWITCH_SEGMENT_RECORD)
                return;

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
     * @param marshalledMode Marshalled mode.
     * @param skipPositionCheck Skip position check mode.
     * @param recordFilter Record type filter. {@link FilteredRecord} is deserialized instead of original record.
     */
    public RecordV2Serializer(
        RecordDataV2Serializer dataSerializer,
        boolean writePointer,
        boolean marshalledMode,
        boolean skipPositionCheck,
        IgniteBiPredicate<RecordType, WALPointer> recordFilter
    ) {
        this.dataSerializer = dataSerializer;
        this.writePointer = writePointer;
        this.marshalledMode = marshalledMode;
        this.skipPositionCheck = skipPositionCheck;
        this.recordFilter = recordFilter;
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
     * @param skipPositionCheck Flag for skipping position check.
     * @return Read file WAL pointer.
     * @throws IOException If failed to write.
     */
    @SuppressWarnings("UnusedReturnValue")
    private static FileWALPointer readPositionAndCheckPoint(
        DataInput in,
        WALPointer expPtr,
        boolean skipPositionCheck,
        WALRecord.RecordType type
    ) throws IgniteCheckedException, IOException {
        long idx = in.readLong();
        int fileOff = in.readInt();
        int len = in.readInt();

        FileWALPointer p = (FileWALPointer)expPtr;

        if (!F.eq(idx, p.index()) || (!skipPositionCheck && !F.eq(fileOff, p.fileOffset())))
            throw new WalSegmentTailReachedException(
                "WAL segment tail reached. [ " +
                    "Expected next state: {Index=" + p.index() + ",Offset=" + p.fileOffset() + "}, " +
                    "Actual state : {Index=" + idx + ",Offset=" + fileOff + "} ] recordType=" + type, null);

        return new FileWALPointer(idx, fileOff, len);
    }

    /**
     * Writes rec file position to given {@code buf}.
     *
     * @param buf Buffer to write rec file position.
     * @param rec WAL rec.
     */
    private static void putPositionOfRecord(ByteBuffer buf, WALRecord rec) {
        FileWALPointer p = (FileWALPointer)rec.position();

        buf.putLong(p.index());
        buf.putInt(p.fileOffset());
        buf.putInt(rec.size());
    }
}

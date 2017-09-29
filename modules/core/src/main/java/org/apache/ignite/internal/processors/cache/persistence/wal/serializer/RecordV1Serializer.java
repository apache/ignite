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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentEofException;
import org.apache.ignite.internal.processors.cache.persistence.wal.WalSegmentTailReachedException;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.PureJavaCrc32;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.io.RecordIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_SKIP_CRC;

/**
 * Record V1 serializer.
 * Stores records in following format:
 * <ul>
 *     <li>Record type from {@link RecordType#ordinal()} incremented by 1</li>
 *     <li>WAL pointer to double check consistency</li>
 *     <li>Data</li>
 *     <li>CRC or zero padding</li>
 * </ul>
 */
public class RecordV1Serializer implements RecordSerializer {
    /** Length of Type */
    public static final int REC_TYPE_SIZE = 1;

    /** Length of WAL Pointer: Index (8) + File offset (4). */
    public static final int FILE_WAL_POINTER_SIZE = 8 + 4;

    /** Length of CRC value */
    public static final int CRC_SIZE = 4;

    /** Total length of HEADER record. */
    public static final int HEADER_RECORD_SIZE = REC_TYPE_SIZE + FILE_WAL_POINTER_SIZE + CRC_SIZE + RecordDataV1Serializer.HEADER_RECORD_DATA_SIZE;

    /** Skip CRC calculation/check flag */
    public static boolean SKIP_CRC = IgniteSystemProperties.getBoolean(IGNITE_PDS_SKIP_CRC, false);

    /** V1 data serializer. */
    private final RecordDataV1Serializer dataSerializer;

    /** Write pointer. */
    private final boolean writePointer;

    /** Record read/write functional interface. */
    private final RecordIO recordIO = new RecordIO() {

        /** {@inheritDoc} */
        @Override public int sizeWithHeaders(WALRecord record) throws IgniteCheckedException {
            return dataSerializer.size(record) + REC_TYPE_SIZE + FILE_WAL_POINTER_SIZE + CRC_SIZE;
        }

        /** {@inheritDoc} */
        @Override public WALRecord readWithHeaders(ByteBufferBackedDataInput in, WALPointer expPtr) throws IOException, IgniteCheckedException {
            RecordType recType = readRecordType(in);

            if (recType == RecordType.SWITCH_SEGMENT_RECORD)
                throw new SegmentEofException("Reached end of segment", null);

            FileWALPointer ptr = readPosition(in);

            if (!F.eq(ptr, expPtr))
                throw new SegmentEofException("WAL segment rollover detected (will end iteration) [expPtr=" + expPtr +
                        ", readPtr=" + ptr + ']', null);

            return dataSerializer.readRecord(recType, in);
        }

        /** {@inheritDoc} */
        @Override public void writeWithHeaders(WALRecord record, ByteBuffer buf) throws IgniteCheckedException {
            // Write record type.
            putRecordType(buf, record);

            // Write record file position.
            putPositionOfRecord(buf, record);

            // Write record data.
            dataSerializer.writeRecord(record, buf);
        }
    };

    /**
     * Create an instance of V1 serializer.
     *
     * @param dataSerializer V1 data serializer.
     * @param writePointer Write pointer.
     */
    public RecordV1Serializer(RecordDataV1Serializer dataSerializer, boolean writePointer) {
        this.dataSerializer = dataSerializer;
        this.writePointer = writePointer;
    }

    /** {@inheritDoc} */
    @Override public int version() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public boolean writePointer() {
        return writePointer;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("CastConflictsWithInstanceof")
    @Override public void writeRecord(WALRecord record, ByteBuffer buf) throws IgniteCheckedException {
        writeWithCrc(record, buf, recordIO);
    }

    /** {@inheritDoc} */
    @Override public WALRecord readRecord(FileInput in0, WALPointer expPtr) throws  IOException, IgniteCheckedException {
        return readWithCrc(in0, expPtr, recordIO);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("CastConflictsWithInstanceof")
    @Override public int size(WALRecord record) throws IgniteCheckedException {
        return recordIO.sizeWithHeaders(record);
    }

    /**
     * Saves position, WAL pointer (requires {@link #FILE_WAL_POINTER_SIZE} bytes)
     * @param buf Byte buffer to serialize version to.
     * @param ptr File WAL pointer to write.
     */
    public static void putPosition(ByteBuffer buf, FileWALPointer ptr) {
        buf.putLong(ptr.index());
        buf.putInt(ptr.fileOffset());
    }

    /**
     * @param in Data input to read pointer from.
     * @return Read file WAL pointer.
     * @throws IOException If failed to write.
     */
    public static FileWALPointer readPosition(DataInput in) throws IOException {
        long idx = in.readLong();
        int fileOffset = in.readInt();

        return new FileWALPointer(idx, fileOffset, 0);
    }

    /**
     * Writes record file position to given {@code buf}.
     *
     * @param buf Buffer to write record file position.
     * @param record WAL record.
     */
    public static void putPositionOfRecord(ByteBuffer buf, WALRecord record) {
        putPosition(buf, (FileWALPointer) record.position());
    }

    /**
     * Writes record type to given {@code buf}.
     *
     * @param buf Buffer to write record type.
     * @param record WAL record.
     */
    public static void putRecordType(ByteBuffer buf, WALRecord record) {
        buf.put((byte)(record.type().ordinal() + 1));
    }

    /**
     * Reads record type from given {@code in}.
     *
     * @param in Buffer to read record type.
     * @return Record type.
     * @throws IgniteCheckedException If logical end of segment is reached.
     * @throws IOException In case of I/O problems.
     */
    public static RecordType readRecordType(DataInput in) throws IgniteCheckedException, IOException {
        int type = in.readUnsignedByte();

        if (type == WALRecord.RecordType.STOP_ITERATION_RECORD_TYPE)
            throw new SegmentEofException("Reached logical end of the segment", null);

        RecordType recType = RecordType.fromOrdinal(type - 1);

        if (recType == null)
            throw new IOException("Unknown record type: " + type);

        return recType;
    }

    /**
     * Reads record from file {@code in0} and validates CRC of record.
     *
     * @param in0 File input.
     * @param expPtr Expected WAL pointer for record. Used to validate actual position against expected from the file.
     * @param reader Record reader I/O interface.
     * @return WAL record.
     * @throws EOFException In case of end of file.
     * @throws IgniteCheckedException If it's unable to read record.
     */
    public static WALRecord readWithCrc(FileInput in0, WALPointer expPtr, RecordIO reader) throws EOFException, IgniteCheckedException {
        long startPos = -1;

        try (FileInput.Crc32CheckingFileInput in = in0.startRead(SKIP_CRC)) {
            startPos = in0.position();

            WALRecord res = reader.readWithHeaders(in, expPtr);

            assert res != null;

            res.size((int)(in0.position() - startPos + CRC_SIZE)); // Account for CRC which will be read afterwards.

            return res;
        }
        catch (EOFException | SegmentEofException | WalSegmentTailReachedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to read WAL record at position: " + startPos, e);
        }
    }

    /**
     * Writes record with calculated CRC to buffer {@code buf}.
     *
     * @param record WAL record.
     * @param buf Buffer to write.
     * @param writer Record write I/O interface.
     * @throws IgniteCheckedException If it's unable to write record.
     */
    public static void writeWithCrc(WALRecord record, ByteBuffer buf, RecordIO writer) throws IgniteCheckedException {
        assert record.size() >= 0 && buf.remaining() >= record.size() : record.size();

        int startPos = buf.position();

        writer.writeWithHeaders(record, buf);

        if (!SKIP_CRC) {
            int curPos = buf.position();

            buf.position(startPos);

            // This call will move buffer position to the end of the record again.
            int crcVal = PureJavaCrc32.calcCrc32(buf, curPos - startPos);

            buf.putInt(crcVal);
        }
        else
            buf.putInt(0);
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
     * Changes the buffer position by the number of read bytes.
     *
     * @param in Data input to read from.
     * @param allowNull Is {@code null}version allowed.
     * @return Read cache version.
     */
    static GridCacheVersion readVersion(ByteBufferBackedDataInput in, boolean allowNull) throws IOException {
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
}

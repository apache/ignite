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
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.FilteredRecord;
import org.apache.ignite.internal.pagemem.wal.record.MarshalledRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferExpander;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentEofException;
import org.apache.ignite.internal.processors.cache.persistence.wal.WalSegmentTailReachedException;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.FileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentFileInputFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SimpleFileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.record.HeaderRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.io.RecordIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_SKIP_CRC;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.SWITCH_SEGMENT_RECORD;

/**
 * Record V1 serializer.
 * Stores records in following format:
 * <ul>
 *     <li>Record type from {@link RecordType#index()} incremented by 1</li>
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
    public static boolean skipCrc = IgniteSystemProperties.getBoolean(IGNITE_PDS_SKIP_CRC, false);

    /** V1 data serializer. */
    private final RecordDataV1Serializer dataSerializer;

    /** Write pointer. */
    private final boolean writePointer;

    /**
     * Record type filter.
     * {@link FilteredRecord} is deserialized instead of original record if type doesn't match filter.
     */
    private final IgniteBiPredicate<RecordType, WALPointer> recordFilter;

    /** Skip position check flag. Should be set for reading compacted wal file with skipped physical records. */
    private final boolean skipPositionCheck;

    /**
     * Marshalled mode.
     * Records are not deserialized in this mode, {@link MarshalledRecord} with binary representation are read instead.
     */
    private final boolean marshalledMode;

    /** Thread-local heap byte buffer. */
    private final ThreadLocal<ByteBuffer> heapTlb = new ThreadLocal<ByteBuffer>() {
        @Override protected ByteBuffer initialValue() {
            ByteBuffer buf = ByteBuffer.allocate(4096);

            buf.order(GridUnsafe.NATIVE_BYTE_ORDER);

            return buf;
        }
    };

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
        @Override public WALRecord readWithHeaders(ByteBufferBackedDataInput in, WALPointer expPtr) throws IOException, IgniteCheckedException {
            RecordType recType = readRecordType(in);

            if (recType == RecordType.SWITCH_SEGMENT_RECORD)
                throw new SegmentEofException("Reached end of segment", null);

            FileWALPointer ptr = readPosition(in);

            if (!skipPositionCheck && !F.eq(ptr, expPtr))
                throw new SegmentEofException("WAL segment rollover detected (will end iteration) [expPtr=" + expPtr +
                        ", readPtr=" + ptr + ']', null);

            if (recType == null)
                throw new IOException("Unknown record type: " + recType);

            final WALRecord rec = dataSerializer.readRecord(recType, in, 0);

            rec.position(ptr);

            if (recType.purpose() != WALRecord.RecordPurpose.INTERNAL
                && recordFilter != null && !recordFilter.apply(rec.type(), ptr))
                return FilteredRecord.INSTANCE;
            else if (marshalledMode) {
                ByteBuffer buf = heapTlb.get();

                int recordSize = size(rec);

                if (buf.capacity() < recordSize)
                    heapTlb.set(buf = ByteBuffer.allocate(recordSize * 3 / 2).order(ByteOrder.nativeOrder()));
                else
                    buf.clear();

                writeRecord(rec, buf);

                buf.flip();

                assert buf.remaining() == recordSize;

                return new MarshalledRecord(rec.type(), rec.position(), buf);
            }
            else
                return rec;
        }

        /** {@inheritDoc} */
        @Override public void writeWithHeaders(WALRecord rec, ByteBuffer buf) throws IgniteCheckedException {
            // Write record type.
            putRecordType(buf, dataSerializer.recordType(rec));

            // SWITCH_SEGMENT_RECORD should have only type, no need to write pointer.
            if (rec.type() == SWITCH_SEGMENT_RECORD)
                return;

            // Write record file position.
            putPositionOfRecord(buf, rec);

            // Write record data.
            dataSerializer.writeRecord(rec, buf);
        }
    };

    /**
     * Create an instance of V1 serializer.
     * @param dataSerializer V1 data serializer.
     * @param writePointer Write pointer.
     * @param marshalledMode Marshalled mode.
     * @param skipPositionCheck Skip position check mode.
     * @param recordFilter Record type filter. {@link FilteredRecord} is deserialized instead of original record
     */
    public RecordV1Serializer(
        RecordDataV1Serializer dataSerializer,
        boolean writePointer,
        boolean marshalledMode,
        boolean skipPositionCheck,
        IgniteBiPredicate<RecordType, WALPointer> recordFilter
    ) {
        this.dataSerializer = dataSerializer;
        this.writePointer = writePointer;
        this.recordFilter = recordFilter;
        this.skipPositionCheck = skipPositionCheck;
        this.marshalledMode = marshalledMode;
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
    @Override public void writeRecord(WALRecord rec, ByteBuffer buf) throws IgniteCheckedException {
        writeWithCrc(rec, buf, recordIO);
    }

    /** {@inheritDoc} */
    @Override public WALRecord readRecord(FileInput in0, WALPointer expPtr) throws IOException, IgniteCheckedException {
        return readWithCrc(in0, expPtr, recordIO);
    }

    /** {@inheritDoc} */
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
     * Reads stored record from provided {@code io}.
     * NOTE: Method mutates position of {@code io}.
     *
     * @param io I/O interface for file.
     * @param segmentFileInputFactory File input factory.
     * @return Instance of {@link SegmentHeader} extracted from the file.
     * @throws IgniteCheckedException If failed to read serializer version.
     */
    public static SegmentHeader readSegmentHeader(SegmentIO io, SegmentFileInputFactory segmentFileInputFactory)
        throws IgniteCheckedException, IOException {
        try (ByteBufferExpander buf = new ByteBufferExpander(HEADER_RECORD_SIZE, ByteOrder.nativeOrder())) {
            ByteBufferBackedDataInput in = segmentFileInputFactory.createFileInput(io, buf);

            in.ensure(HEADER_RECORD_SIZE);

            int recordType = in.readUnsignedByte();

            if (recordType == WALRecord.RecordType.STOP_ITERATION_RECORD_TYPE)
                throw new SegmentEofException("Reached logical end of the segment", null);

            WALRecord.RecordType type = WALRecord.RecordType.fromIndex(recordType - 1);

            if (type != WALRecord.RecordType.HEADER_RECORD)
                throw new IOException("Can't read serializer version", null);

            // Read file pointer.
            FileWALPointer ptr = readPosition(in);

            if (io.getSegmentId() != ptr.index())
                throw new SegmentEofException("Reached logical end of the segment by pointer", null);

            assert ptr.fileOffset() == 0 : "Header record should be placed at the beginning of file " + ptr;

            long hdrMagicNum = in.readLong();

            boolean compacted;

            if (hdrMagicNum == HeaderRecord.REGULAR_MAGIC)
                compacted = false;
            else if (hdrMagicNum == HeaderRecord.COMPACTED_MAGIC)
                compacted = true;
            else {
                throw new IOException("Magic is corrupted [exp=" + U.hexLong(HeaderRecord.REGULAR_MAGIC) +
                    ", actual=" + U.hexLong(hdrMagicNum) + ']');
            }

            // Read serializer version.
            int ver = in.readInt();

            // Read and skip CRC.
            in.readInt();

            return new SegmentHeader(ver, compacted);
        }
    }

    /**
     * @param in Data input to read pointer from.
     * @return Read file WAL pointer.
     * @throws IOException If failed to write.
     */
    public static FileWALPointer readPosition(DataInput in) throws IOException {
        long idx = in.readLong();
        int fileOff = in.readInt();

        return new FileWALPointer(idx, fileOff, 0);
    }

    /**
     * Writes record file position to given {@code buf}.
     *
     * @param buf Buffer to write record file position.
     * @param rec WAL record.
     */
    private static void putPositionOfRecord(ByteBuffer buf, WALRecord rec) {
        putPosition(buf, (FileWALPointer)rec.position());
    }

    /**
     * Writes record type to given {@code buf}.
     *
     * @param buf Buffer to write record type.
     * @param type WAL record type.
     */
    static void putRecordType(ByteBuffer buf, RecordType type) {
        buf.put((byte)(type.index() + 1));
    }

    /**
     * Reads record type from given {@code in}.
     *
     * @param in Buffer to read record type.
     * @return Record type.
     * @throws IgniteCheckedException If logical end of segment is reached.
     * @throws IOException In case of I/O problems.
     */
    static RecordType readRecordType(DataInput in) throws IgniteCheckedException, IOException {
        int type = in.readUnsignedByte();

        if (type == WALRecord.RecordType.STOP_ITERATION_RECORD_TYPE)
            throw new SegmentEofException("Reached logical end of the segment", null);

        return RecordType.fromIndex(type - 1);
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
    static WALRecord readWithCrc(
        FileInput in0,
        WALPointer expPtr,
        RecordIO reader
    ) throws EOFException, IgniteCheckedException {
        long startPos = -1;

        try (SimpleFileInput.Crc32CheckingFileInput in = in0.startRead(skipCrc)) {
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
            long size = -1;

            try {
                size = in0.io().size();
            }
            catch (IOException ignore) {
                // It just for information. Fail calculate file size.
                e.addSuppressed(ignore);
            }

            throw new IgniteCheckedException(
                "Failed to read WAL record at position: " + startPos + ", size: " + size + ", expectedPtr: " + expPtr, e
            );
        }
    }

    /**
     * Writes record with calculated CRC to buffer {@code buf}.
     *
     * @param rec WAL record.
     * @param buf Buffer to write.
     * @param writer Record write I/O interface.
     * @throws IgniteCheckedException If it's unable to write record.
     */
    static void writeWithCrc(WALRecord rec, ByteBuffer buf, RecordIO writer) throws IgniteCheckedException {
        assert rec.size() >= 0 && buf.remaining() >= rec.size() : rec.size();

        boolean switchSegmentRec = rec.type() == RecordType.SWITCH_SEGMENT_RECORD;

        int startPos = buf.position();

        writer.writeWithHeaders(rec, buf);

        // No need calculate and write CRC for SWITCH_SEGMENT_RECORD.
        if (switchSegmentRec)
            return;

        if (!skipCrc) {
            int curPos = buf.position();

            buf.position(startPos);

            // This call will move buffer position to the end of the record again.
            int crcVal = FastCrc.calcCrc(buf, curPos - startPos);

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

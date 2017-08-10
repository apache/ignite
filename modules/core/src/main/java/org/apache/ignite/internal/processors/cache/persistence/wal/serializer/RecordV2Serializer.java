package org.apache.ignite.internal.processors.cache.persistence.wal.serializer;

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
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.io.RecordIO;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Record V2 serializer.
 * Stores records in following format:
 * <ul>
 *     <li>Record type from {@link WALRecord.RecordType#ordinal()} incremented by 1</li>
 *     <li>WAL pointer to double check consistency</li>
 *     TODO: record data length
 *     <li>Data</li>
 *     <li>CRC or zero padding</li>
 * </ul>
 */
public class RecordV2Serializer implements RecordSerializer {
    // TODO: Define additional header size here
    // public static final int RECORD_LENGTH_HEADER_SIZE = 4;

    /** */
    private final RecordDataV2Serializer dataSerializer;

    /** Record read/write functional interface. */
    private final RecordIO recordIO = new RecordIO() {

        @Override public int size(WALRecord record) throws IgniteCheckedException {
            // TODO: Add to size the record length header size
            return dataSerializer.size(record);
        }

        /**
         * Reads record from input, does not read CRC value.
         *
         * @param in Input to read record from
         * @param expPtr expected WAL pointer for record. Used to validate actual position against expected from the file
         * @throws SegmentEofException if end of WAL segment reached
         */
        @Override public WALRecord read(ByteBufferBackedDataInput in, WALPointer expPtr) throws IOException, IgniteCheckedException {
            WALRecord.RecordType recType = RecordV1Serializer.readRecordType(in);

            FileWALPointer ptr = RecordV1Serializer.readPosition(in);

            if (!F.eq(ptr, expPtr))
                throw new SegmentEofException("WAL segment rollover detected (will end iteration) [expPtr=" + expPtr +
                        ", readPtr=" + ptr + ']', null);

            //TODO read record length here

            return dataSerializer.readRecord(recType, in);
        }

        /**
         * Writes record to output, does not write CRC value.
         *
         * @param record
         * @param buf
         * @throws IgniteCheckedException
         */
        @Override public void write(WALRecord record, ByteBuffer buf) throws IgniteCheckedException {
            // Write record type.
            RecordV1Serializer.putRecordType(buf, record);

            // Write record file position.
            RecordV1Serializer.putPositionOfRecord(buf, record);

            // TODO write record length here.

            // Write record data.
            dataSerializer.writeRecord(record, buf);
        }
    };

    /**
     * Create an instance of Record V2 serializer.
     *
     * @param dataSerializer Record data V2 serializer.
     */
    public RecordV2Serializer(RecordDataV2Serializer dataSerializer) {
        this.dataSerializer = dataSerializer;
    }

    /** {@inheritDoc} */
    @Override public int version() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public int size(WALRecord record) throws IgniteCheckedException {
        return RecordV1Serializer.sizeWithHeadersAndCrc(record, recordIO);
    }

    /** {@inheritDoc} */
    @Override public void writeRecord(WALRecord record, ByteBuffer buf) throws IgniteCheckedException {
        RecordV1Serializer.writeWithCrc(record, buf, recordIO);
    }

    /** {@inheritDoc} */
    @Override public WALRecord readRecord(FileInput in, WALPointer expPtr) throws IOException, IgniteCheckedException {
        return RecordV1Serializer.readWithCrc(in, expPtr, recordIO);
    }
}

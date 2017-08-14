package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;

/**
 * Interface to provide size, read and write operations with WAL records
 * <b>without any headers and meta information</b>.
 */
public interface RecordDataSerializer {
    /**
     * Calculates size of record data.
     *
     * @param record WAL record.
     * @return Size of record in bytes.
     * @throws IgniteCheckedException If it's unable to calculate record data size.
     */
    int size(WALRecord record) throws IgniteCheckedException;

    /**
     * Reads record data of {@code type} from buffer {@code in}.
     *
     * @param type Record type.
     * @param in Buffer to read.
     * @return WAL record.
     * @throws IOException In case of I/O problems.
     * @throws IgniteCheckedException If it's unable to read record.
     */
    WALRecord readRecord(WALRecord.RecordType type, ByteBufferBackedDataInput in) throws IOException, IgniteCheckedException;

    /**
     * Writes record data to buffer {@code buf}.
     *
     * @param record WAL record.
     * @param buf Buffer to write.
     * @throws IgniteCheckedException If it's unable to write record.
     */
    void writeRecord(WALRecord record, ByteBuffer buf) throws IgniteCheckedException;
}

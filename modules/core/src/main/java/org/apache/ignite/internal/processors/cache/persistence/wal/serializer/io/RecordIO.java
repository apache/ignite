package org.apache.ignite.internal.processors.cache.persistence.wal.serializer.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;

/**
 * Internal interface to provide size, read and write operations of WAL records
 * including record header and data.
 */
public interface RecordIO {
    /**
     * Calculates and returns size of record data and headers.
     *
     * @param record WAL record.
     * @return Size in bytes.
     * @throws IgniteCheckedException If it's unable to calculate size of record.
     */
    int sizeWithHeaders(WALRecord record) throws IgniteCheckedException;

    /**
     * Reads record data with headers from {@code in}.
     *
     * @param in Buffer to read.
     * @param expPtr Expected WAL pointer for record. Used to validate actual position against expected from the file.
     * @return WAL record.
     * @throws IOException In case of I/O problems.
     * @throws IgniteCheckedException If it's unable to read record.
     */
    WALRecord readWithHeaders(ByteBufferBackedDataInput in, WALPointer expPtr) throws IOException, IgniteCheckedException;

    /**
     * Writes record data with headers to {@code buf}.
     *
     * @param record WAL record.
     * @param buf Buffer to write.
     * @throws IgniteCheckedException If it's unable to write record.
     */
    void writeWithHeaders(WALRecord record, ByteBuffer buf) throws IgniteCheckedException;
}

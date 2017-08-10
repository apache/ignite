package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;

/**
 * Interface to provide size, read and write operations with WAL records
 * including only <b>record without headers and other meta information</b>
 */
public interface RecordDataSerializer {
    /**
     *
     * @param record
     * @return
     * @throws IgniteCheckedException
     */
    int size(WALRecord record) throws IgniteCheckedException;

    /**
     *
     * @param type
     * @param in
     * @return
     * @throws IOException
     * @throws IgniteCheckedException
     */
    WALRecord readRecord(WALRecord.RecordType type, ByteBufferBackedDataInput in) throws IOException, IgniteCheckedException;

    /**
     *
     * @param record
     * @param buf
     * @throws IgniteCheckedException
     */
    void writeRecord(WALRecord record, ByteBuffer buf) throws IgniteCheckedException;
}

package org.apache.ignite.internal.processors.cache.database.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;

/**
 * Record serializer.
 */
public interface RecordSerializer {
    /**
     * @return writer
     */
    public int version();

    /**
     * @param record Record.
     * @return Size in bytes.
     */
    public int size(WALRecord record) throws IgniteCheckedException;

    /**
     * @param record Entry to write.
     * @param buf Buffer.
     */
    public void writeRecord(WALRecord record, ByteBuffer buf) throws IgniteCheckedException;

    /**
     * @param in Data input to read data from.
     * @return Read entry.
     */
    public WALRecord readRecord(FileInput in) throws IOException, IgniteCheckedException;
}

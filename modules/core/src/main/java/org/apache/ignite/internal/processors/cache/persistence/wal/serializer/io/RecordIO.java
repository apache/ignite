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
     *
     * @param record
     * @return
     * @throws IgniteCheckedException
     */
    int size(WALRecord record) throws IgniteCheckedException;

    /**
     *
     * @param in
     * @param expPtr
     * @return
     * @throws IOException
     * @throws IgniteCheckedException
     */
    WALRecord read(ByteBufferBackedDataInput in, WALPointer expPtr) throws IOException, IgniteCheckedException;

    /**
     *
     * @param record
     * @param buf
     * @throws IgniteCheckedException
     */
    void write(WALRecord record, ByteBuffer buf) throws IgniteCheckedException;
}

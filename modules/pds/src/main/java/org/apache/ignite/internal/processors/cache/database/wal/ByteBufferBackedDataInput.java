package org.apache.ignite.internal.processors.cache.database.wal;


import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * ByteBuffer backed data input
 */
public interface ByteBufferBackedDataInput extends DataInput {
    /**
     * @return ByteBuffer hold by data input
     */
    public ByteBuffer buffer();

    /**
     * ensure that requested count of byte is available in data input and will try to load data if not
     * @param requested Requested number of bytes.
     * @throws IOException If failed.
     */
    public void ensure(int requested) throws IOException;
}

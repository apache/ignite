package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * ByteBuffer wrapper for dynamically expand buffer size.
 */
public class ByteBufferExpander {
    /** Byte buffer */
    private ByteBuffer buf;

    public ByteBufferExpander(int initSize, ByteOrder order) {
        ByteBuffer buffer = ByteBuffer.allocate(initSize);
        buffer.order(order);

        this.buf = buffer;
    }

    /**
     * Current byte buffer.
     *
     * @return Current byteBuffer.
     */
    public ByteBuffer buffer() {
        return buf;
    }

    /**
     * Expands current byte buffer to the requested size.
     *
     * @return ByteBuffer with requested size.
     */
    public ByteBuffer expand(int size) {
        ByteBuffer newBuf = ByteBuffer.allocate(size);

        newBuf.order(buf.order());

        newBuf.put(buf);

        newBuf.flip();

        buf = newBuf;

        return newBuf;
    }
}

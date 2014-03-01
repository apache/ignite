/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.store.local;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;

/**
 * Scanner for channel.
 *
 * @author @java.author
 * @version @java.version
 */
class GridCacheFileLocalChannelScanner {
    /** */
    private final ByteBuffer buf;

    /** */
    private final SeekableByteChannel ch;

    /**
     * @param buf Buffer.
     * @param ch Channel.
     */
    GridCacheFileLocalChannelScanner(ByteBuffer buf, SeekableByteChannel ch) {
        assert ch != null;
        assert buf.capacity() >= 8;

        clear(buf);

        this.buf = buf;
        this.ch = ch;
    }

    /**
     * @param bytes Number of bytes.
     * @throws IOException If failed.
     */
    private void need(int bytes) throws IOException {
        if (buf.remaining() < bytes) {
            buf.compact();
            ch.read(buf);
            buf.flip();

            if (buf.remaining() < bytes)
                throw new EOFException();
        }
    }

    /**
     * @return Byte.
     * @throws IOException If failed.
     */
    public byte get() throws IOException {
        need(1);

        return buf.get();
    }

    /**
     * @return Short.
     * @throws IOException If failed.
     */
    public short getShort() throws IOException {
        need(2);

        return buf.getShort();
    }

    /**
     * @return Integer.
     * @throws IOException If failed.
     */
    public int getInt() throws IOException {
        need(4);

        return buf.getInt();
    }

    /**
     * @return Long.
     * @throws IOException If failed.
     */
    public long getLong() throws IOException {
        need(8);

        return buf.getLong();
    }

    /**
     * @param bytes Bytes to skip.
     * @throws IOException If failed.
     * @return {@code false} If position will become greater than size.
     */
    public boolean skip(long bytes) throws IOException {
        assert bytes >= 0 : bytes;

        if (bytes == 0)
            return true;

        if (bytes <= buf.remaining())
            buf.position(buf.position() + (int)bytes);
        else {
            long newPos = position() + bytes;

            if (newPos > ch.size())
                return false;

            clear(buf);

            ch.position(newPos);
        }

        return true;
    }

    /**
     * @param buf Buffer.
     */
    private static void clear(ByteBuffer buf) {
        buf.clear();
        buf.limit(0);
    }

    /**
     * @return Scanner position.
     * @throws IOException If failed.
     */
    public long position() throws IOException {
        return ch.position() - buf.remaining();
    }
}

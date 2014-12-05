/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs.mapreduce;

import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Decorator for regular {@link org.gridgain.grid.ggfs.IgniteFsInputStream} which streams only data within the given range.
 * This stream is used for {@link IgniteFsInputStreamJobAdapter} convenience adapter to create
 * jobs which will be working only with the assigned range. You can also use it explicitly when
 * working with {@link IgniteFsJob} directly.
 */
public final class GridGgfsRangeInputStream extends IgniteFsInputStream {
    /** Base input stream. */
    private final IgniteFsInputStream is;

    /** Start position. */
    private final long start;

    /** Maximum stream length. */
    private final long maxLen;

    /** Current position within the stream. */
    private long pos;

    /**
     * Constructor.
     *
     * @param is Base input stream.
     * @param start Start position.
     * @param maxLen Maximum stream length.
     * @throws IOException In case of exception.
     */
    public GridGgfsRangeInputStream(IgniteFsInputStream is, long start, long maxLen) throws IOException {
        if (is == null)
            throw new IllegalArgumentException("Input stream cannot be null.");

        if (start < 0)
            throw new IllegalArgumentException("Start position cannot be negative.");

        if (start >= is.length())
            throw new IllegalArgumentException("Start position cannot be greater that file length.");

        if (maxLen < 0)
            throw new IllegalArgumentException("Length cannot be negative.");

        if (start + maxLen > is.length())
            throw new IllegalArgumentException("Sum of start position and length cannot be greater than file length.");

        this.is = is;
        this.start = start;
        this.maxLen = maxLen;

        is.seek(start);
    }

    /** {@inheritDoc} */
    @Override public long length() {
        return is.length();
    }

    /**
     * Constructor.
     *
     * @param is Base input stream.
     * @param range File range.
     * @throws IOException In case of exception.
     */
    public GridGgfsRangeInputStream(IgniteFsInputStream is, IgniteFsFileRange range) throws IOException {
        this(is, range.start(), range.length());
    }

    /** {@inheritDoc} */
    @Override public int read() throws IOException {
        if (pos < maxLen) {
            int res = is.read();

            if (res != -1)
                pos++;

            return res;
        }
        else
            return -1;
    }

    /** {@inheritDoc} */
    @Override public int read(@NotNull byte[] b, int off, int len) throws IOException {
        if (pos < maxLen) {
            len = (int)Math.min(len, maxLen - pos);

            int res = is.read(b, off, len);

            if (res != -1)
                pos += res;

            return res;
        }
        else
            return -1;
    }

    /** {@inheritDoc} */
    @Override public int read(long pos, byte[] buf, int off, int len) throws IOException {
        seek(pos);

        return read(buf, off, len);
    }

    /** {@inheritDoc} */
    @Override public void readFully(long pos, byte[] buf) throws IOException {
        readFully(pos, buf, 0, buf.length);
    }

    /** {@inheritDoc} */
    @Override public void readFully(long pos, byte[] buf, int off, int len) throws IOException {
        seek(pos);

        for (int readBytes = 0; readBytes < len;) {
            int read = read(buf, off + readBytes, len - readBytes);

            if (read == -1)
                throw new EOFException("Failed to read stream fully (stream ends unexpectedly) [pos=" + pos +
                    ", buf.length=" + buf.length + ", off=" + off + ", len=" + len + ']');

            readBytes += read;
        }
    }

    /** {@inheritDoc} */
    @Override public void seek(long pos) throws IOException {
        if (pos < 0)
            throw new IOException("Seek position cannot be negative: " + pos);

        is.seek(start + pos);

        this.pos = pos;
    }

    /** {@inheritDoc} */
    @Override public long position() {
        return pos;
    }

    /**
     * Since range input stream represents a part of larger file stream, there is an offset at which this
     * range input stream starts in original input stream. This method returns start offset of this input
     * stream relative to original input stream.
     *
     * @return Start offset in original input stream.
     */
    public long startOffset() {
        return start;
    }

    /** {@inheritDoc} */
    @Override public int available() {
        long l = maxLen - pos;

        if (l < 0)
            return 0;

        if (l > Integer.MAX_VALUE)
            return Integer.MAX_VALUE;

        return (int)l;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        is.close();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridGgfsRangeInputStream.class, this);
    }
}

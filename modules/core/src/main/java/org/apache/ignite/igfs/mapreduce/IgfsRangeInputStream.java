/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.igfs.mapreduce;

import java.io.EOFException;
import java.io.IOException;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Decorator for regular {@link org.apache.ignite.igfs.IgfsInputStream} which streams only data within the given range.
 * This stream is used for {@link IgfsInputStreamJobAdapter} convenience adapter to create
 * jobs which will be working only with the assigned range. You can also use it explicitly when
 * working with {@link IgfsJob} directly.
 */
public final class IgfsRangeInputStream extends IgfsInputStream {
    /** Base input stream. */
    private final IgfsInputStream is;

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
    public IgfsRangeInputStream(IgfsInputStream is, long start, long maxLen) throws IOException {
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
    public IgfsRangeInputStream(IgfsInputStream is, IgfsFileRange range) throws IOException {
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
        return S.toString(IgfsRangeInputStream.class, this);
    }
}
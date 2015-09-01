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

package org.apache.ignite.internal.util.io;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This class defines input stream backed by byte array.
 * It is identical to {@link ByteArrayInputStream} with no synchronization.
 */
public class GridByteArrayInputStream extends InputStream {
    /** */
    private byte buf[];

    /** */
    private int pos;

    /** */
    private int mark;

    /** */
    private int cnt;

    /**
     * @param buf The input buffer.
     */
    public GridByteArrayInputStream(byte buf[]) {
        this.buf = buf;

        pos = 0;
        cnt = buf.length;
    }

    /**
     * @param buf The input buffer.
     * @param off The offset in the buffer of the first byte to read.
     * @param len The maximum number of bytes to read from the buffer.
     */
    public GridByteArrayInputStream(byte buf[], int off, int len) {
        this.buf = buf;

        pos = off;
        cnt = Math.min(off + len, buf.length);
        mark = off;
    }

    /**
     * Reads the next byte of data from this input stream. The value
     * byte is returned as an {@code int} in the range
     * {@code 0} to {@code 255}. If no byte is available
     * because the end of the stream has been reached, the value
     * {@code -1} is returned.
     * <p>
     * This method cannot block.
     *
     * @return The next byte of data, or {@code -1} if the end of the
     *      stream has been reached.
     */
    @Override public int read() {
        return (pos < cnt) ? (buf[pos++] & 0xff) : -1;
    }

    /**
     * Reads up to {@code len} bytes of data into an array of bytes from this input stream.
     * If {@code pos} equals {@code count}, then {@code -1} is returned to indicate
     * end of file. Otherwise, the  number {@code k} of bytes read is equal to the smaller of
     * {@code len} and {@code count-pos}. If {@code k} is positive, then bytes
     * {@code buf[pos]} through {@code buf[pos+k-1]} are copied into {@code b[off]} through
     * {@code b[off+k-1]} in the manner performed by {@code System.arraycopy}. The value
     * {@code k} is added into {@code pos} and {@code k} is returned.
     * <p>
     * This {@code read} method cannot block.
     *
     * @param b The buffer into which the data is read.
     * @param off The start offset in the destination array {@code b}
     * @param len The maximum number of bytes read.
     * @return The total number of bytes read into the buffer, or
     *      {@code -1} if there is no more data because the end of
     *      the stream has been reached.
     * @throws NullPointerException If {@code b} is {@code null}.
     * @throws IndexOutOfBoundsException If {@code off} is negative,
     *      {@code len} is negative, or {@code len} is greater than {@code b.length - off}
     */
    @Override public int read(byte b[], int off, int len) {
        if (b == null)
            throw new NullPointerException();
        else if (off < 0 || len < 0 || len > b.length - off)
            throw new IndexOutOfBoundsException();

        if (pos >= cnt)
            return -1;

        if (pos + len > cnt)
            len = cnt - pos;

        if (len <= 0)
            return 0;

        U.arrayCopy(buf, pos, b, off, len);

        pos += len;

        return len;
    }

    /**
     * Skips {@code n} bytes of input from this input stream. Fewer
     * bytes might be skipped if the end of the input stream is reached.
     * The actual number {@code k}
     * of bytes to be skipped is equal to the smaller
     * of {@code n} and  {@code count-pos}.
     * The value {@code k} is added into {@code pos}
     * and {@code k} is returned.
     *
     * @param n The number of bytes to be skipped.
     * @return The actual number of bytes skipped.
     */
    @Override public long skip(long n) {
        if (pos + n > cnt)
            n = cnt - pos;

        if (n < 0)
            return 0;

        pos += n;

        return n;
    }

    /**
     * Returns the number of remaining bytes that can be read (or skipped over)
     * from this input stream.
     * <p>
     * The value returned is {@code count - pos}, which is the number of bytes
     * remaining to be read from the input buffer.
     *
     * @return The number of remaining bytes that can be read (or skipped
     *      over) from this input stream without blocking.
     */
    @Override public int available() {
        return cnt - pos;
    }

    /**
     * Tests if this {@code InputStream} supports mark/reset.
     * <p>
     * This method always returns {@code true}.
     */
    @Override public boolean markSupported() {
        return true;
    }

    /**
     * Set the current marked position in the stream.
     * ByteArrayInputStream objects are marked at position zero by
     * default when constructed. They may be marked at another
     * position within the buffer by this method.
     * <p>
     * If no mark has been set, then the value of the mark is the
     * offset passed to the constructor (or 0 if the offset was not
     * supplied).
     *
     * <p>
     * Note: The {@code readAheadLimit} for this class has no meaning.
     */
    @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
    @Override public void mark(int readAheadLimit) {
        mark = pos;
    }

    /**
     * Resets the buffer to the marked position. The marked position
     * is 0 unless another position was marked or an offset was specified
     * in the constructor.
     */
    @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
    @Override public void reset() {
        pos = mark;
    }

    /**
     * Closing of this stream has no effect.
     */
    @Override public void close() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridByteArrayInputStream.class, this);
    }
}
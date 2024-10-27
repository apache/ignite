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

package org.apache.ignite.internal.jdbc2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.Objects;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.binary.streams.BinaryAbstractOutputStream.MAX_ARRAY_SIZE;

/**
 * Buffer storing the binary data.
 *
 * <p>Buffer can start working in read-only mode if created wrapping the existing byte array which
 * can not be modified. Any write operation switches it lazily to the read-write mode. This allows
 * to prevent the unnecessary data copying.
 *
 * <p>Data may be read via the InputStream API and modified via the OutputStream one. Changes done via
 * OutputStream are visible via the InputStream even if InputStream is created before changes done.
 *
 * <p>InputStream and OutputStream created remain valid even if the underlying data storage changed from
 * read-only to read-write.
 *
 * <p>Note however that implementation is not thread-safe.
 */
public class JdbcBinaryBuffer {
    /** Byte array storing data. */
    private byte[] arr;

    /** Offset the data starts in the array. */
    private int off;

    /** The length of data. */
    private int length;

    /** Read only flag. */
    private boolean isReadOnly;

    /** Minimum buffer capacity. */
    private static final int MIN_CAP = 256;

    /**
     * Create buffer which wraps the existing byte array and start working in the read-only mode.
     *
     * @param arr The byte array to be wrapped.
     * @param off The offset to the first byte to be wrapped.
     * @param len The length in bytes of the data to be wrapped.
     */
    public static JdbcBinaryBuffer createReadOnly(byte[] arr, int off, int len) {
        return new JdbcBinaryBuffer(arr, off, len, true);
    }

    /**
     * Create buffer which takes ownerhip of and wraps data in the existing byte array and
     * starts working in the read-write mode.
     *
     * @param arr The byte array to be wrapped.
     */
    public static JdbcBinaryBuffer createReadWrite(byte[] arr) {
        return new JdbcBinaryBuffer(arr, 0, arr.length, false);
    }

    /**
     * Create empty buffer which starts working in the read-write mode.
     */
    public static JdbcBinaryBuffer createReadWrite() {
        return new JdbcBinaryBuffer(new byte[MIN_CAP], 0, 0, false);
    }

    /**
     * Private constructor.
     *
     * @param arr The byte array to be wrapped.
     * @param off The offset to the first byte to be wrapped.
     * @param length The length in bytes of the data to be wrapped.
     * @param isReadOnly The read-only flag.
     */
    private JdbcBinaryBuffer(byte[] arr, int off, int length, boolean isReadOnly) {
        this.arr = arr;
        this.off = off;
        this.length = length;
        this.isReadOnly = isReadOnly;
    }

    /**
     * Provide OutputStream through which the data can be written to buffer starting from
     * the (zero-based) {@code pos} position.
     *
     * @param pos The zero-based offset to the first byte to be written. Must not be negative
     *            or greater than total count of bytes in buffer.
     *
     * @return OutputStream instance.
     */
    public OutputStream getOutputStream(int pos) {
        return new BufferOutputStream(pos);
    }

    /**
     * Provide InputStream through which the data can be read starting from the
     * begining.
     *
     * <p>Stream is not limited meaning that it would return any new data
     * written to the buffer after stream creation.
     *
     * @return InputStream instance.
     */
    public InputStream getInputStream() {
        return new BufferInputStream();
    }

    /**
     * Provide InputStream through which the no more than {@code len} bytes can be read
     * from buffer starting from the specified zero-based position {@code pos}.
     *
     * @param pos The zero-based offset to the first byte to be retrieved. Must not be negative
     *            or greater than total count of bytes in buffer.
     * @param len The length in bytes of the data to be retrieved. Must not be negative.
     * @return InputStream instance.
     */
    public InputStream getInputStream(int pos, int len) {
        return new BufferInputStream(pos, len);
    }

    /**
     * Get copy of the buffer data as byte array.
     *
     * @return Byte array containing buffer data.
     */
    public byte[] getBytes() {
        byte[] bytes = new byte[length];

        read(0, bytes, 0, length);

        return bytes;
    }

    /**
     * Truncate data in this buffer to specified length.
     *
     * @param len New length.
     */
    public void truncate(int len) {
        byte[] newArr = new byte[Math.max(MIN_CAP, len)];

        U.arrayCopy(arr, off, newArr, 0, len);

        arr = newArr;

        length = len;

        off = 0;
    }

    /**
     * Create shallow read-only copy of this buffer.
     */
    public JdbcBinaryBuffer shallowCopy() {
        return new JdbcBinaryBuffer(arr, off, length, true);
    }

    /**
     * @return Length of data in this buffer.
     */
    public int length() {
        return length;
    }

    /**
     * Read up to {@code resLen} bytes from this buffer from specified position {@code pos}.
     *
     * @param pos Pointer to a position.
     * @param resBuf Output byte array to write to.
     * @param resOff Offset in the output array to start write to.
     * @param resLen Number of bytes to read.
     * @return Number of bytes read. -1 if end of data reached.
     */
    public int read(int pos, byte[] resBuf, int resOff, int resLen) {
        if (pos >= length)
            return -1;

        int bufOff = pos + off;

        int size = Math.min(resLen, length - pos);

        U.arrayCopy(arr, bufOff, resBuf, resOff, size);

        return size;
    }

    /**
     * Writes {@code inpLen} bytes from the specified byte array {@code bytes} starting at offset {@code off}
     * to this storage to specified position {@code pos}.
     *
     * @param pos Pointer to a position.
     * @param inpBuf Input byte array.
     * @param inpOff Start offset in the input array.
     * @param inpLen Number of bytes to write.
     */
    public void write(int pos, byte[] inpBuf, int inpOff, int inpLen) throws SQLException {
        Objects.checkFromIndexSize(inpOff, inpLen, inpBuf.length);

        if (pos > length)
            throw new SQLException("Writting beyond end of Blob, it probably was truncated after OutputStream was created " +
                    "[pos=" + pos + ", blobLength=" + length + "]");

        if (MAX_ARRAY_SIZE - pos < inpLen)
            throw new SQLException("Too much data. Can't write more then " + MAX_ARRAY_SIZE + " bytes to Blob.");

        int newLen = Math.max(pos + inpLen, length);

        ensureCapacity(newLen);

        U.arrayCopy(inpBuf, inpOff, arr, pos, inpLen);

        length = newLen;
    }

    /**
     * Read a byte from this buffer from specified position {@code pos}.
     *
     * @param pos Position.
     * @return Byte read from the Blob. -1 if end of data reached.
     */
    private int read(int pos) {
        if (pos >= length)
            return -1;

        return arr[pos + off] & 0xff;
    }

    /**
     * Write a byte to this buffer to specified position {@code pos}.
     *
     * <p>The byte to be written is the eight low-order bits of the
     * argument {@code b}. The 24 high-order bits of {@code b}b are ignored.
     *
     * @param pos Pointer to a position.
     * @param b Byte to write.
     */
    private void write(int pos, int b) throws SQLException {
        if (pos > length)
            throw new SQLException("Writting beyond end of Blob, it probably was truncated after OutputStream was created " +
                    "[pos=" + pos + ", blobLength=" + length + "]");

        if (MAX_ARRAY_SIZE - pos < 1)
            throw new SQLException("Too much data. Can't write more then " + MAX_ARRAY_SIZE + " bytes to Blob.");

        int newLen = Math.max(pos + 1, length);

        ensureCapacity(newLen);

        arr[pos] = (byte)b;

        length = newLen;
    }

    /**
     * Ensure capacity.
     *
     * @param newLen The new data length the buffer should be able to hold.
     */
    private void ensureCapacity(int newLen) {
        if (newLen - arr.length > 0 || isReadOnly)
            reallocate(capacity(arr.length, newLen));
    }

    /**
     * Calculate new capacity.
     *
     * @param curCap Current capacity.
     * @param reqLen Required new data length.
     * @return New capacity.
     */
    private static int capacity(int curCap, int reqLen) {
        int newCap;

        if (reqLen < MIN_CAP)
            newCap = MIN_CAP;
        else {
            newCap = Math.max(curCap, MIN_CAP);

            while (newCap < reqLen) {
                newCap <<= 1;

                if (newCap < 0)
                    newCap = MAX_ARRAY_SIZE;
            }
        }

        return newCap;
    }

    /**
     * Allocate the new underlining array and copy data.
     *
     * @param newCapacity New capacity.
     */
    private void reallocate(int newCapacity) {
        byte[] newBuf = new byte[newCapacity];

        U.arrayCopy(arr, off, newBuf, 0, length);

        arr = newBuf;
        off = 0;
        isReadOnly = false;
    }

    /**
     * Input stream to read data from buffer.
     */
    private class BufferInputStream extends InputStream {
        /** Stream starting position. */
        private final int start;

        /** Stream length limit. -1 means no limit. */
        private final int limit;

        /** Current position in the buffer. */
        private int pos;

        /** Remembered buffer position at the moment the {@link InputStream#mark} is called. */
        private int markedPos;

        /**
         * Create unlimited stream to read all data from the buffer starting from the beginning.
         */
        private BufferInputStream() {
            this(0, -1);
        }

        /**
         * Create stream to read data from the buffer starting from the specified {@code start}
         * zero-based position.
         *
         * @param start The zero-based offset to the first byte to be retrieved.
         * @param limit The maximim length in bytes of the data to be retrieved. Unlimited if null.
         */
        private BufferInputStream(int start, int limit) {
            this.start = start;

            pos = start;

            markedPos = start;

            this.limit = limit;
        }

        /** {@inheritDoc} */
        @Override public int read() {
            if (limit != -1 && pos - start >= limit)
                return -1;

            int res = JdbcBinaryBuffer.this.read(pos);

            if (res != -1)
                pos++;

            return res;
        }

        /** {@inheritDoc} */
        @Override public int read(byte[] res, int off, int cnt) {
            Objects.checkFromIndexSize(off, cnt, res.length);

            int toRead = cnt;

            if (limit != -1) {
                if (pos - start >= limit)
                    return -1;

                int availableBytes = limit - (pos - start);

                if (cnt > availableBytes)
                    toRead = availableBytes;
            }

            int read = JdbcBinaryBuffer.this.read(pos, res, off, toRead);

            if (read != -1)
                pos += read;

            return read;
        }

        /** {@inheritDoc} */
        @Override public boolean markSupported() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public synchronized void reset() {
            pos = markedPos;
        }

        /** {@inheritDoc} */
        @Override public synchronized void mark(int readlimit) {
            markedPos = pos;
        }

        /** {@inheritDoc} */
        @Override public long skip(long n) {
            if (n <= 0)
                return 0;

            int step = Math.min((int)Math.min(n, MAX_ARRAY_SIZE), length - pos);

            pos += step;

            return step;
        }
    }

    /**
     * Output stream to write data to buffer.
     */
    private class BufferOutputStream extends OutputStream {
        /** Current position in the buffer. */
        private int pos;

        /**
         * Create stream to write data to the buffer starting from the specified position {@code pos}.
         *
         * @param pos Starting position (zero-based).
         */
        private BufferOutputStream(int pos) {
            this.pos = pos;
        }

        /** {@inheritDoc} */
        @Override public void write(int b) throws IOException {
            try {
                JdbcBinaryBuffer.this.write(pos++, b);
            }
            catch (SQLException e) {
                throw new IOException(e.getMessage());
            }
        }

        /** {@inheritDoc} */
        @Override public void write(byte[] b, int off, int len) throws IOException {
            Objects.checkFromIndexSize(off, len, b.length);

            try {
                JdbcBinaryBuffer.this.write(pos, b, off, len);
            }
            catch (SQLException e) {
                throw new IOException(e.getMessage());
            }

            pos += len;
        }
    }
}

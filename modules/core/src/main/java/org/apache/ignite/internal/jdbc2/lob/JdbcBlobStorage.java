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

package org.apache.ignite.internal.jdbc2.lob;

import java.io.IOException;
import java.util.Objects;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.binary.streams.BinaryAbstractOutputStream.MAX_ARRAY_SIZE;

/**
 * Base class for in-memory storages providing random access to binary data.
 *
 * <p>Used by the {@link JdbcBlobStreams}
 */
public class JdbcBlobStorage {
    /** The total number of bytes in all buffers. */
    private int totalCnt;

    /** External buffer. */
    private byte[] buf;

    /** Offset to the first byte to be wrapped. */
    private int off;

    /** Read only flag. */
    private boolean isReadOnly;

    /** Minimum buffer size. */
    private static final int MIN_BUFFER_SIZE = 8 * 1024;

    /**
     * Create buffer which wraps the existing byte array and start working in the read-only mode.
     *
     * @param buf The byte array to be wrapped.
     * @param off The offset to the first byte to be wrapped.
     * @param len The length in bytes of the data to be wrapped.
     */
    public JdbcBlobStorage(byte[] buf, int off, int len) {
        this.buf = buf;
        this.off = off;
        totalCnt = len;

        isReadOnly = true;
    }

    /**
     * Create empty buffer which starts working in the read-write mode.
     */
    public JdbcBlobStorage() {
        buf = new byte[MIN_BUFFER_SIZE];
        off = 0;
        totalCnt = 0;
        isReadOnly = false;
    }

    /**
     * Create buffer which takes ownerhip of and wraps the existing byte array and starts working in
     * the read-write mode.
     *
     * @param arr The byte array to be wrapped.
     */
    public JdbcBlobStorage(byte[] arr) {
        buf = arr;
        off = 0;
        totalCnt = arr.length;
        isReadOnly = false;
    }

    /**
     * Create shallow copy of the buffer passed.
     *
     * <p>Sharing of the underlying storage is intended.
     *
     * @param other Other buffer.
     */
    public static JdbcBlobStorage shallowCopy(JdbcBlobStorage other) {
        return new JdbcBlobStorage(other.buf, other.off, other.totalCnt);
    }

    /**
     * @return Total number of bytes in the storage.
     */
    public int totalCnt() {
        return totalCnt;
    }

    /**
     * Read a byte from this storage from specified position {@code pos}.
     *
     * @param pos Pointer to a position.
     * @return Byte read from the Blob. -1 if EOF.
     */
    int read(int pos) {
        if (pos >= totalCnt)
            return -1;

        return buf[pos + off] & 0xff;
    }

    /**
     * Read {@code cnt} bytes from this storage from specified position {@code pos}.
     *
     * @param pos Pointer to a position.
     * @param resBuf Output byte array to write to.
     * @param resOff Offset in the output array to start write to.
     * @param cnt Number of bytes to read.
     * @return Number of bytes read. -1 if EOF.
     */
    int read(int pos, byte[] resBuf, int resOff, int cnt) {
        if (pos >= totalCnt)
            return -1;

        int bufOff = pos + off;

        int size = Math.min(cnt, totalCnt - pos);

        U.arrayCopy(buf, bufOff, resBuf, resOff, size);

        return size;
    }

    /**
     * Write a byte to this storage to specified position {@code pos}.
     *
     * <p>The byte to be written is the eight low-order bits of the
     * argument {@code b}. The 24 high-order bits of {@code b}b are ignored.
     *
     * @param pos Pointer to a position.
     * @param b Byte to write.
     * @throws IOException if an I/O error occurs.
     */
    void write(int pos, int b) throws IOException {
        if (pos > totalCnt)
            throw new IOException("Writting beyond end of Blob, it probably was truncated after OutputStream was created " +
                    "[pos=" + pos + ", totalCnt=" + totalCnt + "]");

        if (MAX_ARRAY_SIZE - pos < 1)
            throw new IOException("Too much data. Can't write more then " + MAX_ARRAY_SIZE + " bytes to Blob.");

        int finalLen = Math.max(pos + 1, totalCnt);

        if (isReadOnly) {
            isReadOnly = false;

            grow(finalLen);

            off = 0;
        }
        else
            ensureCapacity(finalLen);

        buf[pos] = (byte)b;

        totalCnt = finalLen;
    }

    /**
     * Writes {@code len} bytes from the specified byte array {@code bytes} starting at offset {@code off}
     * to this storage to specified position {@code pos}.
     *
     * @param pos Pointer to a position.
     * @param resBuf Input byte array.
     * @param resOff Start offset in the input array.
     * @param len Number of bytes to write.
     * @throws IOException if an I/O error occurs.
     */
    void write(int pos, byte[] resBuf, int resOff, int len) throws IOException {
        Objects.checkFromIndexSize(resOff, len, resBuf.length);

        if (pos > totalCnt)
            throw new IOException("Writting beyond end of Blob, it probably was truncated after OutputStream was created " +
                    "[pos=" + pos + ", totalCnt=" + totalCnt + "]");

        if (MAX_ARRAY_SIZE - pos < len)
            throw new IOException("Too much data. Can't write more then " + MAX_ARRAY_SIZE + " bytes to Blob.");

        int finalLen = Math.max(pos + len, totalCnt);

        if (isReadOnly) {
            isReadOnly = false;

            grow(finalLen);

            off = 0;
        }
        else
            ensureCapacity(finalLen);

        U.arrayCopy(resBuf, resOff, buf, pos, len);

        totalCnt = finalLen;
    }

    /**
     * Get copy of the buffer data as byte array.
     *
     * <p>Throws the overflow exception if the result can not fit into byte array
     * which is can only store 2GB of data.
     *
     * @return Byte array containing buffer data.
     */
    public byte[] getData() {
        byte[] bytes = new byte[totalCnt()];

        read(0, bytes, 0, totalCnt);

        return bytes;
    }

    /**
     * Truncate this storage to specified length.
     *
     * @param len Length to truncate to. Must not be less than total bytes count in the storage.
     */
    public void truncate(int len) {
        byte[] newBuf = new byte[Math.max(MIN_BUFFER_SIZE, len)];

        U.arrayCopy(buf, 0, newBuf, 0, len);

        buf = newBuf;

        totalCnt = len;
    }

    /**
     * Increases the capacity if necessary to ensure that it can hold
     * at least the number of elements specified by the minimum
     * capacity argument.
     *
     * @param  minCapacity the desired minimum capacity
     * @throws OutOfMemoryError if {@code minCapacity < 0}.  This is
     * interpreted as a request for the unsatisfiably large capacity
     * {@code (long) Integer.MAX_VALUE + (minCapacity - Integer.MAX_VALUE)}.
     */
    private void ensureCapacity(int minCapacity) {
        // overflow-conscious code
        if (minCapacity - buf.length > 0)
            grow(minCapacity);
    }

    /**
     * Increases the capacity to ensure that it can hold at least the
     * number of elements specified by the minimum capacity argument.
     *
     * @param minCapacity the desired minimum capacity
     */
    private void grow(int minCapacity) {
        // overflow-conscious code
        int oldCapacity = buf.length;

        int newCapacity = oldCapacity << 1;

        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity;

        if (newCapacity - MAX_ARRAY_SIZE > 0)
            newCapacity = hugeCapacity(minCapacity);

        byte[] newBuf = new byte[newCapacity];

        U.arrayCopy(buf, off, newBuf, 0, totalCnt);

        buf = newBuf;
    }

    /**
     */
    private static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) // overflow
            throw new OutOfMemoryError();

        return (minCapacity > MAX_ARRAY_SIZE) ?
                Integer.MAX_VALUE :
                MAX_ARRAY_SIZE;
    }
}

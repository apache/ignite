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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Simple BLOB implementation. Actually there is no such entity as BLOB in Ignite. So using arrays is a preferable way
 * to work with binary objects.
 * <p>
 * This implementation can be useful for reading binary fields of objects through JDBC.
 */
public class JdbcBlob implements Blob {
    /** The list of buffers, which grows and never reduces. */
    private List<byte[]> buffers;

    /** The total count of bytes in the blob. */
    protected long cnt;

    /**
     */
    public JdbcBlob() {
        buffers = new ArrayList<>(1);

        cnt = 0;
    }

    /**
     * @param arr Byte array.
     */
    public JdbcBlob(byte[] arr) {
        buffers = new ArrayList<>(1);

        buffers.add(arr);

        cnt = arr.length;
    }

    /** {@inheritDoc} */
    @Override public long length() throws SQLException {
        ensureNotClosed();

        return cnt;
    }

    /** {@inheritDoc} */
    @Override public byte[] getBytes(long pos, int len) throws SQLException {
        ensureNotClosed();

        if (pos < 1 || (cnt - pos < 0 && cnt > 0) || len < 0)
            throw new SQLException("Invalid argument. Position can't be less than 1 or " +
                "greater than size of underlying byte array. Requested length also can't be negative " +
                "[pos=" + pos + ", len=" + len + ']');

        int idx = (int)(pos - 1);

        int size = len > cnt - idx ? (int)(cnt - idx) : len;

        byte[] res = new byte[size];

        int remaining = size;
        int curPos = 0;

        for (byte[] buf : buffers) {
            if (idx < curPos + buf.length) {
                int toCopy = Math.min(remaining, buf.length - Math.max(idx - curPos, 0));

                U.arrayCopy(buf, Math.max(idx - curPos, 0), res, size - remaining, toCopy);

                remaining -= toCopy;
            }

            curPos += buf.length;

            if (remaining == 0)
                break;
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public InputStream getBinaryStream() throws SQLException {
        ensureNotClosed();

        return getBinaryStreamImpl(1, cnt);
    }

    /** {@inheritDoc} */
    @Override public InputStream getBinaryStream(long pos, long len) throws SQLException {
        ensureNotClosed();

        if (pos < 1 || len < 1 || pos > cnt || len > cnt - pos + 1)
            throw new SQLException("Invalid argument. Position can't be less than 1 or " +
                "greater than size of underlying byte array. Requested length can't be negative and can't be " +
                "greater than available bytes from given position [pos=" + pos + ", len=" + len + ']');

        return getBinaryStreamImpl(pos, len);
    }

    /**
     * @param pos the offset to the first byte of the partial value to be
     *        retrieved. The first byte in the {@code Blob} is at position 1.
     * @param len the length in bytes of the partial value to be retrieved
     * @return {@code InputStream} through which
     *         the partial {@code Blob} value can be read.
     */
    private InputStream getBinaryStreamImpl(long pos, long len) {
        int idx = (int)(pos - 1);

        long remaining = Math.min(len, cnt - idx);

        final List<ByteArrayInputStream> list = new ArrayList<>(buffers.size());

        int curPos = 0;

        for (byte[] buf : buffers) {
            if (idx < curPos + buf.length) {
                int toCopy = (int)Math.min(remaining, buf.length - Math.max(idx - curPos, 0));

                list.add(new ByteArrayInputStream(buf, Math.max(idx - curPos, 0), toCopy));

                remaining -= toCopy;
            }

            curPos += buf.length;

            if (remaining == 0)
                break;
        }

        return new SequenceInputStream(Collections.enumeration(list));
    }

    /** {@inheritDoc} */
    @Override public long position(byte[] ptrn, long start) throws SQLException {
        ensureNotClosed();

        if (start < 1 || start > cnt || ptrn.length == 0 || ptrn.length > cnt)
            return -1;

        long idx = start - 1;

        int curBufIdx = 0;
        int patternStartBufIdx = 0;
        int patternStartBufPos = 0;
        boolean patternStarted = false;
        int curBufPos = 0;

        int i;
        long pos;
        for (i = 0, pos = 0; pos < cnt;) {
            if (pos < idx) {
                if (idx > pos + buffers.get(curBufIdx).length - 1) {
                    pos += buffers.get(curBufIdx++).length;

                    continue;
                }
                else {
                    curBufPos = (int)(idx - pos);
                    pos = idx;
                }
            }

            if (buffers.get(curBufIdx)[curBufPos] == ptrn[i]) {
                if (i == 0) {
                    patternStarted = true;
                    patternStartBufIdx = curBufIdx;
                    patternStartBufPos = curBufPos;
                }

                pos++;

                curBufPos++;

                if (curBufPos == buffers.get(curBufIdx).length) {
                    curBufIdx++;

                    curBufPos = 0;
                }

                i++;

                if (i == ptrn.length)
                    return pos - ptrn.length + 1;
            }
            else {
                pos = pos - i + 1;

                i = 0;

                if (patternStarted) {
                    if (patternStartBufPos + 1 < buffers.get(patternStartBufIdx).length) {
                        curBufIdx = patternStartBufIdx;
                        curBufPos = patternStartBufPos + 1;
                    }
                    else {
                        curBufIdx = patternStartBufIdx + 1;
                        curBufPos = 0;
                    }
                }
                else {
                    if (curBufPos + 1 < buffers.get(curBufIdx).length) {
                        curBufPos++;
                    }
                    else {
                        curBufIdx++;
                        curBufPos = 0;
                    }
                }
            }
        }

        return -1;
    }

    /** {@inheritDoc} */
    @Override public long position(Blob ptrn, long start) throws SQLException {
        ensureNotClosed();

        if (start < 1 || start > cnt || ptrn.length() == 0 || ptrn.length() > cnt)
            return -1;

        return position(ptrn.getBytes(1, (int)ptrn.length()), start);
    }

    /** {@inheritDoc} */
    @Override public int setBytes(long pos, byte[] bytes) throws SQLException {
        return setBytes(pos, bytes, 0, bytes.length);
    }

    /** {@inheritDoc} */
    @Override public int setBytes(long pos, byte[] bytes, int off, int len) throws SQLException {
        ensureNotClosed();

        if (pos < 1)
            throw new SQLException("Invalid argument. Position can't be less than 1 [pos=" + pos + ']');

        if (pos - 1 > cnt || off < 0 || off >= bytes.length || off + len > bytes.length)
            throw new ArrayIndexOutOfBoundsException();

        return setBytesImpl(pos, bytes, off, len);
    }

    /**
     * @param pos the position in the {@code BLOB} object at which
     *        to start writing; the first position is 1
     * @param bytes the array of bytes to be written to this {@code BLOB}
     *        object
     * @param off the offset into the array {@code bytes} at which
     *        to start reading the bytes to be set
     * @param len the number of bytes to be written to the {@code BLOB}
     *        value from the array of bytes {@code bytes}
     * @return the number of bytes written
     */
    private int setBytesImpl(long pos, byte[] bytes, int off, int len) {
        int idx = (int)(pos - 1);

        long newCnt = idx + len > cnt ? idx + len : cnt;

        int remaining = len;
        int curPos = 0;

        for (byte[] buf : buffers) {
            if (idx < curPos + buf.length) {
                int toCopy = Math.min(remaining, buf.length - (idx - curPos));

                U.arrayCopy(bytes, off + len - remaining, buf, Math.max(0, idx - curPos), toCopy);

                remaining -= toCopy;
            }

            if (remaining == 0)
                break;

            curPos += buf.length;
        }

        if (remaining > 0) {
            addNewBuffer(newCnt);

            U.arrayCopy(bytes, off + len - remaining, buffers.get(buffers.size() - 1), 0, remaining);
        }

        cnt = newCnt;

        return len;
    }

    /**
     * Makes a new buffer available
     *
     * @param newCount the new size of the Blob
     */
    private void addNewBuffer(final long newCount) {
        final int newBufSize;

        if (buffers.isEmpty()) {
            newBufSize = (int)newCount;
        }
        else {
            newBufSize = Math.max(
                    buffers.get(buffers.size() - 1).length << 1,
                    (int)(newCount - cnt));
        }

        buffers.add(new byte[newBufSize]);
    }

    /** {@inheritDoc} */
    @Override public OutputStream setBinaryStream(long pos) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    @Override public void truncate(long len) throws SQLException {
        ensureNotClosed();

        if (len < 0 || len > cnt)
            throw new SQLException("Invalid argument. Length can't be " +
                "less than zero or greater than Blob length [len=" + len + ']');

        cnt = len;
    }

    /** {@inheritDoc} */
    @Override public void free() throws SQLException {
        if (buffers != null) {
            buffers.clear();

            buffers = null;
        }
    }

    /**
     *
     */
    private void ensureNotClosed() throws SQLException {
        if (buffers == null)
            throw new SQLException("Blob instance can't be used after free() has been called.");
    }
}

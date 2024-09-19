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

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Simple BLOB implementation. Actually there is no such entity as BLOB in Ignite. So using arrays is a preferable way
 * to work with binary objects.
 * <p>
 * This implementation can be useful for reading binary fields of objects through JDBC.
 */
public class JdbcBlob extends JdbcMemoryBuffer implements Blob {
    /**
     */
    public JdbcBlob() {
    }

    /**
     * @param arr Byte array.
     */
    public JdbcBlob(byte[] arr) {
        buffers.add(arr);

        totalCnt = arr.length;
    }

    /** {@inheritDoc} */
    @Override public long length() throws SQLException {
        ensureNotClosed();

        return totalCnt;
    }

    /** {@inheritDoc} */
    @Override public byte[] getBytes(long pos, int len) throws SQLException {
        ensureNotClosed();

        if (pos < 1 || (totalCnt - pos < 0 && totalCnt > 0) || len < 0)
            throw new SQLException("Invalid argument. Position can't be less than 1 or " +
                "greater than size of underlying byte array. Requested length also can't be negative " +
                "[pos=" + pos + ", len=" + len + ']');

        int idx = (int)(pos - 1);

        int size = len > totalCnt - idx ? (int)(totalCnt - idx) : len;

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

        return getInputStream(0, totalCnt);
    }

    /** {@inheritDoc} */
    @Override public InputStream getBinaryStream(long pos, long len) throws SQLException {
        ensureNotClosed();

        if (pos < 1 || len < 1 || pos > totalCnt || len > totalCnt - pos + 1)
            throw new SQLException("Invalid argument. Position can't be less than 1 or " +
                "greater than size of underlying byte array. Requested length can't be negative and can't be " +
                "greater than available bytes from given position [pos=" + pos + ", len=" + len + ']');

        return getInputStream(pos - 1, len);
    }

    /** {@inheritDoc} */
    @Override public long position(byte[] ptrn, long start) throws SQLException {
        ensureNotClosed();

        if (start < 1 || start > totalCnt || ptrn.length == 0 || ptrn.length > totalCnt)
            return -1;

        long idx = start - 1;

        int curBufIdx = 0;
        int patternStartBufIdx = 0;
        int patternStartBufPos = 0;
        boolean patternStarted = false;
        int curBufPos = 0;

        int i;
        long pos;
        for (i = 0, pos = 0; pos < totalCnt;) {
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

        if (start < 1 || start > totalCnt || ptrn.length() == 0 || ptrn.length() > totalCnt)
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

        if (pos - 1 > totalCnt || off < 0 || off >= bytes.length || off + len > bytes.length)
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

        int curPos = 0;
        int bufIdx;
        for (bufIdx = 0; bufIdx < buffers.size(); bufIdx++) {
            byte[] buf = buffers.get(bufIdx);

            if (idx >= curPos + buf.length) {
                curPos += buf.length;
            }
            else {
                break;
            }
        }

        int written = setBytesImpl0(bufIdx, idx - curPos, bytes, off, len);

        totalCnt = idx + written > totalCnt ? idx + written : totalCnt;

        return written;
    }

    /**
     * @param bufIdx index of buffer to write bytes
     * @param bufPos the in buffer position at which
     *        to start writing; the first position is 0
     * @param bytes the array of bytes to be written to this {@code BLOB}
     *        object
     * @param off the offset into the array {@code bytes} at which
     *        to start reading the bytes to be set
     * @param len the number of bytes to be written to the {@code BLOB}
     *        value from the array of bytes {@code bytes}
     * @return the number of bytes written
     */
    private int setBytesImpl0(int bufIdx, int bufPos, byte[] bytes, int off, int len) {
        int remaining = len;
        int curPos = bufPos;

        for (int i = bufIdx; i < buffers.size(); i++) {
            byte[] buf = buffers.get(i);

            int toCopy = Math.min(remaining, buf.length - curPos);

            U.arrayCopy(bytes, off + len - remaining, buf, curPos, toCopy);

            remaining -= toCopy;

            if (remaining == 0)
                break;

            curPos = 0;
        }

        if (remaining > 0) {
            addNewBuffer(remaining);

            U.arrayCopy(bytes, off + len - remaining, buffers.get(buffers.size() - 1), 0, remaining);
        }

        return len;
    }

    /** {@inheritDoc} */
    @Override public OutputStream setBinaryStream(long pos) throws SQLException {
        ensureNotClosed();

        if (pos < 1 || pos > totalCnt + 1)
            throw new SQLException("Invalid argument. Position can't be less than 1 or greater than Blob length + 1 [pos=" + pos + ']');

        return getOutputStream(pos - 1);
    }

    /** {@inheritDoc} */
    @Override public void truncate(long len) throws SQLException {
        ensureNotClosed();

        if (len < 0 || len > totalCnt)
            throw new SQLException("Invalid argument. Length can't be " +
                "less than zero or greater than Blob length [len=" + len + ']');

        totalCnt = len;
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

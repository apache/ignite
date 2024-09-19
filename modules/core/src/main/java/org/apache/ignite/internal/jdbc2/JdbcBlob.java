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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

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

        try {
            long idx = pos - 1;

            int size = len > totalCnt - idx ? (int)(totalCnt - idx) : len;

            byte[] res = new byte[size];

            getInputStream(idx, len).read(res);

            return res;
        }
        catch (IOException e) {
            throw new SQLException(e);
        }
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

        try {
            long idx = positionImpl(new ByteArrayInputStream(ptrn), ptrn.length, start - 1);

            return idx == -1 ? -1 : idx + 1;
        }
        catch (IOException e) {
            throw new SQLException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long position(Blob ptrn, long start) throws SQLException {
        ensureNotClosed();

        if (start < 1 || start > totalCnt || ptrn.length() == 0 || ptrn.length() > totalCnt)
            return -1;

        try {
            long idx = positionImpl(ptrn.getBinaryStream(), ptrn.length(), start - 1);

            return idx == -1 ? -1 : idx + 1;
        }
        catch (IOException e) {
            throw new SQLException(e);
        }
    }

    /**
     *
     * @param ptrn Pattern
     * @param ptrnLen Pattern length
     * @param idx Start index
     * @return Position
     */
    private long positionImpl(InputStream ptrn, long ptrnLen, long idx) throws IOException {
        assert ptrn.markSupported();

        InputStream is = getInputStream(idx, totalCnt - idx);

        boolean patternStarted = false;

        long i;
        long pos;
        int b;
        for (i = 0, pos = idx; (b = is.read()) != -1; ) {
            int p = ptrn.read();

            if (b == p) {
                if (!patternStarted) {
                    patternStarted = true;

                    is.mark(Integer.MAX_VALUE);
                }

                pos++;

                i++;

                if (i == ptrnLen)
                    return pos - ptrnLen;
            }
            else {
                pos = pos - i + 1;

                i = 0;
                ptrn.reset();

                if (patternStarted) {
                    patternStarted = false;

                    is.reset();
                }
            }
        }

        return -1;
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

        try {
            getOutputStream(pos - 1).write(bytes, off, len);
        }
        catch (Exception e) {
            throw new SQLException(e);
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

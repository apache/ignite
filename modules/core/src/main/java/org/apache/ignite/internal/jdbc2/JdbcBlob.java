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
 *
 * <p>This implementation can be useful for writting and reading binary fields of objects through JDBC.
 */
public class JdbcBlob implements Blob {
    /** Buffer to store actial data. */
    private JdbcBinaryBuffer buf;

    /**
     * Create empty Blob.
     *
     * <p>It's supposed to be called when client application creates Blob calling the
     * {@link java.sql.Connection#createBlob}.
     */
    public JdbcBlob() {
        buf = JdbcBinaryBuffer.createReadWrite();
    }

    /**
     * Create Blob which wraps the existing data stored in the buffer.
     *
     * <p>It's supposed to be called to create Blob for query result in the {@link java.sql.ResultSet}.
     *
     * @param buf Existing buffer with data.
     */
    public JdbcBlob(JdbcBinaryBuffer buf) {
        this.buf = buf;
    }

    /**
     * Create Blob which wraps the existing byte array.
     *
     * @param arr Byte array.
     */
    public JdbcBlob(byte[] arr) {
        this(JdbcBinaryBuffer.createReadWrite(arr));
    }

    /** {@inheritDoc} */
    @Override public long length() throws SQLException {
        ensureNotClosed();

        return buf.length();
    }

    /** {@inheritDoc} */
    @Override public byte[] getBytes(long pos, int len) throws SQLException {
        ensureNotClosed();

        int blobLen = buf.length();

        if (pos < 1 || (pos > blobLen && blobLen > 0) || len < 0)
            throw new SQLException("Invalid argument. Position can't be less than 1 or " +
                "greater than Blob length. Requested length also can't be negative " +
                "[pos=" + pos + ", len=" + len + ", blobLen=" + blobLen + "]");

        int idx = (int)pos - 1;

        int size = Math.min(len, blobLen - idx);

        byte[] res = new byte[size];

        if (size == 0)
            return res;

        int readCnt;

        readCnt = buf.read(idx, res, 0, size);

        if (readCnt == -1)
            throw new SQLException("Failed to read bytes from Blob [pos=" + pos + ", len=" + len + "]");

        return res;
    }

    /** {@inheritDoc} */
    @Override public InputStream getBinaryStream() throws SQLException {
        ensureNotClosed();

        return buf.getInputStream();
    }

    /** {@inheritDoc} */
    @Override public InputStream getBinaryStream(long pos, long len) throws SQLException {
        ensureNotClosed();

        int blobLen = buf.length();

        if (pos < 1 || len < 1 || pos > blobLen || len > blobLen - (pos - 1))
            throw new SQLException("Invalid argument. Position can't be less than 1 or " +
                "greater than Blob length. Requested length can't be negative and can't be " +
                "greater than available bytes from given position [pos=" + pos + ", len=" + len + ", blobLen=" + blobLen + "]");

        return buf.getInputStream((int)pos - 1, (int)len);
    }

    /** {@inheritDoc} */
    @Override public long position(byte[] ptrn, long start) throws SQLException {
        ensureNotClosed();

        int blobLen = buf.length();

        if (start < 1)
            throw new SQLException("Invalid argument. Start position can't be less than 1 [start=" + start + "]");

        if (start > blobLen || ptrn.length == 0 || ptrn.length > blobLen)
            return -1;

        long idx = position(new ByteArrayInputStream(ptrn), ptrn.length, (int)start - 1);

        return idx == -1 ? -1 : idx + 1;
    }

    /** {@inheritDoc} */
    @Override public long position(Blob ptrn, long start) throws SQLException {
        ensureNotClosed();

        int blobLen = buf.length();

        if (start < 1)
            throw new SQLException("Invalid argument. Start position can't be less than 1 [start=" + start + "]");

        if (start > blobLen || ptrn.length() == 0 || ptrn.length() > blobLen)
            return -1;

        long idx = position(ptrn.getBinaryStream(), (int)ptrn.length(), (int)start - 1);

        return idx == -1 ? -1 : idx + 1;
    }

    /** {@inheritDoc} */
    @Override public int setBytes(long pos, byte[] bytes) throws SQLException {
        return setBytes(pos, bytes, 0, bytes.length);
    }

    /** {@inheritDoc} */
    @Override public int setBytes(long pos, byte[] bytes, int off, int len) throws SQLException {
        ensureNotClosed();

        int blobLen = buf.length();

        if (pos < 1 || pos - 1 > blobLen)
            throw new SQLException("Invalid argument. Position can't be less than 1 or " +
                "greater than Blob length + 1 [pos=" + pos + ", blobLen=" + blobLen + "]");

        buf.write((int)pos - 1, bytes, off, len);

        return len;
    }

    /** {@inheritDoc} */
    @Override public OutputStream setBinaryStream(long pos) throws SQLException {
        ensureNotClosed();

        int blobLen = buf.length();

        if (pos < 1 || pos - 1 > blobLen)
            throw new SQLException("Invalid argument. Position can't be less than 1 or greater than Blob length + 1 " +
                    "[pos=" + pos + ", blobLen=" + blobLen + "]");

        return buf.getOutputStream((int)pos - 1);
    }

    /** {@inheritDoc} */
    @Override public void truncate(long len) throws SQLException {
        ensureNotClosed();

        int blobLen = buf.length();

        if (len < 0 || len > blobLen)
            throw new SQLException("Invalid argument. Length can't be " +
                "less than zero or greater than Blob length [len=" + len + ", blobLen=" + blobLen + "]");

        buf.truncate((int)len);
    }

    /** {@inheritDoc} */
    @Override public void free() throws SQLException {
        if (buf != null)
            buf = null;
    }

    /**
     * Actial implementation of the pattern search.
     *
     * @param ptrn InputStream containing the pattern.
     * @param ptrnLen Pattern length.
     * @param idx Zero-based index in Blob to start search from.
     * @return Zero-based position at which the pattern appears, else -1.
     */
    private long position(InputStream ptrn, int ptrnLen, int idx) throws SQLException {
        assert ptrn.markSupported() : "input stream supports mark() method";

        try {
            InputStream blob = buf.getInputStream(idx, buf.length() - idx);

            boolean patternStarted = false;

            int ptrnPos = 0;
            int blobPos = idx;
            int b;

            while ((b = blob.read()) != -1) {
                if (b == ptrn.read()) {
                    if (!patternStarted) {
                        patternStarted = true;

                        blob.mark(Integer.MAX_VALUE);
                    }

                    blobPos++;

                    ptrnPos++;

                    if (ptrnPos == ptrnLen)
                        return blobPos - ptrnLen;
                }
                else {
                    blobPos = blobPos - ptrnPos + 1;

                    ptrnPos = 0;
                    ptrn.reset();

                    if (patternStarted) {
                        patternStarted = false;

                        blob.reset();
                    }
                }
            }

            return -1;
        }
        catch (IOException e) {
            throw new SQLException(e);
        }
    }

    /**
     *
     */
    private void ensureNotClosed() throws SQLException {
        if (buf == null)
            throw new SQLException("Blob instance can't be used after free() has been called.");
    }
}

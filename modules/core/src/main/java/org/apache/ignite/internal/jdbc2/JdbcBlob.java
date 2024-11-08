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

        if (pos < 1 || (buf.length() - pos < 0 && buf.length() > 0) || len < 0) {
            throw new SQLException("Invalid argument. Position can't be less than 1 or " +
                "greater than Blob length. Requested length also can't be negative " +
                "[pos=" + pos + ", len=" + len + ", blobLen=" + buf.length() + "]");
        }

        int idx = (int)(pos - 1);

        int size = len > buf.length() - idx ? buf.length() - idx : len;

        byte[] res = new byte[size];

        if (size == 0)
            return res;

        buf.read(idx, res, 0, size);

        return res;
    }

    /** {@inheritDoc} */
    @Override public InputStream getBinaryStream() throws SQLException {
        ensureNotClosed();

        return buf.inputStream();
    }

    /** {@inheritDoc} */
    @Override public InputStream getBinaryStream(long pos, long len) throws SQLException {
        ensureNotClosed();

        if (pos < 1 || len < 1 || pos > buf.length() || len > buf.length() - pos + 1) {
            throw new SQLException("Invalid argument. Position can't be less than 1 or " +
                "greater than Blob length. Requested length can't be negative and can't be " +
                "greater than available bytes from given position [pos=" + pos + ", len=" + len + ", blobLen=" + buf.length() + "]");
        }

        return buf.inputStream((int)pos - 1, (int)len);
    }

    /** {@inheritDoc} */
    @Override public long position(byte[] ptrn, long start) throws SQLException {
        ensureNotClosed();

        if (start < 1)
            throw new SQLException("Invalid argument. Start position can't be less than 1 [start=" + start + "]");

        if (start > buf.length() || ptrn.length == 0 || ptrn.length > buf.length())
            return -1;

        return position(new ByteArrayInputStream(ptrn), ptrn.length, (int)start);
    }

    /** {@inheritDoc} */
    @Override public long position(Blob ptrn, long start) throws SQLException {
        ensureNotClosed();

        if (start < 1)
            throw new SQLException("Invalid argument. Start position can't be less than 1 [start=" + start + "]");

        if (start > buf.length() || ptrn.length() == 0 || ptrn.length() > buf.length())
            return -1;

        return position(ptrn.getBinaryStream(), (int)ptrn.length(), (int)start);
    }

    /** {@inheritDoc} */
    @Override public int setBytes(long pos, byte[] bytes) throws SQLException {
        return setBytes(pos, bytes, 0, bytes.length);
    }

    /** {@inheritDoc} */
    @Override public int setBytes(long pos, byte[] bytes, int off, int len) throws SQLException {
        ensureNotClosed();

        if (pos < 1 || pos - 1 > buf.length()) {
            throw new SQLException("Invalid argument. Position can't be less than 1 or " +
                "greater than Blob length + 1 [pos=" + pos + ", blobLen=" + buf.length() + "]");
        }

        try {
            buf.write((int)pos - 1, bytes, off, len);
        }
        catch (IOException e) {
            throw new SQLException(e);
        }

        return len;
    }

    /** {@inheritDoc} */
    @Override public OutputStream setBinaryStream(long pos) throws SQLException {
        ensureNotClosed();

        if (pos < 1 || pos - 1 > buf.length()) {
            throw new SQLException("Invalid argument. Position can't be less than 1 or greater than Blob length + 1 " +
                    "[pos=" + pos + ", blobLen=" + buf.length() + "]");
        }

        return buf.outputStream((int)pos - 1);
    }

    /** {@inheritDoc} */
    @Override public void truncate(long len) throws SQLException {
        ensureNotClosed();

        if (len < 0 || len > buf.length()) {
            throw new SQLException("Invalid argument. Length can't be " +
                "less than zero or greater than Blob length [len=" + len + ", blobLen=" + buf.length() + "]");
        }

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
     * @param idx 1-based index in Blob to start search from.
     * @return 1-based position at which the pattern appears, else -1.
     */
    private long position(InputStream ptrn, int ptrnLen, int idx) throws SQLException {
        try {
            InputStream blob = buf.inputStream(idx - 1, buf.length() - idx + 1);

            boolean patternStarted = false;

            int ptrnPos = 0;
            int blobPos = idx - 1;
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
                        return blobPos - ptrnLen + 1;
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

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
import org.apache.ignite.internal.jdbc2.lob.JdbcBlobBuffer;

/**
 * Simple BLOB implementation. Actually there is no such entity as BLOB in Ignite. So using arrays is a preferable way
 * to work with binary objects.
 *
 * <p>This implementation can be useful for writting and reading binary fields of objects through JDBC.
 *
 * <p>This implementation stores data in memory until the configured data size limit is reached. After that
 * data will be saved to temp file. And all subsequent operations will work with data stored in temp file.
 */
public class JdbcBlob implements Blob, AutoCloseable {
    /** Buffer to store actial data. */
    private JdbcBlobBuffer data;

    /**
     * Create empty Blob.
     *
     * <p>It's supposed to be called when client application creates Blob calling the
     * {@link java.sql.Connection#createBlob}.
     *
     * <p>Once any write operation would increase the size of underlying data above the
     * maximum value passed as {@code maxMemoryBufferBytes} data will be saved to temp file.
     * And all subsequent operations will work with data stored in temp file.
     *
     * @param maxMemoryBufferBytes Max in-memory buffer size.
     */
    public JdbcBlob(long maxMemoryBufferBytes) {
        data = new JdbcBlobBuffer(maxMemoryBufferBytes);
    }

    /**
     * Create Blob which wraps the existing data.
     *
     * <p>It's supposed to be called to create Blob for query result in the {@link java.sql.ResultSet}.
     *
     * <p>Starts working in in-memory mode even if the passed {@code buf} data is larger than limit
     * specifed via the {@code maxMemoryBufferBytes}. It's done so since memory is already allocated
     * and there is no need to save it.
     *
     * <p>It also can start in the read-only mode if the input {@code buf} wraps external byte array.
     * It lazyly switched to read-write mode (creating a copy of the data) once any write
     * operation invoked. This allows to prevent the unnecessary data copying.
     *
     * <p>Once any write operation would increase the size of underlying data above the
     * maximum value passed as {@code maxMemoryBufferBytes} data will be saved to temp file.
     * And all subsequent operations will work with data stored in temp file.
     *
     * @param maxMemoryBufferBytes Max in-memory buffer size.
     * @param buf Existing buffer with data.
     */
    public JdbcBlob(long maxMemoryBufferBytes, JdbcBlobBuffer buf) {
        buf.setMaxMemoryBufferBytes(maxMemoryBufferBytes);

        data = buf;
    }

    /**
     * Create empty Blob which always stores data in memory.
     */
    public JdbcBlob() {
        this(Long.MAX_VALUE);
    }

    /**
     * Create Blob which wraps the existing byte array and always stores data in memory.
     *
     * @param arr Byte array.
     */
    public JdbcBlob(byte[] arr) {
        this(Long.MAX_VALUE, new JdbcBlobBuffer(Long.MAX_VALUE, arr));
    }

    /** {@inheritDoc} */
    @Override public long length() throws SQLException {
        ensureNotClosed();

        return data.totalCnt();
    }

    /** {@inheritDoc} */
    @Override public byte[] getBytes(long pos, int len) throws SQLException {
        ensureNotClosed();

        long totalCnt = data.totalCnt();

        if (pos < 1 || (pos > totalCnt && totalCnt > 0) || len < 0)
            throw new SQLException("Invalid argument. Position can't be less than 1 or " +
                "greater than Blob total bytes count. Requested length also can't be negative " +
                "[pos=" + pos + ", len=" + len + ", totalCnt=" + totalCnt + "]");

        long idx = pos - 1;

        int size = len > totalCnt - idx ? (int)(totalCnt - idx) : len;

        byte[] res = new byte[size];

        if (size == 0)
            return res;

        int readCnt;

        try {
            readCnt = data.getInputStream(idx, size).read(res);
        }
        catch (Exception e) {
            throw new SQLException(e);
        }

        if (readCnt == -1)
            throw new SQLException("Failed to read bytes from Blob [pos=" + pos + ", len=" + len + "]");

        return res;
    }

    /** {@inheritDoc} */
    @Override public InputStream getBinaryStream() throws SQLException {
        ensureNotClosed();

        return data.getInputStream();
    }

    /** {@inheritDoc} */
    @Override public InputStream getBinaryStream(long pos, long len) throws SQLException {
        ensureNotClosed();

        long totalCnt = data.totalCnt();

        if (pos < 1 || len < 1 || pos > totalCnt || len > totalCnt - (pos - 1))
            throw new SQLException("Invalid argument. Position can't be less than 1 or " +
                "greater than Blob total bytes count. Requested length can't be negative and can't be " +
                "greater than available bytes from given position [pos=" + pos + ", len=" + len + ", totalCnt=" + totalCnt + "]");

        return data.getInputStream(pos - 1, len);
    }

    /** {@inheritDoc} */
    @Override public long position(byte[] ptrn, long start) throws SQLException {
        ensureNotClosed();

        if (start < 1)
            throw new SQLException("Invalid argument. Start position can't be less than 1 [start=" + start + "]");

        if (start > data.totalCnt() || ptrn.length == 0 || ptrn.length > data.totalCnt())
            return -1;

        long idx = positionImpl(new ByteArrayInputStream(ptrn), ptrn.length, start - 1);

        return idx == -1 ? -1 : idx + 1;
    }

    /** {@inheritDoc} */
    @Override public long position(Blob ptrn, long start) throws SQLException {
        ensureNotClosed();

        if (start < 1)
            throw new SQLException("Invalid argument. Start position can't be less than 1 [start=" + start + "]");

        if (start > data.totalCnt() || ptrn.length() == 0 || ptrn.length() > data.totalCnt())
            return -1;

        long idx = positionImpl(ptrn.getBinaryStream(), ptrn.length(), start - 1);

        return idx == -1 ? -1 : idx + 1;
    }

    /** {@inheritDoc} */
    @Override public int setBytes(long pos, byte[] bytes) throws SQLException {
        return setBytes(pos, bytes, 0, bytes.length);
    }

    /** {@inheritDoc} */
    @Override public int setBytes(long pos, byte[] bytes, int off, int len) throws SQLException {
        ensureNotClosed();

        long totalCnt = data.totalCnt();

        if (pos < 1 || pos - 1 > totalCnt)
            throw new SQLException("Invalid argument. Position can't be less than 1 [pos=" + pos + ']');

        if (off < 0 || off >= bytes.length || off + len > bytes.length)
            throw new SQLException("Invalid argument.",
                    new ArrayIndexOutOfBoundsException("[off=" + off + ", len=" + len + ", bytes.length=" + bytes.length + "]"));

        try {
            data.getOutputStream(pos - 1).write(bytes, off, len);
        }
        catch (Exception e) {
            throw new SQLException(e);
        }

        return len;
    }

    /** {@inheritDoc} */
    @Override public OutputStream setBinaryStream(long pos) throws SQLException {
        ensureNotClosed();

        long totalCnt = data.totalCnt();

        if (pos < 1 || pos > totalCnt + 1)
            throw new SQLException("Invalid argument. Position can't be less than 1 or greater than Blob total bytes count + 1 " +
                    "[pos=" + pos + ", totalCnt=" + totalCnt + "]");

        try {
            return data.getOutputStream(pos - 1);
        }
        catch (Exception e) {
            throw new SQLException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void truncate(long len) throws SQLException {
        ensureNotClosed();

        long totalCnt = data.totalCnt();

        if (len < 0 || len > totalCnt)
            throw new SQLException("Invalid argument. Length can't be " +
                "less than zero or greater than Blob total bytes count [len=" + len + ", totalCnt=" + totalCnt + "]");

        try {
            data.truncate(len);
        }
        catch (IOException e) {
            throw new SQLException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void free() throws SQLException {
        if (data != null) {
            data.close();

            data = null;
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws SQLException {
        free();
    }

    /**
     * Actial implementation of the pattern search.
     *
     * @param ptrn InputStream containing the pattern.
     * @param ptrnLen Pattern length.
     * @param idx Zero-based index in Blob to start search from.
     * @return Zero-based position at which the pattern appears, else -1.
     */
    private long positionImpl(InputStream ptrn, long ptrnLen, long idx) throws SQLException {
        assert ptrn.markSupported();

        try {
            InputStream blob = data.getInputStream(idx, data.totalCnt() - idx);

            boolean patternStarted = false;

            long ptrnPos = 0;
            long blobPos = idx;
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
        if (data == null)
            throw new SQLException("Blob instance can't be used after free() has been called.");
    }
}

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
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Simple BLOB implementation. Actually there is no such entity as BLOB in Ignite. So using arrays is preferable way
 * to work with binary objects.
 *
 * This implementation can be useful for reading binary fields of objects through JDBC.
 */
public class JdbcBlob implements Blob {
    /** Byte array. */
    private byte[] arr;

    /**
     * @param arr Byte array.
     */
    public JdbcBlob(byte[] arr) {
        this.arr = arr;
    }

    /** {@inheritDoc} */
    @Override public long length() throws SQLException {
        ensureNotClosed();

        return arr.length;
    }

    /** {@inheritDoc} */
    @Override public byte[] getBytes(long pos, int len) throws SQLException {
        ensureNotClosed();

        if (pos < 1 || arr.length - pos < 0 || len < 0)
            throw new SQLException("Invalid argument. Position can't be less than 1 or " +
                "greater than size of underlying byte array. Requested length also can't be negative " + "" +
                "[pos=" + pos + ", len=" + len + ']');

        int idx = (int)(pos - 1);

        int size = len > arr.length - idx ? arr.length - idx : len;

        byte[] res = new byte[size];

        U.arrayCopy(arr, idx, res, 0, size);

        return res;
    }

    /** {@inheritDoc} */
    @Override public InputStream getBinaryStream() throws SQLException {
        ensureNotClosed();

        return new ByteArrayInputStream(arr);
    }

    /** {@inheritDoc} */
    @Override public InputStream getBinaryStream(long pos, long len) throws SQLException {
        ensureNotClosed();

        if (pos < 1 || len < 1 || pos > arr.length || len > arr.length - pos + 1)
            throw new SQLException("Invalid argument. Position can't be less than 1 or " +
                "greater than size of underlying byte array. Requested length can't be negative and can't be " +
                "greater than available bytes from given position [pos=" + pos + ", len=" + len + ']');

        return new ByteArrayInputStream(arr, (int)(pos - 1), (int)len);
    }

    /** {@inheritDoc} */
    @Override public long position(byte[] ptrn, long start) throws SQLException {
        ensureNotClosed();

        if (start < 1 || start > arr.length || ptrn.length == 0 || ptrn.length > arr.length)
            return -1;

        for (int i = 0, pos = (int)(start - 1); pos < arr.length;) {
            if (arr[pos] == ptrn[i]) {
                pos++;

                i++;

                if (i == ptrn.length)
                    return pos - ptrn.length + 1;
            }
            else {
                pos = pos - i + 1;

                i = 0;
            }
        }

        return -1;
    }

    /** {@inheritDoc} */
    @Override public long position(Blob ptrn, long start) throws SQLException {
        ensureNotClosed();

        if (start < 1 || start > arr.length || ptrn.length() == 0 || ptrn.length() > arr.length)
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

        int idx = (int)(pos - 1);

        if (pos - 1 > arr.length || off < 0 || off >= bytes.length || off + len > bytes.length)
            throw new ArrayIndexOutOfBoundsException();

        byte[] dst = arr;

        if (idx + len > arr.length) {
            dst = new byte[arr.length + (len - (arr.length - idx))];

            U.arrayCopy(arr, 0, dst, 0, idx);

            arr = dst;
        }

        U.arrayCopy(bytes, off, dst, idx, len);

        return len;
    }

    /** {@inheritDoc} */
    @Override public OutputStream setBinaryStream(long pos) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    @Override public void truncate(long len) throws SQLException {
        ensureNotClosed();

        if (len < 0 || len > arr.length)
            throw new SQLException("Invalid argument. Length can't be " +
                "less than zero or greater than Blob length [len=" + len + ']');

        arr = Arrays.copyOf(arr, (int)len);

    }

    /** {@inheritDoc} */
    @Override public void free() throws SQLException {
        if (arr != null)
            arr = null;
    }

    /**
     *
     */
    private void ensureNotClosed() throws SQLException {
        if (arr == null)
            throw new SQLException("Blob instance can't be used after free() has been called.");
    }
}

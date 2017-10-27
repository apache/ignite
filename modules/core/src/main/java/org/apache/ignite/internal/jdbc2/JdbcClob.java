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
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * CLOB implementation for Ignite JDBC driver.
 */
public class JdbcClob implements Clob {
    /** CLOB's character sequence. */
    private String chars;

    /**
     * @param chars CLOB's character sequence.
     */
    public JdbcClob(String chars) {
        this.chars = chars;
    }

    /** {@inheritDoc} */
    @Override public long length() throws SQLException {
        ensureNotClosed();

        return chars.length();
    }

    /** {@inheritDoc} */
    @Override public String getSubString(long pos, int len) throws SQLException {
        ensureNotClosed();

        if (pos < 1 || len < 0 || pos - 1 + len > chars.length())
            throw new SQLException("Invalid argument. Position should be greater than 0. Length should not be " +
                "negative. Position + length should be less than CLOB size [pos=" + pos + ", length=" + len + ']');

        return chars.substring((int)pos - 1, (int)pos - 1 + len);
    }

    /** {@inheritDoc} */
    @Override public Reader getCharacterStream() throws SQLException {
        ensureNotClosed();

        return new StringReader(chars);
    }

    /** {@inheritDoc} */
    @Override public Reader getCharacterStream(long pos, long len) throws SQLException {
        return new StringReader(getSubString(pos, (int)len));
    }

    /** {@inheritDoc} */
    @Override public InputStream getAsciiStream() throws SQLException {
        ensureNotClosed();

        return new ByteArrayInputStream(chars.getBytes());
    }

    /** {@inheritDoc} */
    @Override public long position(String searchStr, long start) throws SQLException {
        ensureNotClosed();

        if (start < 1)
            throw new SQLException("Invalid argument. Start position should be greater than zero [start=" +
                start + ']');

        int idx = chars.indexOf(searchStr, (int)start - 1);

        return idx == -1 ? -1 : idx + 1;
    }

    /** {@inheritDoc} */
    @Override public long position(Clob searchStr, long start) throws SQLException {
        return position(searchStr.getSubString(1, (int)searchStr.length()), start);
    }

    /** {@inheritDoc} */
    @Override public int setString(long pos, String str) throws SQLException {
        ensureNotClosed();

        if (pos < 1 || str == null || pos > chars.length())
            throw new SQLException("Invalid argument. Position should be greater than zero. " +
                "Position should not exceed CLOB length. Source string should not be null " +
                "[pos=" + pos + ", str=" + str + ']');

        StringBuilder strBuilder = new StringBuilder(chars);

        // Ensure string buffer capacity
        if (pos - 1 + str.length() > chars.length())
            strBuilder.setLength((int)pos - 1 + str.length());

        strBuilder.replace((int)pos - 1, (int)pos - 1 + str.length(), str);

        chars = strBuilder.toString();

        return str.length();
    }

    /** {@inheritDoc} */
    @Override public int setString(long pos, String str, int off, int len) throws SQLException {
        ensureNotClosed();

        if (pos < 1 || str == null || pos > chars.length() || off < 0 || len < 0 || off + len > str.length())
            throw new SQLException("Invalid argument. Position should be greater than zero. " +
                "Position should not exceed CLOB length. Source string should not be null.  " +
                "Offset and length shouldn't be negative. Offset + length should not exceed source string length " +
                "[pos=" + pos + ", str=" + str + ", offset=" + off + ", len=" + len + ']');

        StringBuilder strBuilder = new StringBuilder(chars);

        // Ensure string buffer capacity
        if (pos - 1 + str.length() > chars.length())
            strBuilder.setLength((int)pos - 1 + str.length());

        String replaceStr = str.substring(off, off + len);
        strBuilder.replace((int)pos - 1, (int)pos - 1 + replaceStr.length(), replaceStr);

        chars = strBuilder.toString();

        return replaceStr.length();
    }

    /** {@inheritDoc} */
    @Override public void truncate(long len) throws SQLException {
        ensureNotClosed();

        if (len < 0 || len > chars.length())
            throw new SQLException("Invalid argument. Truncation length should not be negative. Truncation length " +
                "should not exceed data length [len=" + len + ']');

        chars = chars.substring(0, (int)len);
    }

    /** {@inheritDoc} */
    @Override public OutputStream setAsciiStream(long pos) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    @Override public Writer setCharacterStream(long pos) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    @Override public void free() throws SQLException {
        chars = null;
    }

    /**
     * Ensures CLOB hasn't been closed.
     */
    private void ensureNotClosed() throws SQLException {
        if (chars == null)
            throw new SQLException("Clob instance can't be used after free() has been called.");
    }
}

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
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.charset.StandardCharsets.UTF_8;

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

        long zeroBasedPos = pos - 1;

        if (zeroBasedPos < 0 || len < 0 || zeroBasedPos + len > chars.length())
            throw new SQLException("Invalid argument. Position should be greater than 0. Length should not be " +
                "negative. Position + length should be less than CLOB size [pos=" + pos + ", length=" + len + ']');

        return getSubStringInternal((int)zeroBasedPos, len);
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

        // Encode to UTF-8 since Ignite internally stores strings in UTF-8 by default.
        return new Utf8EncodedStringInputStream(chars);
    }

    /** {@inheritDoc} */
    @Override public long position(String searchStr, long start) throws SQLException {
        ensureNotClosed();

        if (start < 1)
            throw new SQLException("Invalid argument. Start position should be greater than zero [start=" +
                start + ']');

        long zeroBasedIdx = positionInternal(searchStr, start - 1);

        return zeroBasedIdx == -1 ? -1 : zeroBasedIdx + 1;
    }

    /** {@inheritDoc} */
    @Override public long position(Clob searchStr, long start) throws SQLException {
        return position(searchStr.getSubString(1, (int)searchStr.length()), start);
    }

    /** {@inheritDoc} */
    @Override public int setString(long pos, String str) throws SQLException {
        ensureNotClosed();

        long zeroBasedPos = pos - 1;

        if (zeroBasedPos < 0 || str == null || zeroBasedPos > chars.length())
            throw new SQLException("Invalid argument. Position should be greater than zero. " +
                "Position should not exceed CLOB length+1. Source string should not be null " +
                "[pos=" + pos + ", str=" + str + ']');

        return setStringInternal((int)zeroBasedPos, str);
    }

    /** {@inheritDoc} */
    @Override public int setString(long pos, String str, int off, int len) throws SQLException {
        ensureNotClosed();

        long zeroBasedPos = pos - 1;

        if (zeroBasedPos < 0 || str == null || zeroBasedPos > chars.length() || off < 0 || len < 0 || off + len > str.length())
            throw new SQLException("Invalid argument. Position should be greater than zero. " +
                "Position should not exceed CLOB length+1. Source string should not be null.  " +
                "Offset and length shouldn't be negative. Offset + length should not exceed source string length " +
                "[pos=" + pos + ", str=" + str + ", offset=" + off + ", len=" + len + ']');

        return setStringInternal((int)zeroBasedPos, str, off, len);
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

    /**
     * Internal getSubString implementation with zero-based position parameter.
     */
    private String getSubStringInternal(int zeroBasedPos, int len) {
        return chars.substring(zeroBasedPos, zeroBasedPos + len);
    }

    /**
     * Internal position implementation with zero-based start parameter.
     */
    private long positionInternal(String searchStr, long zeroBasedStart) {
        return chars.indexOf(searchStr, (int)zeroBasedStart);
    }

    /**
     * Internal setString implementation with zero-based position parameter.
     */
    private int setStringInternal(int zeroBasedPos, String str) {
        StringBuilder strBuilder = new StringBuilder(chars);

        // Ensure string buffer capacity
        if (zeroBasedPos + str.length() > chars.length())
            strBuilder.setLength(zeroBasedPos + str.length());

        strBuilder.replace(zeroBasedPos, zeroBasedPos + str.length(), str);

        chars = strBuilder.toString();

        return str.length();
    }

    /**
     * Internal setString implementation with zero-based position parameter.
     */
    private int setStringInternal(int zeroBasedPos, String str, int off, int len) {
        StringBuilder strBuilder = new StringBuilder(chars);

        // Ensure string buffer capacity
        if (zeroBasedPos + str.length() > chars.length())
            strBuilder.setLength(zeroBasedPos + str.length());

        String replaceStr = str.substring(off, off + len);
        strBuilder.replace(zeroBasedPos, zeroBasedPos + replaceStr.length(), replaceStr);

        chars = strBuilder.toString();

        return replaceStr.length();
    }

    /**
     * Input stream which encodes the given string to UTF-8.
     * To save memory for large strings it does it by chunks.
     */
    private static class Utf8EncodedStringInputStream extends InputStream {
        /** String to encode. */
        private final String chars;

        /** String length. */
        private final int length;

        /** Start index of the next chunk (substring) to be encoded. */
        private int charsPos;

        /** Default chunk size. */
        private static final int DEFAULT_CHUNK_SIZE = 8192;

        /** Buffer containing the current chunk encoding. */
        private byte[] buf;

        /** Current position in the buffer - index of the next byte to be read from the input stream. */
        private int bufPos;

        /**
         * @param chars String to be encoded.
         */
        Utf8EncodedStringInputStream(String chars) {
            this.chars = chars;

            length = chars.length();
            charsPos = 0;
        }

        /** {@inheritDoc} */
        @Override public synchronized int read() {
            if (buf == null || buf.length == 0 || bufPos >= buf.length) {
                if (charsPos >= length)
                    return -1;

                bufPos = 0;

                encodeNextChunk();
            }

            return buf[bufPos++] & 0xFF;
        }

        /** {@inheritDoc} */
        @Override public synchronized int read(byte[] b, int off, int len) {
            if (b == null)
                throw new NullPointerException();

            if (off < 0 || len < 0 || len > b.length - off)
                throw new IndexOutOfBoundsException(String.format("Range [%s, %<s + %s) out of bounds for length %s", off, len, b.length));

            if (len == 0)
                return 0;

            int i = 0;

            while (i < len) {
                if (buf == null || buf.length == 0 || bufPos >= buf.length) {
                    if (charsPos >= length)
                        return i > 0 ? i : -1;

                    bufPos = 0;

                    encodeNextChunk();
                }

                int encodedChunkSize = Math.min(len - i, buf.length - bufPos);

                U.arrayCopy(buf, bufPos, b, off + i, encodedChunkSize);

                bufPos += encodedChunkSize;
                i += encodedChunkSize;
            }

            return i;
        }

        /**
         * Encodes the next chunk of the string.
         * <p>
         * Makes sure that chunk doesn't contain the malformed surrogate element at the end
         * (high surrogate that is not followed by a low surrogate).
         */
        private void encodeNextChunk() {
            int remainingSize = chars.length() - charsPos;

            assert remainingSize > 0;

            int chunkSize;

            if (remainingSize <= DEFAULT_CHUNK_SIZE) {
                chunkSize = remainingSize;
            }
            else if (Character.isHighSurrogate(chars.charAt(charsPos + DEFAULT_CHUNK_SIZE - 1))) {
                chunkSize = DEFAULT_CHUNK_SIZE + 1;
            }
            else {
                chunkSize = DEFAULT_CHUNK_SIZE;
            }

            String subs = chars.substring(charsPos, charsPos + chunkSize);
            buf = subs.getBytes(UTF_8);

            charsPos += chunkSize;
        }
    }
}

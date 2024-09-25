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

package org.apache.ignite.internal.processors.odbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import org.apache.ignite.internal.jdbc2.JdbcDataBufferImpl;

/**
 * InputStream wrapper for limited streams.
 */
public class SqlInputStreamWrapper implements AutoCloseable {
    /** */
    private InputStream inputStream;

    /** Memory buffer for .*/
    private JdbcDataBufferImpl rawData;

    /** */
    private final int len;

    /**
     * The maximum size of array to allocate.
     * Some VMs reserve some header words in an array.
     * Attempts to allocate larger arrays may result in
     * OutOfMemoryError: Requested array size exceeds VM limit
     * @see java.util.ArrayList#MAX_ARRAY_SIZE
     */
    public static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    /**
     * Constructs wrapper for stream with known length.
     *
     * @param inputStream Input stream.
     * @param len Length of data in the input stream.
     * @return Input stream wrapper.
     */
    public static SqlInputStreamWrapper withKnownLength(InputStream inputStream, int len) throws SQLException {
        try {
            return new SqlInputStreamWrapper(inputStream, len, null);
        }
        catch (IOException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Constructs wrapper for stream if length is unknown.
     * <p>
     * It would try to determine the data length reading the whole stream syncroniously. If the stream length is
     * less than {@code maxMemoryBufferBytes} data will be stored in heap memory. Otherwise, data will be written
     * to temporary file.
     *
     * @param inputStream Input stream.
     * @param maxMemoryBufferBytes Maximum memory buffer size in bytes.
     * @return Input stream wrapper.
     */
    public static SqlInputStreamWrapper withUnknownLength(InputStream inputStream, int maxMemoryBufferBytes) throws SQLException {
        try {
            return new SqlInputStreamWrapper(inputStream, null, maxMemoryBufferBytes);
        }
        catch (IOException e) {
            throw new SQLException(e);
        }
    }

    /**
     * @param inputStream Input stream.
     * @param len Length of data in the input stream. May be null if unknown.
     * @param maxMemoryBufferBytes Maximum memory buffer size in bytes. Is null if len is not null.
     */
    protected SqlInputStreamWrapper(InputStream inputStream, Integer len, Integer maxMemoryBufferBytes)
            throws IOException, SQLException {
        if (len != null) {
            this.inputStream = inputStream;
            this.len = len;
            return;
        }

        rawData = new JdbcDataBufferImpl(maxMemoryBufferBytes);

        copyStream(inputStream, rawData.getOutputStream(), MAX_ARRAY_SIZE);

        this.len = Math.toIntExact(rawData.getLength());
    }

    /**
     * Returns input stream for the enclosed data.
     *
     * @return Input stream.
     */
    public InputStream getInputStream() throws IOException {
        if (inputStream == null)
            inputStream = rawData.getInputStream();

        return inputStream;
    }

    /**
     * @return Length of data in the input stream.
     */
    public int getLength() {
        return len;
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        if (inputStream != null)
            inputStream.close();
    }

    /**
     * Copy data from the input stream to the output stream.
     * <p>
     * Stops and retuen -1 if count of bytes copied exceeds the {@code limit}.
     *
     * @param inputStream input stream
     * @param outputStream output stream
     * @param limit Maximum bytes to copy.
     * @throws SQLException if limit exceeds.
     * @return Count of bytes copied.
     */
    private static int copyStream(InputStream inputStream, OutputStream outputStream, long limit) throws IOException, SQLException {
        int totalLength = 0;

        byte[] buf = new byte[8192];

        int readLength = inputStream.read(buf, 0, (int)Math.min(buf.length, limit));

        while (readLength > 0) {
            totalLength += readLength;

            outputStream.write(buf, 0, readLength);

            if (totalLength == limit)
                throw new SQLFeatureNotSupportedException("Invalid argument. InputStreams with length greater than " +
                        MAX_ARRAY_SIZE + " are not supported.");

            readLength = inputStream.read(buf, 0, (int)Math.min(buf.length, limit - totalLength));
        }

        return totalLength;
    }
}

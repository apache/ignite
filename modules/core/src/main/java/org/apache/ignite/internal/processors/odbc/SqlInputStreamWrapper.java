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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * InputStream wrapper for limited streams.
 */
public class SqlInputStreamWrapper implements AutoCloseable {
    /** */
    private InputStream inputStream;

    /** */
    private final byte[] rawData;

    /** */
    private Path tempFile;

    /** */
    private final Integer len;

    /** */
    private static final int MAX_MEMORY_BUFFER_BYTES = 51200;

    /** */
    private static final String TEMP_FILE_PREFIX = "ignite-jdbc-stream";

    /** */
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    /**
     * @param inputStream Input stream.
     * @param len Length of data in the input stream.
     */
    public SqlInputStreamWrapper(InputStream inputStream, Integer len) {
        this.inputStream = inputStream;
        this.len = len;

        rawData = null;
        tempFile = null;
    }

    /**
     * @param inputStream Input stream.
     */
    public SqlInputStreamWrapper(InputStream inputStream) throws SQLException {
        try {
            ByteArrayOutputStream memoryOutputStream = new ByteArrayOutputStream();
            final int memoryLength = copyStream(inputStream, memoryOutputStream, MAX_MEMORY_BUFFER_BYTES);
            byte[] rawData = memoryOutputStream.toByteArray();

            if (memoryLength == -1) {
                final int diskLength;

                tempFile = Files.createTempFile(TEMP_FILE_PREFIX, ".tmp");

                try (OutputStream diskOutputStream = Files.newOutputStream(tempFile)) {
                    diskOutputStream.write(rawData);

                    diskLength = copyStream(inputStream, diskOutputStream, MAX_ARRAY_SIZE - rawData.length);

                    if (diskLength == -1)
                        throw new SQLFeatureNotSupportedException("Invalid argument. InputStreams with length > " +
                                MAX_ARRAY_SIZE + " are not supported.");
                }
                catch (RuntimeException | Error | SQLException e) {
                    try {
                        tempFile.toFile().delete();
                    }
                    catch (Throwable ignore) {
                        // No-op
                    }

                    throw e;
                }

                this.rawData = null;

                this.inputStream = null;

                len = rawData.length + diskLength;
            }
            else {
                this.rawData = rawData;

                this.inputStream = null;

                len = rawData.length;
            }
        }
        catch (IOException e) {
            throw new SQLException("An I/O error occurred while sending to the backend.", e);
        }
    }

    /** */
    public InputStream getStream() throws IOException {
        if (inputStream != null) {
            return inputStream;
        }
        else if (tempFile != null) {
            inputStream = Files.newInputStream(tempFile);
            return inputStream;
        }
        else {
            inputStream = new ByteArrayInputStream(rawData, 0, len);
        }

        return inputStream;
    }

    /** */
    public Integer getLength() {
        return len;
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        if (tempFile != null) {
            tempFile.toFile().delete();
            tempFile = null;

            if (inputStream != null) {
                inputStream.close();
                inputStream = null;
            }
        }
    }

    /** */
    private static int copyStream(InputStream inputStream, OutputStream outputStream, int limit) throws IOException {
        int totalLength = 0;

        byte[] buf = new byte[8192];

        int readLength = inputStream.read(buf);

        while (readLength > 0) {
            totalLength += readLength;

            outputStream.write(buf, 0, readLength);

            if (totalLength >= limit) {
                return -1;
            }

            readLength = inputStream.read(buf);
        }
        return totalLength;
    }
}

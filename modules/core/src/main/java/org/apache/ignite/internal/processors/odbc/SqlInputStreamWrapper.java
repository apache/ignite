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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.Cleaner;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.internal.jdbc2.JdbcMemoryBuffer;

/**
 * InputStream wrapper for limited streams.
 */
public class SqlInputStreamWrapper implements AutoCloseable {
    /** */
    private InputStream inputStream;

    /** Memory buffer for .*/
    private JdbcMemoryBuffer rawData;

    /** Temporary file holder. */
    private TempFileHolder tempFileHolder;

    /** */
    private Cleaner.Cleanable tempFileCleaner;

    /** */
    private final int len;

    /** */
    private static final String TEMP_FILE_PREFIX = "ignite-jdbc-stream";

    /** */
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    /**
     * Constructs wrapper for stream with known length.
     *
     * @param inputStream Input stream.
     * @param len Length of data in the input stream.
     * @return Input stream wrapper.
     */
    public static SqlInputStreamWrapper withKnownLength(InputStream inputStream, int len) throws SQLException, IOException {
        return new SqlInputStreamWrapper(inputStream, len, null);
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
    public static SqlInputStreamWrapper withUnknownLength(InputStream inputStream, int maxMemoryBufferBytes)
            throws SQLException, IOException {
        return new SqlInputStreamWrapper(inputStream, null, maxMemoryBufferBytes);
    }

    /**
     * @param inputStream Input stream.
     * @param len Length of data in the input stream. May be null if unknown.
     * @param maxMemoryBufferBytes Maximum memory buffer size in bytes. Is null if len is not null.
     */
    protected SqlInputStreamWrapper(InputStream inputStream, Integer len, Integer maxMemoryBufferBytes)
            throws IOException, SQLFeatureNotSupportedException {
        if (len != null) {
            this.inputStream = inputStream;
            this.len = len;
            return;
        }

        rawData = new JdbcMemoryBuffer();
        final int memoryLength = copyStream(inputStream, rawData.getOutputStream(), maxMemoryBufferBytes + 1);

        if (memoryLength == -1) {
            final int diskLength;

            File tempFile = File.createTempFile(TEMP_FILE_PREFIX, ".tmp");
            tempFile.deleteOnExit();

            tempFileHolder = new TempFileHolder(tempFile.toPath());

            tempFileCleaner = ((IgniteJdbcThinDriver)IgniteJdbcThinDriver.register())
                    .getCleaner()
                    .register(this, tempFileHolder);

            try (OutputStream diskOutputStream = Files.newOutputStream(tempFile.toPath())) {
                copyStream(rawData.getInputStream(), diskOutputStream, rawData.getLength());

                diskLength = copyStream(inputStream, diskOutputStream, MAX_ARRAY_SIZE - rawData.getLength());

                if (diskLength == -1)
                    throw new SQLFeatureNotSupportedException("Invalid argument. InputStreams with length greater than " +
                            MAX_ARRAY_SIZE + " are not supported.");
            }
            catch (RuntimeException | Error | SQLException e) {
                tempFileCleaner.clean();

                throw e;
            }

            this.len = Math.toIntExact(rawData.getLength() + diskLength);
        }
        else {
            this.len = Math.toIntExact(rawData.getLength());
        }
    }

    /**
     * Returns input stream for the enclosed data.
     *
     * @return Input stream.
     */
    public InputStream getInputStream() throws IOException {
        if (inputStream != null)
            return inputStream;

        if (tempFileHolder != null)
            inputStream = tempFileHolder.getInputStream();
        else
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
        if (tempFileCleaner != null)
            tempFileCleaner.clean();
    }

    /**
     * Copy data from the input stream to the output stream.
     * <p>
     * Stops and retuen -1 if count of bytes copied exceeds the {@code limit}.
     *
     * @param inputStream input stream
     * @param outputStream output stream
     * @param limit Maximum bytes to copy.
     * @return Count of bytes copied. -1 if limit exceeds.
     */
    private static int copyStream(InputStream inputStream, OutputStream outputStream, long limit) throws IOException {
        int totalLength = 0;

        byte[] buf = new byte[8192];

        int readLength = inputStream.read(buf, 0, (int)Math.min(buf.length, limit));

        while (readLength > 0) {
            totalLength += readLength;

            outputStream.write(buf, 0, readLength);

            if (totalLength == limit)
                return -1;

            readLength = inputStream.read(buf, 0, (int)Math.min(buf.length, limit - totalLength));
        }

        return totalLength;
    }

    /**
     * Holder for the temporary file.
     * <p>
     * Used to remove the temp file once the stream wrapper object has become phantom reachable.
     * It may be if the large stream was passed as argumant to statement and this sattement
     * was abandoned without being closed.
     */
    private static class TempFileHolder implements Runnable {
        /** Full path to temp file. */
        private final Path tempFile;

        /** Input stream opened for this temp file if any. */
        private InputStream inputStream;

        /**
         * @param tempFile Full path to temp file.
         */
        TempFileHolder(Path tempFile) {
            this.tempFile = tempFile;
        }

        /**
         * @return Input stream for reading from temp file.
         */
        InputStream getInputStream() throws IOException {
            if (inputStream == null)
                inputStream = Files.newInputStream(tempFile);

            return inputStream;
        }

        /** The cleaning action to be called by the {@link java.lang.ref.Cleaner}. */
        @Override public void run() {
            clean();
        }

        /** Cleans the temp file and input stream if it was created. */
        private void clean() {
            try {
                tempFile.toFile().delete();

                if (inputStream != null) {
                    inputStream.close();

                    inputStream = null;
                }
            }
            catch (IOException ignore) {
                // No-op
            }
        }
    }
}

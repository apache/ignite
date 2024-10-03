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

package org.apache.ignite.internal.jdbc2.lob;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Temporary file based implementattion of {@link JdbcBlobStorage}.
 *
 * <p>Keeps data in temporary file.
 *
 * <p>Makes sure file is removed once the instance become phantom reachable.
 * In other words even if {@code close()} wasn't called properly. Say if the large InputStream
 * or Blob was passed as argumеnt to prepared statement and statement was abandoned by client
 * application without being closed.
 */
class JdbcBlobTmpFileStorage implements JdbcBlobStorage {
    /** Prefix for temp file name. */
    private static final String TEMP_FILE_PREFIX = "ignite-jdbc-temp-data";

    /** File clean action. */
    private final Cleaner.Cleanable fileCleaner;

    /** File channel.*/
    private final FileChannel fileChannel;

    /** Cleaner instance used to remove temp files. */
    private static final Cleaner cleaner = Cleaner.create();

    /** The total number of bytes in the storage. */
    private long totalCnt;

    /**
     * Creates new temp file storage coping data from the given input stream.
     *
     * @param stream Input stream with data.
     * @throws IOException if an error occurred.
     */
    JdbcBlobTmpFileStorage(InputStream stream) throws IOException {
        File tempFile = File.createTempFile(TEMP_FILE_PREFIX, ".tmp");
        tempFile.deleteOnExit();

        fileCleaner = cleaner.register(this, new FileCleanAction(tempFile.toPath()));

        try (OutputStream diskOutputStream = Files.newOutputStream(tempFile.toPath())) {
            stream.transferTo(diskOutputStream);
        }
        catch (Exception e) {
            fileCleaner.clean();

            throw e;
        }

        fileChannel = FileChannel.open(tempFile.toPath(), WRITE, READ);

        totalCnt = fileChannel.size();
    }

    /** {@inheritDoc} */
    @Override public long totalCnt() {
        return totalCnt;
    }

    /** {@inheritDoc} */
    @Override public JdbcBlobBufferPointer createPointer() {
        return new JdbcBlobBufferPointer();
    }

    /** {@inheritDoc} */
    @Override public int read(JdbcBlobBufferPointer pos) throws IOException {
        byte[] res = new byte[1];

        int read = fileChannel.read(ByteBuffer.wrap(res), pos.getPos());

        if (read == -1) {
            return -1;
        }
        else {
            advance(pos, read);

            return res[0] & 0xff;
        }
    }

    /** {@inheritDoc} */
    @Override public int read(JdbcBlobBufferPointer pos, byte[] res, int off, int cnt) throws IOException {
        int read = fileChannel.read(ByteBuffer.wrap(res, off, cnt), pos.getPos());

        if (read != -1)
            advance(pos, read);

        return read;
    }

    /** {@inheritDoc} */
    @Override public void write(JdbcBlobBufferPointer pos, int b) throws IOException {
        write(pos, new byte[]{(byte)b}, 0, 1);
    }

    /** {@inheritDoc} */
    @Override public void write(JdbcBlobBufferPointer pos, byte[] bytes, int off, int len) throws IOException {
        int written = fileChannel.write(ByteBuffer.wrap(bytes, off, len), pos.getPos());

        advance(pos, written);

        totalCnt = Math.max(pos.getPos(), totalCnt);
    }

    /** {@inheritDoc} */
    @Override public void truncate(long len) throws IOException {
        fileChannel.truncate(len);

        totalCnt = fileChannel.size();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try {
            fileChannel.close();
        }
        catch (IOException ignore) {
            // No-op
        }

        fileCleaner.clean();
    }

    /** {@inheritDoc} */
    @Override public void advance(JdbcBlobBufferPointer pos, long step) {
        pos.setPos(pos.getPos() + step);
    }

    /**
     * Holder for the temporary file.
     * <p>
     * Used to remove the temp file once the stream wrapper object has become phantom reachable.
     * It may be if the large stream was passed as argumant to statement and this statement
     * was abandoned without being closed.
     */
    private static class FileCleanAction implements Runnable {
        /** Full path to temp file. */
        private final Path path;

        /**
         * @param path Full path to temp file.
         */
        public FileCleanAction(Path path) {
            this.path = path;
        }

        /** The cleaning action to be called by the {@link java.lang.ref.Cleaner}. */
        @Override public void run() {
            try {
                Files.deleteIfExists(path);
            }
            catch (IOException ignore) {
                // No-op
            }
        }
    }
}

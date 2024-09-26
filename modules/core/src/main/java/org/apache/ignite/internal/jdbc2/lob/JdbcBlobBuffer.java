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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Buffer storing the binary data either in memory or temporary file on disk depending
 * on the data size.
 *
 * <p>Initially data is stored in memory. Once the size exceeds the {@code maxMemoryBufferBytes} limit
 * it's saved to temporary file.
 *
 * <p>Once switched to the file mode buffer doesn't return to the in-memory even if it's truncated
 * to size lower than limit.
 *
 * <p>If buffer initially wraps the existing byte array which exceeds the limit it still starts in
 * the in-memory mode. And switches to the file mode only if any write operation increases the
 * buffer size even more.
 *
 * <p> Data is read via the InputStream API and modified via the OutputStream one. Changes done via
 * OutputStream are visible via the InputStream even if InputStream is created before changes done.
 *
 * <p>Note however that implementation is not thread-safe.
 */
public class JdbcBlobBuffer {
    /** The underlying data storage. */
    private JdbcBlobStorage storage;

    /** Maximum data size to be stored in-memory. */
    private final Integer maxMemoryBufferBytes;

    /**
     * Create empty buffer.
     *
     * @param maxMemoryBufferBytes Maximum data size to be stored in-memory.
     */
    public JdbcBlobBuffer(int maxMemoryBufferBytes) {
        storage = new JdbcBlobMemoryStorage();

        this.maxMemoryBufferBytes = maxMemoryBufferBytes;
    }

    /**
     * Create buffer wrapping the existing byte array.
     *
     * @param maxMemoryBufferBytes Maximum data size to be stored in-memory.
     * @param arr The byte array to be wrapped.
     */
    public JdbcBlobBuffer(int maxMemoryBufferBytes, byte[] arr) {
        storage = new JdbcBlobMemoryStorage(arr);

        this.maxMemoryBufferBytes = maxMemoryBufferBytes;
    }

    /**
     * @return Total number of bytes in the buffer.
     */
    public long totalCnt() {
        return storage.totalCnt();
    }

    /**
     * Provide OutputStream through which the data can be written to buffer starting from
     * the (zero-based) position.
     *
     * @param pos The zero-based offset to the first byte to be written. Must not be negative
     *            or greater than total count of bytes in buffer.
     *
     * @return OutputStream instance.
     */
    public OutputStream getOutputStream(long pos) {
        assert pos >= 0;
        assert pos <= storage.totalCnt();

        return new BufferOutputStream(pos);
    }

    /**
     * Provide InputStream through which the data can be read starting from the
     * begining.
     *
     * <p>Stream is not limited meaning that it would return any new data
     * written to the buffer after stream creation.
     *
     * @return InputStream instance.
     */
    public InputStream getInputStream() {
        return new BufferInputStream();
    }

    /**
     * Provide InputStream through which the no more than {@code len} bytes can be read
     * from buffer starting from the specified zero-based position {@code pos}.
     *
     * @param pos The zero-based offset to the first byte to be retrieved. Must not be negative
     *            or greater than total count of bytes in buffer.
     * @param len The length in bytes of the data to be retrieved. Must not be negative.
     * @return InputStream instance.
     */
    public InputStream getInputStream(long pos, long len) {
        assert len >= 0;
        assert pos >= 0;
        assert pos < storage.totalCnt() || (storage.totalCnt() == 0 && pos == 0);

        if (storage.totalCnt() == 0 || len == 0)
            return InputStream.nullInputStream();

        return new BufferInputStream(pos, len);
    }

    /**
     * Truncate the buffer to the specified length.
     *
     * @param len The length in bytes to be truncated. Must not be negative
     *            or greater than total count of bytes in buffer.
     */
    public void truncate(long len) throws IOException {
        assert len >= 0;
        assert len <= storage.totalCnt();

        storage.truncate(len);
    }

    /**
     * Close the buffer and free any resources.
     */
    public void close() {
        storage.close();
    }

    /**
     * Switch to file mode.
     * <p>
     * Copies all data from the in-memory storage to the temporary file storage.
     */
    private void switchToFileStorage() throws IOException {
        JdbcBlobStorage newStorage = new JdbcBlobFileStorage(getInputStream());

        storage.close();

        storage = newStorage;
    }

    /**
     * Input stream to read data from buffer.
     */
    private class BufferInputStream extends InputStream {
        /** Current position in the buffer. */
        private final JdbcBlobBufferPointer curPointer;

        /** Stream starting position. */
        private final long start;

        /** Stream length limit. May be null which means no limit. */
        private final Long limit;

        /** Remembered buffer position at the moment the {@link InputStream#mark} is called. */
        private final JdbcBlobBufferPointer markedPointer;

        /**
         * Create unlimited stream to read all data from the buffer starting from the beginning.
         */
        private BufferInputStream() {
            this(0, null);
        }

        /**
         * Create stream to read data from the buffer starting from the specified {@code start}
         * zere-based position.
         *
         * @param start The zero-based offset to the first byte to be retrieved.
         * @param limit The maximim length in bytes of the data to be retrieved. Unlimited if null.
         */
        private BufferInputStream(long start, Long limit) {
            this.start = start;

            this.limit = limit;

            curPointer = storage.createPointer();

            if (start > 0)
                storage.advance(curPointer, start);

            markedPointer = storage.createPointer().set(curPointer);
        }

        /** {@inheritDoc} */
        @Override public int read() throws IOException {
            if (limit != null && curPointer.getPos() >= start + limit)
                return -1;

            return storage.read(curPointer);
        }

        /** {@inheritDoc} */
        @Override public int read(byte res[], int off, int cnt) throws IOException {
            int toRead = cnt;

            if (limit != null) {
                if (curPointer.getPos() >= start + limit)
                    return -1;

                long availableBytes = start + limit - curPointer.getPos();

                if (cnt > availableBytes)
                    toRead = (int)availableBytes;
            }

            return storage.read(curPointer, res, off, toRead);
        }

        /** {@inheritDoc} */
        @Override public boolean markSupported() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public synchronized void reset() {
            curPointer.set(markedPointer);
        }

        /** {@inheritDoc} */
        @Override public synchronized void mark(int readlimit) {
            markedPointer.set(curPointer);
        }
    }

    /**
     * Output stream to write data to buffer.
     */
    private class BufferOutputStream extends OutputStream {
        /** Current position in the buffer. */
        private final JdbcBlobBufferPointer bufPos;

        /**
         * Create stream to write data to the buffer starting from the specified position {@code pos}.
         *
         * @param pos Starting position (zero-based).
         */
        private BufferOutputStream(long pos) {
            bufPos = storage.createPointer();

            if (pos > 0)
                storage.advance(bufPos, pos);
        }

        /** {@inheritDoc} */
        @Override public void write(int b) throws IOException {
            storage.write(bufPos, b);
        }

        /** {@inheritDoc} */
        @Override public void write(byte[] bytes, int off, int len) throws IOException {
            if (Math.max(bufPos.getPos() + len, storage.totalCnt()) > maxMemoryBufferBytes)
                switchToFileStorage();

            storage.write(bufPos, bytes, off, len);
        }
    }
}

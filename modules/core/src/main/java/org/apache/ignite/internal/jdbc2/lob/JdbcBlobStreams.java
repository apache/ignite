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
import java.util.Objects;

import static org.apache.ignite.internal.binary.streams.BinaryAbstractOutputStream.MAX_ARRAY_SIZE;

/**
 * Buffer storing the binary data.
 *
 * <p>Buffer can start working in read-only mode if created wrapping the existing byte array which
 * can not be modified. Any write operation switches it lazily to the read-write mode. This allows
 * to prevent the unnecessary data copying.
 *
 * <p>Data is read via the InputStream API and modified via the OutputStream one. Changes done via
 * OutputStream are visible via the InputStream even if InputStream is created before changes done.
 *
 * <p>InputStream and OutputStream created remain valid even if the underlying data storage changed from
 * read-only to read-write.
 *
 * <p>Note however that implementation is not thread-safe.
 */
public class JdbcBlobStreams {
    /**
     * Provide OutputStream through which the data can be written to buffer starting from
     * the (zero-based) position.
     *
     * @param pos The zero-based offset to the first byte to be written. Must not be negative
     *            or greater than total count of bytes in buffer.
     *
     * @return OutputStream instance.
     */
    public static OutputStream getOutputStream(JdbcBlobStorage storage, int pos) {
        return new BufferOutputStream(storage, pos);
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
    public static InputStream getInputStream(JdbcBlobStorage storage) {
        return new BufferInputStream(storage);
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
    public static InputStream getInputStream(JdbcBlobStorage storage, int pos, Integer len) {
        return new BufferInputStream(storage, pos, len);
    }

    /**
     * Input stream to read data from buffer.
     */
    private static class BufferInputStream extends InputStream {
        /** Current position in the buffer. */
        private final JdbcBlobBufferPointer curPointer;

        /** Storage reference. */
        private final JdbcBlobStorage storage;

        /** Stream starting position. */
        private final int start;

        /** Stream length limit. May be null which means no limit. */
        private final Integer limit;

        /** Remembered buffer position at the moment the {@link InputStream#mark} is called. */
        private final JdbcBlobBufferPointer markedPointer;

        /**
         * Create unlimited stream to read all data from the buffer starting from the beginning.
         */
        private BufferInputStream(JdbcBlobStorage storage) {
            this(storage, 0, null);
        }

        /**
         * Create stream to read data from the buffer starting from the specified {@code start}
         * zero-based position.
         *
         * @param storage Storage reference.
         * @param start The zero-based offset to the first byte to be retrieved.
         * @param limit The maximim length in bytes of the data to be retrieved. Unlimited if null.
         */
        private BufferInputStream(JdbcBlobStorage storage, int start, Integer limit) {
            this.storage = storage;

            this.start = start;

            this.limit = limit;

            curPointer = storage.createPointer();

            skip(start);

            markedPointer = storage.createPointer().set(curPointer);
        }

        /** {@inheritDoc} */
        @Override public int read() {
            if (limit != null && curPointer.getPos() - start >= limit)
                return -1;

            return storage.read(curPointer);
        }

        /** {@inheritDoc} */
        @Override public int read(byte[] res, int off, int cnt) {
            Objects.checkFromIndexSize(off, cnt, res.length);

            int toRead = cnt;

            if (limit != null) {
                if (curPointer.getPos() - start >= limit)
                    return -1;

                int availableBytes = limit - (curPointer.getPos() - start);

                if (cnt > availableBytes)
                    toRead = availableBytes;
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

        /** {@inheritDoc} */
        @Override public long skip(long n) {
            if (n <= 0)
                return 0;

            return storage.advancePointer(curPointer, (int)Math.min(n, MAX_ARRAY_SIZE));
        }
    }

    /**
     * Output stream to write data to buffer.
     */
    private static class BufferOutputStream extends OutputStream {
        /** Current position in the buffer. */
        private final JdbcBlobBufferPointer bufPos;

        /** Storage reference. */
        private final JdbcBlobStorage storage;

        /**
         * Create stream to write data to the buffer starting from the specified position {@code pos}.
         *
         * @param storage Storage reference.
         * @param pos Starting position (zero-based).
         */
        private BufferOutputStream(JdbcBlobStorage storage, int pos) {
            this.storage = storage;

            bufPos = storage.createPointer();

            if (pos > 0)
                storage.advancePointer(bufPos, pos);
        }

        /** {@inheritDoc} */
        @Override public void write(int b) throws IOException {
            storage.write(bufPos, b);
        }

        /** {@inheritDoc} */
        @Override public void write(byte[] bytes, int off, int len) throws IOException {
            Objects.checkFromIndexSize(off, len, bytes.length);

            storage.write(bufPos, bytes, off, len);
        }
    }
}

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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Memory buffer.
 */
public class JdbcDataBufferImpl implements JdbcDataBuffer {
    /** The list of buffers. */
    private List<byte[]> buffers;

    /** */
    private static final String TEMP_FILE_PREFIX = "ignite-jdbc-temp-data";

    /** */
    private TempFileHolder tempFileHolder;

    /** */
    private Cleaner.Cleanable tempFileCleaner;

    /** */
    private FileChannel tempFileChannel;

    /** */
    private final Integer maxMemoryBufferBytes;

    /** The total count of bytes. */
    private long totalCnt;

    /** */
    public JdbcDataBufferImpl(int maxMemoryBufferBytes) {
        buffers = new ArrayList<>();

        totalCnt = 0;

        this.maxMemoryBufferBytes = maxMemoryBufferBytes;
    }

    /** */
    public JdbcDataBufferImpl(int maxMemoryBufferBytes, byte[] arr) {
        buffers = new ArrayList<>();

        buffers.add(arr);

        totalCnt = arr.length;

        this.maxMemoryBufferBytes = maxMemoryBufferBytes;
    }

    /** */
    @Override public long getLength() {
        return totalCnt;
    }

    /** */
    @Override public OutputStream getOutputStream(long pos) {
        return new BufferOutputStream(pos);
    }

    /**
     * @param pos the zero-based offset to the first byte to be retrieved.
     * @param len the length in bytes of the data to be retrieved.
     * @return {@code InputStream} through which the data can be read.
     */
    @Override public InputStream getInputStream(long pos, long len) {
        if (pos < 0 || len < 0 || pos > totalCnt)
            throw new RuntimeException("Invalid argument. Position can't be less than 0 or " +
                    "greater than size of underlying memory buffers. Requested length can't be negative and can't be " +
                    "greater than available bytes from given position [pos=" + pos + ", len=" + len + ']');

        if (totalCnt == 0 || len == 0 || pos == totalCnt)
            return InputStream.nullInputStream();

        return new BufferInputStream(pos, len);
    }

    /**
     * Makes a new buffer available
     *
     * @param newCount the new size of the Blob
     */
    private void addNewBuffer(final int newCount) {
        final int newBufSize;

        if (buffers.isEmpty()) {
            newBufSize = newCount;
        }
        else {
            newBufSize = Math.max(
                    buffers.get(buffers.size() - 1).length << 1,
                    (newCount));
        }

        buffers.add(new byte[newBufSize]);
    }

    /** */
    @Override public void truncate(long len) throws IOException {
        totalCnt = len;

        if (tempFileHolder != null)
            tempFileChannel.truncate(len);

        // TODO free memory buffers as well???
    }

    /** */
    @Override public void close() {
        buffers.clear();

        buffers = null;
    }

    /** */
    private void switchToFile() throws IOException {
        File tempFile = File.createTempFile(TEMP_FILE_PREFIX, ".tmp");
        tempFile.deleteOnExit();

        tempFileHolder = new TempFileHolder(tempFile.toPath());

        tempFileCleaner = ((IgniteJdbcThinDriver)IgniteJdbcThinDriver.register())
                .getCleaner()
                .register(this, tempFileHolder);

        try (OutputStream diskOutputStream = Files.newOutputStream(tempFile.toPath())) {
            getInputStream().transferTo(diskOutputStream);
        }
        catch (RuntimeException | Error e) {
            tempFileCleaner.clean();

            throw e;
        }

        buffers.clear();

        tempFileChannel = FileChannel.open(tempFileHolder.getPath(), WRITE, READ);
    }

    private BufPosition getInMemoryBufferPosition(long pos) {
        BufPosition res = new BufPosition();

        res.idx = 0;

        for (long p = 0; p < totalCnt; ) {
            if (pos > p + buffers.get(res.idx).length - 1) {
                p += buffers.get(res.idx).length;
                res.idx++;
            }
            else {
                res.pos = (int)(pos - p);
                break;
            }
        }

        return res;
    }

    /** */
    private class BufPosition {
        /** The index of the current buffer. */
        public int idx;

        /** Current position in the current buffer. */
        public int pos;

        BufPosition() {
            idx = 0;

            pos = 0;
        }

        void set(BufPosition x)  {
            idx = x.idx;
            pos = x.pos;
        }

        void set(int idx, int pos) {
            this.idx = idx;
            this.pos = pos;
        }

        void advance() {
            pos++;

            if (pos == buffers.get(idx).length) {
                idx++;

                pos = 0;
            }
        }

        void advance(int step) {
            pos += step;

            if (pos == buffers.get(idx).length) {
                idx++;

                pos = 0;
            }
        }
    }

    /**
     *
     */
    private class BufferInputStream extends InputStream {
        /** Current position in the in-memory storage. */
        private BufPosition curBuf;

        /** Current stream position. */
        private long pos;

        /** Stream starting position. */
        private final long start;

        /** Stream length. */
        private final long len;

        /** Remembered buffer position at the moment the {@link BufferInputStream#mark} is called. */
        private BufPosition markedCurBuf = new BufPosition();

        /** Remembered pos at the moment the {@link BufferInputStream#mark} is called. */
        private Long markedPos;

        /**
         * @param start starting position.
         */
        private BufferInputStream(long start, long len) {
            this.start = pos = markedPos = start;

            this.len = len;

            if (tempFileHolder == null) {
                curBuf = getInMemoryBufferPosition(start);

                markedCurBuf.set(curBuf);
            }
        }

        /** {@inheritDoc} */
        @Override public int read() throws IOException {
            if (tempFileHolder == null)
                return readFromMemory();
            else
                return readFromFile();
        }

        /** */
        private int readFromMemory() {
            if (pos >= start + len || pos >= totalCnt)
                return -1;

            int res = buffers.get(curBuf.idx)[curBuf.pos] & 0xff;

            pos++;

            curBuf.advance();
//            curBuf.pos++;
//
//            if (pos < start + len) {
//                if (curBuf.pos == buffers.get(curBuf.idx).length) {
//                    curBuf.idx++;
//
//                    curBuf.pos = 0;
//                }
//            }

            return res;
        }

        /** */
        private int readFromFile() throws IOException {
            byte[] res = new byte[1];

            return tempFileChannel.read(ByteBuffer.wrap(res), pos);
        }

        /** {@inheritDoc} */
        @Override public int read(byte res[], int off, int cnt) throws IOException {
            if (tempFileHolder == null) {
                return readFromMemory(res, off, cnt);
            }
            else {
                return readFromFile(res, off, cnt);
            }
        }

        /** */
        private int readFromMemory(byte res[], int off, int cnt) {
            if (pos >= start + len || pos >= totalCnt)
                return -1;

            long availableBytes = Math.min(start + len, totalCnt) - pos;

            int size = cnt < availableBytes ? cnt : (int)availableBytes;

            int remaining = size;

            while (remaining > 0 && curBuf.idx < buffers.size()) {
                byte[] buf = buffers.get(curBuf.idx);

                int toCopy = Math.min(remaining, buf.length - curBuf.pos);

                U.arrayCopy(buf, Math.max(curBuf.pos, 0), res, off + (size - remaining), toCopy);

                remaining -= toCopy;

                pos += toCopy;
                curBuf.advance(toCopy);
//                curBuf.pos += toCopy;
//
//                if (curBuf.pos == buffers.get(curBuf.idx).length) {
//                    curBuf.pos = 0;
//
//                    curBuf.idx++;
//                }
            }

            return size;
        }

        /** */
        private int readFromFile(byte[] res, int off, int cnt) throws IOException {
            return tempFileChannel.read(ByteBuffer.wrap(res, off, cnt), pos);
        }

        /** {@inheritDoc} */
        @Override public boolean markSupported() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public synchronized void reset() {
            if (tempFileHolder == null)
                curBuf.set(markedCurBuf);

            pos = markedPos;
        }

        /** {@inheritDoc} */
        @Override public synchronized void mark(int readlimit) {
            if (tempFileHolder == null)
                markedCurBuf.set(curBuf);

            markedPos = pos;
        }
    }

    /** */
    private class BufferOutputStream extends OutputStream {
        /** Current position in the in-memory storage. */
        private BufPosition bufPos;

        /** Current stream position. */
        private long pos;

        /**
         * @param pos starting position.
         */
        private BufferOutputStream(long pos) {
            this.pos = pos;

            if (tempFileHolder == null)
                bufPos = getInMemoryBufferPosition(pos);
        }

        /** {@inheritDoc} */
        @Override public void write(int b) throws IOException {
            write(new byte[] {(byte)b}, 0, 1);
        }

        /** {@inheritDoc} */
        @Override public void write(byte[] bytes, int off, int len) throws IOException {
            if (Math.max(pos + len, totalCnt) > maxMemoryBufferBytes)
                switchToFile();

            if (tempFileHolder == null) {
                writeToMemory(bytes, off, len);
            }
            else {
                writeToTempFile(bytes, off, len);
            }

            totalCnt = Math.max(pos + len, totalCnt);

            pos += len;
        }

        /** */
        private void writeToMemory(byte[] bytes, int off, int len) {
            int remaining = len;

            for (; bufPos.idx < buffers.size(); bufPos.idx++) {
                byte[] buf = buffers.get(bufPos.idx);

                int toCopy = Math.min(remaining, buf.length - bufPos.pos);

                U.arrayCopy(bytes, off + len - remaining, buf, bufPos.pos, toCopy);

                remaining -= toCopy;

                if (remaining == 0) {
                    bufPos.pos += toCopy;

                    break;
                }
                else {
                    bufPos.pos = 0;
                }
            }

            if (remaining > 0) {
                addNewBuffer(remaining);

                U.arrayCopy(bytes, off + len - remaining, buffers.get(buffers.size() - 1), 0, remaining);

                bufPos.idx = buffers.size() - 1;
                bufPos.pos = remaining;
            }
        }

        /** */
        private void writeToTempFile(byte[] bytes, int off, int len) throws IOException {
            tempFileChannel.position(pos);

            tempFileChannel.write(ByteBuffer.wrap(bytes, off, len));
        }
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
        private final Path path;

        /**
         * @param path Full path to temp file.
         */
        public TempFileHolder(Path path) {
            this.path = path;
        }

        /**
         * @return Full path to temp file.
         */
        public Path getPath() {
            return path;
        }

        /** The cleaning action to be called by the {@link java.lang.ref.Cleaner}. */
        @Override public void run() {
            clean();
        }

        /** Cleans the temp file and input stream if it was created. */
        private void clean() {
            path.toFile().delete();
        }
    }
}

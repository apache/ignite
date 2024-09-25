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
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Memory buffer.
 */
public class JdbcDataBufferImpl implements JdbcDataBuffer {
    /** */
    private Storage storage;

    /** */
    private final Integer maxMemoryBufferBytes;

    /** The total count of bytes. */
    private long totalCnt;

    /** */
    public JdbcDataBufferImpl(int maxMemoryBufferBytes) {
        storage = new MemoryStorage();

        totalCnt = 0;

        this.maxMemoryBufferBytes = maxMemoryBufferBytes;
    }

    /** */
    public JdbcDataBufferImpl(int maxMemoryBufferBytes, byte[] arr) {
        storage = new MemoryStorage(arr);

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

    /** */
    @Override public void truncate(long len) throws IOException {
        totalCnt = len;

        storage.truncate(len);
    }

    /** */
    @Override public void close() {
        storage.close();
    }

    /** */
    private void switchToFileStorage() throws IOException {
        Storage newStorage = new FileStorage(getInputStream());

        storage.close();

        storage = newStorage;
    }

    private interface StoragePointerContext {
        StoragePointerContext copy();
    }

    /** */
    private static class StoragePointer {
        /** Current stream position. */
        private long pos;

        protected StoragePointerContext context;

        StoragePointer() {
            pos = 0;
        }

        StoragePointer(StoragePointer x) {
            set(x);
        }

        StoragePointer set(StoragePointer x)  {
            pos = x.pos;

            if (x.context != null)
                context = x.context.copy();

            return this;
        }

        StoragePointer setPos(long pos) {
            this.pos = pos;

            return this;
        }

        StoragePointer setContext(StoragePointerContext context) {
            this.context = context;

            return this;
        }

        public long getPos() {
            return pos;
        }
    }

    private interface Storage {
        StoragePointer createPointer();
        int read(StoragePointer pos) throws IOException;
        int read(StoragePointer pos, byte res[], int off, int cnt) throws IOException;
        void write(StoragePointer pos, int b) throws IOException;
        void write(StoragePointer pos, byte[] bytes, int off, int len) throws IOException;
        void advance(StoragePointer pos, long step);
        void truncate(long len) throws IOException;
        void close();
    }

    private static class MemoryStoragePointerContext implements StoragePointerContext {
        /** The index of the current buffer. */
        private int idx;

        /** Current position in the current buffer. */
        private int inBufPos;

        public MemoryStoragePointerContext(int idx, int inBufPos) {
            set(idx, inBufPos);
        }

        public void set(int idx, int inBufPos) {
            this.idx = idx;
            this.inBufPos = inBufPos;
        }

        @Override public StoragePointerContext copy() {
            return new MemoryStoragePointerContext(idx, inBufPos);
        }
    }

    private static class MemoryStorage implements Storage {
        /** The list of buffers. */
        private List<byte[]> buffers = new ArrayList<>();

        public MemoryStorage() {
            // No-op
        }

        public MemoryStorage(byte[] arr) {
            if (arr.length > 0)
                buffers.add(arr);
        }

        @Override public StoragePointer createPointer() {
            return new StoragePointer().setContext(new MemoryStoragePointerContext(0, 0));
        }

        @Override public int read(StoragePointer pos) {
            byte[] buf = getBuf(pos);

            if (buf == null)
                return -1;

            int res = buf[getBufPos(pos)] & 0xff;

            advance(pos, 1);

            return res;
        }

        @Override public int read(StoragePointer pos, byte[] res, int off, int cnt) {
            byte[] buf = getBuf(pos);

            if (buf == null)
                return -1;

            int remaining = cnt;

            while (remaining > 0 && buf!= null) {
                int toCopy = Math.min(remaining, buf.length - getBufPos(pos));

                U.arrayCopy(buf, getBufPos(pos), res, off + (cnt - remaining), toCopy);

                remaining -= toCopy;

                advance(pos, toCopy);
                buf = getBuf(pos);
            }

            return cnt;
        }

        @Override public void write(StoragePointer pos, int b) {
            if (getBuf(pos) == null)
                addNewBuffer(1);

            getBuf(pos)[getBufPos(pos)] = (byte)(b & 0xff);

            advance(pos, 1);
        }

        @Override public void write(StoragePointer pos, byte[] bytes, int off, int len) {
            int remaining = len;

            byte[] buf;

            while (remaining > 0 && (buf = getBuf(pos)) != null) {
                int toCopy = Math.min(remaining, buf.length - getBufPos(pos));

                U.arrayCopy(bytes, off + len - remaining, buf, getBufPos(pos), toCopy);

                remaining -= toCopy;

                advance(pos, toCopy);
            }

            if (remaining > 0) {
                addNewBuffer(remaining);

                U.arrayCopy(bytes, off + len - remaining, getBuf(pos), 0, remaining);

                advance(pos, remaining);
            }
        }

        @Override public void advance(StoragePointer pos, long step) {
            int inBufPos = getBufPos(pos);
            int idx = getBufIdx(pos);
            long remain = step;

            while (remain > 0) {
                if (remain >= buffers.get(idx).length - inBufPos) {
                    remain -= buffers.get(idx).length - inBufPos;

                    inBufPos = 0;

                    idx++;
                }
                else {
                    inBufPos += Math.toIntExact(remain);

                    remain = 0;
                }
            }

            pos.setPos(pos.getPos() + step);

            ((MemoryStoragePointerContext)pos.context).set(idx, inBufPos);
        }

        @Override public void truncate(long len) {
            StoragePointer pos = createPointer();

            advance(pos, len);

            if (buffers.size() > getBufIdx(pos) + 1)
                buffers.subList(getBufIdx(pos) + 1, buffers.size()).clear();
        }

        @Override public void close() {
            buffers.clear();
            buffers = null;
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

        private byte[] getBuf(StoragePointer pos) {
            return getBufIdx(pos) < buffers.size() ? buffers.get(getBufIdx(pos)) : null;
        }

        private int getBufPos(StoragePointer pos) {
            return ((MemoryStoragePointerContext)pos.context).inBufPos;
        }

        private int getBufIdx(StoragePointer pos) {
            return ((MemoryStoragePointerContext)pos.context).idx;
        }
    }

    private static class FileStorage implements Storage {
        /** */
        private static final String TEMP_FILE_PREFIX = "ignite-jdbc-temp-data";

        /** */
        private final TempFileHolder tempFileHolder;

        /** */
        private final Cleaner.Cleanable tempFileCleaner;

        /** */
        private final FileChannel tempFileChannel;

        /** Cleaner to remove temp files. */
        private static final Cleaner cleaner = Cleaner.create();

        FileStorage(InputStream memoryStorage) throws IOException {
            File tempFile = File.createTempFile(TEMP_FILE_PREFIX, ".tmp");
            tempFile.deleteOnExit();

            tempFileHolder = new TempFileHolder(tempFile.toPath());

            tempFileCleaner = cleaner.register(this, tempFileHolder);

            try (OutputStream diskOutputStream = Files.newOutputStream(tempFile.toPath())) {
                memoryStorage.transferTo(diskOutputStream);
            }
            catch (RuntimeException | Error e) {
                tempFileCleaner.clean();

                throw e;
            }

            tempFileChannel = FileChannel.open(tempFileHolder.getPath(), WRITE, READ);
        }

        @Override public StoragePointer createPointer() {
            return new StoragePointer();
        }

        @Override public int read(StoragePointer pos) throws IOException {
            byte[] res = new byte[1];

            int read = tempFileChannel.read(ByteBuffer.wrap(res), pos.getPos());

            if (read == -1) {
                return -1;
            }
            else {
                advance(pos, read);

                return res[0] & 0xff;
            }
        }

        @Override public int read(StoragePointer pos, byte[] res, int off, int cnt) throws IOException {
            int read = tempFileChannel.read(ByteBuffer.wrap(res, off, cnt), pos.getPos());

            if (read != -1)
                advance(pos, read);

            return read;
        }

        @Override public void write(StoragePointer pos, int b) throws IOException {
            write(pos, new byte[] {(byte)b}, 0, 1);
        }

        @Override public void write(StoragePointer pos, byte[] bytes, int off, int len) throws IOException {
            int written = tempFileChannel.write(ByteBuffer.wrap(bytes, off, len), pos.getPos());

            advance(pos, written);
        }

        @Override public void truncate(long len) throws IOException {
            tempFileChannel.truncate(len);
        }

        @Override public void close() {
            try {
                tempFileChannel.close();
            }
            catch (IOException ignore) {
                // No-op
            }

            tempFileCleaner.clean();
        }

        @Override public void advance(StoragePointer pos, long step) {
            pos.setPos(pos.getPos() + step);
        }
    }
    
    /**
     *
     */
    private class BufferInputStream extends InputStream {
        /** Current position in the storage. */
        private final StoragePointer curPointer;

        /** Stream starting position. */
        private final long start;

        /** Stream length. */
        private final long len;

        /** Remembered buffer position at the moment the {@link BufferInputStream#mark} is called. */
        private final StoragePointer markedPointer;

        /**
         * @param start starting position.
         */
        private BufferInputStream(long start, long len) {
            this.start = start;

            this.len = len;

            curPointer = storage.createPointer();

            if (start > 0)
                storage.advance(curPointer, start);

            markedPointer = new StoragePointer(curPointer);
        }

        /** {@inheritDoc} */
        @Override public int read() throws IOException {
            if (curPointer.getPos() >= start + len)
                return -1;

            return storage.read(curPointer);
        }

        /** {@inheritDoc} */
        @Override public int read(byte res[], int off, int cnt) throws IOException {
            if (curPointer.getPos() >= start + len || curPointer.getPos() >= totalCnt)
                return -1;

            long availableBytes = Math.min(start + len, totalCnt) - curPointer.getPos();

            int toRead = cnt < availableBytes ? cnt : (int)availableBytes;

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

    /** */
    private class BufferOutputStream extends OutputStream {
        /** Current position in the in-memory storage. */
        private final StoragePointer bufPos;

        /**
         * @param pos starting position.
         */
        private BufferOutputStream(long pos) {
            bufPos = storage.createPointer();

            if (pos > 0)
                storage.advance(bufPos, pos);
        }

        /** {@inheritDoc} */
        @Override public void write(int b) throws IOException {
            storage.write(bufPos, b);

            totalCnt = Math.max(bufPos.getPos(), totalCnt);
        }

        /** {@inheritDoc} */
        @Override public void write(byte[] bytes, int off, int len) throws IOException {
            if (Math.max(bufPos.getPos() + len, totalCnt) > maxMemoryBufferBytes)
                switchToFileStorage();

            storage.write(bufPos, bytes, off, len);

            totalCnt = Math.max(bufPos.getPos(), totalCnt);
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

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

package org.apache.ignite.internal.processors.hadoop.igfs;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.igfs.common.IgfsLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 * IGFS input stream wrapper for hadoop interfaces.
 */
@SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
public final class HadoopIgfsInputStream extends InputStream implements Seekable, PositionedReadable,
    HadoopIgfsStreamEventListener {
    /** Minimum buffer size. */
    private static final int MIN_BUF_SIZE = 4 * 1024;

    /** Server stream delegate. */
    private HadoopIgfsStreamDelegate delegate;

    /** Stream ID used by logger. */
    private long logStreamId;

    /** Stream position. */
    private long pos;

    /** Stream read limit. */
    private long limit;

    /** Mark position. */
    private long markPos = -1;

    /** Prefetch buffer. */
    private DoubleFetchBuffer buf = new DoubleFetchBuffer();

    /** Buffer half size for double-buffering. */
    private int bufHalfSize;

    /** Closed flag. */
    private volatile boolean closed;

    /** Flag set if stream was closed due to connection breakage. */
    private boolean connBroken;

    /** Logger. */
    private Log log;

    /** Client logger. */
    private IgfsLogger clientLog;

    /** Read time. */
    private long readTime;

    /** User time. */
    private long userTime;

    /** Last timestamp. */
    private long lastTs;

    /** Amount of read bytes. */
    private long total;

    /**
     * Creates input stream.
     *
     * @param delegate Server stream delegate.
     * @param limit Read limit.
     * @param bufSize Buffer size.
     * @param log Log.
     * @param clientLog Client logger.
     */
    public HadoopIgfsInputStream(HadoopIgfsStreamDelegate delegate, long limit, int bufSize, Log log,
        IgfsLogger clientLog, long logStreamId) {
        assert limit >= 0;

        this.delegate = delegate;
        this.limit = limit;
        this.log = log;
        this.clientLog = clientLog;
        this.logStreamId = logStreamId;

        bufHalfSize = Math.max(bufSize, MIN_BUF_SIZE);

        lastTs = System.nanoTime();

        delegate.hadoop().addEventListener(delegate, this);
    }

    /**
     * Read start.
     */
    private void readStart() {
        long now = System.nanoTime();

        userTime += now - lastTs;

        lastTs = now;
    }

    /**
     * Read end.
     */
    private void readEnd() {
        long now = System.nanoTime();

        readTime += now - lastTs;

        lastTs = now;
    }

    /** {@inheritDoc} */
    @Override public synchronized int read() throws IOException {
        checkClosed();

        readStart();

        try {
            if (eof())
                return -1;

            buf.refreshAhead(pos);

            int res = buf.atPosition(pos);

            pos++;
            total++;

            buf.refreshAhead(pos);

            return res;
        }
        catch (IgniteCheckedException e) {
            throw HadoopIgfsUtils.cast(e);
        }
        finally {
            readEnd();
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized int read(@NotNull byte[] b, int off, int len) throws IOException {
        checkClosed();

        if (eof())
            return -1;

        readStart();

        try {
            long remaining = limit - pos;

            int read = buf.flatten(b, pos, off, len);

            pos += read;
            total += read;
            remaining -= read;

            if (remaining > 0 && read != len) {
                int readAmt = (int)Math.min(remaining, len - read);

                delegate.hadoop().readData(delegate, pos, readAmt, b, off + read, len - read).get();

                read += readAmt;
                pos += readAmt;
                total += readAmt;
            }

            buf.refreshAhead(pos);

            return read;
        }
        catch (IgniteCheckedException e) {
            throw HadoopIgfsUtils.cast(e);
        }
        finally {
            readEnd();
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized long skip(long n) throws IOException {
        checkClosed();

        if (clientLog.isLogEnabled())
            clientLog.logSkip(logStreamId, n);

        long oldPos = pos;

        if (pos + n <= limit)
            pos += n;
        else
            pos = limit;

        buf.refreshAhead(pos);

        return pos - oldPos;
    }

    /** {@inheritDoc} */
    @Override public synchronized int available() throws IOException {
        checkClosed();

        int available = buf.available(pos);

        assert available >= 0;

        return available;
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() throws IOException {
        if (!closed) {
            readStart();

            if (log.isDebugEnabled())
                log.debug("Closing input stream: " + delegate);

            delegate.hadoop().closeStream(delegate);

            readEnd();

            if (clientLog.isLogEnabled())
                clientLog.logCloseIn(logStreamId, userTime, readTime, total);

            markClosed(false);

            if (log.isDebugEnabled())
                log.debug("Closed stream [delegate=" + delegate + ", readTime=" + readTime +
                    ", userTime=" + userTime + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void mark(int readLimit) {
        markPos = pos;

        if (clientLog.isLogEnabled())
            clientLog.logMark(logStreamId, readLimit);
    }

    /** {@inheritDoc} */
    @Override public synchronized void reset() throws IOException {
        checkClosed();

        if (clientLog.isLogEnabled())
            clientLog.logReset(logStreamId);

        if (markPos == -1)
            throw new IOException("Stream was not marked.");

        pos = markPos;

        buf.refreshAhead(pos);
    }

    /** {@inheritDoc} */
    @Override public boolean markSupported() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public synchronized int read(long position, byte[] buf, int off, int len) throws IOException {
        long remaining = limit - position;

        int read = (int)Math.min(len, remaining);

        // Return -1 at EOF.
        if (read == 0)
            return -1;

        readFully(position, buf, off, read);

        return read;
    }

    /** {@inheritDoc} */
    @Override public synchronized void readFully(long position, byte[] buf, int off, int len) throws IOException {
        long remaining = limit - position;

        checkClosed();

        if (len > remaining)
            throw new EOFException("End of stream reached before data was fully read.");

        readStart();

        try {
            int read = this.buf.flatten(buf, position, off, len);

            total += read;

            if (read != len) {
                int readAmt = len - read;

                delegate.hadoop().readData(delegate, position + read, readAmt, buf, off + read, readAmt).get();

                total += readAmt;
            }

            if (clientLog.isLogEnabled())
                clientLog.logRandomRead(logStreamId, position, len);
        }
        catch (IgniteCheckedException e) {
            throw HadoopIgfsUtils.cast(e);
        }
        finally {
            readEnd();
        }
    }

    /** {@inheritDoc} */
    @Override public void readFully(long position, byte[] buf) throws IOException {
        readFully(position, buf, 0, buf.length);
    }

    /** {@inheritDoc} */
    @Override public synchronized void seek(long pos) throws IOException {
        A.ensure(pos >= 0, "position must be non-negative");

        checkClosed();

        if (clientLog.isLogEnabled())
            clientLog.logSeek(logStreamId, pos);

        if (pos > limit)
            pos = limit;

        if (log.isDebugEnabled())
            log.debug("Seek to position [delegate=" + delegate + ", pos=" + pos + ", oldPos=" + this.pos + ']');

        this.pos = pos;

        buf.refreshAhead(pos);
    }

    /** {@inheritDoc} */
    @Override public synchronized long getPos() {
        return pos;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean seekToNewSource(long targetPos) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void onClose() {
        markClosed(true);
    }

    /** {@inheritDoc} */
    @Override public void onError(String errMsg) {
        // No-op.
    }

    /**
     * Marks stream as closed.
     *
     * @param connBroken {@code True} if connection with server was lost.
     */
    private void markClosed(boolean connBroken) {
        // It is ok to have race here.
        if (!closed) {
            closed = true;

            this.connBroken = connBroken;

            delegate.hadoop().removeEventListener(delegate);
        }
    }

    /**
     * @throws IOException If check failed.
     */
    private void checkClosed() throws IOException {
        if (closed) {
            if (connBroken)
                throw new IOException("Server connection was lost.");
            else
                throw new IOException("Stream is closed.");
        }
    }

    /**
     * @return {@code True} if end of stream reached.
     */
    private boolean eof() {
        return limit == pos;
    }

    /**
     * Asynchronous prefetch buffer.
     */
    private static class FetchBufferPart {
        /** Read future. */
        private IgniteInternalFuture<byte[]> readFut;

        /** Position of cached chunk in file. */
        private long pos;

        /** Prefetch length. Need to store as read future result might be not available yet. */
        private int len;

        /**
         * Creates fetch buffer part.
         *
         * @param readFut Read future for this buffer.
         * @param pos Read position.
         * @param len Chunk length.
         */
        private FetchBufferPart(IgniteInternalFuture<byte[]> readFut, long pos, int len) {
            this.readFut = readFut;
            this.pos = pos;
            this.len = len;
        }

        /**
         * Copies cached data if specified position matches cached region.
         *
         * @param dst Destination buffer.
         * @param pos Read position in file.
         * @param dstOff Offset in destination buffer from which start writing.
         * @param len Maximum number of bytes to copy.
         * @return Number of bytes copied.
         * @throws IgniteCheckedException If read future failed.
         */
        public int flatten(byte[] dst, long pos, int dstOff, int len) throws IgniteCheckedException {
            // If read start position is within cached boundaries.
            if (contains(pos)) {
                byte[] data = readFut.get();

                int srcPos = (int)(pos - this.pos);
                int cpLen = Math.min(len, data.length - srcPos);

                U.arrayCopy(data, srcPos, dst, dstOff, cpLen);

                return cpLen;
            }

            return 0;
        }

        /**
         * @return {@code True} if data is ready to be read.
         */
        public boolean ready() {
            return readFut.isDone();
        }

        /**
         * Checks if current buffer part contains given position.
         *
         * @param pos Position to check.
         * @return {@code True} if position matches buffer region.
         */
        public boolean contains(long pos) {
            return this.pos <= pos && this.pos + len > pos;
        }
    }

    private class DoubleFetchBuffer {
        /**  */
        private FetchBufferPart first;

        /** */
        private FetchBufferPart second;

        /**
         * Copies fetched data from both buffers to destination array if cached region matched read position.
         *
         * @param dst Destination buffer.
         * @param pos Read position in file.
         * @param dstOff Destination buffer offset.
         * @param len Maximum number of bytes to copy.
         * @return Number of bytes copied.
         * @throws IgniteCheckedException If any read operation failed.
         */
        public int flatten(byte[] dst, long pos, int dstOff, int len) throws IgniteCheckedException {
            assert dstOff >= 0;
            assert dstOff + len <= dst.length : "Invalid indices [dst.length=" + dst.length + ", dstOff=" + dstOff +
                ", len=" + len + ']';

            int bytesCopied = 0;

            if (first != null) {
                bytesCopied += first.flatten(dst, pos, dstOff, len);

                if (bytesCopied != len && second != null) {
                    assert second.pos == first.pos + first.len;

                    bytesCopied += second.flatten(dst, pos + bytesCopied, dstOff + bytesCopied, len - bytesCopied);
                }
            }

            return bytesCopied;
        }

        /**
         * Gets byte at specified position in buffer.
         *
         * @param pos Stream position.
         * @return Read byte.
         * @throws IgniteCheckedException If read failed.
         */
        public int atPosition(long pos) throws IgniteCheckedException {
            // Should not reach here if stream contains no data.
            assert first != null;

            if (first.contains(pos)) {
                byte[] bytes = first.readFut.get();

                return bytes[((int)(pos - first.pos))] & 0xFF;
            }
            else {
                assert second != null;
                assert second.contains(pos);

                byte[] bytes = second.readFut.get();

                return bytes[((int)(pos - second.pos))] & 0xFF;
            }
        }

        /**
         * Starts asynchronous buffer refresh if needed, depending on current position.
         *
         * @param pos Current stream position.
         */
        public void refreshAhead(long pos) {
            if (fullPrefetch(pos)) {
                first = fetch(pos, bufHalfSize);
                second = fetch(pos + bufHalfSize, bufHalfSize);
            }
            else if (needFlip(pos)) {
                first = second;

                second = fetch(first.pos + first.len, bufHalfSize);
            }
        }

        /**
         * @param pos Position from which read is expected.
         * @return Number of bytes available to be read without blocking.
         */
        public int available(long pos) {
            int available = 0;

            if (first != null) {
                if (first.contains(pos)) {
                    if (first.ready()) {
                        available += (pos - first.pos);

                        if (second != null && second.ready())
                            available += second.len;
                    }
                }
                else {
                    if (second != null && second.contains(pos) && second.ready())
                        available += (pos - second.pos);
                }
            }

            return available;
        }

        /**
         * Checks if position shifted enough to forget previous buffer.
         *
         * @param pos Current position.
         * @return {@code True} if need flip buffers.
         */
        private boolean needFlip(long pos) {
            // Return true if we read more then half of second buffer.
            return second != null && second.contains(pos);
        }

        /**
         * Determines if all cached bytes should be discarded and new region should be
         * prefetched.
         *
         * @param curPos Current stream position.
         * @return {@code True} if need to refresh both blocks.
         */
        private boolean fullPrefetch(long curPos) {
            // If no data was prefetched yet, return true.
            return first == null || curPos < first.pos || (second != null && curPos >= second.pos + second.len);
        }

        /**
         * Starts asynchronous fetch for given region.
         *
         * @param pos Position to read from.
         * @param size Number of bytes to read.
         * @return Fetch buffer part.
         */
        private FetchBufferPart fetch(long pos, int size) {
            long remaining = limit - pos;

            size = (int)Math.min(size, remaining);

            return size <= 0 ? null :
                new FetchBufferPart(delegate.hadoop().readData(delegate, pos, size, null, 0, 0), pos, size);
        }
    }
}
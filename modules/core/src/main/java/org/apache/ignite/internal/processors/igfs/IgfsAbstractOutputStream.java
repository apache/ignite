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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.events.IgfsEvent;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.ignite.events.EventType.EVT_IGFS_FILE_CLOSED_WRITE;

/**
 * Output stream to store data into grid cache with separate blocks.
 */
abstract class IgfsAbstractOutputStream extends IgfsOutputStream {
    /** IGFS context. */
    protected final IgfsContext igfsCtx;

    /** Path to file. */
    protected final IgfsPath path;

    /** Buffer size. */
    protected final int bufSize;

    /** File worker batch. */
    protected final IgfsFileWorkerBatch batch;

    /** Mutex for synchronization. */
    protected final Object mux = new Object();

    /** Flag for this stream open/closed state. */
    protected boolean closed;

    /** Local buffer to store stream data as consistent block. */
    protected ByteBuffer buf;

    /** Bytes written. */
    protected long bytes;

    /** Time consumed by write operations. */
    protected long time;

    /**
     * Constructs file output stream.
     *
     * @param igfsCtx IGFS context.
     * @param path Path to stored file.
     * @param bufSize The size of the buffer to be used.
     * @param batch Optional secondary file system batch.
     */
    IgfsAbstractOutputStream(IgfsContext igfsCtx, IgfsPath path, int bufSize, @Nullable IgfsFileWorkerBatch batch) {
        synchronized (mux) {
            this.path = path;
            this.bufSize = optimizeBufferSize(bufSize);
            this.igfsCtx = igfsCtx;
            this.batch = batch;
        }

        igfsCtx.metrics().incrementFilesOpenedForWrite();
    }

    /**
     * Optimize buffer size.
     *
     * @param bufSize Original byffer size.
     * @return Optimized buffer size.
     */
    protected abstract int optimizeBufferSize(int bufSize);

    /** {@inheritDoc} */
    @Override public void write(int b) throws IOException {
        synchronized (mux) {
            checkClosed(null, 0);

            b &= 0xFF;

            long startTime = System.nanoTime();

            if (buf == null)
                buf = allocateNewBuffer();

            buf.put((byte)b);

            sendBufferIfFull();

            time += System.nanoTime() - startTime;
        }
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] b, int off, int len) throws IOException {
        A.notNull(b, "b");

        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException("Invalid bounds [data.length=" + b.length + ", offset=" + off +
                ", length=" + len + ']');
        }

        synchronized (mux) {
            checkClosed(null, 0);

            // Check if there is anything to write.
            if (len == 0)
                return;

            long startTime = System.nanoTime();

            if (buf == null) {
                if (len >= bufSize) {
                    // Send data right away.
                    ByteBuffer tmpBuf = ByteBuffer.wrap(b, off, len);

                    send(tmpBuf, tmpBuf.remaining());
                }
                else {
                    buf = allocateNewBuffer();

                    buf.put(b, off, len);
                }
            }
            else {
                // Re-allocate buffer if needed.
                if (buf.remaining() < len)
                    buf = ByteBuffer.allocate(buf.position() + len).put((ByteBuffer)buf.flip());

                buf.put(b, off, len);

                sendBufferIfFull();
            }

            time += System.nanoTime() - startTime;
        }
    }

    /** {@inheritDoc} */
    @Override public void transferFrom(DataInput in, int len) throws IOException {
        synchronized (mux) {
            checkClosed(in, len);

            long startTime = System.nanoTime();

            // Clean-up local buffer before streaming.
            sendBufferIfNotEmpty();

            // Perform transfer.
            send(in, len);

            time += System.nanoTime() - startTime;
        }
    }

    /**
     * Validate this stream is open.
     *
     * @param in Data input.
     * @param len Data len in bytes.
     * @throws IOException If this stream is closed.
     */
    protected void checkClosed(@Nullable DataInput in, int len) throws IOException {
        assert Thread.holdsLock(mux);

        if (closed) {
            // Must read data from stream before throwing exception.
            if (in != null)
                in.skipBytes(len);

            throw new IOException("Stream has been closed: " + this);
        }
    }

    /**
     * Send local buffer if it full.
     *
     * @throws IOException If failed.
     */
    private void sendBufferIfFull() throws IOException {
        if (buf.position() >= bufSize)
            sendBuffer();
    }

    /**
     * Send local buffer if at least something is stored there.
     *
     * @throws IOException If failed.
     */
    void sendBufferIfNotEmpty() throws IOException {
        if (buf != null && buf.position() > 0)
            sendBuffer();
    }

    /**
     * Send all local-buffered data to server.
     *
     * @throws IOException In case of IO exception.
     */
    private void sendBuffer() throws IOException {
        buf.flip();

        send(buf, buf.remaining());

        buf = null;
    }

    /**
     * Store data block.
     *
     * @param data Block.
     * @param writeLen Write length.
     * @throws IOException If failed.
     */
    protected abstract void send(Object data, int writeLen) throws IOException;

    /**
     * Allocate new buffer.
     *
     * @return New buffer.
     */
    private ByteBuffer allocateNewBuffer() {
        return ByteBuffer.allocate(bufSize);
    }

    /**
     * Updates IGFS metrics when the stream is closed.
     */
    protected void updateMetricsOnClose() {
        IgfsLocalMetrics metrics = igfsCtx.metrics();

        metrics.addWrittenBytesTime(bytes, time);
        metrics.decrementFilesOpenedForWrite();

        GridEventStorageManager evts = igfsCtx.kernalContext().event();

        if (evts.isRecordable(EVT_IGFS_FILE_CLOSED_WRITE))
            evts.record(new IgfsEvent(path, igfsCtx.localNode(),
                EVT_IGFS_FILE_CLOSED_WRITE, bytes));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsAbstractOutputStream.class, this);
    }

}

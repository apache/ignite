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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.events.IgfsEvent;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

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
    private long bytes;

    /** Time consumed by write operations. */
    protected long time;

    /** Space in file to write data. */
    protected long space;

    /** Data length in remainder. */
    protected int remainderDataLen;

    /** Intermediate remainder to keep data. */
    private byte[] remainder;

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

    /**
     * @return Length of file.
     */
    protected abstract long length();

    /**
     * @return Block size for send.
     */
    protected abstract int sendBlockSize();

    /**
     * Stores data blocks read from ByteBuffer.
     *
     * @param reservedLen Reserved length.
     * @param remainder Remainder.
     * @param remainderLen Remainder length.
     * @param src Source to read bytes.
     * @param srcLen Data length to read from source.
     * @param flush Flush flag.
     * @param batch Optional secondary file system worker batch.
     * @return Data remainder if {@code flush} flag is {@code false}.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract byte[] storeDataBlocks(long reservedLen, byte[] remainder, int remainderLen, ByteBuffer src,
        int srcLen, boolean flush,
        IgfsFileWorkerBatch batch) throws IgniteCheckedException;

    /**
     * Stores data blocks read from DataInput.
     *
     * @param reservedLen Reserved length.
     * @param remainder Remainder.
     * @param remainderLen Remainder length.
     * @param src Source to read bytes.
     * @param srcLen Data length to read from source.
     * @param flush Flush flag.
     * @param batch Optional secondary file system worker batch.
     * @return Data remainder if {@code flush} flag is {@code false}.
     * @throws IgniteCheckedException If failed.
     * @throws IOException If failed.
     */
    protected abstract byte[] storeDataBlocks(long reservedLen, byte[] remainder, int remainderLen, DataInput src,
        int srcLen, boolean flush,
        IgfsFileWorkerBatch batch) throws IgniteCheckedException, IOException;

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
    @SuppressWarnings("NullableProblems")
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
     * Flush remainder.
     *
     * @throws IOException If failed.
     */
    protected void flushRemainder() throws IOException {
        try {
            if (remainder != null) {

                remainder = storeDataBlocks(length() + space, null,
                    0, ByteBuffer.wrap(remainder, 0, remainderDataLen), remainderDataLen, true, batch);

                remainder = null;
                remainderDataLen = 0;
            }
        }
        catch (IgniteCheckedException e) {
            throw new IOException("Failed to flush data (remainder) [path=" + path + ", space=" + space + ']', e);
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
    private void send(Object data, int writeLen) throws IOException {
        assert Thread.holdsLock(mux);
        assert data instanceof ByteBuffer || data instanceof DataInput;

        try {
            // Increment metrics.
            bytes += writeLen;
            space += writeLen;

            int blockSize = sendBlockSize();

            // If data length is not enough to fill full block, fill the remainder and return.
            if (remainderDataLen + writeLen < blockSize) {
                if (remainder == null)
                    remainder = new byte[blockSize];
                else if (remainder.length != blockSize) {
                    assert remainderDataLen == remainder.length;

                    byte[] allocated = new byte[blockSize];

                    U.arrayCopy(remainder, 0, allocated, 0, remainder.length);

                    remainder = allocated;
                }

                if (data instanceof ByteBuffer)
                    ((ByteBuffer)data).get(remainder, remainderDataLen, writeLen);
                else
                    ((DataInput)data).readFully(remainder, remainderDataLen, writeLen);

                remainderDataLen += writeLen;
            }
            else {
                if (data instanceof ByteBuffer) {
                    remainder = storeDataBlocks(length() + space, remainder,
                        remainderDataLen, (ByteBuffer)data, ((ByteBuffer)data).remaining(), false, batch);
                }
                else {
                    remainder = storeDataBlocks(length() + space, remainder,
                        remainderDataLen, (DataInput)data, writeLen, false, batch);
                }

                remainderDataLen = remainder == null ? 0 : remainder.length;
            }
        }
        catch (IgniteCheckedException e) {
            throw new IOException("Failed to store data into file: " + path, e);
        }
    }

    /**
     * Allocate new buffer.
     *
     * @return New buffer.
     */
    private ByteBuffer allocateNewBuffer() {
        return ByteBuffer.allocate(bufSize);
    }

    /**
     *
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
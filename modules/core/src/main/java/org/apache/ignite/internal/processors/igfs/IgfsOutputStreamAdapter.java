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
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Output stream to store data into grid cache with separate blocks.
 */
@SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
abstract class IgfsOutputStreamAdapter extends IgfsOutputStream {
    /** Path to file. */
    protected final IgfsPath path;

    /** Buffer size. */
    private final int bufSize;

    /** Flag for this stream open/closed state. */
    private boolean closed;

    /** Local buffer to store stream data as consistent block. */
    private ByteBuffer buf;

    /** Bytes written. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    protected long bytes;

    /** Time consumed by write operations. */
    protected long time;

    /**
     * Constructs file output stream.
     *
     * @param path Path to stored file.
     * @param bufSize The size of the buffer to be used.
     */
    IgfsOutputStreamAdapter(IgfsPath path, int bufSize) {
        assert path != null;
        assert bufSize > 0;

        this.path = path;
        this.bufSize = bufSize;
    }

    /**
     * Gets number of written bytes.
     *
     * @return Written bytes.
     */
    public long bytes() {
        return bytes;
    }

    /** {@inheritDoc} */
    @Override public synchronized void write(int b) throws IOException {
        checkClosed(null, 0);

        long startTime = System.nanoTime();

        b &= 0xFF;

        if (buf == null)
            buf = ByteBuffer.allocate(bufSize);

        buf.put((byte)b);

        if (buf.position() >= bufSize)
            sendData(true); // Send data to server.

        time += System.nanoTime() - startTime;
    }

    /** {@inheritDoc} */
    @Override public synchronized void write(byte[] b, int off, int len) throws IOException {
        A.notNull(b, "b");

        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException("Invalid bounds [data.length=" + b.length + ", offset=" + off +
                ", length=" + len + ']');
        }

        checkClosed(null, 0);

        if (len == 0)
            return; // Done.

        long startTime = System.nanoTime();

        if (buf == null) {
            // Do not allocate and copy byte buffer if will send data immediately.
            if (len >= bufSize) {
                buf = ByteBuffer.wrap(b, off, len);

                sendData(false);

                return;
            }

            buf = ByteBuffer.allocate(Math.max(bufSize, len));
        }

        if (buf.remaining() < len)
            // Expand buffer capacity, if remaining size is less then data size.
            buf = ByteBuffer.allocate(buf.position() + len).put((ByteBuffer)buf.flip());

        assert len <= buf.remaining() : "Expects write data size less or equal then remaining buffer capacity " +
            "[len=" + len + ", buf.remaining=" + buf.remaining() + ']';

        buf.put(b, off, len);

        if (buf.position() >= bufSize)
            sendData(true); // Send data to server.

        time += System.nanoTime() - startTime;
    }

    /** {@inheritDoc} */
    @Override public synchronized void transferFrom(DataInput in, int len) throws IOException {
        checkClosed(in, len);

        long startTime = System.nanoTime();

        // Send all IPC data from the local buffer before streaming.
        if (buf != null && buf.position() > 0)
            sendData(true);

        try {
            storeDataBlocks(in, len);
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e.getMessage(), e);
        }

        time += System.nanoTime() - startTime;
    }

    /**
     * Flushes this output stream and forces any buffered output bytes to be written out.
     *
     * @exception IOException  if an I/O error occurs.
     */
    @Override public synchronized void flush() throws IOException {
        checkClosed(null, 0);

        // Send all IPC data from the local buffer.
        if (buf != null && buf.position() > 0)
            sendData(true);
    }

    /** {@inheritDoc} */
    @Override public final synchronized void close() throws IOException {
        // Do nothing if stream is already closed.
        if (closed)
            return;

        try {
            // Send all IPC data from the local buffer.
            try {
                flush();
            }
            finally {
                onClose(); // "onClose()" routine must be invoked anyway!
            }
        }
        finally {
            // Mark this stream closed AFTER flush.
            closed = true;
        }
    }

    /**
     * Store data blocks in file.<br/>
     * Note! If file concurrently deleted we'll get lost blocks.
     *
     * @param data Data to store.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract void storeDataBlock(ByteBuffer data) throws IgniteCheckedException, IOException;

    /**
     * Store data blocks in file reading appropriate number of bytes from given data input.
     *
     * @param in Data input to read from.
     * @param len Data length to store.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract void storeDataBlocks(DataInput in, int len) throws IgniteCheckedException, IOException;

    /**
     * Close callback. It will be called only once in synchronized section.
     *
     * @throws IOException If failed.
     */
    protected void onClose() throws IOException {
        // No-op.
    }

    /**
     * Validate this stream is open.
     *
     * @throws IOException If this stream is closed.
     */
    private void checkClosed(@Nullable DataInput in, int len) throws IOException {
        assert Thread.holdsLock(this);

        if (closed) {
            // Must read data from stream before throwing exception.
            if (in != null)
                in.skipBytes(len);

            throw new IOException("Stream has been closed: " + this);
        }
    }

    /**
     * Send all local-buffered data to server.
     *
     * @param flip Whether to flip buffer on sending data. We do not want to flip it if sending wrapped
     *      byte array.
     * @throws IOException In case of IO exception.
     */
    private void sendData(boolean flip) throws IOException {
        assert Thread.holdsLock(this);

        try {
            if (flip)
                buf.flip();

            storeDataBlock(buf);
        }
        catch (IgniteCheckedException e) {
            throw new IOException("Failed to store data into file: " + path, e);
        }

        buf = null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsOutputStreamAdapter.class, this);
    }
}
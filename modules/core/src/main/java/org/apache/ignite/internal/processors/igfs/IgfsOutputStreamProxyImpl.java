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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Output stream to store data into grid cache with separate blocks.
 */
class IgfsOutputStreamProxyImpl extends IgfsAbstractOutputStream {
    /** File info. */
    private IgfsFile info;

    /**
     * Constructs file output stream.
     *
     * @param igfsCtx IGFS context.
     * @param path Path to stored file.
     * @param info File info.
     * @param bufSize The size of the buffer to be used.
     * @param batch Optional secondary file system batch.
     */
    IgfsOutputStreamProxyImpl(IgfsContext igfsCtx, IgfsPath path, IgfsFile info, int bufSize,
        @Nullable IgfsFileWorkerBatch batch) {
        super(igfsCtx, path, bufSize, batch);

        assert batch != null;

        this.info = info;
    }

    /** {@inheritDoc} */
    @Override protected int optimizeBufferSize(int bufSize) {
        assert bufSize > 0;

        return bufSize;
    }

    /**
     * Flushes this output stream and forces any buffered output bytes to be written out.
     *
     * @throws IOException if an I/O error occurs.
     */
    @Override public void flush() throws IOException {
        synchronized (mux) {
            checkClosed(null, 0);

            sendBufferIfNotEmpty();
        }
    }

    /** {@inheritDoc} */
    @Override public final void close() throws IOException {
        synchronized (mux) {
            // Do nothing if stream is already closed.
            if (closed)
                return;

            // Set closed flag immediately.
            closed = true;

            // Flush data.
            IOException err = null;

            try {
                sendBufferIfNotEmpty();
            }
            catch (Exception e) {
                err = new IOException("Failed to flush data during stream close [path=" + path +
                    ", fileInfo=" + info + ']', e);
            }

            // Finish batch before file unlocking to support the assertion that unlocked file batch,
            // if any, must be in finishing state (e.g. append see more IgfsImpl.newBatch)
            batch.finish();

            // Finally, await secondary file system flush.
            try {
                batch.await();
            }
            catch (IgniteCheckedException e) {
                if (err == null)
                    err = new IOException("Failed to close secondary file system stream [path=" + path +
                        ", fileInfo=" + info + ']', e);
                else
                    err.addSuppressed(e);
            }

            // Throw error, if any.
            if (err != null)
                throw err;

            updateMetricsOnClose();
        }
    }

    /** {@inheritDoc} */
    @Override protected void send(Object data, int writeLen) throws IOException {
        assert Thread.holdsLock(mux);
        assert data instanceof ByteBuffer || data instanceof DataInput;

        try {
            // Increment metrics.
            bytes += writeLen;

            byte [] dataBuf = new byte[writeLen];

            if (data instanceof ByteBuffer) {
                ByteBuffer byteBuf = (ByteBuffer)data;

                byteBuf.get(dataBuf);
            }
            else {
                DataInput dataIn = (DataInput)data;

                try {
                    dataIn.readFully(dataBuf);
                }
                catch (IOException e) {
                    throw new IgniteCheckedException(e);
                }
            }

            if (!batch.write(dataBuf))
                throw new IgniteCheckedException("Cannot write more data to the secondary file system output " +
                    "stream because it was marked as closed: " + batch.path());
            else
                igfsCtx.metrics().addWriteBlocks(1, 1);

        }
        catch (IgniteCheckedException e) {
            throw new IOException("Failed to store data into file: " + path, e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsOutputStreamProxyImpl.class, this);
    }
}

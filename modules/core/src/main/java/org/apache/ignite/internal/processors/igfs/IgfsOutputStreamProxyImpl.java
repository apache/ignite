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
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsFile;
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
class IgfsOutputStreamProxyImpl extends IgfsAbstractOutputStream {
    /** Data input blocks writer. */
    DataInputBlocksWriter dataInputBlocksWriter = new DataInputBlocksWriter();

    /** Byte buffer blocks writer. */
    ByteBufferBlocksWriter byteBufferBlocksWriter = new ByteBufferBlocksWriter();

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

        this.info = info;
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

                flushRemainder();
            }
            catch (Exception e) {
                err = new IOException("Failed to flush data during stream close [path=" + path +
                    ", fileInfo=" + info + ']', e);
            }

            // Finish batch before file unlocking to support the assertion that unlocked file batch,
            // if any, must be in finishing state (e.g. append see more IgfsImpl.newBatch)
            if (batch != null)
                batch.finish();


            // Finally, await secondary file system flush.
            if (batch != null) {
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
            }

            // Throw error, if any.
            if (err != null)
                throw err;

            updateMetricsOnClose();
        }
    }

    /** {@inheritDoc} */
    @Override protected int sendBlockSize() {
        return bufSize;
    }

    @Override
    protected byte[] storeDataBlocks(long reservedLen, byte[] remainder, int remainderLen, ByteBuffer src, int srcLen,
        boolean flush, IgfsFileWorkerBatch batch) throws IgniteCheckedException {
        return byteBufferBlocksWriter.storeDataBlocks(reservedLen, remainder, remainderLen, src, srcLen, flush, batch);
    }

    @Override
    protected byte[] storeDataBlocks(long reservedLen, byte[] remainder, int remainderLen, DataInput src, int srcLen,
        boolean flush, IgfsFileWorkerBatch batch) throws IgniteCheckedException {
        return dataInputBlocksWriter.storeDataBlocks(reservedLen, remainder, remainderLen, src, srcLen, flush, batch);
    }

    /** {@inheritDoc} */
    @Override protected int optimizeBufferSize(int bufSize) {
        assert bufSize > 0;

        return bufSize;
    }

    @Override protected long length() {
        return info.length();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsOutputStreamProxyImpl.class, this);
    }

    /**
     * @param <T> Data source type.
     */
    private abstract class BlocksWriter<T> {
        /**
         * Stores data blocks read from abstracted source.
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
        @SuppressWarnings("ConstantConditions")
        @Nullable public byte[] storeDataBlocks(
            long reservedLen,
            @Nullable byte[] remainder,
            final int remainderLen,
            T src,
            int srcLen,
            boolean flush,
            @Nullable IgfsFileWorkerBatch batch
        ) throws IgniteCheckedException {
            A.notNull(batch, "Secondary writer batch");

            int blockSize = bufSize;

            int len = remainderLen + srcLen;

            if (len > reservedLen)
                throw new IgfsException("Not enough space reserved to store data [file=" + info +
                    ", reservedLen=" + reservedLen + ", remainderLen=" + remainderLen +
                    ", data.length=" + srcLen + ']');

            long start = reservedLen - len;
            long first = start / blockSize;
            long limit = (start + len + blockSize - 1) / blockSize;
            int written = 0;
            int remainderOff = 0;

            for (long block = first; block < limit; block++) {
                final long blockStartOff = block == first ? (start % blockSize) : 0;
                final long blockEndOff = block == (limit - 1) ? (start + len - 1) % blockSize : (blockSize - 1);

                final long size = blockEndOff - blockStartOff + 1;

                assert size > 0 && size <= blockSize;
                assert blockStartOff + size <= blockSize;

                final byte[] portion = new byte[(int)size];

                // Data length to copy from remainder.
                int portionOff = Math.min((int)size, remainderLen - remainderOff);

                if (remainderOff != remainderLen) {
                    U.arrayCopy(remainder, remainderOff, portion, 0, portionOff);

                    remainderOff += portionOff;
                }

                if (portionOff < size)
                    readData(src, portion, portionOff);

                if (!batch.write(portion))
                    throw new IgniteCheckedException("Cannot write more data to the secondary file system output " +
                        "stream because it was marked as closed: " + batch.path());

                igfsCtx.igfs().localMetrics().addWriteBlocks(0, 1);

                written += portion.length;
            }

            assert written == len;

            return null;
        }

        /**
         * Fully reads data from specified source into the specified byte array.
         *
         * @param src Data source.
         * @param dst Destination.
         * @param dstOff Destination buffer offset.
         * @throws IgniteCheckedException If read failed.
         */
        protected abstract void readData(T src, byte[] dst, int dstOff) throws IgniteCheckedException;

    }

    /**
     * Byte buffer writer.
     */
    private class ByteBufferBlocksWriter extends BlocksWriter<ByteBuffer> {
        /** {@inheritDoc} */
        @Override protected void readData(ByteBuffer src, byte[] dst, int dstOff) {
            src.get(dst, dstOff, dst.length - dstOff);
        }
    }

    /**
     * Data input writer.
     */
    private class DataInputBlocksWriter extends BlocksWriter<DataInput> {
        /** {@inheritDoc} */
        @Override protected void readData(DataInput src, byte[] dst, int dstOff)
            throws IgniteCheckedException {
            try {
                src.readFully(dst, dstOff, dst.length - dstOff);
            }
            catch (IOException e) {
                throw new IgniteCheckedException(e);
            }
        }
    }

}
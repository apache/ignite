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
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathNotFoundException;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.igfs.IgfsMode.PROXY;

/**
 * Output stream to store data into grid cache with separate blocks.
 */
class IgfsOutputStreamImpl extends IgfsOutputStreamAdapter {
    /** Maximum number of blocks in buffer. */
    private static final int MAX_BLOCKS_CNT = 16;

    /** IGFS context. */
    private IgfsContext igfsCtx;

    /** Meta info manager. */
    private final IgfsMetaManager meta;

    /** Data manager. */
    private final IgfsDataManager data;

    /** File descriptor. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private IgfsEntryInfo fileInfo;

    /** Space in file to write data. How many bytes are waiting to be written since last flush. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private long space;

    /** Intermediate remainder to keep data. */
    private byte[] remainder;

    /** Data length in remainder. */
    private int remainderDataLen;

    /** "Aggregated" write completion future. */
    private GridCompoundFuture<Boolean, Boolean> aggregateFut;

    /** IGFS mode. */
    private final IgfsMode mode;

    /** File worker batch. */
    private final IgfsFileWorkerBatch batch;

    /** Ensures that onClose)_ routine is called no more than once. */
    private final AtomicBoolean onCloseGuard = new AtomicBoolean();

    /** Local IGFS metrics. */
    private final IgfsLocalMetrics metrics;

    /** Affinity written by this output stream. */
    private IgfsFileAffinityRange streamRange;

    /**
     * Constructs file output stream.
     *
     * @param igfsCtx IGFS context.
     * @param path Path to stored file.
     * @param fileInfo File info to write binary data to.
     * @param bufSize The size of the buffer to be used.
     * @param mode Grid IGFS mode.
     * @param batch Optional secondary file system batch.
     * @param metrics Local IGFS metrics.
     */
    IgfsOutputStreamImpl(IgfsContext igfsCtx, IgfsPath path, IgfsEntryInfo fileInfo, int bufSize, IgfsMode mode,
        @Nullable IgfsFileWorkerBatch batch, IgfsLocalMetrics metrics) {
        super(path, optimizeBufferSize(bufSize, fileInfo));

        assert fileInfo != null;
        assert fileInfo.isFile() : "Unexpected file info: " + fileInfo;
        assert mode != null && mode != PROXY;
        assert mode == PRIMARY && batch == null || batch != null;
        assert metrics != null;

        // File hasn't been locked.
        if (fileInfo.lockId() == null)
            throw new IgfsException("Failed to acquire file lock (concurrently modified?): " + path);

        assert !IgfsUtils.DELETE_LOCK_ID.equals(fileInfo.lockId());

        this.igfsCtx = igfsCtx;
        meta = igfsCtx.meta();
        data = igfsCtx.data();

        this.fileInfo = fileInfo;
        this.mode = mode;
        this.batch = batch;
        this.metrics = metrics;

        streamRange = initialStreamRange(fileInfo);
    }

    /**
     * Optimize buffer size.
     *
     * @param bufSize Requested buffer size.
     * @param fileInfo File info.
     * @return Optimized buffer size.
     */
    @SuppressWarnings("IfMayBeConditional")
    private static int optimizeBufferSize(int bufSize, IgfsEntryInfo fileInfo) {
        assert bufSize > 0;

        if (fileInfo == null)
            return bufSize;

        int blockSize = fileInfo.blockSize();

        if (blockSize <= 0)
            return bufSize;

        if (bufSize <= blockSize)
            // Optimize minimum buffer size to be equal file's block size.
            return blockSize;

        int maxBufSize = blockSize * MAX_BLOCKS_CNT;

        if (bufSize > maxBufSize)
            // There is no profit or optimization from larger buffers.
            return maxBufSize;

        if (fileInfo.length() == 0)
            // Make buffer size multiple of block size (optimized for new files).
            return bufSize / blockSize * blockSize;

        return bufSize;
    }

    /** {@inheritDoc} */
    @Override protected final synchronized void storeDataBlock(ByteBuffer blockByteBuf) throws IgniteCheckedException, IOException {
        storeDataBlock0(blockByteBuf, IgfsDataManager.byteBufReader, blockByteBuf.remaining());
    }

    /** {@inheritDoc} */
    @Override protected final synchronized void storeDataBlocks(DataInput in, int len) throws IgniteCheckedException, IOException {
        storeDataBlock0(in, IgfsDataManager.dataInputReader, len);
    }

    /**
     * Implementation of data block storing for any kind of input source.
     *
     * @param src The data source.
     * @param reader The reader that can read the data from the source.
     * @param len The length to read.
     * @param <T> Data source type.
     * @throws IOException On I/O error.
     * @throws IgniteCheckedException On other error.
     */
    private <T> void storeDataBlock0(T src, IgfsDataManager.AbstractBlockReader<T> reader, final int len)
        throws IOException, IgniteCheckedException {
        preStoreDataBlocks(len);

        final int blockSize = fileInfo.blockSize();

        // If data length is not enough to fill full block, fill the remainder and return.
        if (remainderDataLen + len < blockSize) {
            if (remainder == null)
                remainder = new byte[blockSize];
            else if (remainder.length != blockSize) {
                assert remainderDataLen == remainder.length;

                byte[] allocated = new byte[blockSize];

                U.arrayCopy(remainder, 0, allocated, 0, remainder.length);

                remainder = allocated;
            }

            reader.readData(src, remainder, remainderDataLen, len);

            remainderDataLen += len;
        }
        else {
            remainder = data.storeDataBlocks(fileInfo, fileInfo.length() + space, remainder, remainderDataLen, reader,
                src, len, false, streamRange, batch, aggregateFut);

            remainderDataLen = remainder == null ? 0 : remainder.length;
        }
    }

    /**
     * Initializes data loader if it was not initialized yet and updates written space.
     *
     * @param len Data length to be written.
     */
    private void preStoreDataBlocks(int len) throws IgniteCheckedException, IOException {
        assert Thread.holdsLock(this);

        if (aggregateFut == null)
            aggregateFut = new GridCompoundFuture<>();

        bytes += len;
        space += len;
    }

    /**
     * Flushes this output stream and forces any buffered output bytes to be written out.
     *
     * @exception IOException  if an I/O error occurs.
     */
    @Override public synchronized void flush() throws IOException {
        boolean exists;

        try {
            exists = meta.exists(fileInfo.id());
        }
        catch (IgniteCheckedException e) {
            throw new IOException("File to read file metadata: " + path, e);
        }

        if (!exists) {
            onClose(true);

            throw new IOException("File was concurrently deleted: " + path);
        }

        // This will store all the full blocks and update the remainder from the internal 'buf' ByteBuffer:
        super.flush();

        try {
            if (remainder != null) {
                ByteBuffer wrappedRemainderByteBuf = ByteBuffer.wrap(remainder, 0, remainderDataLen);

                byte[] rem = data.storeDataBlocks(fileInfo,
                    fileInfo.length() + space, null/*remainder*/, 0/*remainderLen*/,
                    IgfsDataManager.byteBufReader, wrappedRemainderByteBuf, wrappedRemainderByteBuf.remaining(),
                    true/*flush*/, streamRange, batch, aggregateFut);

                assert rem == null; // The remainder must be absent.

                remainder = null;
                remainderDataLen = 0;
            }

            if (space > 0) {
                awaitAllPendingBlcoks();

                IgfsEntryInfo fileInfo0 = meta.reserveSpace(path, fileInfo.id(), space, streamRange);

                if (fileInfo0 == null)
                    throw new IOException("File was concurrently deleted: " + path);
                else
                    fileInfo = fileInfo0;

                streamRange = initialStreamRange(fileInfo);

                space = 0;
            }
            else
                aggregateFut = null;
        }
        catch (IgniteCheckedException e) {
            throw new IOException("Failed to flush data [path=" + path + ", space=" + space + ']', e);
        }
    }

    /**
     * Awaits for the main block sequence future and (optional) the remainder future.
     *
     * @throws IgniteCheckedException On error.
     */
    private void awaitAllPendingBlcoks() throws IgniteCheckedException {
        assert Thread.holdsLock(this);

        if (aggregateFut != null) {
            aggregateFut.markInitialized();

            aggregateFut.get();

            aggregateFut = null;
        }
    }

    /** {@inheritDoc} */
    @Override protected void onClose() throws IOException {
        onClose(false);
    }

    /**
     * Close callback. It will be called only once in synchronized section.
     *
     * @param deleted Whether we already know that the file was deleted.
     * @throws IOException If failed.
     */
    private void onClose(boolean deleted) throws IOException {
        assert Thread.holdsLock(this);

        if (onCloseGuard.compareAndSet(false, true)) {
            // Notify backing secondary file system batch to finish.
            if (mode != PRIMARY) {
                assert batch != null;

                batch.finish();
            }

            // Ensure file existence.
            boolean exists;

            try {
                exists = !deleted && meta.exists(fileInfo.id());
            }
            catch (IgniteCheckedException e) {
                throw new IOException("File to read file metadata: " + path, e);
            }

            if (exists) {
                IOException err = null;

                try {
                    // Wait for all blocks to be written:
                    awaitAllPendingBlcoks();
                }
                catch (IgniteCheckedException e) {
                    err = new IOException("Failed to close stream [path=" + path + ", fileInfo=" + fileInfo + ']', e);
                }

                metrics.addWrittenBytesTime(bytes, time);

                // Await secondary file system processing to finish.
                if (mode == DUAL_SYNC) {
                    try {
                        batch.await();
                    }
                    catch (IgniteCheckedException e) {
                        if (err == null)
                            err = new IOException("Failed to close secondary file system stream [path=" + path +
                                ", fileInfo=" + fileInfo + ']', e);
                    }
                }

                long modificationTime = System.currentTimeMillis();

                try {
                    meta.unlock(fileInfo, modificationTime);
                }
                catch (IgfsPathNotFoundException ignore) {
                    data.delete(fileInfo); // Safety to ensure that all data blocks are deleted.

                    throw new IOException("File was concurrently deleted: " + path);
                }
                catch (IgniteCheckedException e) {
                    throw new IOException("File to read file metadata: " + path, e);
                }

                if (err != null)
                    throw err;
            }
            else {
                aggregateFut = null;

                try {
                    if (mode == DUAL_SYNC)
                        batch.await();
                }
                catch (IgniteCheckedException e) {
                    throw new IOException("Failed to close secondary file system stream [path=" + path +
                        ", fileInfo=" + fileInfo + ']', e);
                }
                finally {
                    data.delete(fileInfo);
                }
            }
        }
    }

    /**
     * Gets initial affinity range. This range will have 0 length and will start from first
     * non-occupied file block.
     *
     * @param fileInfo File info to build initial range for.
     * @return Affinity range.
     */
    private IgfsFileAffinityRange initialStreamRange(IgfsEntryInfo fileInfo) {
        if (!igfsCtx.configuration().isFragmentizerEnabled())
            return null;

        if (!Boolean.parseBoolean(fileInfo.properties().get(IgfsUtils.PROP_PREFER_LOCAL_WRITES)))
            return null;

        int blockSize = fileInfo.blockSize();

        // Find first non-occupied block offset.
        long off = ((fileInfo.length() + blockSize - 1) / blockSize) * blockSize;

        // Need to get last affinity key and reuse it if we are on the same node.
        long lastBlockOff = off - fileInfo.blockSize();

        if (lastBlockOff < 0)
            lastBlockOff = 0;

        IgfsFileMap map = fileInfo.fileMap();

        IgniteUuid prevAffKey = map == null ? null : map.affinityKey(lastBlockOff, false);

        IgniteUuid affKey = data.nextAffinityKey(prevAffKey);

        return affKey == null ? null : new IgfsFileAffinityRange(off, off, affKey);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsOutputStreamImpl.class, this);
    }
}
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
import org.apache.ignite.events.IgfsEvent;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.ignite.events.EventType.EVT_IGFS_FILE_CLOSED_WRITE;
import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.igfs.IgfsMode.PROXY;

/**
 * Output stream to store data into grid cache with separate blocks.
 */
class IgfsOutputStreamImpl extends IgfsAbstractOutputStream {
    /** Maximum number of blocks in buffer. */
    private static final int MAX_BLOCKS_CNT = 16;

    /** IGFS mode. */
    private final IgfsMode mode;

    /** Write completion future. */
    private final IgniteInternalFuture<Boolean> writeFut;

    /** File descriptor. */
    private IgfsEntryInfo fileInfo;

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
     */
    IgfsOutputStreamImpl(IgfsContext igfsCtx, IgfsPath path, IgfsEntryInfo fileInfo, int bufSize, IgfsMode mode,
        @Nullable IgfsFileWorkerBatch batch) {
        super(igfsCtx, path, bufSize, batch);

        assert fileInfo != null && fileInfo.isFile() : "Unexpected file info: " + fileInfo;
        assert mode != null && mode != PROXY && (mode == PRIMARY && batch == null || batch != null);

        // File hasn't been locked.
        if (fileInfo.lockId() == null)
            throw new IgfsException("Failed to acquire file lock (concurrently modified?): " + path);

        synchronized (mux) {
            this.fileInfo = fileInfo;
            this.mode = mode;

            streamRange = initialStreamRange(fileInfo);

            writeFut = igfsCtx.data().writeStart(fileInfo.id());
        }
    }

    /**
     * Flushes this output stream and forces any buffered output bytes to be written out.
     *
     * @exception IOException  if an I/O error occurs.
     */
    @Override public void flush() throws IOException {
        synchronized (mux) {
            checkClosed(null, 0);

            sendBufferIfNotEmpty();

            flushRemainder();

            awaitAcks();

            // Update file length if needed.
            if (igfsCtx.configuration().isUpdateFileLengthOnFlush() && space > 0) {
                try {
                    IgfsEntryInfo fileInfo0 = igfsCtx.meta().reserveSpace(fileInfo.id(), space, streamRange);

                    if (fileInfo0 == null)
                        throw new IOException("File was concurrently deleted: " + path);
                    else
                        fileInfo = fileInfo0;

                    streamRange = initialStreamRange(fileInfo);

                    space = 0;
                }
                catch (IgniteCheckedException e) {
                    throw new IOException("Failed to update file length data [path=" + path +
                        ", space=" + space + ']', e);
                }
            }
        }
    }

    /**
     * Await acknowledgments.
     *
     * @throws IOException If failed.
     */
    private void awaitAcks() throws IOException {
        try {
            igfsCtx.data().awaitAllAcksReceived(fileInfo.id());
        }
        catch (IgniteCheckedException e) {
            throw new IOException("Failed to wait for flush acknowledge: " + fileInfo.id, e);
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

            boolean flushSuccess = false;

            try {
                sendBufferIfNotEmpty();

                flushRemainder();

                igfsCtx.data().writeClose(fileInfo.id());

                writeFut.get();

                flushSuccess = true;
            }
            catch (Exception e) {
                err = new IOException("Failed to flush data during stream close [path=" + path +
                    ", fileInfo=" + fileInfo + ']', e);
            }

            // Finish batch before file unlocking to support the assertion that unlocked file batch,
            // if any, must be in finishing state (e.g. append see more IgfsImpl.newBatch)
            if (batch != null)
                batch.finish();

            // Unlock the file after data is flushed.
            try {
                if (flushSuccess && space > 0)
                    igfsCtx.meta().unlock(fileInfo.id(), fileInfo.lockId(), System.currentTimeMillis(), true,
                        space, streamRange);
                else
                    igfsCtx.meta().unlock(fileInfo.id(), fileInfo.lockId(), System.currentTimeMillis());
            }
            catch (Exception e) {
                if (err == null)
                    err = new IOException("File to release file lock: " + path, e);
                else
                    err.addSuppressed(e);
            }

            // Finally, await secondary file system flush.
            if (batch != null) {
                if (mode == DUAL_SYNC) {
                    try {
                        batch.await();
                    }
                    catch (IgniteCheckedException e) {
                        if (err == null)
                            err = new IOException("Failed to close secondary file system stream [path=" + path +
                                ", fileInfo=" + fileInfo + ']', e);
                        else
                            err.addSuppressed(e);
                    }
                }
            }

            // Throw error, if any.
            if (err != null)
                throw err;

            igfsCtx.igfs().localMetrics().addWrittenBytesTime(bytes, time);
            igfsCtx.igfs().localMetrics().decrementFilesOpenedForWrite();

            GridEventStorageManager evts = igfsCtx.kernalContext().event();

            if (evts.isRecordable(EVT_IGFS_FILE_CLOSED_WRITE))
                evts.record(new IgfsEvent(path, igfsCtx.kernalContext().discovery().localNode(),
                    EVT_IGFS_FILE_CLOSED_WRITE, bytes));
        }
    }

    /** {@inheritDoc} */
    @Override protected int sendBlockSize() {
        return fileInfo.blockSize();
    }

    /** {@inheritDoc} */
    @Override protected byte[] storeDataBlocks(long reservedLen, byte[] remainder, int remainderLen, ByteBuffer src,
        int srcLen, boolean flush, IgfsFileWorkerBatch batch) throws IgniteCheckedException {
        return igfsCtx.data().storeDataBlocks(fileInfo, reservedLen, remainder,
            remainderLen, src, flush, streamRange, batch);
    }

    /** {@inheritDoc} */
    @Override protected byte[] storeDataBlocks(long reservedLen, byte[] remainder, int remainderLen, DataInput src,
        int srcLen, boolean flush, IgfsFileWorkerBatch batch) throws IgniteCheckedException, IOException {
        return igfsCtx.data().storeDataBlocks(fileInfo, reservedLen, remainder,
            remainderLen, src, srcLen, flush, streamRange, batch);
    }

    /** {@inheritDoc} */
    @Override protected int optimizeBufferSize(int bufSize) {
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
    @Override protected long length() {
        return fileInfo.length();
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

        IgniteUuid affKey = igfsCtx.data().nextAffinityKey(prevAffKey);

        return affKey == null ? null : new IgfsFileAffinityRange(off, off, affKey);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsOutputStreamImpl.class, this);
    }
}
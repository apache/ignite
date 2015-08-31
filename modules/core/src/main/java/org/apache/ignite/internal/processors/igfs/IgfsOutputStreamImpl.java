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
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathNotFoundException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

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
    private IgfsFileInfo fileInfo;

    /** Parent ID. */
    private final IgniteUuid parentId;

    /** File name. */
    private final String fileName;

    /** Space in file to write data. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private long space;

    /** Intermediate remainder to keep data. */
    private byte[] remainder;

    /** Data length in remainder. */
    private int remainderDataLen;

    /** Write completion future. */
    private final IgniteInternalFuture<Boolean> writeCompletionFut;

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
    IgfsOutputStreamImpl(IgfsContext igfsCtx, IgfsPath path, IgfsFileInfo fileInfo, IgniteUuid parentId,
        int bufSize, IgfsMode mode, @Nullable IgfsFileWorkerBatch batch, IgfsLocalMetrics metrics) {
        super(path, optimizeBufferSize(bufSize, fileInfo));

        assert fileInfo != null;
        assert fileInfo.isFile() : "Unexpected file info: " + fileInfo;
        assert mode != null && mode != PROXY;
        assert mode == PRIMARY && batch == null || batch != null;
        assert metrics != null;

        // File hasn't been locked.
        if (fileInfo.lockId() == null)
            throw new IgfsException("Failed to acquire file lock (concurrently modified?): " + path);

        this.igfsCtx = igfsCtx;
        meta = igfsCtx.meta();
        data = igfsCtx.data();

        this.fileInfo = fileInfo;
        this.mode = mode;
        this.batch = batch;
        this.parentId = parentId;
        this.metrics = metrics;

        streamRange = initialStreamRange(fileInfo);

        fileName = path.name();

        writeCompletionFut = data.writeStart(fileInfo);
    }

    /**
     * Optimize buffer size.
     *
     * @param bufSize Requested buffer size.
     * @param fileInfo File info.
     * @return Optimized buffer size.
     */
    @SuppressWarnings("IfMayBeConditional")
    private static int optimizeBufferSize(int bufSize, IgfsFileInfo fileInfo) {
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
    @Override protected synchronized void storeDataBlock(ByteBuffer block) throws IgniteCheckedException, IOException {
        int writeLen = block.remaining();

        preStoreDataBlocks(null, writeLen);

        int blockSize = fileInfo.blockSize();

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

            block.get(remainder, remainderDataLen, writeLen);

            remainderDataLen += writeLen;
        }
        else {
            remainder = data.storeDataBlocks(fileInfo, fileInfo.length() + space, remainder, remainderDataLen, block,
                false, streamRange, batch);

            remainderDataLen = remainder == null ? 0 : remainder.length;
        }
    }

    /** {@inheritDoc} */
    @Override protected synchronized void storeDataBlocks(DataInput in, int len) throws IgniteCheckedException, IOException {
        preStoreDataBlocks(in, len);

        int blockSize = fileInfo.blockSize();

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

            in.readFully(remainder, remainderDataLen, len);

            remainderDataLen += len;
        }
        else {
            remainder = data.storeDataBlocks(fileInfo, fileInfo.length() + space, remainder, remainderDataLen, in, len,
                false, streamRange, batch);

            remainderDataLen = remainder == null ? 0 : remainder.length;
        }
    }

    /**
     * Initializes data loader if it was not initialized yet and updates written space.
     *
     * @param len Data length to be written.
     */
    private void preStoreDataBlocks(@Nullable DataInput in, int len) throws IgniteCheckedException, IOException {
        // Check if any exception happened while writing data.
        if (writeCompletionFut.isDone()) {
            assert ((GridFutureAdapter)writeCompletionFut).isFailed();

            if (in != null)
                in.skipBytes(len);

            writeCompletionFut.get();
        }

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
            throw new IOException("File to read file metadata: " + fileInfo.path(), e);
        }

        if (!exists) {
            onClose(true);

            throw new IOException("File was concurrently deleted: " + path);
        }

        super.flush();

        try {
            if (remainder != null) {
                data.storeDataBlocks(fileInfo, fileInfo.length() + space, null, 0,
                    ByteBuffer.wrap(remainder, 0, remainderDataLen), true, streamRange, batch);

                remainder = null;
                remainderDataLen = 0;
            }

            if (space > 0) {
                data.awaitAllAcksReceived(fileInfo.id());

                IgfsFileInfo fileInfo0 = meta.updateInfo(fileInfo.id(),
                    new ReserveSpaceClosure(space, streamRange));

                if (fileInfo0 == null)
                    throw new IOException("File was concurrently deleted: " + path);
                else
                    fileInfo = fileInfo0;

                streamRange = initialStreamRange(fileInfo);

                space = 0;
            }
        }
        catch (IgniteCheckedException e) {
            throw new IOException("Failed to flush data [path=" + path + ", space=" + space + ']', e);
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
                throw new IOException("File to read file metadata: " + fileInfo.path(), e);
            }

            if (exists) {
                IOException err = null;

                try {
                    data.writeClose(fileInfo);

                    writeCompletionFut.get();
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
                    throw new IOException("File to read file metadata: " + fileInfo.path(), e);
                }

                meta.updateParentListingAsync(parentId, fileInfo.id(), fileName, bytes, modificationTime);

                if (err != null)
                    throw err;
            }
            else {
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
    private IgfsFileAffinityRange initialStreamRange(IgfsFileInfo fileInfo) {
        if (!igfsCtx.configuration().isFragmentizerEnabled())
            return null;

        if (!Boolean.parseBoolean(fileInfo.properties().get(IgfsEx.PROP_PREFER_LOCAL_WRITES)))
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

    /**
     * Helper closure to reserve specified space and update file's length
     */
    @GridInternal
    private static final class ReserveSpaceClosure implements IgniteClosure<IgfsFileInfo, IgfsFileInfo>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Space amount (bytes number) to increase file's length. */
        private long space;

        /** Affinity range for this particular update. */
        private IgfsFileAffinityRange range;

        /**
         * Empty constructor required for {@link Externalizable}.
         *
         */
        public ReserveSpaceClosure() {
            // No-op.
        }

        /**
         * Constructs the closure to reserve specified space and update file's length.
         *
         * @param space Space amount (bytes number) to increase file's length.
         * @param range Affinity range specifying which part of file was colocated.
         */
        private ReserveSpaceClosure(long space, IgfsFileAffinityRange range) {
            this.space = space;
            this.range = range;
        }

        /** {@inheritDoc} */
        @Override public IgfsFileInfo apply(IgfsFileInfo oldInfo) {
            IgfsFileMap oldMap = oldInfo.fileMap();

            IgfsFileMap newMap = new IgfsFileMap(oldMap);

            newMap.addRange(range);

            // Update file length.
            IgfsFileInfo updated = new IgfsFileInfo(oldInfo, oldInfo.length() + space);

            updated.fileMap(newMap);

            return updated;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(space);
            out.writeObject(range);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            space = in.readLong();
            range = (IgfsFileAffinityRange)in.readObject();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ReserveSpaceClosure.class, this);
        }
    }
}
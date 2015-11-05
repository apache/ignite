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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.igfs.IgfsMode.PROXY;
import static org.apache.ignite.internal.processors.igfs.IgfsUtils.calculateNextReservedDelta;

/**
 * Output stream to store data into grid cache with separate blocks.
 */
class IgfsOutputStreamImpl extends IgfsOutputStream {
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
    WriteCompletionFuture writeCompletionFut = new WriteCompletionFuture();

    /** IGFS mode. */
    private final IgfsMode mode;

    /** File worker batch. */
    private final IgfsFileWorkerBatch batch;

    /** Local IGFS metrics. */
    private final IgfsLocalMetrics metrics;

    /** Affinity written by this output stream. */
    private IgfsFileAffinityRange streamRange;

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
     * Gets number of written bytes.
     *
     * @return Written bytes.
     */
    protected final long bytes() {
        return bytes;
    }

    /** {@inheritDoc} */
    @Override public final synchronized void write(int b) throws IOException {
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
    @Override public final synchronized void write(byte[] b, int off, int len) throws IOException {
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
    @Override public final synchronized void transferFrom(DataInput in, int len) throws IOException {
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

                afterClose();
            }
        }
        finally {
            // Mark this stream closed AFTER flush.
            closed = true;
        }
    }

    /**
     * Hook method to take an action after the stream closing.
     */
    protected void afterClose() {
        // Noop.
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
        assert path != null;
        this.path = path;

        int optBufSize = optimizeBufferSize(bufSize, fileInfo);
        assert optBufSize > 0;
        this.bufSize = optBufSize;

        assert fileInfo != null;
        assert fileInfo.isFile() : "Unexpected file info: " + fileInfo;
        assert mode != null && mode != PROXY;
        assert mode == PRIMARY && batch == null || batch != null;
        assert metrics != null;

        // File hasn't been locked.
        if (fileInfo.lockId() == null)
            throw new IgfsException("Failed to acquire file lock (concurrently modified?): " + path);

        assert !IgfsMetaManager.DELETE_LOCK_ID.equals(fileInfo.lockId());

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
    protected synchronized void storeDataBlock(ByteBuffer block) throws IgniteCheckedException, IOException {
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
                false, streamRange, batch, writeCompletionFut);

            remainderDataLen = remainder == null ? 0 : remainder.length;
        }
    }

    /** {@inheritDoc} */
    protected synchronized void storeDataBlocks(DataInput in, int len) throws IgniteCheckedException,
        IOException {
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
                false, streamRange, batch, writeCompletionFut);

            remainderDataLen = remainder == null ? 0 : remainder.length;
        }
    }

    /**
     * Initializes data loader if it was not initialized yet and updates written space.
     *
     * @param len Data length to be written.
     */
    private void preStoreDataBlocks(@Nullable DataInput in, final int len) throws IgniteCheckedException, IOException {
        assert Thread.holdsLock(this);

        // Check if any exception happened while writing data.
        if (writeCompletionFut.isDone()) {
            assert writeCompletionFut.isFailed();

            if (in != null)
                in.skipBytes(len);

            writeCompletionFut.get();
        }

        long reservedDelta = fileInfo.reservedDelta();

        if (len > reservedDelta) {
            // Must invoke flush to renew the space reservation:
            flush(len);

            assert remainder == null;
            assert remainderDataLen == 0;
            assert space == 0;
        }

        reservedDelta = fileInfo.reservedDelta(); // Renew the delta

        assert len <= reservedDelta : "Requested len = " + len + ", reserved = " + reservedDelta;

        bytes += len;
        space += len;
    }

    /**
     * Flushes this output stream and forces any buffered output bytes to be written out.
     *
     * @exception IOException  if an I/O error occurs.
     */
    @Override public synchronized void flush() throws IOException {
        flush(0L);
    }

    /**
     * Flushes data with some expected length to be written.
     *
     * @param expWriteAmount The amount of data expected to be written.
     * @throws IOException
     */
    private void flush(long expWriteAmount) throws IOException {
        assert Thread.holdsLock(this);

        checkClosed(null, 0);

        // Send all IPC data from the local buffer.
        if (buf != null && buf.position() > 0)
            sendData(true/*flip*/);

        try {
            if (remainder != null) {
                data.storeDataBlocks(fileInfo, fileInfo.length() + space, null, 0,
                    ByteBuffer.wrap(remainder, 0, remainderDataLen), true, streamRange, batch, writeCompletionFut);

                remainder = null;
                remainderDataLen = 0;
            }

            long newReservedDelta = calculateNextReservedDelta(fileInfo.blockSize(),
                fileInfo.length() + space, expWriteAmount);

            if (space > 0 || newReservedDelta > fileInfo.reservedDelta()) {
                awaitForWriteCompletionFuture();

                // Now renew the future:
                writeCompletionFut = new WriteCompletionFuture();

                IgfsFileInfo fileInfo0 = meta.updateInfo(fileInfo.id(),
                    new UpdateLengthClosure(space, newReservedDelta, streamRange));

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
    @SuppressWarnings("ThrowFromFinallyBlock")
    protected final void onClose() throws IOException {
        assert Thread.holdsLock(this);
        assert !closed;

        final long modificationTime = System.currentTimeMillis();

        try {
            // Notify backing secondary file system batch to finish.
            if (mode != PRIMARY) {
                assert batch != null;

                batch.finish();
            }

            IOException err = null;

            try {
                awaitForWriteCompletionFuture();
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

            IgfsFileInfo fi;

            try {
                // Zero the reserved delta:
                fi = meta.updateInfo(fileInfo.id(), new UpdateLengthClosure(0L, 0L, streamRange));

                if (fi == null)
                    throw new AssertionError("File was not found.");

                fileInfo = fi;
            }
            catch (IgniteCheckedException ioe) {
                throw new IOException("Failed to update " +
                    "length [path=" + path + ", fileInfo=" + fileInfo + ']', ioe);
            }

            assert fileInfo.reservedDelta() == 0L;

            meta.updateParentListingAsync(parentId, fileInfo.id(), fileName, bytes, modificationTime);

            if (err != null)
                throw err;
        }
        finally {
            // Removing of the write lock:
            try {
                meta.unlock(fileInfo, modificationTime);
            }
            catch (IgniteCheckedException e) {
                throw new IOException("File to read file metadata: " + fileInfo.path(), e);
            }
        }
    }

    /**
     * Waits for write completion to happen.
     *
     * @throws IgniteCheckedException
     */
    private void awaitForWriteCompletionFuture() throws IgniteCheckedException {
        assert writeCompletionFut != null;

        // If #flush() was not invoked on the stream, we should allow the future to complete:
        writeCompletionFut.markWaitingLastAck();

        // The future should already be completed at this point,
        // but we need to get Exception, if the future was failed.
        writeCompletionFut.get();
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
    static final class UpdateLengthClosure implements IgniteClosure<IgfsFileInfo, IgfsFileInfo>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Space amount (bytes number) to increase file's length. */
        private long space;

        /** New delta to reserve. */
        private long reservedDelta;

        /** Affinity range for this particular update. */
        private IgfsFileAffinityRange range;

        /**
         * Empty constructor required for {@link Externalizable}.
         *
         */
        public UpdateLengthClosure() {
            // No-op.
        }

        /**
         * Constructs the closure to reserve specified space and update file's length.
         *
         * @param space Space amount (bytes number) to increase file's length.
         * @param reservedDelta The number of bytes that can be written before next flush.
         * @param range Affinity range specifying which part of file was colocated.
         */
        UpdateLengthClosure(long space, long reservedDelta, IgfsFileAffinityRange range) {
            this.space = space;
            this.range = range;
            this.reservedDelta = reservedDelta;
        }

        /** {@inheritDoc} */
        @Override public IgfsFileInfo apply(IgfsFileInfo oldInfo) {
            IgfsFileMap oldMap = oldInfo.fileMap();

            IgfsFileMap newMap = new IgfsFileMap(oldMap);

            newMap.addRange(range); // Add range.

            // Update file length and reserved delta:
           return new IgfsFileInfo(oldInfo.length() + space, reservedDelta, oldInfo, newMap);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(space);
            out.writeLong(reservedDelta);
            out.writeObject(range);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            space = in.readLong();
            reservedDelta = in.readLong();
            range = (IgfsFileAffinityRange)in.readObject();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(UpdateLengthClosure.class, this);
        }
    }
}
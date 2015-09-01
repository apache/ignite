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

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.igfs.IgfsCorruptedFileException;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathNotFoundException;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystemPositionedReadable;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Input stream to read data from grid cache with separate blocks.
 */
public class IgfsInputStreamImpl extends IgfsInputStreamAdapter {
    /** Empty chunks result. */
    private static final byte[][] EMPTY_CHUNKS = new byte[0][];

    /** Meta manager. */
    private final IgfsMetaManager meta;

    /** Data manager. */
    private final IgfsDataManager data;

    /** Secondary file system reader. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private final IgfsSecondaryFileSystemPositionedReadable secReader;

    /** Logger. */
    private IgniteLogger log;

    /** Path to file. */
    protected final IgfsPath path;

    /** File descriptor. */
    private volatile IgfsFileInfo fileInfo;

    /** The number of already read bytes. Important! Access to the property is guarded by this object lock. */
    private long pos;

    /** Local cache. */
    private final Map<Long, IgniteInternalFuture<byte[]>> locCache;

    /** Maximum local cache size. */
    private final int maxLocCacheSize;

    /** Pending data read futures which were evicted from the local cache before completion. */
    private final Set<IgniteInternalFuture<byte[]>> pendingFuts;

    /** Pending futures lock. */
    private final Lock pendingFutsLock = new ReentrantLock();

    /** Pending futures condition. */
    private final Condition pendingFutsCond = pendingFutsLock.newCondition();

    /** Closed flag. */
    private boolean closed;

    /** Number of blocks to prefetch asynchronously. */
    private int prefetchBlocks;

    /** Numbed of blocks that must be read sequentially before prefetch is triggered. */
    private int seqReadsBeforePrefetch;

    /** Bytes read. */
    private long bytes;

    /** Index of the previously read block. Initially it is set to -1 indicating that no reads has been made so far. */
    private long prevBlockIdx = -1;

    /** Amount of sequential reads performed. */
    private int seqReads;

    /** Time consumed on reading. */
    private long time;

    /** Local IGFS metrics. */
    private final IgfsLocalMetrics metrics;

    /**
     * Constructs file output stream.
     *
     * @param igfsCtx IGFS context.
     * @param path Path to stored file.
     * @param fileInfo File info to write binary data to.
     * @param prefetchBlocks Number of blocks to prefetch.
     * @param seqReadsBeforePrefetch Amount of sequential reads before prefetch is triggered.
     * @param secReader Optional secondary file system reader.
     * @param metrics Local IGFS metrics.
     */
    IgfsInputStreamImpl(IgfsContext igfsCtx, IgfsPath path, IgfsFileInfo fileInfo, int prefetchBlocks,
        int seqReadsBeforePrefetch, @Nullable IgfsSecondaryFileSystemPositionedReadable secReader, IgfsLocalMetrics metrics) {
        assert igfsCtx != null;
        assert path != null;
        assert fileInfo != null;
        assert metrics != null;

        this.path = path;
        this.fileInfo = fileInfo;
        this.prefetchBlocks = prefetchBlocks;
        this.seqReadsBeforePrefetch = seqReadsBeforePrefetch;
        this.secReader = secReader;
        this.metrics = metrics;

        meta = igfsCtx.meta();
        data = igfsCtx.data();

        log = igfsCtx.kernalContext().log(IgfsInputStream.class);

        maxLocCacheSize = (prefetchBlocks > 0 ? prefetchBlocks : 1) * 3 / 2;

        locCache = new LinkedHashMap<>(maxLocCacheSize, 1.0f);

        pendingFuts = new GridConcurrentHashSet<>(prefetchBlocks > 0 ? prefetchBlocks : 1);
    }

    /**
     * Gets bytes read.
     *
     * @return Bytes read.
     */
    public synchronized long bytes() {
        return bytes;
    }

    /** {@inheritDoc} */
    @Override public IgfsFileInfo fileInfo() {
        return fileInfo;
    }

    /** {@inheritDoc} */
    @Override public synchronized int read() throws IOException {
        byte[] buf = new byte[1];

        int read = read(buf, 0, 1);

        if (read == -1)
            return -1; // EOF.

        return buf[0] & 0xFF; // Cast to int and cut to *unsigned* byte value.
    }

    /** {@inheritDoc} */
    @Override public synchronized int read(@NotNull byte[] b, int off, int len) throws IOException {
        int read = readFromStore(pos, b, off, len);

        if (read != -1)
            pos += read;

        return read;
    }

    /** {@inheritDoc} */
    @Override public synchronized void seek(long pos) throws IOException {
        if (pos < 0)
            throw new IOException("Seek position cannot be negative: " + pos);


        this.pos = pos;
    }

    /** {@inheritDoc} */
    @Override public synchronized long position() throws IOException {
        return pos;
    }

    /** {@inheritDoc} */
    @Override public synchronized int available() throws IOException {
        long l = fileInfo.length() - pos;

        if (l < 0)
            return 0;

        if (l > Integer.MAX_VALUE)
            return Integer.MAX_VALUE;

        return (int)l;
    }

    /** {@inheritDoc} */
    @Override public synchronized void readFully(long pos, byte[] buf) throws IOException {
        readFully(pos, buf, 0, buf.length);
    }

    /** {@inheritDoc} */
    @Override public synchronized void readFully(long pos, byte[] buf, int off, int len) throws IOException {
        for (int readBytes = 0; readBytes < len; ) {
            int read = readFromStore(pos + readBytes, buf, off + readBytes, len - readBytes);

            if (read == -1)
                throw new EOFException("Failed to read stream fully (stream ends unexpectedly)" +
                    "[pos=" + pos + ", buf.length=" + buf.length + ", off=" + off + ", len=" + len + ']');

            readBytes += read;
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized int read(long pos, byte[] buf, int off, int len) throws IOException {
        return readFromStore(pos, buf, off, len);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("IfMayBeConditional")
    @Override public synchronized byte[][] readChunks(long pos, int len) throws IOException {
        // Readable bytes in the file, starting from the specified position.
        long readable = fileInfo.length() - pos;

        if (readable <= 0)
            return EMPTY_CHUNKS;

        long startTime = System.nanoTime();

        if (readable < len)
            len = (int)readable; // Truncate expected length to available.

        assert len > 0;

        bytes += len;

        int start = (int)(pos / fileInfo.blockSize());
        int end = (int)((pos + len - 1) / fileInfo.blockSize());

        int chunkCnt = end - start + 1;

        byte[][] chunks = new byte[chunkCnt][];

        for (int i = 0; i < chunkCnt; i++) {
            byte[] block = blockFragmentizerSafe(start + i);

            int blockOff = (int)(pos % fileInfo.blockSize());
            int blockLen = Math.min(len, block.length - blockOff);

            // If whole block can be used as result, do not do array copy.
            if (blockLen == block.length)
                chunks[i] = block;
            else {
                // Only first or last block can have non-full data.
                assert i == 0 || i == chunkCnt - 1;

                chunks[i] = Arrays.copyOfRange(block, blockOff, blockOff + blockLen);
            }

            len -= blockLen;
            pos += blockLen;
        }

        assert len == 0;

        time += System.nanoTime() - startTime;

        return chunks;
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() throws IOException {
        try {
            if (secReader != null) {
                // Close secondary input stream.
                secReader.close();

                // Ensuring local cache futures completion.
                for (IgniteInternalFuture<byte[]> fut : locCache.values()) {
                    try {
                        fut.get();
                    }
                    catch (IgniteCheckedException ignore) {
                        // No-op.
                    }
                }

                // Ensuring pending evicted futures completion.
                while (!pendingFuts.isEmpty()) {
                    pendingFutsLock.lock();

                    try {
                        pendingFutsCond.await(100, TimeUnit.MILLISECONDS);
                    }
                    catch (InterruptedException ignore) {
                        // No-op.
                    }
                    finally {
                        pendingFutsLock.unlock();
                    }
                }

                // Safety to ensure no orphaned data blocks exist in case file was concurrently deleted.
               if (!meta.exists(fileInfo.id()))
                    data.delete(fileInfo);
            }
        }
        catch (IgniteCheckedException e) {
            throw new IOException("File to close the file: " + fileInfo.path(), e);
        }
        finally {
            closed = true;

            metrics.addReadBytesTime(bytes, time);

            locCache.clear();
        }
    }

    /**
     * @param pos Position to start reading from.
     * @param buf Data buffer to save read data to.
     * @param off Offset in the buffer to write data from.
     * @param len Length of the data to read from the stream.
     * @return Number of actually read bytes.
     * @throws IOException In case of any IO exception.
     */
    private int readFromStore(long pos, byte[] buf, int off, int len) throws IOException {
        if (pos < 0)
            throw new IllegalArgumentException("Read position cannot be negative: " + pos);

        if (buf == null)
            throw new NullPointerException("Destination buffer cannot be null.");

        if (off < 0 || len < 0 || buf.length < len + off)
            throw new IndexOutOfBoundsException("Invalid buffer boundaries " +
                "[buf.length=" + buf.length + ", off=" + off + ", len=" + len + ']');

        if (len == 0)
            return 0; // Fully read done: read zero bytes correctly.

        // Readable bytes in the file, starting from the specified position.
        long readable = fileInfo.length() - pos;

        if (readable <= 0)
            return -1; // EOF.

        long startTime = System.nanoTime();

        if (readable < len)
            len = (int)readable; // Truncate expected length to available.

        assert len > 0;

        byte[] block = blockFragmentizerSafe(pos / fileInfo.blockSize());

        // Skip bytes to expected position.
        int blockOff = (int)(pos % fileInfo.blockSize());

        len = Math.min(len, block.length - blockOff);

        U.arrayCopy(block, blockOff, buf, off, len);

        bytes += len;
        time += System.nanoTime() - startTime;

        return len;
    }

    /**
     * Method to safely retrieve file block. In case if file block is missing this method will check file map
     * and update file info. This may be needed when file that we are reading is concurrently fragmented.
     *
     * @param blockIdx Block index to read.
     * @return Block data.
     * @throws IOException If read failed.
     */
    private byte[] blockFragmentizerSafe(long blockIdx) throws IOException {
        try {
            try {
                return block(blockIdx);
            }
            catch (IgfsCorruptedFileException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to fetch file block [path=" + path + ", fileInfo=" + fileInfo +
                        ", blockIdx=" + blockIdx + ", errMsg=" + e.getMessage() + ']');

                // This failure may be caused by file being fragmented.
                if (fileInfo.fileMap() != null && !fileInfo.fileMap().ranges().isEmpty()) {
                    IgfsFileInfo newInfo = meta.info(fileInfo.id());

                    // File was deleted.
                    if (newInfo == null)
                        throw new IgfsPathNotFoundException("Failed to read file block (file was concurrently " +
                                "deleted) [path=" + path + ", blockIdx=" + blockIdx + ']');

                    fileInfo = newInfo;

                    // Must clear cache as it may have failed futures.
                    locCache.clear();

                    if (log.isDebugEnabled())
                        log.debug("Updated input stream file info after block fetch failure [path=" + path
                            + ", fileInfo=" + fileInfo + ']');

                    return block(blockIdx);
                }

                throw new IOException(e.getMessage(), e);
            }
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * @param blockIdx Block index.
     * @return File block data.
     * @throws IOException If failed.
     * @throws IgniteCheckedException If failed.
     */
    private byte[] block(long blockIdx) throws IOException, IgniteCheckedException {
        assert blockIdx >= 0;

        IgniteInternalFuture<byte[]> bytesFut = locCache.get(blockIdx);

        if (bytesFut == null) {
            if (closed)
                throw new IOException("Stream is already closed: " + this);

            seqReads = (prevBlockIdx != -1 && prevBlockIdx + 1 == blockIdx) ? ++seqReads : 0;

            prevBlockIdx = blockIdx;

            bytesFut = dataBlock(fileInfo, blockIdx);

            assert bytesFut != null;

            addLocalCacheFuture(blockIdx, bytesFut);
        }

        // Schedule the next block(s) prefetch.
        if (prefetchBlocks > 0 && seqReads >= seqReadsBeforePrefetch - 1) {
            for (int i = 1; i <= prefetchBlocks; i++) {
                // Ensure that we do not prefetch over file size.
                if (fileInfo.blockSize() * (i + blockIdx) >= fileInfo.length())
                    break;
                else if (locCache.get(blockIdx + i) == null)
                    addLocalCacheFuture(blockIdx + i, dataBlock(fileInfo, blockIdx + i));
            }
        }

        byte[] bytes = bytesFut.get();

        if (bytes == null)
            throw new IgfsCorruptedFileException("Failed to retrieve file's data block (corrupted file?) " +
                "[path=" + path + ", blockIdx=" + blockIdx + ']');

        int blockSize = fileInfo.blockSize();

        if (blockIdx == fileInfo.blocksCount() - 1)
            blockSize = (int)(fileInfo.length() % blockSize);

        // If part of the file was reserved for writing, but was not actually written.
        if (bytes.length < blockSize)
            throw new IOException("Inconsistent file's data block (incorrectly written?)" +
                " [path=" + path + ", blockIdx=" + blockIdx + ", blockSize=" + bytes.length +
                ", expectedBlockSize=" + blockSize + ", fileBlockSize=" + fileInfo.blockSize() +
                ", fileLen=" + fileInfo.length() + ']');

        return bytes;
    }

    /**
     * Add local cache future.
     *
     * @param idx Block index.
     * @param fut Future.
     */
    private void addLocalCacheFuture(long idx, IgniteInternalFuture<byte[]> fut) {
        assert Thread.holdsLock(this);

        if (!locCache.containsKey(idx)) {
            if (locCache.size() == maxLocCacheSize) {
                final IgniteInternalFuture<byte[]> evictFut = locCache.remove(locCache.keySet().iterator().next());

                if (!evictFut.isDone()) {
                    pendingFuts.add(evictFut);

                    evictFut.listen(new IgniteInClosure<IgniteInternalFuture<byte[]>>() {
                        @Override
                        public void apply(IgniteInternalFuture<byte[]> t) {
                            pendingFuts.remove(evictFut);

                            pendingFutsLock.lock();

                            try {
                                pendingFutsCond.signalAll();
                            } finally {
                                pendingFutsLock.unlock();
                            }
                        }
                    });
                }
            }

            locCache.put(idx, fut);
        }
    }

    /**
     * Get data block for specified block index.
     *
     * @param fileInfo File info.
     * @param blockIdx Block index.
     * @return Requested data block or {@code null} if nothing found.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected IgniteInternalFuture<byte[]> dataBlock(IgfsFileInfo fileInfo, long blockIdx) throws IgniteCheckedException {
        return data.dataBlock(fileInfo, path, blockIdx, secReader);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsInputStreamImpl.class, this);
    }
}
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
import org.apache.ignite.igfs.IgfsCorruptedFileException;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathNotFoundException;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystemPositionedReadable;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Input stream to read data from grid cache with separate blocks.
 */
public class IgfsInputStreamImpl extends IgfsAbstractInputStream {
    /** Meta manager. */
    private final IgfsMetaManager meta;

    /** Data manager. */
    private final IgfsDataManager data;

    /** File descriptor. */
    private volatile IgfsEntryInfo fileInfo;

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
    IgfsInputStreamImpl(IgfsContext igfsCtx, IgfsPath path, IgfsEntryInfo fileInfo, int prefetchBlocks,
        int seqReadsBeforePrefetch, @Nullable IgfsSecondaryFileSystemPositionedReadable secReader,
        IgfsLocalMetrics metrics) {
        super(igfsCtx, path, prefetchBlocks, seqReadsBeforePrefetch, secReader, metrics);

        assert fileInfo != null;

        this.fileInfo = fileInfo;
        meta = igfsCtx.meta();
        data = igfsCtx.data();
    }

    /** {@inheritDoc} */
    @Override public long length() {
        return fileInfo.length();
    }


    /** {@inheritDoc} */
    @Override protected int blockSize() {
        return fileInfo.blockSize();
    }

    /** {@inheritDoc} */
    @Override protected long blocksCount() {
        return fileInfo.blocksCount();
    }

    /** {@inheritDoc} */
    @Override public synchronized void close0() throws IOException {
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
            }
        }
        catch (Exception e) {
            throw new IOException("File to close the file: " + path, e);
        }
        finally {
            closed = true;

            metrics.addReadBytesTime(bytes, time);

            locCache.clear();
        }
    }

    /** {@inheritDoc} */
    @Override protected byte[] blockFragmentizerSafe(long blockIdx) throws IOException {
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
                    IgfsEntryInfo newInfo = meta.info(fileInfo.id());

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

    /** {@inheritDoc} */
    @Override @Nullable protected IgniteInternalFuture<byte[]> dataBlock(long blockIdx)
        throws IgniteCheckedException {
        return data.dataBlock(fileInfo, path, blockIdx, secReader);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsInputStreamImpl.class, this);
    }
}
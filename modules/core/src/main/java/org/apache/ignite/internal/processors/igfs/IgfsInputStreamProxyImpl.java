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

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsCorruptedFileException;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystemPositionedReadable;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Input stream to read data from grid cache with separate blocks.
 */
public class IgfsInputStreamProxyImpl extends IgfsAbstractInputStream {
    /** File info. */
    private final IgfsFile info;

    /**
     * Constructs file output stream.
     * @param igfsCtx IGFS context.
     * @param path Path to stored file.
     * @param info IGFS file info.
     * @param prefetchBlocks Number of blocks to prefetch.
     * @param seqReadsBeforePrefetch Amount of sequential reads before prefetch is triggered.
     * @param secReader Optional secondary file system reader.
     * @param metrics Local IGFS metrics.
     */
    IgfsInputStreamProxyImpl(IgfsContext igfsCtx, IgfsPath path, IgfsFile info, int prefetchBlocks,
        int seqReadsBeforePrefetch, @Nullable IgfsSecondaryFileSystemPositionedReadable secReader,
        IgfsLocalMetrics metrics) {
        super(igfsCtx, path, prefetchBlocks, seqReadsBeforePrefetch, secReader, metrics);

        this.info = info;
    }


    /** {@inheritDoc} */
    @Override public long length() {
        return info.length();
    }



    /** {@inheritDoc} */
    @Override public synchronized void close0() throws IOException {
        try {
            if (secReader != null) {
                // Close secondary input stream.
                secReader.close();
            }
        }
        catch (Exception e) {
            throw new IOException("File to close the file: " + path, e);
        }
        finally {
            closed = true;

            metrics.addReadBytesTime(bytes, time);
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
                    log.debug("Failed to fetch file block [path=" + path + ", fileInfo=" + info +
                        ", blockIdx=" + blockIdx + ", errMsg=" + e.getMessage() + ']');

                // This failure may be caused by file being fragmented.
                throw new IOException(e.getMessage(), e);
            }
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * Get data block for specified block index.
     *
     * @param blockIdx Block index.
     * @return Requested data block or {@code null} if nothing found.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected IgniteInternalFuture<byte[]> dataBlock(final long blockIdx)
        throws IgniteCheckedException {

        return igfsCtx.kernalContext().closure().callLocalSafe(new Callable<byte[]>() {
            @Override public byte[] call() throws Exception {
                return secondaryDataBlock(blockIdx);
            }
        });
    }

    /**
     * @param blockIdx Block index/
     * @return Requested data block or {@code null} if nothing found.
     */
    private byte[] secondaryDataBlock(long blockIdx) {
        int blockSize = blockSize();

        long pos = blockIdx * blockSize; // Calculate position for Hadoop

        byte[] res = new byte[blockSize];

        int read = 0;

        synchronized (secReader) {
            try {
                // Delegate to the secondary file system.
                while (read < blockSize) {
                    int r = secReader.read(pos + read, res, read, blockSize - read);

                    if (r < 0)
                        break;

                    read += r;
                }
            }
            catch (IOException e) {
                throw new IgfsException("Failed to read data due to secondary file system " +
                    "exception: " + e.getMessage(), e);
            }
        }

        // If we did not read full block at the end of the file - trim it.
        if (read != blockSize)
            res = Arrays.copyOf(res, read);

        metrics.addReadBlocks(0, 1);
        return res;
    }

    /** {@inheritDoc} */
    @Override protected int blockSize() {
        return 4096;
    }

    /** {@inheritDoc} */
    @Override protected long blocksCount() {
        long bc = length() / blockSize();
        return (length() % blockSize() != 0) ? bc + 1 : bc;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsInputStreamProxyImpl.class, this);
    }
}
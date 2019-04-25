/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.OpenOption;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.compress.FileSystemUtils;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * File I/O implementation based on {@link AsynchronousFileChannel}.
 */
public class AsyncFileIO extends AbstractFileIO {
    /**
     * File channel associated with {@code file}
     */
    private final AsynchronousFileChannel ch;

    /** Native file descriptor. */
    private final int fd;

    /** */
    private final int fsBlockSize;

    /**
     * Channel's position.
     */
    private volatile long position;

    /** */
    private final ThreadLocal<ChannelOpFuture> holder;

    /** */
    private GridConcurrentHashSet<ChannelOpFuture> asyncFuts = new GridConcurrentHashSet<>();

    /**
     * Creates I/O implementation for specified {@code file}
     * @param file Random access file
     * @param modes Open modes.
     */
    public AsyncFileIO(File file, ThreadLocal<ChannelOpFuture> holder, OpenOption... modes) throws IOException {
        ch = AsynchronousFileChannel.open(file.toPath(), modes);
        fd = getFileDescriptor(ch);
        fsBlockSize = FileSystemUtils.getFileSystemBlockSize(fd);
        this.holder = holder;
    }

    /**
     * @param ch File channel.
     * @return Native file descriptor.
     */
    private static int getFileDescriptor(AsynchronousFileChannel ch) {
         FileDescriptor fd = U.field(ch, "fdObj");
         return U.field(fd, "fd");
    }

    /** {@inheritDoc} */
    @Override public int getFileSystemBlockSize() {
        return fsBlockSize;
    }

    /** {@inheritDoc} */
    @Override public long getSparseSize() {
        return FileSystemUtils.getSparseFileSize(fd);
    }

    /** {@inheritDoc} */
    @Override public int punchHole(long position, int len) {
        return (int)FileSystemUtils.punchHole(fd, position, len, fsBlockSize);
    }

    /** {@inheritDoc} */
    @Override public long position() throws IOException {
        return position;
    }

    /** {@inheritDoc} */
    @Override public void position(long newPosition) throws IOException {
        this.position = newPosition;
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf) throws IOException {
        ChannelOpFuture fut = holder.get();
        fut.reset();

        ch.read(destBuf, position, this, fut);

        try {
            return fut.getUninterruptibly();
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf, long position) throws IOException {
        ChannelOpFuture fut = holder.get();
        fut.reset();

        ch.read(destBuf, position, null, fut);

        try {
            return fut.getUninterruptibly();
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
        finally {
            asyncFuts.remove(fut);
        }
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] buf, int off, int
        length) throws IOException {
        ChannelOpFuture fut = holder.get();
        fut.reset();

        ch.read(ByteBuffer.wrap(buf, off, length), position, this, fut);

        try {
            return fut.getUninterruptibly();
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf) throws IOException {
        ChannelOpFuture fut = holder.get();
        fut.reset();

        ch.write(srcBuf, position, this, fut);

        try {
            return fut.getUninterruptibly();
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
        ChannelOpFuture fut = holder.get();
        fut.reset();

        asyncFuts.add(fut);

        ch.write(srcBuf, position, null, fut);

        try {
            return fut.getUninterruptibly();
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
        finally {
            asyncFuts.remove(fut);
        }
    }

    /** {@inheritDoc} */
    @Override public int write(byte[] buf, int off, int len) throws IOException {
        ChannelOpFuture fut = holder.get();
        fut.reset();

        ch.write(ByteBuffer.wrap(buf, off, len), position, this, fut);

        try {
            return fut.getUninterruptibly();
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MappedByteBuffer map(int sizeBytes) throws IOException {
        throw new UnsupportedOperationException("AsynchronousFileChannel doesn't support mmap.");
    }

    /** {@inheritDoc} */
    @Override public void force() throws IOException {
        force(false);
    }

    /** {@inheritDoc} */
    @Override public void force(boolean withMetadata) throws IOException {
        ch.force(withMetadata);
    }

    /** {@inheritDoc} */
    @Override public long size() throws IOException {
        return ch.size();
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IOException {
        ch.truncate(0);

        this.position = 0;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        for (ChannelOpFuture asyncFut : asyncFuts) {
            try {
                asyncFut.getUninterruptibly(); // Ignore interrupts while waiting for channel close.
            }
            catch (IgniteCheckedException e) {
                throw new IOException(e);
            }
        }

        ch.close();
    }

    /** */
    static class ChannelOpFuture extends GridFutureAdapter<Integer> implements CompletionHandler<Integer, AsyncFileIO>  {
        /** {@inheritDoc} */
        @Override public void completed(Integer res, AsyncFileIO attach) {
            if (attach != null) {
                if (res != -1)
                    attach.position += res;
            }

            // Release waiter and allow next operation to begin.
            super.onDone(res, null);
        }

        /** {@inheritDoc} */
        @Override public void failed(Throwable exc, AsyncFileIO attach) {
            super.onDone(exc);
        }
    }
}
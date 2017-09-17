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

package org.apache.ignite.internal.processors.cache.persistence.file;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicReference;

/**
 * File I/O implementation based on {@link AsynchronousFileChannel}.
 */
public class AsyncFileIO implements FileIO {
    /**
     * File channel associated with {@code file}
     */
    private final AsynchronousFileChannel channel;

    /**
     * Channel's position.
     */
    private long position;

    private static final CompletionHolderImpl H = new CompletionHolderImpl();

    /** */
    private AtomicReference<GridFutureAdapter<Integer>> lastFut = new AtomicReference<>();

    /**
     * Creates I/O implementation for specified {@code file}
     *
     * @param file Random access file
     */
    public AsyncFileIO(File file) throws IOException {
        this.channel = AsynchronousFileChannel.open(file.toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
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
    @Override public int read(ByteBuffer destinationBuffer) throws IOException {
        ChannelOpFuture fut = awaitLastFut(true);

        channel.read(destinationBuffer, position, fut, H);

        try {
            return fut.getUninterruptibly();
        } catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destinationBuffer, long position) throws IOException {
        ChannelOpFuture fut = awaitLastFut(false);

        channel.read(destinationBuffer, position, fut, H);

        try {
            return fut.getUninterruptibly();
        } catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] buffer, int offset, int length) throws IOException {
        ChannelOpFuture fut = awaitLastFut(false);

        channel.read(ByteBuffer.wrap(buffer, offset, length), position, fut, H);

        try {
            return fut.getUninterruptibly();
        } catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer sourceBuffer) throws IOException {
        ChannelOpFuture fut = awaitLastFut(true);

        channel.write(sourceBuffer, position, fut, H);

        try {
            return fut.getUninterruptibly();
        } catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer sourceBuffer, long position) throws IOException {
        ChannelOpFuture fut = awaitLastFut(false);

        channel.write(sourceBuffer, position, fut, H);

        try {
            return fut.getUninterruptibly();
        } catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] buffer, int offset, int length) throws IOException {
        ChannelOpFuture fut = awaitLastFut(false);

        channel.write(ByteBuffer.wrap(buffer, offset, length), position, fut, H);

        try {
            fut.getUninterruptibly();
        } catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void force() throws IOException {
        channel.force(false);
    }

    /** {@inheritDoc} */
    @Override public long size() throws IOException {
        return channel.size();
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IOException {
        channel.truncate(0);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        channel.close();
    }

    /**
     * Awaits last future if it exists.
     *
     * @return Future for current async operation.
     */
    private ChannelOpFuture awaitLastFut(boolean changePos) throws IOException {
        ChannelOpFuture fut = new ChannelOpFuture(changePos);

        while (true) {
            if (lastFut.compareAndSet(null, fut))
                return fut;
            else
                try {
                    fut.getUninterruptibly(); // Wait for future to complete.
                } catch (IgniteCheckedException e) {
                    throw new IOException(e);
                }
        }
    }

    /** */
    private class ChannelOpFuture extends GridFutureAdapter<Integer> {
        /** */
        private boolean changePos;

        /**
         * @param changePos {@code true} if change channel position.
         */
        public ChannelOpFuture(boolean changePos) {
            this.changePos = changePos;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Integer res, @Nullable Throwable err) {
            if (changePos)
                AsyncFileIO.this.position += res;

            lastFut.set(null);

            // Release waiter and allow next operation to begin.
            return super.onDone(res, err);
        }
    }

    /** */
    private static class CompletionHolderImpl implements CompletionHandler<Integer, GridFutureAdapter<Integer>> {
        /** {@inheritDoc} */
        @Override public void completed(Integer result, GridFutureAdapter<Integer> attachment) {
            attachment.onDone(result);
        }

        /** {@inheritDoc} */
        @Override public void failed(Throwable exc, GridFutureAdapter<Integer> attachment) {
            attachment.onDone(exc);
        }
    }
}
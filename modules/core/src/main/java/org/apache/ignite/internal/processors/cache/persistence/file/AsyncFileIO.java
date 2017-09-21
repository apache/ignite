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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.EnumSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.NotNull;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * File I/O implementation based on {@link AsynchronousFileChannel}.
 */
public class AsyncFileIO implements FileIO {
    /**
     * File channel associated with {@code file}
     */
    private final AsynchronousFileChannel ch;

    /**
     * Channel's position.
     */
    private volatile long position;

    /** */
    private GridConcurrentHashSet<ChannelOpFuture> asyncFuts = new GridConcurrentHashSet<>();

    /** */
    private ThreadLocal<ChannelOpFuture> cachedFut = new ThreadLocal<ChannelOpFuture>() {
        @Override protected ChannelOpFuture initialValue() {
            return new ChannelOpFuture(true);
        }
    };

    /**
     * Creates I/O implementation for specified {@code file}
     *
     * @param file Random access file
     * @param concurrency Async I/O concurrency hint.
     */
    public AsyncFileIO(File file, int concurrency) throws IOException {
        ExecutorService execSvc = Executors.newFixedThreadPool(concurrency, new ThreadFactory() {
            AtomicInteger threadNum = new AtomicInteger();

            @Override public Thread newThread(@NotNull Runnable r) {
                return new Thread(Thread.currentThread().getThreadGroup(), r,
                    "ignite-async-io-thread-" + threadNum.getAndIncrement());
            }
        });

        this.ch = AsynchronousFileChannel.open(file.toPath(), EnumSet.of(CREATE, READ, WRITE), execSvc);
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
        ChannelOpFuture fut = cachedFut.get();
        fut.reset();

        ch.read(destinationBuffer, position, null, fut);

        try {
            return fut.getUninterruptibly();
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destinationBuffer, long position) throws IOException {
        ChannelOpFuture fut = new ChannelOpFuture(false);

        ch.read(destinationBuffer, position, null, fut);

        try {
            return fut.getUninterruptibly();
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] buffer, int offset, int length) throws IOException {
        ChannelOpFuture fut = cachedFut.get();
        fut.reset();

        ch.read(ByteBuffer.wrap(buffer, offset, length), position, null, fut);

        try {
            return fut.getUninterruptibly();
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer sourceBuffer) throws IOException {
        ChannelOpFuture fut = cachedFut.get();
        fut.reset();

        ch.write(sourceBuffer, position, null, fut);

        try {
            return fut.getUninterruptibly();
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer sourceBuffer, long position) throws IOException {
        ChannelOpFuture fut = new ChannelOpFuture(false);

        asyncFuts.add(fut);

        ch.write(sourceBuffer, position, null, fut);

        try {
            return fut.getUninterruptibly();
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] buffer, int offset, int length) throws IOException {
        ChannelOpFuture fut = cachedFut.get();
        fut.reset();

        ch.write(ByteBuffer.wrap(buffer, offset, length), position, null, fut);

        try {
            fut.getUninterruptibly();
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void force() throws IOException {
        ch.force(false);
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
    private class ChannelOpFuture extends GridFutureAdapter<Integer> implements CompletionHandler<Integer, Void>  {
        /** */
        private boolean advancePos;

        /**
         * @param advancePos {@code true} if change channel position.
         */
        public ChannelOpFuture(boolean advancePos) {
            this.advancePos = advancePos;
        }

        /** {@inheritDoc} */
        @Override public void completed(Integer result, Void attachment) {
            if (advancePos) {
                if (result != -1)
                    AsyncFileIO.this.position += result;
            }
            else
                asyncFuts.remove(this);

            // Release waiter and allow next operation to begin.
            super.onDone(result, null);
        }

        /** {@inheritDoc} */
        @Override public void failed(Throwable exc, Void attachment) {
            if (!advancePos)
                asyncFuts.remove(this);

            super.onDone(exc);
        }
    }
}
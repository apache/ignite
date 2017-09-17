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
    private final AsynchronousFileChannel ch;

    /**
     * Channel's position.
     */
    private volatile long position;

    /** */
    private AtomicReference<GridFutureAdapter<Integer>> lastFut = new AtomicReference<>();

    /**
     * Creates I/O implementation for specified {@code file}
     *
     * @param file Random access file
     */
    public AsyncFileIO(File file) throws IOException {
        this.ch = AsynchronousFileChannel.open(file.toPath(),
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

        ch.read(destinationBuffer, position, null, fut);

        try {
            return fut.getUninterruptibly();
        } catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destinationBuffer, long position) throws IOException {
        ChannelOpFuture fut = new ChannelOpFuture(false);

        ch.read(destinationBuffer, position, null, fut);

        try {
            return fut.getUninterruptibly();
        } catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] buffer, int offset, int length) throws IOException {
        ChannelOpFuture fut = awaitLastFut(true);

        ch.read(ByteBuffer.wrap(buffer, offset, length), position, null, fut);

        try {
            return fut.getUninterruptibly();
        } catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer sourceBuffer) throws IOException {
        ChannelOpFuture fut = awaitLastFut(true);

        ch.write(sourceBuffer, position, null, fut);

        try {
            return fut.getUninterruptibly();
        } catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer sourceBuffer, long position) throws IOException {
        ChannelOpFuture fut = new ChannelOpFuture(false);

        ch.write(sourceBuffer, position, null, fut);

        try {
            return fut.getUninterruptibly();
        } catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] buffer, int offset, int length) throws IOException {
        ChannelOpFuture fut = awaitLastFut(true);

        ch.write(ByteBuffer.wrap(buffer, offset, length), position, null, fut);

        try {
            fut.getUninterruptibly();
        } catch (IgniteCheckedException e) {
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
        ch.close();
    }

    /**
     * Awaits last future if it exists.
     *
     * @param advancePos {@code true} to advance position.
     * @return Future for current async operation.
     *
     * @throws IOException
     */
    private ChannelOpFuture awaitLastFut(boolean advancePos) throws IOException {
        ChannelOpFuture fut = new ChannelOpFuture(advancePos);

        while (true) {
            GridFutureAdapter<Integer> curFut = lastFut.get();

            if (curFut == null && lastFut.compareAndSet(null, fut))
                return fut;
            else if (curFut != null)
                try {
                    curFut.get(); // Wait for future to complete.
                } catch (IgniteCheckedException e) {
                    throw new IOException(e);
                }
        }
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
                assert lastFut.get() == this;

                if (result != -1)
                    AsyncFileIO.this.position += result;

                lastFut.set(null);
            }

            // Release waiter and allow next operation to begin.
            super.onDone(result, null);
        }

        /** {@inheritDoc} */
        @Override public void failed(Throwable exc, Void attachment) {
            if (advancePos)
                lastFut.set(null);

            super.onDone(exc);
        }
    }
}
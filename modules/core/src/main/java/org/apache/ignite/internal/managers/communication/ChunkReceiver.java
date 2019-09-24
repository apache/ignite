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

package org.apache.ignite.internal.managers.communication;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Chunk receiver used to read a given {@link SocketChannel} by chunks of predefined size into
 * an allocated {@link ByteBuffer}.
 */
class ChunkReceiver extends TransmissionReceiver {
    /** Handler which accepts received data from the given socket. */
    private final Consumer<ByteBuffer> hnd;

    /** Destination buffer to transfer data to\from. */
    private ByteBuffer buf;

    /**
     * @param meta Initial file meta info.
     * @param chunkSize Size of the chunk.
     * @param stopChecker Node stop or prcoess interrupt checker.
     * @param hnd Transmission handler to process received data.
     * @param log Ignite logger.
     */
    public ChunkReceiver(
        TransmissionMeta meta,
        int chunkSize,
        BooleanSupplier stopChecker,
        Consumer<ByteBuffer> hnd,
        IgniteLogger log
    ) {
        super(meta, stopChecker, log, chunkSize);

        A.notNull(hnd, "ChunkHandler must be provided by transmission handler");

        this.hnd = hnd;

        buf = ByteBuffer.allocate(chunkSize);
        buf.order(ByteOrder.nativeOrder());
    }

    /** {@inheritDoc} */
    @Override protected void readChunk(ReadableByteChannel ch) throws IOException {
        assert buf != null : "Buffer cannot be null since it is used to receive the data from channel: " + this;

        buf.rewind();

        int read = 0;
        int res;

        // Read data from input channel until the buffer will be completely filled
        // (buf.remaining() returns 0) or partitially filled buffer if it was the last chunk.
        while (true) {
            res = ch.read(buf);

            // Read will return -1 if remote node close connection.
            if (res < 0) {
                if (transferred + read != meta.count()) {
                    throw new IOException("Input data channel reached its end, but file has not fully loaded " +
                        "[transferred=" + transferred + ", read=" + read + ", total=" + meta.count() + ']');
                }

                break;
            }

            read += res;

            if (read == buf.capacity() || buf.position() == buf.capacity())
                break;
        }

        if (read == 0)
            return;

        transferred += read;

        buf.flip();

        hnd.accept(buf);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChunkReceiver.class, this, "super", super.toString());
    }
}

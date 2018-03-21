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

package org.apache.ignite.internal.util.nio.compression;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.GridNioException;

import static org.apache.ignite.internal.util.nio.compression.CompressionEngineResult.BUFFER_OVERFLOW;
import static org.apache.ignite.internal.util.nio.compression.CompressionEngineResult.OK;

/**
 * Blocking compression handler.
 */
public final class BlockingCompressionHandler {
    /** Size of a net buffers. */
    private static final int NET_BUF_SIZE = 1 << 15;

    /** Logger. */
    private final IgniteLogger log;

    /** Compression engine. */
    private final CompressionEngine compressionEngine;

    /** Output buffer into which compressed data will be written. */
    private ByteBuffer outNetBuf;

    /** Input buffer from which compression engine will decompress data. */
    private ByteBuffer inNetBuf;

    /** Application buffer. */
    private ByteBuffer appBuf;

    /**
     * @param compressionEngine compressionEngine.
     * @param directBuf Direct buffer flag.
     * @param order Byte order.
     * @param log Logger.
     */
    public BlockingCompressionHandler(CompressionEngine compressionEngine,
        boolean directBuf,
        ByteOrder order,
        IgniteLogger log) {
        assert compressionEngine != null;
        assert order != null;
        assert log != null;

        this.log = log;
        this.compressionEngine = compressionEngine;

        outNetBuf = directBuf ? ByteBuffer.allocateDirect(NET_BUF_SIZE) : ByteBuffer.allocate(NET_BUF_SIZE);
        outNetBuf.order(order);

        // Initially buffer is empty.
        outNetBuf.position(0);
        outNetBuf.limit(0);

        inNetBuf = directBuf ? ByteBuffer.allocateDirect(NET_BUF_SIZE) : ByteBuffer.allocate(NET_BUF_SIZE);
        inNetBuf.order(order);

        appBuf = ByteBuffer.allocate(NET_BUF_SIZE * 2);
        appBuf.order(order);

        if (log.isDebugEnabled())
            log.debug("Started compression session [netBufSize=" + NET_BUF_SIZE +
                ", appBufSize=" + appBuf.capacity() + ']');
    }

    /** */
    public ByteBuffer inputBuffer(){
        return inNetBuf;
    }

    /**
     * @return Application buffer with decompressed data.
     */
    public ByteBuffer applicationBuffer() {
        appBuf.flip();

        return appBuf;
    }

    /**
     * Compress data to be written to the network.
     *
     * @param src data to compress.
     * @throws IOException on errors.
     * @return Output buffer with compressed data.
     */
    public ByteBuffer compress(ByteBuffer src) throws IOException {
        assert src != null;

        // The data buffer is (must be) empty, we can reuse the entire
        // buffer.
        outNetBuf.clear();

        // Loop until there is no more data in src
        while (src.hasRemaining()) {
            CompressionEngineResult res = compressionEngine.compress(src, outNetBuf);

            if (res == BUFFER_OVERFLOW) {
                assert outNetBuf.capacity() <= Integer.MAX_VALUE / 2;

                outNetBuf = expandBuffer(outNetBuf, outNetBuf.capacity() * 2);

                if (log.isDebugEnabled())
                    log.debug("Expanded output net buffer [outNetBufCapacity=" + outNetBuf.capacity() + ']');
            }
        }

        outNetBuf.flip();

        return outNetBuf;
    }

    /**
     * Called by compression filter when new message was received.
     *
     * @param buf Received message.
     * @throws GridNioException If exception occurred while forwarding events to underlying filter.
     * @throws IOException If failed to process compress data.
     */
    public ByteBuffer decompress(ByteBuffer buf) throws IOException {
        assert buf != null;

        appBuf.clear();

        if (buf.limit() > inNetBuf.remaining()) {
            assert inNetBuf.capacity() + buf.limit() * 2 <= Integer.MAX_VALUE;

            inNetBuf = expandBuffer(inNetBuf, inNetBuf.capacity() + buf.limit() * 2);

            if (log.isDebugEnabled())
                log.debug("Expanded buffer [inNetBufCapacity=" + inNetBuf.capacity() + ']');
        }

        inNetBuf.put(buf);

        unwrapData();

        appBuf.flip();

        return appBuf;
    }

    /**
     * Unwraps user data to the application buffer.
     *
     * @throws IOException If failed to process compress data.
     */
    private void unwrapData() throws IOException {
        if (log.isDebugEnabled())
            log.debug("Unwrapping received data.");

        // Flip buffer so we can read it.
        inNetBuf.flip();

        CompressionEngineResult res;

        do {
            res = compressionEngine.decompress(inNetBuf, appBuf);

            if (res == BUFFER_OVERFLOW) {
                assert appBuf.capacity() <= Integer.MAX_VALUE / 2;

                appBuf = expandBuffer(appBuf, appBuf.capacity() * 2);
            }
        }
        while (res == OK || res == BUFFER_OVERFLOW);

        // Prepare to be written again.
        inNetBuf.compact();
    }

    /**
     * Expands the given byte buffer to the requested capacity.
     *
     * @param original Original byte buffer.
     * @param cap Requested capacity.
     * @return Expanded byte buffer.
     */
    private static ByteBuffer expandBuffer(ByteBuffer original, int cap) {
        ByteBuffer res = original.isDirect() ? ByteBuffer.allocateDirect(cap) : ByteBuffer.allocate(cap);

        res.order(original.order());

        original.flip();

        res.put(original);

        return res;
    }
}

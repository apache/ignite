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
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.GridNioException;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.nio.compression.CompressionEngineResult.BUFFER_OVERFLOW;
import static org.apache.ignite.internal.util.nio.compression.CompressionEngineResult.OK;

/**
 * Class that encapsulate the compression state, compress and decompress logic.
 */
public final class GridNioCompressionHandler {
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

    /** Lock. */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Creates handler.
     *
     * @param engine Compression engine instance for this handler.
     * @param log Logger to use.
     * @param directBuf Direct buffer flag.
     * @param order Byte order.
     * @param encBuf Encoded buffer to be used.
     */
    public GridNioCompressionHandler(
        CompressionEngine engine,
        boolean directBuf,
        ByteOrder order,
        IgniteLogger log,
        @Nullable ByteBuffer encBuf) {
        assert engine != null;
        assert order != null;
        assert log != null;

        this.log = log;

        compressionEngine = engine;

        outNetBuf = directBuf ? ByteBuffer.allocateDirect(NET_BUF_SIZE) : ByteBuffer.allocate(NET_BUF_SIZE);
        outNetBuf.order(order);

        inNetBuf = directBuf ? ByteBuffer.allocateDirect(NET_BUF_SIZE) : ByteBuffer.allocate(NET_BUF_SIZE);
        inNetBuf.order(order);

        if (encBuf != null) {
            encBuf.flip();

            // Buffer contains bytes read but not handled by compressionEngine at BlockingCompressionHandler.
            inNetBuf.put(encBuf);
        }

        // Initially buffer is empty.
        outNetBuf.position(0);
        outNetBuf.limit(0);

        int appBufSize = NET_BUF_SIZE * 2;

        appBuf = directBuf ? ByteBuffer.allocateDirect(appBufSize) : ByteBuffer.allocate(appBufSize);
        appBuf.order(order);

        if (log.isDebugEnabled())
            log.debug("Started compression handler [netBufSize=" + NET_BUF_SIZE + ", appBufSize=" + appBufSize + ']');
    }

    /**
     * Compress data.
     *
     * @param buf Byte buffer to compress.
     * @return Output buffer with compressed data.
     * @throws IOException If failed to compress data.
     */
    public ByteBuffer compress(ByteBuffer buf) throws IOException {
        assert buf != null;
        assert lock.isHeldByCurrentThread();

        // The data buffer is (must be) empty, we can reuse the entire buffer.
        outNetBuf.clear();

        // Loop until there is no more data in buffer.
        while (buf.hasRemaining()) {
            CompressionEngineResult res = compressionEngine.compress(buf, outNetBuf);

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
     * Decompress data.
     *
     * @param buf Byte buffer to decomress.
     * @return Application buffer with decompressed data.
     * @throws IOException If failed to decompress data.
     */
    public ByteBuffer decompress(ByteBuffer buf) throws IOException {
        assert buf != null;

        if (buf.limit() > inNetBuf.remaining()) {
            assert inNetBuf.capacity() <= Integer.MAX_VALUE / 2;

            inNetBuf = expandBuffer(inNetBuf, inNetBuf.capacity() * 2);

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
     * @throws IOException On error.
     */
    private void unwrapData() throws IOException {
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
     * Lock handler.
     */
    public void lock() {
        lock.lock();
    }

    /**
     * Unlock handler.
     */
    public void unlock() {
        lock.unlock();
    }

    /**
     * @return Application buffer with decompressed data.
     */
    public ByteBuffer getApplicationBuffer() {
        return appBuf;
    }

    /**
     * @return Input buffer.
     */
    public ByteBuffer getInputBuffer() {
        return inNetBuf;
    }

    /**
     * @return Output buffer.
     */
    public ByteBuffer getOutputBuffer() {
        return outNetBuf;
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

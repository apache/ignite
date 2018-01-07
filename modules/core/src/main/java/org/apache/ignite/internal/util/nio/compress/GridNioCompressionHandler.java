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

package org.apache.ignite.internal.util.nio.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.GridNioException;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.lang.IgniteInClosure;

import static org.apache.ignite.internal.util.nio.compress.CompressionEngineResult.BUFFER_OVERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressionEngineResult.OK;

/**
 * Class that encapsulate the per-session compression state, compress and decompress logic.
 */
class GridNioCompressionHandler extends ReentrantLock {
    /** Size of a net buffers. */
    private static final int netBufSize = 32768;

    /** */
    private static final long serialVersionUID = 0L;

    /** Grid logger. */
    private IgniteLogger log;

    /** Compress engine. */
    private CompressionEngine compressEngine;

    /** Order. */
    private ByteOrder order;

    /** Allocate direct buffer or heap buffer. */
    private boolean directBuf;

    /** Session of this handler. */
    private GridNioSession ses;

    /** Output buffer into which compressed data will be written. */
    private ByteBuffer outNetBuf;

    /** Input buffer from which compress engine will decrypt data. */
    private ByteBuffer inNetBuf;

    /** Application buffer. */
    private ByteBuffer appBuf;

    /** Parent filter. */
    private GridNioCompressionFilter parent;

    /**
     * Creates handler.
     *
     * @param parent Parent compress filter.
     * @param ses Session for which this handler was created.
     * @param engine compress engine instance for this handler.
     * @param log Logger to use.
     * @param directBuf Direct buffer flag.
     * @param order Byte order.
     * @param encBuf encoded buffer to be used.
     */
    GridNioCompressionHandler(GridNioCompressionFilter parent,
        GridNioSession ses,
        CompressionEngine engine,
        boolean directBuf,
        ByteOrder order,
        IgniteLogger log,
        ByteBuffer encBuf) {
        assert parent != null;
        assert ses != null;
        assert engine != null;
        assert log != null;

        this.parent = parent;
        this.ses = ses;
        this.order = order;
        this.directBuf = directBuf;
        this.log = log;

        compressEngine = engine;

        outNetBuf = directBuf ? ByteBuffer.allocateDirect(netBufSize) : ByteBuffer.allocate(netBufSize);
        outNetBuf.order(order);

        inNetBuf = directBuf ? ByteBuffer.allocateDirect(netBufSize) : ByteBuffer.allocate(netBufSize);
        inNetBuf.order(order);

        if (encBuf != null) {
            encBuf.flip();

            inNetBuf.put(encBuf); // Buffer contains bytes read but not handled by compressEngine at BlockingCompressionHandler.
        }

        // Initially buffer is empty.
        outNetBuf.position(0);
        outNetBuf.limit(0);

        int appBufSize = netBufSize * 2;

        appBuf = directBuf ? ByteBuffer.allocateDirect(appBufSize) : ByteBuffer.allocate(appBufSize);
        appBuf.order(order);

        if (log.isDebugEnabled())
            log.debug("Started compress session [netBufSize=" + netBufSize + ", appBufSize=" + appBufSize + ']');
    }

    /**
     * @return Application buffer with decoded data.
     */
    ByteBuffer getApplicationBuffer() {
        return appBuf;
    }

    /**
     * Called by compress filter when new message was received.
     *
     * @param buf Received message.
     * @throws GridNioException If exception occurred while forwarding events to underlying filter.
     * @throws IOException If failed to process compress data.
     */
    void messageReceived(ByteBuffer buf) throws IgniteCheckedException, IOException {
        if (buf.limit() > inNetBuf.remaining()) {
            inNetBuf = expandBuffer(inNetBuf, inNetBuf.capacity() + buf.limit() * 2);

            if (log.isDebugEnabled())
                log.debug("Expanded buffer [inNetBufCapacity=" + inNetBuf.capacity() + "], ses=" + ses + ", ");
        }

        // append buf to inNetBuffer
        inNetBuf.put(buf);

        unwrapData();
    }

    /**
     * Compress data to be written to the network.
     *
     * @param src data to compress.
     * @throws IOException on errors.
     * @return Output buffer with compressed data.
     */
    ByteBuffer compress(ByteBuffer src) throws IOException {
        assert isHeldByCurrentThread();

        // The data buffer is (must be) empty, we can reuse the entire
        // buffer.
        outNetBuf.clear();

        // Loop until there is no more data in src
        while (src.hasRemaining()) {
            CompressionEngineResult res = compressEngine.wrap(src, outNetBuf);

            if (res == BUFFER_OVERFLOW) {
                outNetBuf = expandBuffer(outNetBuf, Math.max(
                    outNetBuf.position() + src.remaining() * 2, outNetBuf.capacity() * 2));

                if (log.isDebugEnabled())
                    log.debug("Expanded output net buffer [outNetBufCapacity=" + outNetBuf.capacity() + ", ses=" +
                        ses + ']');
            }
        }

        outNetBuf.flip();

        return outNetBuf;
    }

    /**
     * Copies data from out net buffer and passes it to the underlying chain.
     *
     * @return Write future.
     * @param ackC Closure invoked when message ACK is received.
     * @throws GridNioException If send failed.
     */
    GridNioFuture<?> writeNetBuffer(IgniteInClosure<IgniteException> ackC) throws IgniteCheckedException {
        assert isHeldByCurrentThread();

        ByteBuffer cp = copy(outNetBuf);

        return parent.proceedSessionWrite(ses, cp, true, ackC);
    }

    /**
     * Unwraps user data to the application buffer.
     *
     * @throws IOException If failed to process compress data.
     * @throws GridNioException If failed to pass events to the next filter.
     */
    private void unwrapData() throws IgniteCheckedException, IOException {
        if (log.isDebugEnabled())
            log.debug("Unwrapping received data: " + ses);

        // Flip buffer so we can read it.
        inNetBuf.flip();

        CompressionEngineResult res;

        do {
            res = compressEngine.unwrap(inNetBuf, appBuf);

            if (res == BUFFER_OVERFLOW)
                appBuf = expandBuffer(appBuf, appBuf.capacity() * 2);
        }
        while (res == OK || res == BUFFER_OVERFLOW);

        // prepare to be written again
        inNetBuf.compact();
    }

    /**
     * Expands the given byte buffer to the requested capacity.
     *
     * @param original Original byte buffer.
     * @param cap Requested capacity.
     * @return Expanded byte buffer.
     */
    private ByteBuffer expandBuffer(ByteBuffer original, int cap) {
        ByteBuffer res = directBuf ? ByteBuffer.allocateDirect(cap) : ByteBuffer.allocate(cap);

        res.order(order);

        original.flip();

        res.put(original);

        return res;
    }

    /**
     * Copies the given byte buffer.
     *
     * @param original Byte buffer to copy.
     * @return Copy of the original byte buffer.
     */
    private ByteBuffer copy(ByteBuffer original) {
        ByteBuffer cp = directBuf ? ByteBuffer.allocateDirect(original.remaining()) :
            ByteBuffer.allocate(original.remaining());

        cp.order(order);

        cp.put(original);

        cp.flip();

        return cp;
    }
}

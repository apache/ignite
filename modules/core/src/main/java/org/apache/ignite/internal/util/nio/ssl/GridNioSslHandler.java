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

package org.apache.ignite.internal.util.nio.ssl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.GridNioEmbeddedFuture;
import org.apache.ignite.internal.util.nio.GridNioException;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioFutureImpl;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.internal.U;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_TASK;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
import static javax.net.ssl.SSLEngineResult.Status;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_UNDERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.CLOSED;
import static org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter.HANDSHAKE_FUT_META_KEY;

/**
 * Class that encapsulate the per-session SSL state, encoding and decoding logic.
 */
class GridNioSslHandler extends ReentrantLock {
    /** */
    private static final long serialVersionUID = 0L;

    /** Grid logger. */
    private IgniteLogger log;

    /** SSL engine. */
    private SSLEngine sslEngine;

    /** Order. */
    private ByteOrder order;

    /** Allocate direct buffer or heap buffer. */
    private boolean directBuf;

    /** Session of this handler. */
    private GridNioSession ses;

    /** Handshake completion flag. */
    private boolean handshakeFinished;

    /** Flag to initiate session opened event on first handshake. */
    private boolean initHandshakeComplete;

    /** Engine handshake status. */
    private HandshakeStatus handshakeStatus;

    /** Output buffer into which encrypted data will be written. */
    private ByteBuffer outNetBuf;

    /** Input buffer from which SSL engine will decrypt data. */
    private ByteBuffer inNetBuf;

    /** Empty buffer used in handshake procedure.  */
    private ByteBuffer handshakeBuf = ByteBuffer.allocate(0);

    /** Application buffer. */
    private ByteBuffer appBuf;

    /** Parent filter. */
    private GridNioSslFilter parent;

    /** Pre-handshake write requests. */
    private Queue<WriteRequest> deferredWriteQueue = new LinkedList<>();

    /**
     * Creates handler.
     *
     * @param parent Parent SSL filter.
     * @param ses Session for which this handler was created.
     * @param engine SSL engine instance for this handler.
     * @param log Logger to use.
     * @param directBuf Direct buffer flag.
     * @param order Byte order.
     * @param handshake is handshake required.
     * @param encBuf encoded buffer to be used.
     * @throws SSLException If exception occurred when starting SSL handshake.
     */
    GridNioSslHandler(GridNioSslFilter parent,
        GridNioSession ses,
        SSLEngine engine,
        boolean directBuf,
        ByteOrder order,
        IgniteLogger log,
        boolean handshake,
        ByteBuffer encBuf) throws SSLException {
        assert parent != null;
        assert ses != null;
        assert engine != null;
        assert log != null;

        this.parent = parent;
        this.ses = ses;
        this.order = order;
        this.directBuf = directBuf;
        this.log = log;

        sslEngine = engine;

        if (handshake)
            sslEngine.beginHandshake();
        else {
            handshakeFinished = true;
            initHandshakeComplete = true;
        }

        handshakeStatus = sslEngine.getHandshakeStatus();

        // Allocate a little bit more so SSL engine would not return buffer overflow status.
        int netBufSize = sslEngine.getSession().getPacketBufferSize() + 50;

        outNetBuf = directBuf ? ByteBuffer.allocateDirect(netBufSize) : ByteBuffer.allocate(netBufSize);

        outNetBuf.order(order);

        inNetBuf = directBuf ? ByteBuffer.allocateDirect(netBufSize) : ByteBuffer.allocate(netBufSize);

        inNetBuf.order(order);

        if (encBuf != null) {
            encBuf.flip();

            inNetBuf.put(encBuf); // Buffer contains bytes read but not handled by sslEngine at BlockingSslHandler.
        }

        // Initially buffer is empty.
        outNetBuf.position(0);
        outNetBuf.limit(0);

        int appBufSize = Math.max(sslEngine.getSession().getApplicationBufferSize() + 50, netBufSize * 2);

        appBuf = directBuf ? ByteBuffer.allocateDirect(appBufSize) : ByteBuffer.allocate(appBufSize);

        appBuf.order(order);

        if (log.isDebugEnabled())
            log.debug("Started SSL session [netBufSize=" + netBufSize + ", appBufSize=" + appBufSize + ']');
    }

    /**
     * @return Application buffer with decoded data.
     */
    ByteBuffer getApplicationBuffer() {
        return appBuf;
    }

    /**
     * Shuts down the handler.
     */
    void shutdown() {
        try {
            sslEngine.closeInbound();
        }
        catch (SSLException e) {
            // According to javadoc, the only case when exception is thrown is when no close_notify
            // message was received before TCP connection get closed.
            if (log.isDebugEnabled())
                log.debug("Unable to correctly close inbound data stream (will ignore) [msg=" + e.getMessage() +
                    ", ses=" + ses + ']');
        }
    }

    /**
     * Performs handshake procedure with remote peer.
     *
     * @throws GridNioException If filter processing has thrown an exception.
     * @throws SSLException If failed to process SSL data.
     */
    void handshake() throws IgniteCheckedException, SSLException {
        if (log.isDebugEnabled())
            log.debug("Entered handshake(): [handshakeStatus=" + handshakeStatus + ", ses=" + ses + ']');

        lock();

        try {
            boolean loop = true;

            while (loop) {
                switch (handshakeStatus) {
                    case NOT_HANDSHAKING:
                    case FINISHED: {
                        SSLSession sslSes = sslEngine.getSession();

                        if (log.isDebugEnabled())
                            log.debug("Finished ssl handshake [protocol=" + sslSes.getProtocol() + ", cipherSuite=" +
                                sslSes.getCipherSuite() + ", ses=" + ses + ']');

                        handshakeFinished = true;

                        if (!initHandshakeComplete) {
                            initHandshakeComplete = true;

                            GridNioFutureImpl<?> fut = ses.removeMeta(HANDSHAKE_FUT_META_KEY);

                            if (fut != null)
                                fut.onDone();

                            parent.proceedSessionOpened(ses);
                        }

                        loop = false;

                        break;
                    }

                    case NEED_TASK: {
                        if (log.isDebugEnabled())
                            log.debug("Need to run ssl tasks: " + ses);

                        handshakeStatus = runTasks();

                        break;
                    }

                    case NEED_UNWRAP: {
                        if (log.isDebugEnabled())
                            log.debug("Need to unwrap incoming data: " + ses);

                        Status status = unwrapHandshake();

                        if (status == BUFFER_UNDERFLOW && handshakeStatus != FINISHED ||
                            sslEngine.isInboundDone())
                            // Either there is no enough data in buffer or session was closed.
                            loop = false;

                        break;
                    }

                    case NEED_WRAP: {
                        // If the output buffer has remaining data, clear it.
                        if (outNetBuf.hasRemaining())
                            U.warn(log, "Output net buffer has unsent bytes during handshake (will clear): " + ses);

                        outNetBuf.clear();

                        SSLEngineResult res = sslEngine.wrap(handshakeBuf, outNetBuf);

                        outNetBuf.flip();

                        handshakeStatus = res.getHandshakeStatus();

                        if (log.isDebugEnabled())
                            log.debug("Wrapped handshake data [status=" + res.getStatus() + ", handshakeStatus=" +
                                handshakeStatus + ", ses=" + ses + ']');

                        writeNetBuffer();

                        break;
                    }

                    default: {
                        throw new IllegalStateException("Invalid handshake status in handshake method [handshakeStatus=" +
                            handshakeStatus + ", ses=" + ses + ']');
                    }
                }
            }
        }
        finally {
            unlock();
        }

        if (log.isDebugEnabled())
            log.debug("Leaved handshake(): [handshakeStatus=" + handshakeStatus + ", ses=" + ses + ']');
    }

    /**
     * Called by SSL filter when new message was received.
     *
     * @param buf Received message.
     * @throws GridNioException If exception occurred while forwarding events to underlying filter.
     * @throws SSLException If failed to process SSL data.
     */
    void messageReceived(ByteBuffer buf) throws IgniteCheckedException, SSLException {
        if (buf.limit() > inNetBuf.remaining()) {
            inNetBuf = expandBuffer(inNetBuf, inNetBuf.capacity() + buf.limit() * 2);

            appBuf = expandBuffer(appBuf, inNetBuf.capacity() * 2);

            if (log.isDebugEnabled())
                log.debug("Expanded buffers [inNetBufCapacity=" + inNetBuf.capacity() + ", appBufCapacity=" +
                    appBuf.capacity() + ", ses=" + ses + ", ");
        }

        // append buf to inNetBuffer
        inNetBuf.put(buf);

        if (!handshakeFinished)
            handshake();
        else
            unwrapData();

        if (isInboundDone()) {
            int newPosition = buf.position() - inNetBuf.position();

            if (newPosition >= 0) {
                buf.position(newPosition);

                // If we received close_notify but not all bytes has been read by SSL engine, print a warning.
                if (buf.hasRemaining())
                    U.warn(log, "Got unread bytes after receiving close_notify message (will ignore): " + ses);
            }

            inNetBuf.clear();
        }
    }

    /**
     * Encrypts data to be written to the network.
     *
     * @param src data to encrypt.
     * @throws SSLException on errors.
     * @return Output buffer with encrypted data.
     */
    ByteBuffer encrypt(ByteBuffer src) throws SSLException {
        assert handshakeFinished;
        assert isHeldByCurrentThread();

        // The data buffer is (must be) empty, we can reuse the entire
        // buffer.
        outNetBuf.clear();

        // Loop until there is no more data in src
        while (src.hasRemaining()) {
            int outNetRemaining = outNetBuf.capacity() - outNetBuf.position();

            if (outNetRemaining < src.remaining() * 2) {
                outNetBuf = expandBuffer(outNetBuf, Math.max(
                    outNetBuf.position() + src.remaining() * 2, outNetBuf.capacity() * 2));

                if (log.isDebugEnabled())
                    log.debug("Expanded output net buffer [outNetBufCapacity=" + outNetBuf.capacity() + ", ses=" +
                        ses + ']');
            }

            SSLEngineResult res = sslEngine.wrap(src, outNetBuf);

            if (log.isDebugEnabled())
                log.debug("Encrypted data [status=" + res.getStatus() + ", handshakeStaus=" +
                    res.getHandshakeStatus() + ", ses=" + ses + ']');

            if (res.getStatus() == SSLEngineResult.Status.OK) {
                if (res.getHandshakeStatus() == NEED_TASK)
                    runTasks();
            }
            else
                throw new SSLException("Failed to encrypt data (SSL engine error) [status=" + res.getStatus() +
                    ", handshakeStatus=" + res.getHandshakeStatus() + ", ses=" + ses + ']');
        }

        outNetBuf.flip();

        return outNetBuf;
    }

    /**
     * Checks if SSL handshake is finished.
     *
     * @return {@code True} if handshake is finished.
     */
    boolean isHandshakeFinished() {
        return handshakeFinished;
    }

    /**
     * @return {@code True} if inbound data stream has ended, i.e. SSL engine received
     * <tt>close_notify</tt> message.
     */
    boolean isInboundDone() {
        return sslEngine.isInboundDone();
    }

    /**
     * @return {@code True} if outbound data stream has closed, i.e. SSL engine encoded
     * <tt>close_notify</tt> message.
     */
    boolean isOutboundDone() {
        return sslEngine.isOutboundDone();
    }

    /**
     * Adds write request to the queue.
     *
     * @param buf Buffer to write.
     * @return Write future.
     */
    GridNioFuture<?> deferredWrite(ByteBuffer buf) {
        assert isHeldByCurrentThread();

        GridNioEmbeddedFuture<Object> fut = new GridNioEmbeddedFuture<>();

        ByteBuffer cp = copy(buf);

        deferredWriteQueue.offer(new WriteRequest(fut, cp));

        return fut;
    }

    /**
     * Flushes all deferred write events.
     * @throws GridNioException If failed to forward writes to the filter.
     */
    void flushDeferredWrites() throws IgniteCheckedException {
        assert isHeldByCurrentThread();
        assert handshakeFinished;

        while (!deferredWriteQueue.isEmpty()) {
            WriteRequest req = deferredWriteQueue.poll();

            req.future().onDone((GridNioFuture<Object>)parent.proceedSessionWrite(ses, req.buffer(), true));
        }
    }

    /**
     * Writes close_notify message to the network output buffer.
     *
     * @throws SSLException If wrap failed or SSL engine does not get closed
     * after wrap.
     * @return {@code True} if <tt>close_notify</tt> message was encoded, {@code false} if outbound
     *      stream was already closed.
     */
    boolean closeOutbound() throws SSLException {
        assert isHeldByCurrentThread();

        if (!sslEngine.isOutboundDone()) {
            sslEngine.closeOutbound();

            outNetBuf.clear();

            SSLEngineResult res = sslEngine.wrap(handshakeBuf, outNetBuf);

            if (res.getStatus() != CLOSED)
                throw new SSLException("Incorrect SSL engine status after closeOutbound call [status=" +
                    res.getStatus() + ", handshakeStatus=" + res.getHandshakeStatus() + ", ses=" + ses + ']');

            outNetBuf.flip();

            return true;
        }

        return false;
    }

    /**
     * Copies data from out net buffer and passes it to the underlying chain.
     *
     * @return Write future.
     * @throws GridNioException If send failed.
     */
    GridNioFuture<?> writeNetBuffer() throws IgniteCheckedException {
        assert isHeldByCurrentThread();

        ByteBuffer cp = copy(outNetBuf);

        return parent.proceedSessionWrite(ses, cp, true);
    }

    /**
     * Unwraps user data to the application buffer.
     *
     * @throws SSLException If failed to process SSL data.
     * @throws GridNioException If failed to pass events to the next filter.
     */
    private void unwrapData() throws IgniteCheckedException, SSLException {
        if (log.isDebugEnabled())
            log.debug("Unwrapping received data: " + ses);

        // Flip buffer so we can read it.
        inNetBuf.flip();

        SSLEngineResult res = unwrap0();

        // prepare to be written again
        inNetBuf.compact();

        checkStatus(res);

        renegotiateIfNeeded(res);
    }

    /**
     * Unwraps handshake data and processes it.
     *
     * @return Status.
     * @throws SSLException If SSL exception occurred while unwrapping.
     * @throws GridNioException If failed to pass event to the next filter.
     */
    private Status unwrapHandshake() throws SSLException, IgniteCheckedException {
        // Flip input buffer so we can read the collected data.
        inNetBuf.flip();

        SSLEngineResult res = unwrap0();
        handshakeStatus = res.getHandshakeStatus();

        checkStatus(res);

        // If handshake finished, no data was produced, and the status is still ok,
        // try to unwrap more
        if (handshakeStatus == FINISHED && res.getStatus() == Status.OK && inNetBuf.hasRemaining()) {
            res = unwrap0();

            handshakeStatus = res.getHandshakeStatus();

            // prepare to be written again
            inNetBuf.compact();

            renegotiateIfNeeded(res);
        }
        else
            // prepare to be written again
            inNetBuf.compact();

        return res.getStatus();
    }

    /**
     * Check status and retry the negotiation process if needed.
     *
     * @param res Result.
     * @throws GridNioException If exception occurred during handshake.
     * @throws SSLException If failed to process SSL data
     */
    private void renegotiateIfNeeded(SSLEngineResult res) throws IgniteCheckedException, SSLException {
        if (res.getStatus() != CLOSED && res.getStatus() != BUFFER_UNDERFLOW
            && res.getHandshakeStatus() != NOT_HANDSHAKING) {
            // Renegotiation required.
            handshakeStatus = res.getHandshakeStatus();

            if (log.isDebugEnabled())
                log.debug("Renegotiation requested [status=" + res.getStatus() + ", handshakeStatus = " +
                    handshakeStatus + "ses=" + ses + ']');

            handshakeFinished = false;

            handshake();
        }
    }

    /**
     * @param res SSL engine result.
     * @throws SSLException If status is not acceptable.
     */
    private void checkStatus(SSLEngineResult res)
        throws SSLException {

        SSLEngineResult.Status status = res.getStatus();

        if (status != Status.OK && status != CLOSED && status != BUFFER_UNDERFLOW)
            throw new SSLException("Failed to unwrap incoming data (SSL engine error) [ses" + ses + ", status=" +
                status + ']');
    }

    /**
     * Performs raw unwrap from network read buffer.
     *
     * @return Result.
     * @throws SSLException If SSL exception occurs.
     */
    private SSLEngineResult unwrap0() throws SSLException {
        SSLEngineResult res;

        do {
            res = sslEngine.unwrap(inNetBuf, appBuf);

            if (log.isDebugEnabled())
                log.debug("Unwrapped raw data [status=" + res.getStatus() + ", handshakeStatus=" +
                    res.getHandshakeStatus() + ", ses=" + ses + ']');

            if (res.getStatus() == Status.BUFFER_OVERFLOW)
                appBuf = expandBuffer(appBuf, appBuf.capacity() * 2);
        }
        while ((res.getStatus() == Status.OK || res.getStatus() == Status.BUFFER_OVERFLOW) &&
            (handshakeFinished && res.getHandshakeStatus() == NOT_HANDSHAKING || res.getHandshakeStatus() == NEED_UNWRAP));

        return res;
    }

    /**
     * Runs all tasks needed to continue SSL work.
     *
     * @return Handshake status after running all tasks.
     */
    private HandshakeStatus runTasks() {
        Runnable runnable;

        while ((runnable = sslEngine.getDelegatedTask()) != null) {
            if (log.isDebugEnabled())
                log.debug("Running SSL engine task [task=" + runnable + ", ses=" + ses + ']');

            runnable.run();
        }

        if (log.isDebugEnabled())
            log.debug("Finished running SSL engine tasks [handshakeStatus=" + sslEngine.getHandshakeStatus() +
                ", ses=" + ses + ']');

        return sslEngine.getHandshakeStatus();
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

    /**
     * Write request for cases while handshake is not finished yet.
     */
    private static class WriteRequest {
        /** Future that should be completed. */
        private GridNioEmbeddedFuture<Object> fut;

        /** Buffer needed to be written. */
        private ByteBuffer buf;

        /**
         * Creates write request.
         *
         * @param fut Future.
         * @param buf Buffer to write.
         */
        private WriteRequest(GridNioEmbeddedFuture<Object> fut, ByteBuffer buf) {
            this.fut = fut;
            this.buf = buf;
        }

        /**
         * @return Future.
         */
        public GridNioEmbeddedFuture<Object> future() {
            return fut;
        }

        /**
         * @return Buffer.
         */
        public ByteBuffer buffer() {
            return buf;
        }
    }
}
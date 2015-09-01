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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.GridNioException;
import org.apache.ignite.internal.util.typedef.internal.U;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_TASK;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_UNDERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.CLOSED;
import static javax.net.ssl.SSLEngineResult.Status.OK;

/**
 *
 */
public class BlockingSslHandler {
    /** Logger. */
    private IgniteLogger log;

    /** Socket channel. */
    private SocketChannel ch;

    /** Order. */
    private final ByteOrder order;

    /** SSL engine. */
    private final SSLEngine sslEngine;

    /** Handshake completion flag. */
    private boolean handshakeFinished;

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

    /**
     * @param sslEngine SSLEngine.
     * @param ch Socket channel.
     * @param directBuf Direct buffer flag.
     * @param order Byte order.
     * @param log Logger.
     */
    public BlockingSslHandler(SSLEngine sslEngine,
        SocketChannel ch,
        boolean directBuf,
        ByteOrder order,
        IgniteLogger log)
        throws SSLException {
        this.ch = ch;
        this.log = log;
        this.sslEngine = sslEngine;
        this.order = order;

        // Allocate a little bit more so SSL engine would not return buffer overflow status.
        int netBufSize = sslEngine.getSession().getPacketBufferSize() + 50;

        outNetBuf = directBuf ? ByteBuffer.allocateDirect(netBufSize) : ByteBuffer.allocate(netBufSize);
        outNetBuf.order(order);

        // Initially buffer is empty.
        outNetBuf.position(0);
        outNetBuf.limit(0);

        inNetBuf = directBuf ? ByteBuffer.allocateDirect(netBufSize) : ByteBuffer.allocate(netBufSize);
        inNetBuf.order(order);

        appBuf = allocateAppBuff();

        handshakeStatus = sslEngine.getHandshakeStatus();

        if (log.isDebugEnabled())
            log.debug("Started SSL session [netBufSize=" + netBufSize + ", appBufSize=" + appBuf.capacity() + ']');
    }

    /**
     * Performs handshake procedure with remote peer.
     *
     * @throws GridNioException If filter processing has thrown an exception.
     * @throws SSLException If failed to process SSL data.
     */
    public boolean handshake() throws IgniteCheckedException, SSLException {
        if (log.isDebugEnabled())
            log.debug("Entered handshake. Handshake status: " + handshakeStatus + '.');

        sslEngine.beginHandshake();

        handshakeStatus = sslEngine.getHandshakeStatus();

        boolean loop = true;

        while (loop) {
            switch (handshakeStatus) {
                case NOT_HANDSHAKING:
                case FINISHED: {
                    handshakeFinished = true;

                    loop = false;

                    break;
                }

                case NEED_TASK: {
                    handshakeStatus = runTasks();

                    break;
                }

                case NEED_UNWRAP: {
                    Status status = unwrapHandshake();

                    handshakeStatus = sslEngine.getHandshakeStatus();

                    if (status == BUFFER_UNDERFLOW && sslEngine.isInboundDone())
                        // Either there is no enough data in buffer or session was closed.
                        loop = false;

                    break;
                }

                case NEED_WRAP: {
                    // If the output buffer has remaining data, clear it.
                    if (outNetBuf.hasRemaining())
                        U.warn(log, "Output net buffer has unsent bytes during handshake (will clear). ");

                    outNetBuf.clear();

                    SSLEngineResult res = sslEngine.wrap(handshakeBuf, outNetBuf);

                    outNetBuf.flip();

                    handshakeStatus = res.getHandshakeStatus();

                    if (log.isDebugEnabled())
                        log.debug("Wrapped handshake data [status=" + res.getStatus() + ", handshakeStatus=" +
                        handshakeStatus + ']');

                    writeNetBuffer();

                    break;
                }

                default: {
                    throw new IllegalStateException("Invalid handshake status in handshake method [handshakeStatus=" +
                        handshakeStatus + ']');
                }
            }
        }

        if (log.isDebugEnabled())
            log.debug("Leaved handshake. Handshake status:" + handshakeStatus + '.');

        return handshakeFinished;
    }

    /**
     * @return Application buffer with decoded data.
     */
    public ByteBuffer applicationBuffer() {
        appBuf.flip();

        return appBuf;
    }

    /**
     * Encrypts data to be written to the network.
     *
     * @param src data to encrypt.
     * @throws SSLException on errors.
     * @return Output buffer with encrypted data.
     */
    public ByteBuffer encrypt(ByteBuffer src) throws SSLException {
        assert handshakeFinished;

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
                    log.debug("Expanded output net buffer: " + outNetBuf.capacity());
            }

            SSLEngineResult res = sslEngine.wrap(src, outNetBuf);

            if (log.isDebugEnabled())
                log.debug("Encrypted data [status=" + res.getStatus() + ", handshakeStaus=" +
                    res.getHandshakeStatus() + ']');

            if (res.getStatus() == OK) {
                if (res.getHandshakeStatus() == NEED_TASK)
                    runTasks();
            }
            else
                throw new SSLException("Failed to encrypt data (SSL engine error) [status=" + res.getStatus() +
                    ", handshakeStatus=" + res.getHandshakeStatus() + ']');
        }

        outNetBuf.flip();

        return outNetBuf;
    }

    /**
     * Called by SSL filter when new message was received.
     *
     * @param buf Received message.
     * @throws GridNioException If exception occurred while forwarding events to underlying filter.
     * @throws SSLException If failed to process SSL data.
     */
    public ByteBuffer decode(ByteBuffer buf) throws IgniteCheckedException, SSLException {
        inNetBuf.clear();

        if (buf.limit() > inNetBuf.remaining()) {
            inNetBuf = expandBuffer(inNetBuf, inNetBuf.capacity() + buf.limit() * 2);

            appBuf = expandBuffer(appBuf, inNetBuf.capacity() * 2);

            if (log.isDebugEnabled())
                log.debug("Expanded buffers [inNetBufCapacity=" + inNetBuf.capacity() + ", appBufCapacity=" +
                    appBuf.capacity() + ']');
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
                    U.warn(log, "Got unread bytes after receiving close_notify message (will ignore).");
            }

            inNetBuf.clear();
        }

        appBuf.flip();

        return appBuf;
    }

    /**
     * @return {@code True} if inbound data stream has ended, i.e. SSL engine received
     * <tt>close_notify</tt> message.
     */
    boolean isInboundDone() {
        return sslEngine.isInboundDone();
    }

    /**
     * Unwraps user data to the application buffer.
     *
     * @throws SSLException If failed to process SSL data.
     * @throws GridNioException If failed to pass events to the next filter.
     */
    private void unwrapData() throws IgniteCheckedException, SSLException {
        if (log.isDebugEnabled())
            log.debug("Unwrapping received data.");

        // Flip buffer so we can read it.
        inNetBuf.flip();

        SSLEngineResult res = unwrap0();

        // prepare to be written again
        inNetBuf.compact();

        checkStatus(res);

        renegotiateIfNeeded(res);
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
                log.debug("Running SSL engine task: " + runnable + '.');

            runnable.run();
        }

        if (log.isDebugEnabled())
            log.debug("Finished running SSL engine tasks. HandshakeStatus: " + sslEngine.getHandshakeStatus());

        return sslEngine.getHandshakeStatus();
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
        readFromNet();

        inNetBuf.flip();

        SSLEngineResult res = unwrap0();
        handshakeStatus = res.getHandshakeStatus();

        checkStatus(res);

        // If handshake finished, no data was produced, and the status is still ok,
        // try to unwrap more
        if (handshakeStatus == FINISHED && res.getStatus() == OK && inNetBuf.hasRemaining()) {
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
                    res.getHandshakeStatus() + ']');

            if (res.getStatus() == Status.BUFFER_OVERFLOW)
                appBuf = expandBuffer(appBuf, appBuf.capacity() * 2);
        }
        while ((res.getStatus() == OK || res.getStatus() == Status.BUFFER_OVERFLOW) &&
            (handshakeFinished && res.getHandshakeStatus() == NOT_HANDSHAKING
                || res.getHandshakeStatus() == NEED_UNWRAP));

        return res;
    }

    /**
     * @param res SSL engine result.
     * @throws SSLException If status is not acceptable.
     */
    private void checkStatus(SSLEngineResult res)
        throws SSLException {

        Status status = res.getStatus();

        if (status != OK && status != CLOSED && status != BUFFER_UNDERFLOW)
            throw new SSLException("Failed to unwrap incoming data (SSL engine error). Status: " + status);
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
                    handshakeStatus + ']');

            handshakeFinished = false;

            handshake();
        }
    }

    /**
     * Allocate application buffer.
     */
    private ByteBuffer allocateAppBuff() {
        int netBufSize = sslEngine.getSession().getPacketBufferSize() + 50;

        int appBufSize = Math.max(sslEngine.getSession().getApplicationBufferSize() + 50, netBufSize * 2);

        ByteBuffer buf = ByteBuffer.allocate(appBufSize);
        buf.order(order);

        return buf;
    }

    /**
     * Read data from net buffer.
     */
    private void readFromNet() throws IgniteCheckedException {
        try {
            inNetBuf.clear();

            int read = ch.read(inNetBuf);

            if (read == -1)
                throw new IgniteCheckedException("Failed to read remote node response (connection closed).");
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to write byte to socket.", e);
        }
    }

    /**
     * Copies data from out net buffer and passes it to the underlying chain.
     *
     * @throws GridNioException If send failed.
     */
    private void writeNetBuffer() throws IgniteCheckedException {
        try {
            ch.write(outNetBuf);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to write byte to socket.", e);
        }
    }

    /**
     * Expands the given byte buffer to the requested capacity.
     *
     * @param original Original byte buffer.
     * @param cap Requested capacity.
     * @return Expanded byte buffer.
     */
    private ByteBuffer expandBuffer(ByteBuffer original, int cap) {
        ByteBuffer res = ByteBuffer.allocate(cap);

        res.order(ByteOrder.nativeOrder());

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
        ByteBuffer cp = ByteBuffer.allocate(original.remaining());

        cp.put(original);

        cp.flip();

        return cp;
    }
}
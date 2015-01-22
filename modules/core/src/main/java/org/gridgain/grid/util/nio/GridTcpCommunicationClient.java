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

package org.gridgain.grid.util.nio;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.direct.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.locks.*;

/**
 * Grid client for NIO server.
 */
public class GridTcpCommunicationClient extends GridAbstractCommunicationClient {
    /** Socket. */
    private final Socket sock;

    /** Output stream. */
    private final UnsafeBufferedOutputStream out;

    /** Minimum buffered message count. */
    private final int minBufferedMsgCnt;

    /** Communication buffer size ratio. */
    private final double bufSizeRatio;

    /** */
    private final GridNioMessageWriter msgWriter;

    /** */
    private final ByteBuffer writeBuf;

    /**
     * @param metricsLsnr Metrics listener.
     * @param msgWriter Message writer.
     * @param addr Address.
     * @param locHost Local address.
     * @param connTimeout Connect timeout.
     * @param tcpNoDelay Value for {@code TCP_NODELAY} socket option.
     * @param sockRcvBuf Socket receive buffer.
     * @param sockSndBuf Socket send buffer.
     * @param bufSize Buffer size (or {@code 0} to disable buffer).
     * @param minBufferedMsgCnt Minimum buffered message count.
     * @param bufSizeRatio Communication buffer size ratio.
     * @throws IgniteCheckedException If failed.
     */
    public GridTcpCommunicationClient(
        GridNioMetricsListener metricsLsnr,
        GridNioMessageWriter msgWriter,
        InetSocketAddress addr,
        InetAddress locHost,
        long connTimeout,
        boolean tcpNoDelay,
        int sockRcvBuf,
        int sockSndBuf,
        int bufSize,
        int minBufferedMsgCnt,
        double bufSizeRatio
    ) throws IgniteCheckedException {
        super(metricsLsnr);

        assert metricsLsnr != null;
        assert msgWriter != null;
        assert addr != null;
        assert locHost != null;
        assert connTimeout >= 0;
        assert bufSize >= 0;

        A.ensure(minBufferedMsgCnt >= 0,
            "Value of minBufferedMessageCount property cannot be less than zero.");
        A.ensure(bufSizeRatio > 0 && bufSizeRatio < 1,
            "Value of bufSizeRatio property must be between 0 and 1 (exclusive).");

        this.msgWriter = msgWriter;
        this.minBufferedMsgCnt = minBufferedMsgCnt;
        this.bufSizeRatio = bufSizeRatio;

        writeBuf = ByteBuffer.allocate(8 << 10);

        writeBuf.order(ByteOrder.nativeOrder());

        sock = new Socket();

        boolean success = false;

        try {
            sock.bind(new InetSocketAddress(locHost, 0));

            sock.setTcpNoDelay(tcpNoDelay);

            if (sockRcvBuf > 0)
                sock.setReceiveBufferSize(sockRcvBuf);

            if (sockSndBuf > 0)
                sock.setSendBufferSize(sockSndBuf);

            sock.connect(addr, (int)connTimeout);

            out = new UnsafeBufferedOutputStream(sock.getOutputStream(), bufSize);

            success = true;
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to connect to remote host " +
                "[addr=" + addr + ", localHost=" + locHost + ']', e);
        }
        finally {
            if (!success)
                U.closeQuiet(sock);
        }
    }

    /** {@inheritDoc} */
    @Override public void doHandshake(IgniteInClosure2X<InputStream, OutputStream> handshakeC) throws IgniteCheckedException {
        try {
            handshakeC.applyx(sock.getInputStream(), sock.getOutputStream());
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to access IO streams when executing handshake with remote node: " +
                sock.getRemoteSocketAddress(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean close() {
        boolean res = super.close();

        if (res) {
            U.closeQuiet(out);
            U.closeQuiet(sock);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void forceClose() {
        super.forceClose();

        try {
            out.flush();
        }
        catch (IOException ignored) {
            // No-op.
        }

        // Do not call (directly or indirectly) out.close() here
        // since it may cause a deadlock.
        out.forceClose();

        U.closeQuiet(sock);
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(byte[] data, int len) throws IgniteCheckedException {
        if (closed())
            throw new IgniteCheckedException("Client was closed: " + this);

        try {
            out.write(data, 0, len);

            metricsLsnr.onBytesSent(len);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to send message to remote node: " + sock.getRemoteSocketAddress(), e);
        }

        markUsed();
    }

    /** {@inheritDoc} */
    @Override public boolean sendMessage(@Nullable UUID nodeId, GridTcpCommunicationMessageAdapter msg)
        throws IgniteCheckedException {
        if (closed())
            throw new IgniteCheckedException("Client was closed: " + this);

        assert writeBuf.hasArray();

        try {
            int cnt = msgWriter.writeFully(nodeId, msg, out, writeBuf);

            metricsLsnr.onBytesSent(cnt);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to send message to remote node: " + sock.getRemoteSocketAddress(), e);
        }

        markUsed();

        return false;
    }

    /**
     * @param timeout Timeout.
     * @throws IOException If failed.
     */
    @Override public void flushIfNeeded(long timeout) throws IOException {
        assert timeout > 0;

        out.flushOnTimeout(timeout);
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(ByteBuffer data) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpCommunicationClient.class, this, super.toString());
    }

    /**
     *
     */
    private class UnsafeBufferedOutputStream extends FilterOutputStream {
        /** The internal buffer where data is stored. */
        private final byte buf[];

        /** Current size. */
        private int size;

        /** Count. */
        private int cnt;

        /** Message count. */
        private int msgCnt;

        /** Total messages size. */
        private int totalCnt;

        /** Lock. */
        private final ReentrantLock lock = new ReentrantLock();

        /** Last flushed timestamp. */
        private volatile long lastFlushed = U.currentTimeMillis();

        /** Cached flush timeout. */
        private volatile long flushTimeout;

        /** Buffer adjusted timestamp. */
        private long lastAdjusted = U.currentTimeMillis();

        /**
         * Creates a new buffered output stream to write data to the
         * specified underlying output stream.
         *
         * @param out The underlying output stream.
         */
        UnsafeBufferedOutputStream(OutputStream out) {
            this(out, 8192);
        }

        /**
         * Creates a new buffered output stream to write data to the
         * specified underlying output stream with the specified buffer
         * size.
         *
         * @param out The underlying output stream.
         * @param size The buffer size.
         */
        UnsafeBufferedOutputStream(OutputStream out, int size) {
            super(out);

            assert size >= 0;

            this.size = size;
            buf = size > 0 ? new byte[size] : null;
        }

        /** {@inheritDoc} */
        @Override public void write(int b) throws IOException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void write(byte[] b, int off, int len) throws IOException {
            assert b != null;
            assert off == 0;

            // No buffering.
            if (buf == null) {
                lock.lock();

                try {
                    out.write(b, 0, len);
                }
                finally {
                    lock.unlock();
                }

                return;
            }

            // Buffering is enabled.
            lock.lock();

            try {
                msgCnt++;
                totalCnt += len;

                if (len >= size) {
                    flushLocked();

                    out.write(b, 0, len);

                    lastFlushed = U.currentTimeMillis();

                    adjustBufferIfNeeded();

                    return;
                }

                if (cnt + len > size) {
                    flushLocked();

                    messageToBuffer0(b, off, len, buf, 0);

                    cnt = len;

                    assert cnt < size;

                    adjustBufferIfNeeded();

                    return;
                }

                messageToBuffer0(b, 0, len, buf, cnt);

                cnt += len;

                if (cnt == size)
                    flushLocked();
                else
                    flushIfNeeded();
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @throws IOException If failed.
         */
        private void flushIfNeeded() throws IOException {
            assert lock.isHeldByCurrentThread();
            assert buf != null;

            long flushTimeout0 = flushTimeout;

            if (flushTimeout0 > 0)
                flushOnTimeoutLocked(flushTimeout0);
        }

        /**
         *
         */
        private void adjustBufferIfNeeded() {
            assert lock.isHeldByCurrentThread();
            assert buf != null;

            long flushTimeout0 = flushTimeout;

            if (flushTimeout0 > 0)
                adjustBufferLocked(flushTimeout0);
        }

        /** {@inheritDoc} */
        @Override public void flush() throws IOException {
            lock.lock();

            try {
                flushLocked();
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @param timeout Timeout.
         * @throws IOException If failed.
         */
        public void flushOnTimeout(long timeout) throws IOException {
            assert buf != null;
            assert timeout > 0;

            // Overwrite cached value.
            flushTimeout = timeout;

            if (lastFlushed + timeout > U.currentTimeMillis() || !lock.tryLock())
                return;

            try {
                flushOnTimeoutLocked(timeout);
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @param timeout Timeout.
         * @throws IOException If failed.
         */
        private void flushOnTimeoutLocked(long timeout) throws IOException {
            assert lock.isHeldByCurrentThread();
            assert timeout > 0;

            // Double check.
            if (cnt == 0 || lastFlushed + timeout > U.currentTimeMillis())
                return;

            flushLocked();

            adjustBufferLocked(timeout);
        }

        /**
         * @param timeout Timeout.
         */
        private void adjustBufferLocked(long timeout) {
            assert lock.isHeldByCurrentThread();
            assert timeout > 0;

            long time = U.currentTimeMillis();

            if (lastAdjusted + timeout < time) {
                if (msgCnt <= minBufferedMsgCnt)
                    size = 0;
                else {
                    size = (int)(totalCnt * bufSizeRatio);

                    if (size > buf.length)
                        size = buf.length;
                }

                msgCnt = 0;
                totalCnt = 0;

                lastAdjusted = time;
            }
        }

        /**
         * @throws IOException If failed.
         */
        private void flushLocked() throws IOException {
            assert lock.isHeldByCurrentThread();

            if (buf != null && cnt > 0) {
                out.write(buf, 0, cnt);

                cnt = 0;
            }

            out.flush();

            lastFlushed = U.currentTimeMillis();
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            lock.lock();

            try {
                flushLocked();
            }
            finally {
                try {
                    out.close();
                }
                finally {
                    lock.unlock();
                }
            }
        }

        /**
         * Forcibly closes underlying stream ignoring any possible exception.
         */
        public void forceClose() {
            try {
                out.close();
            }
            catch (IOException ignored) {
                // No-op.
            }
        }

        /**
         * @param b Buffer to copy from.
         * @param off Offset in source buffer.
         * @param len Length.
         * @param resBuf Result buffer.
         * @param resOff Result offset.
         */
        private void messageToBuffer(byte[] b, int off, int len, byte[] resBuf, int resOff) {
            assert b.length == len;
            assert off == 0;
            assert resBuf.length >= resOff + len + 4;

            U.intToBytes(len, resBuf, resOff);

            U.arrayCopy(b, off, resBuf, resOff + 4, len);
        }

        /**
         * @param b Buffer to copy from (length included).
         * @param off Offset in source buffer.
         * @param len Length.
         * @param resBuf Result buffer.
         * @param resOff Result offset.
         */
        private void messageToBuffer0(byte[] b, int off, int len, byte[] resBuf, int resOff) {
            assert off == 0;
            assert resBuf.length >= resOff + len;

            U.arrayCopy(b, off, resBuf, resOff, len);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            lock.lock();

            try {
                return S.toString(UnsafeBufferedOutputStream.class, this);
            }
            finally {
                lock.unlock();
            }
        }
    }
}

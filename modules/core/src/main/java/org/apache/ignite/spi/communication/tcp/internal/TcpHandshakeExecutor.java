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

package org.apache.ignite.spi.communication.tcp.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.ssl.BlockingSslHandler;
import org.apache.ignite.internal.util.nio.ssl.GridSslMeta;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage;
import org.apache.ignite.spi.communication.tcp.messages.NodeIdMessage;
import org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage;

import static org.apache.ignite.plugin.extensions.communication.Message.DIRECT_TYPE_SIZE;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.HANDSHAKE_WAIT_MSG_TYPE;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.makeMessageType;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.NEED_WAIT;

/**
 * Executor for synchronously establishing a connection with a node.
 */
public class TcpHandshakeExecutor {
    /** Logger. */
    private final IgniteLogger log;

    /** State provider. */
    private final ClusterStateProvider stateProvider;

    /** {@code true} if direct buffer for ssl handler is required. */
    private final boolean directBuffer;

    /**
     *
     * @param log Logger.
     * @param stateProvider State provider.
     * @param directBuffer {@code true} if direct buffer for ssl handler is required.
     */
    public TcpHandshakeExecutor(IgniteLogger log, ClusterStateProvider stateProvider, boolean directBuffer) {
        this.log = log;
        this.stateProvider = stateProvider;
        this.directBuffer = directBuffer;
    }

    /**
     * Establish the first connection to the node and receive connection recovery information.
     *
     * @param ch Socket channel which using for handshake.
     * @param rmtNodeId Expected remote node.
     * @param sslMeta Required data for ssl.
     * @param msg Handshake message which should be sent during handshake.
     * @return Handshake response from predefined variants from {@link RecoveryLastReceivedMessage}.
     * @throws IgniteCheckedException If handshake failed.
     */
    public long tcpHandshake(
        SocketChannel ch,
        UUID rmtNodeId,
        GridSslMeta sslMeta,
        HandshakeMessage msg
    ) throws IgniteCheckedException {
        BlockingTransport transport = stateProvider.isSslEnabled() ?
            new SslTransport(sslMeta, ch, directBuffer, log) : new TcpTransport(ch);

        ByteBuffer buf = transport.receiveNodeId();

        if (buf == null)
            return NEED_WAIT;

        UUID rmtNodeId0 = U.bytesToUuid(buf.array(), DIRECT_TYPE_SIZE);

        if (!rmtNodeId.equals(rmtNodeId0))
            throw new HandshakeException("Remote node ID is not as expected [expected=" + rmtNodeId + ", rcvd=" + rmtNodeId0 + ']');
        else if (log.isDebugEnabled())
            log.debug("Received remote node ID: " + rmtNodeId0);

        if (log.isDebugEnabled())
            log.debug("Writing handshake message [rmtNode=" + rmtNodeId + ", msg=" + msg + ']');

        transport.sendHandshake(msg);

        buf = transport.receiveAcknowledge();

        long rcvCnt = buf.getLong(DIRECT_TYPE_SIZE);

        if (log.isDebugEnabled())
            log.debug("Received handshake message [rmtNode=" + rmtNodeId + ", rcvCnt=" + rcvCnt + ']');

        if (rcvCnt == -1) {
            if (log.isDebugEnabled())
                log.debug("Connection rejected, will retry client creation [rmtNode=" + rmtNodeId + ']');
        }

        transport.onHandshakeFinished(sslMeta);

        return rcvCnt;
    }

    /**
     * Encapsulates handshake logic.
     */
    private abstract static class BlockingTransport {
        /**
         * Receive {@link NodeIdMessage}.
         *
         * @return Buffer with {@link NodeIdMessage}.
         * @throws IgniteCheckedException If failed.
         */
        ByteBuffer receiveNodeId() throws IgniteCheckedException {
            ByteBuffer buf = ByteBuffer.allocate(NodeIdMessage.MESSAGE_FULL_SIZE)
                    .order(ByteOrder.LITTLE_ENDIAN);

            for (int totalBytes = 0; totalBytes < NodeIdMessage.MESSAGE_FULL_SIZE; ) {
                int readBytes = read(buf);

                if (readBytes == -1)
                    throw new HandshakeException("Failed to read remote node ID (connection closed).");

                if (readBytes >= DIRECT_TYPE_SIZE) {
                    short msgType = makeMessageType(buf.get(0), buf.get(1));

                    if (msgType == HANDSHAKE_WAIT_MSG_TYPE)
                        return null;
                }

                totalBytes += readBytes;
            }

            return buf;
        }

        /**
         * Send {@link HandshakeMessage} to remote node.
         *
         * @param msg Handshake message.
         * @throws IgniteCheckedException If failed.
         */
        void sendHandshake(HandshakeMessage msg) throws IgniteCheckedException {
            ByteBuffer buf = ByteBuffer.allocate(msg.getMessageSize() + U.IGNITE_HEADER.length)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .put(U.IGNITE_HEADER);

            msg.writeTo(buf, null);
            buf.flip();

            write(buf);
        }

        /**
         * Receive {@link RecoveryLastReceivedMessage} acknowledge message.
         *
         * @return Buffer with message.
         * @throws IgniteCheckedException If failed.
         */
        ByteBuffer receiveAcknowledge() throws IgniteCheckedException {
            ByteBuffer buf = ByteBuffer.allocate(RecoveryLastReceivedMessage.MESSAGE_FULL_SIZE)
                    .order(ByteOrder.LITTLE_ENDIAN);

            for (int totalBytes = 0; totalBytes < RecoveryLastReceivedMessage.MESSAGE_FULL_SIZE; ) {
                int readBytes = read(buf);

                if (readBytes == -1)
                    throw new HandshakeException("Failed to read remote node recovery handshake " +
                            "(connection closed).");

                totalBytes += readBytes;
            }

            return buf;
        }

        /**
         * Read data from media.
         *
         * @param buf Buffer to read into.
         * @return Bytes read.
         * @throws IgniteCheckedException If failed.
         */
        abstract int read(ByteBuffer buf) throws IgniteCheckedException;

        /**
         * Write data fully.
         * @param buf Buffer to write.
         * @throws IgniteCheckedException If failed.
         */
        abstract void write(ByteBuffer buf) throws IgniteCheckedException;

        /**
         * Do some post-handshake job if needed.
         *
         * @param sslMeta Ssl meta.
         */
        void onHandshakeFinished(GridSslMeta sslMeta) {
            // No-op.
        }
    }

    /**
     * Tcp plaintext transport.
     */
    private static class TcpTransport extends BlockingTransport {
        /** */
        private final SocketChannel ch;

        /** */
        TcpTransport(SocketChannel ch) {
            this.ch = ch;
        }

        /** {@inheritDoc} */
        @Override int read(ByteBuffer buf) throws IgniteCheckedException {
            try {
                return ch.read(buf);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to read from channel", e);
            }
        }

        /** {@inheritDoc} */
        @Override void write(ByteBuffer buf) throws IgniteCheckedException {
            try {
                U.writeFully(ch, buf);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to write to channel", e);
            }
        }
    }

    /** Ssl transport */
    private static class SslTransport extends BlockingTransport {
        /** */
        private static final int READ_BUFFER_CAPACITY = 1024;

        /** */
        private final BlockingSslHandler handler;

        /** */
        private final SocketChannel ch;

        /** */
        private final ByteBuffer readBuf;

        /** */
        SslTransport(GridSslMeta meta, SocketChannel ch, boolean directBuf, IgniteLogger log) throws IgniteCheckedException {
            try {
                this.ch = ch;
                handler = new BlockingSslHandler(meta.sslEngine(), ch, directBuf, ByteOrder.LITTLE_ENDIAN, log);

                if (!handler.handshake())
                    throw new HandshakeException("SSL handshake is not completed.");

                readBuf = directBuf ? ByteBuffer.allocateDirect(READ_BUFFER_CAPACITY) : ByteBuffer.allocate(READ_BUFFER_CAPACITY);

                readBuf.order(ByteOrder.LITTLE_ENDIAN);
            }
            catch (SSLException e) {
                throw new IgniteCheckedException("SSL handhshake failed", e);
            }
        }

        /** {@inheritDoc} */
        @Override int read(ByteBuffer buf) throws IgniteCheckedException {
            ByteBuffer appBuff = handler.applicationBuffer();

            int read = copy(appBuff, buf);

            if (read > 0)
                return read;

            try {
                while (read == 0) {
                    readBuf.clear();

                    if (ch.read(readBuf) < 0)
                        return -1;

                    readBuf.flip();

                    handler.decode(readBuf);

                    read = copy(appBuff, buf);
                }
            }
            catch (SSLException e) {
                throw new IgniteCheckedException("Failed to decrypt data", e);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to read from channel", e);
            }

            return read;
        }

        /** {@inheritDoc} */
        @Override void write(ByteBuffer buf) throws IgniteCheckedException {
            try {
                U.writeFully(ch, handler.encrypt(buf));
            }
            catch (SSLException e) {
                throw new IgniteCheckedException("Failed to encrypt data", e);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to write to channel", e);
            }
        }

        /** {@inheritDoc} */
        @Override void onHandshakeFinished(GridSslMeta sslMeta) {
            ByteBuffer appBuff = handler.applicationBuffer();
            if (appBuff.hasRemaining())
                sslMeta.decodedBuffer(appBuff);

            ByteBuffer inBuf = handler.inputBuffer();

            if (inBuf.position() > 0) {
                inBuf.flip();

                sslMeta.encodedBuffer(inBuf);
            }
        }

        /**
         * @param src Source buffer.
         * @param dst Destination buffer.
         * @return Bytes copied.
         */
        private int copy(ByteBuffer src, ByteBuffer dst) {
            int remaining = Math.min(src.remaining(), dst.remaining());

            if (remaining > 0) {
                int oldLimit = src.limit();

                src.limit(src.position() + remaining);

                dst.put(src);

                src.limit(oldLimit);
            }

            src.compact();

            return remaining;
        }
    }
}

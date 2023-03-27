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
     * @param msg Handshake message which should be send during handshake.
     * @return Handshake response from predefined variants from {@link RecoveryLastReceivedMessage}.
     * @throws IgniteCheckedException If not related to IO exception happened.
     * @throws IOException If reading or writing to socket is failed.
     */
    public long tcpHandshake(
        SocketChannel ch,
        UUID rmtNodeId,
        GridSslMeta sslMeta,
        HandshakeMessage msg
    ) throws IgniteCheckedException, IOException {
        long rcvCnt;

        BlockingSslHandler sslHnd = null;

        ByteBuffer buf;

        // Step 1. Get remote node response with the remote nodeId value.
        if (stateProvider.isSslEnabled()) {
            assert sslMeta != null;

            sslHnd = new BlockingSslHandler(sslMeta.sslEngine(), ch, directBuffer, ByteOrder.LITTLE_ENDIAN, log);

            if (!sslHnd.handshake())
                throw new HandshakeException("SSL handshake is not completed.");

            ByteBuffer handBuff = sslHnd.applicationBuffer();

            if (handBuff.remaining() >= DIRECT_TYPE_SIZE) {
                short msgType = makeMessageType(handBuff.get(0), handBuff.get(1));

                if (msgType == HANDSHAKE_WAIT_MSG_TYPE)
                    return NEED_WAIT;
            }

            if (handBuff.remaining() < NodeIdMessage.MESSAGE_FULL_SIZE) {
                ByteBuffer readBuf = ByteBuffer.allocate(1000);

                while (handBuff.remaining() < NodeIdMessage.MESSAGE_FULL_SIZE) {
                    int read = ch.read(readBuf);

                    if (read == -1)
                        throw new HandshakeException("Failed to read remote node ID (connection closed).");

                    readBuf.flip();

                    sslHnd.decode(readBuf);

                    if (handBuff.remaining() >= DIRECT_TYPE_SIZE) {
                        break;
                    }

                    readBuf.flip();
                }

                buf = handBuff;

                if (handBuff.remaining() >= DIRECT_TYPE_SIZE) {
                    short msgType = makeMessageType(handBuff.get(0), handBuff.get(1));

                    if (msgType == HANDSHAKE_WAIT_MSG_TYPE)
                        return NEED_WAIT;
                }
            }
            else
                buf = handBuff;
        }
        else {
            buf = ByteBuffer.allocate(NodeIdMessage.MESSAGE_FULL_SIZE);

            for (int i = 0; i < NodeIdMessage.MESSAGE_FULL_SIZE; ) {
                int read = ch.read(buf);

                if (read == -1)
                    throw new HandshakeException("Failed to read remote node ID (connection closed).");

                if (read >= DIRECT_TYPE_SIZE) {
                    short msgType = makeMessageType(buf.get(0), buf.get(1));

                    if (msgType == HANDSHAKE_WAIT_MSG_TYPE)
                        return NEED_WAIT;
                }

                i += read;
            }
        }

        UUID rmtNodeId0 = U.bytesToUuid(buf.array(), DIRECT_TYPE_SIZE);

        if (!rmtNodeId.equals(rmtNodeId0))
            throw new HandshakeException("Remote node ID is not as expected [expected=" + rmtNodeId +
                ", rcvd=" + rmtNodeId0 + ']');
        else if (log.isDebugEnabled())
            log.debug("Received remote node ID: " + rmtNodeId0);

        if (stateProvider.isSslEnabled()) {
            assert sslHnd != null;

            U.writeFully(ch, sslHnd.encrypt(ByteBuffer.wrap(U.IGNITE_HEADER)));
        }
        else
            U.writeFully(ch, ByteBuffer.wrap(U.IGNITE_HEADER));

        // Step 2. Prepare Handshake message to send to the remote node.
        if (log.isDebugEnabled())
            log.debug("Writing handshake message [rmtNode=" + rmtNodeId + ", msg=" + msg + ']');

        buf = ByteBuffer.allocate(msg.getMessageSize());

        buf.order(ByteOrder.LITTLE_ENDIAN);

        boolean written = msg.writeTo(buf, null);

        assert written;

        buf.flip();

        if (stateProvider.isSslEnabled()) {
            assert sslHnd != null;

            U.writeFully(ch, sslHnd.encrypt(buf));
        }
        else
            U.writeFully(ch, buf);

        if (log.isDebugEnabled())
            log.debug("Waiting for handshake [rmtNode=" + rmtNodeId + ']');

        // Step 3. Waiting for response from the remote node with their receive count message.
        if (stateProvider.isSslEnabled()) {
            assert sslHnd != null;

            buf = ByteBuffer.allocate(1000);
            buf.order(ByteOrder.LITTLE_ENDIAN);

            ByteBuffer decode = ByteBuffer.allocate(2 * buf.capacity());
            decode.order(ByteOrder.LITTLE_ENDIAN);

            for (int i = 0; i < RecoveryLastReceivedMessage.MESSAGE_FULL_SIZE; ) {
                int read = ch.read(buf);

                if (read == -1)
                    throw new HandshakeException("Failed to read remote node recovery handshake " +
                        "(connection closed).");

                buf.flip();

                ByteBuffer decode0 = sslHnd.decode(buf);

                i += decode0.remaining();

                decode = appendAndResizeIfNeeded(decode, decode0);

                buf.clear();
            }

            decode.flip();

            rcvCnt = decode.getLong(DIRECT_TYPE_SIZE);

            if (decode.limit() > RecoveryLastReceivedMessage.MESSAGE_FULL_SIZE) {
                decode.position(RecoveryLastReceivedMessage.MESSAGE_FULL_SIZE);

                sslMeta.decodedBuffer(decode);
            }

            ByteBuffer inBuf = sslHnd.inputBuffer();

            if (inBuf.position() > 0)
                sslMeta.encodedBuffer(inBuf);
        }
        else {
            buf = ByteBuffer.allocate(RecoveryLastReceivedMessage.MESSAGE_FULL_SIZE);

            buf.order(ByteOrder.LITTLE_ENDIAN);

            for (int i = 0; i < RecoveryLastReceivedMessage.MESSAGE_FULL_SIZE; ) {
                int read = ch.read(buf);

                if (read == -1)
                    throw new HandshakeException("Failed to read remote node recovery handshake " +
                        "(connection closed).");

                i += read;
            }

            rcvCnt = buf.getLong(DIRECT_TYPE_SIZE);
        }

        if (log.isDebugEnabled())
            log.debug("Received handshake message [rmtNode=" + rmtNodeId + ", rcvCnt=" + rcvCnt + ']');

        if (rcvCnt == -1) {
            if (log.isDebugEnabled())
                log.debug("Connection rejected, will retry client creation [rmtNode=" + rmtNodeId + ']');
        }

        return rcvCnt;
    }

    /**
     * @param target Target buffer to append to.
     * @param src Source buffer to get data.
     * @return Original or expanded buffer.
     */
    private ByteBuffer appendAndResizeIfNeeded(ByteBuffer target, ByteBuffer src) {
        if (target.remaining() < src.remaining()) {
            int newSize = Math.max(target.capacity() * 2, target.capacity() + src.remaining());

            ByteBuffer tmp = ByteBuffer.allocate(newSize);

            tmp.order(target.order());

            target.flip();

            tmp.put(target);

            target = tmp;
        }

        target.put(src);

        return target;
    }
}

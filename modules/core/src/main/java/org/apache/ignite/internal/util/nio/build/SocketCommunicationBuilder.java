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

package org.apache.ignite.internal.util.nio.build;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.util.nio.ssl.BlockingSslHandler;
import org.apache.ignite.internal.util.nio.ssl.GridSslMeta;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.communication.tcp.internal.HandshakeException;
import org.apache.ignite.spi.communication.tcp.internal.HandshakeTimeoutException;
import org.apache.ignite.spi.communication.tcp.internal.HandshakeTimeoutObject;
import org.apache.ignite.spi.communication.tcp.internal.NeedWaitConnectionException;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage;
import org.apache.ignite.spi.communication.tcp.messages.NodeIdMessage;
import org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage;

import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.ALREADY_CONNECTED;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.NEED_WAIT;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.NODE_STOPPING;

/**
 * 3-step node communication handshake:
 * 1) receive NodeIdMessage
 * 2) write header IGNITE_HEADER and HandshakeMessage
 * 3) receive RecoveryLastReceivedMessage
 */
public class SocketCommunicationBuilder implements CommunicationBuilder<SocketChannel> {
    /** */
    private IgniteLogger log;

    /** */
    private boolean directBuf;
    /** */
    private boolean tcpNoDelay;
    /** */
    private int sockSndBufSize;
    /** */
    private int sockRcvBufSize;
    /** */
    private long connTimeout;
    /** */
    private ClusterNode remoteNode;
    /** */
    private IgniteSpiOperationTimeoutHelper timeoutHelper;
    /** */
    private HandshakeMessage handshakeMsg;

    /**
     * @param log
     */
    public SocketCommunicationBuilder(IgniteLogger log) {
        this.log = log;
    }

    /** */
    @Override public SocketChannel build(CommunicationBuilderContext ctx, InetSocketAddress addr) throws Exception {
        return build(ctx, addr, null);
    }

    /** */
    @Override public SocketChannel build(
        CommunicationBuilderContext ctx,
        InetSocketAddress addr,
        CompletionHandler hndlr
    ) throws Exception {
        final ClusterNode locNode = ctx.spiContext().localNode();

        if (locNode == null)
            throw new IgniteCheckedException("Local node has not been started or " +
                "fully initialized [isStopping=" + ctx.spiContext().isStopping() + ']');

        long rcvCnt = 0;

        boolean isCreated = false;

        boolean isSslEnabled = ctx.sslMeta() != null;

        SocketChannel ch = SocketChannel.open();

        HandshakeTimeoutObject obj = new HandshakeTimeoutObject<>(ch,
            U.currentTimeMillis() + timeoutHelper.nextTimeoutChunk(connTimeout));

        ctx.spiContext().addTimeoutObject(obj);

        try {
            ch.configureBlocking(true);
            ch.socket().setTcpNoDelay(tcpNoDelay);
            ch.socket().setKeepAlive(true);

            if (sockRcvBufSize > 0)
                ch.socket().setReceiveBufferSize(sockRcvBufSize);

            if (sockSndBufSize > 0)
                ch.socket().setSendBufferSize(sockSndBufSize);

            ch.socket().connect(addr, (int)timeoutHelper.nextTimeoutChunk(connTimeout));

            BlockingSslHandler sslHnd = null;

            ByteBuffer buf;

            // STEP-1: Read the NodeIdMessage.
            if (isSslEnabled) {
                sslHnd = new BlockingSslHandler(ctx.sslMeta().sslEngine(), ch, directBuf, ByteOrder.nativeOrder(), log);

                if (!sslHnd.handshake())
                    throw new HandshakeException("SSL handshake is not completed.");

                ByteBuffer handBuff = sslHnd.applicationBuffer();

                if (handBuff.remaining() < NodeIdMessage.MESSAGE_FULL_SIZE) {
                    buf = ByteBuffer.allocate(1000);

                    int read = ch.read(buf);

                    if (read == -1)
                        throw new HandshakeException("Failed to read remote node ID (connection closed).");

                    buf.flip();

                    buf = sslHnd.decode(buf);
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

                    i += read;
                }
            }

            UUID rmtNodeId0 = U.bytesToUuid(buf.array(), Message.DIRECT_TYPE_SIZE);

            if (!remoteNode.id().equals(rmtNodeId0))
                throw new HandshakeException("Remote node ID is not as expected [expected=" + remoteNode.id() +
                    ", rcvd=" + rmtNodeId0 + ']');

            if (log.isDebugEnabled())
                log.debug("Received remote node ID: " + rmtNodeId0);

            // STEP-2: Write IGNITE_HEADER.
            if (isSslEnabled)
                ch.write(sslHnd.encrypt(ByteBuffer.wrap(U.IGNITE_HEADER)));
            else
                ch.write(ByteBuffer.wrap(U.IGNITE_HEADER));

            if (log.isDebugEnabled())
                log.debug("Writing handshake message [locNodeId=" + locNode.id() +
                    ", rmtNode=" + remoteNode.id() + ", msg=" + handshakeMsg + ']');

            // STEP-3: Write the HandshakeMessage.
            buf = ByteBuffer.allocate(handshakeMsg.getMessageSize());

            buf.order(ByteOrder.nativeOrder());

            boolean written = handshakeMsg.writeTo(buf, null);

            assert written;

            buf.flip();

            if (isSslEnabled) {
                assert sslHnd != null;

                ch.write(sslHnd.encrypt(buf));
            }
            else
                ch.write(buf);

            if (log.isDebugEnabled())
                log.debug("Waiting for handshake [rmtNode=" + remoteNode.id() + ']');

            // STEP-4: Read the RecoveryLastReceivedMessage.
            if (isSslEnabled) {
                assert sslHnd != null;

                buf = ByteBuffer.allocate(1000);
                buf.order(ByteOrder.nativeOrder());

                ByteBuffer decode = ByteBuffer.allocate(2 * buf.capacity());
                decode.order(ByteOrder.nativeOrder());

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

                rcvCnt = decode.getLong(Message.DIRECT_TYPE_SIZE);

                if (decode.limit() > RecoveryLastReceivedMessage.MESSAGE_FULL_SIZE) {
                    decode.position(RecoveryLastReceivedMessage.MESSAGE_FULL_SIZE);

                    ctx.sslMeta().decodedBuffer(decode);
                }

                ByteBuffer inBuf = sslHnd.inputBuffer();

                if (inBuf.position() > 0)
                    ctx.sslMeta().encodedBuffer(inBuf);
            }
            else {
                buf = ByteBuffer.allocate(RecoveryLastReceivedMessage.MESSAGE_FULL_SIZE);

                buf.order(ByteOrder.nativeOrder());

                for (int i = 0; i < RecoveryLastReceivedMessage.MESSAGE_FULL_SIZE; ) {
                    int read = ch.read(buf);

                    if (read == -1)
                        throw new HandshakeException("Failed to read remote node recovery handshake " +
                            "(connection closed).");

                    i += read;
                }

                rcvCnt = buf.getLong(Message.DIRECT_TYPE_SIZE);
            }

            if (log.isDebugEnabled())
                log.debug("Received handshake message [rmtNode=" + remoteNode.id() + ", rcvCnt=" + rcvCnt + ']');

            // END of handshake procedure.
            if (rcvCnt == ALREADY_CONNECTED)
                throw new AlreadyConnectedException();
            else if (rcvCnt == NODE_STOPPING)
                throw new ClusterTopologyCheckedException("Remote node started stop procedure: " + remoteNode.id());
            else if (rcvCnt == NEED_WAIT)
                throw new NeedWaitConnectionException();

            if (hndlr != null)
                hndlr.onHandshake(rcvCnt);

            isCreated = true;
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to read from channel.", e);
        }
        finally {
            if (!isCreated)
                U.closeQuiet(ch);

            boolean cancelled = obj.cancel();

            if (cancelled)
                ctx.spiContext().removeTimeoutObject(obj);

            // Ignoring whatever happened after timeout - reporting only timeout event.
            if (!cancelled)
                throw new HandshakeTimeoutException(
                    new IgniteSpiOperationTimeoutException("Failed to perform handshake due to timeout " +
                        "(consider increasing 'connectionTimeout' configuration property)."));
        }

        return ch;
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

    /** */
    public SocketCommunicationBuilder setDirectBuf(boolean directBuf) {
        this.directBuf = directBuf;

        return this;
    }

    /** */
    public SocketCommunicationBuilder setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;

        return this;
    }

    /** */
    public SocketCommunicationBuilder setSockSndBufSize(int sockSndBufSize) {
        this.sockSndBufSize = sockSndBufSize;

        return this;
    }

    /** */
    public SocketCommunicationBuilder setSockRcvBufSize(int sockRcvBufSize) {
        this.sockRcvBufSize = sockRcvBufSize;

        return this;
    }

    /** */
    public SocketCommunicationBuilder setConnTimeout(long connTimeout) {
        this.connTimeout = connTimeout;

        return this;
    }

    /** */
    public SocketCommunicationBuilder setRemoteNode(ClusterNode remoteNode) {
        this.remoteNode = remoteNode;

        return this;
    }

    /** */
    public SocketCommunicationBuilder setTimeoutHelper(IgniteSpiOperationTimeoutHelper timeoutHelper) {
        this.timeoutHelper = timeoutHelper;

        return this;
    }

    /** */
    public SocketCommunicationBuilder setHandshakeMsg(HandshakeMessage handshakeMsg) {
        this.handshakeMsg = handshakeMsg;

        return this;
    }
}

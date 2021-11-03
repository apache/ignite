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

package org.apache.ignite.internal.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.client.IgniteClientAuthenticationException;
import org.apache.ignite.client.IgniteClientAuthorizationException;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.internal.client.io.ClientConnection;
import org.apache.ignite.internal.client.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.client.io.ClientConnectionStateHandler;
import org.apache.ignite.internal.client.io.ClientMessageHandler;
import org.apache.ignite.internal.client.proto.ClientErrorCode;
import org.apache.ignite.internal.client.proto.ClientMessageCommon;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ProtocolVersion;
import org.apache.ignite.internal.client.proto.ServerMessageType;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Implements {@link ClientChannel} over TCP.
 */
class TcpClientChannel implements ClientChannel, ClientMessageHandler, ClientConnectionStateHandler {
    /** Protocol version used by default on first connection attempt. */
    private static final ProtocolVersion DEFAULT_VERSION = ProtocolVersion.LATEST_VER;

    /** Supported protocol versions. */
    private static final Collection<ProtocolVersion> supportedVers = Collections.singletonList(
            ProtocolVersion.V3_0_0
    );

    /** Protocol context. */
    private volatile ProtocolContext protocolCtx;

    /** Channel. */
    private final ClientConnection sock;

    /** Request id. */
    private final AtomicLong reqId = new AtomicLong(1);

    /** Pending requests. */
    private final Map<Long, ClientRequestFuture> pendingReqs = new ConcurrentHashMap<>();

    /** Closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /** Executor for async operation listeners. */
    private final Executor asyncContinuationExecutor;

    /** Connect timeout in milliseconds. */
    private final long connectTimeout;

    /**
     * Constructor.
     *
     * @param cfg     Config.
     * @param connMgr Connection multiplexer.
     */
    TcpClientChannel(ClientChannelConfiguration cfg, ClientConnectionMultiplexer connMgr) {
        validateConfiguration(cfg);

        asyncContinuationExecutor = ForkJoinPool.commonPool();

        connectTimeout = cfg.clientConfiguration().connectTimeout();

        sock = connMgr.open(cfg.getAddress(), this, this);

        handshake(DEFAULT_VERSION);
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        close(null);
    }

    /**
     * Close the channel with cause.
     */
    private void close(Exception cause) {
        if (closed.compareAndSet(false, true)) {
            sock.close();

            for (ClientRequestFuture pendingReq : pendingReqs.values()) {
                pendingReq.completeExceptionally(new IgniteClientConnectionException("Channel is closed", cause));
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onMessage(ByteBuf buf) {
        processNextMessage(buf);
    }

    /** {@inheritDoc} */
    @Override
    public void onDisconnected(@Nullable Exception e) {
        close(e);
    }

    /** {@inheritDoc} */
    @Override
    public <T> CompletableFuture<T> serviceAsync(
            int opCode,
            PayloadWriter payloadWriter,
            PayloadReader<T> payloadReader
    ) {
        try {
            ClientRequestFuture fut = send(opCode, payloadWriter);

            return receiveAsync(fut, payloadReader);
        } catch (Throwable t) {
            CompletableFuture<T> fut = new CompletableFuture<>();
            fut.completeExceptionally(t);

            return fut;
        }
    }

    /**
     * @param opCode        Operation code.
     * @param payloadWriter Payload writer to stream or {@code null} if request has no payload.
     * @return Request future.
     */
    private ClientRequestFuture send(int opCode, PayloadWriter payloadWriter)
            throws IgniteClientException {
        long id = reqId.getAndIncrement();

        if (closed()) {
            throw new IgniteClientConnectionException("Channel is closed");
        }

        ClientRequestFuture fut = new ClientRequestFuture();

        pendingReqs.put(id, fut);

        PayloadOutputChannel payloadCh = new PayloadOutputChannel(this, new ClientMessagePacker(sock.getBuffer()));

        try {
            var req = payloadCh.out();

            req.packInt(opCode);
            req.packLong(id);

            if (payloadWriter != null) {
                payloadWriter.accept(payloadCh);
            }

            write(req).addListener(f -> {
                if (!f.isSuccess()) {
                    fut.completeExceptionally(new IgniteClientConnectionException("Failed to send request", f.cause()));
                }
            });

            return fut;
        } catch (Throwable t) {
            // Close buffer manually on fail. Successful write closes the buffer automatically.
            payloadCh.close();
            pendingReqs.remove(id);

            throw convertException(t);
        }
    }

    /**
     * Receives the response asynchronously.
     *
     * @param pendingReq    Request future.
     * @param payloadReader Payload reader from stream.
     * @return Future for the operation.
     */
    private <T> CompletableFuture<T> receiveAsync(ClientRequestFuture pendingReq, PayloadReader<T> payloadReader) {
        return pendingReq.thenApplyAsync(payload -> {
            if (payload == null || payloadReader == null) {
                return null;
            }

            try (var in = new PayloadInputChannel(this, payload)) {
                return payloadReader.apply(in);
            } catch (Exception e) {
                throw new IgniteException("Failed to deserialize server response: " + e.getMessage(), e);
            }
        }, asyncContinuationExecutor);
    }

    /**
     * Converts exception to {@link IgniteClientException}.
     *
     * @param e Exception to convert.
     * @return Resulting exception.
     */
    private IgniteClientException convertException(Throwable e) {
        // For every known class derived from IgniteClientException, wrap cause in a new instance.
        // We could rethrow e.getCause() when instanceof IgniteClientException,
        // but this results in an incomplete stack trace from the receiver thread.
        // This is similar to IgniteUtils.exceptionConverters.
        if (e.getCause() instanceof IgniteClientConnectionException) {
            return new IgniteClientConnectionException(e.getMessage(), e.getCause());
        }

        if (e.getCause() instanceof IgniteClientAuthorizationException) {
            return new IgniteClientAuthorizationException(e.getMessage(), e.getCause());
        }

        return new IgniteClientException(e.getMessage(), ClientErrorCode.FAILED, e);
    }

    /**
     * Process next message from the input stream and complete corresponding future.
     */
    private void processNextMessage(ByteBuf buf) throws IgniteClientException {
        var unpacker = new ClientMessageUnpacker(buf);

        if (protocolCtx == null) {
            // Process handshake.
            pendingReqs.remove(-1L).complete(unpacker);
            return;
        }

        var type = unpacker.unpackInt();

        if (type != ServerMessageType.RESPONSE) {
            throw new IgniteClientException("Unexpected message type: " + type);
        }

        Long resId = unpacker.unpackLong();

        int status = unpacker.unpackInt();

        ClientRequestFuture pendingReq = pendingReqs.remove(resId);

        if (pendingReq == null) {
            throw new IgniteClientException(String.format("Unexpected response ID [%s]", resId));
        }

        if (status == 0) {
            pendingReq.complete(unpacker);
        } else {
            var errMsg = unpacker.unpackString();
            var err = new IgniteClientException(errMsg, status);
            pendingReq.completeExceptionally(err);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean closed() {
        return closed.get();
    }

    private static void validateConfiguration(ClientChannelConfiguration cfg) {
        String error = null;

        InetSocketAddress addr = cfg.getAddress();

        if (addr == null) {
            error = "At least one Ignite server node must be specified in the Ignite client configuration";
        } else if (addr.getPort() < 1024 || addr.getPort() > 49151) {
            error = String.format("Ignite client port %s is out of valid ports range 1024...49151", addr.getPort());
        }

        if (error != null) {
            throw new IllegalArgumentException(error);
        }
    }

    /** Client handshake. */
    private void handshake(ProtocolVersion ver)
            throws IgniteClientConnectionException {
        ClientRequestFuture fut = new ClientRequestFuture();
        pendingReqs.put(-1L, fut);

        try {
            handshakeReq(ver);

            var res = connectTimeout > 0 ? fut.get(connectTimeout, TimeUnit.MILLISECONDS) : fut.get();
            handshakeRes(res, ver);
        } catch (Throwable e) {
            throw convertException(e);
        }
    }

    /** Send handshake request. */
    private void handshakeReq(ProtocolVersion proposedVer) {
        sock.send(Unpooled.wrappedBuffer(ClientMessageCommon.MAGIC_BYTES));

        var req = new ClientMessagePacker(sock.getBuffer());
        req.packInt(proposedVer.major());
        req.packInt(proposedVer.minor());
        req.packInt(proposedVer.patch());

        req.packInt(2); // Client type: general purpose.

        req.packBinaryHeader(0); // Features.
        req.packMapHeader(0); // Extensions.

        write(req).syncUninterruptibly();
    }

    /**
     * @param ver Protocol version.
     * @return Protocol context for a version.
     */
    private ProtocolContext protocolContextFromVersion(ProtocolVersion ver) {
        return new ProtocolContext(ver, ProtocolBitmaskFeature.allFeaturesAsEnumSet());
    }

    /** Receive and handle handshake response. */
    private void handshakeRes(ClientMessageUnpacker unpacker, ProtocolVersion proposedVer)
            throws IgniteClientConnectionException, IgniteClientAuthenticationException {
        try (unpacker) {
            ProtocolVersion srvVer = new ProtocolVersion(unpacker.unpackShort(), unpacker.unpackShort(),
                    unpacker.unpackShort());

            var errCode = unpacker.unpackInt();

            if (errCode != ClientErrorCode.SUCCESS) {
                var msg = unpacker.unpackString();

                if (errCode == ClientErrorCode.AUTH_FAILED) {
                    throw new IgniteClientAuthenticationException(msg);
                } else if (proposedVer.equals(srvVer)) {
                    throw new IgniteClientException("Client protocol error: unexpected server response.");
                } else if (!supportedVers.contains(srvVer)) {
                    throw new IgniteClientException(String.format(
                            "Protocol version mismatch: client %s / server %s. Server details: %s",
                            proposedVer,
                            srvVer,
                            msg
                    ));
                } else { // Retry with server version.
                    handshake(srvVer);
                }

                throw new IgniteClientConnectionException(msg);
            }

            var featuresLen = unpacker.unpackBinaryHeader();
            unpacker.skipValue(featuresLen);

            var extensionsLen = unpacker.unpackMapHeader();
            unpacker.skipValue(extensionsLen);

            protocolCtx = protocolContextFromVersion(srvVer);
        }
    }

    /** Write bytes to the output stream. */
    private ChannelFuture write(ClientMessagePacker packer) throws IgniteClientConnectionException {
        var buf = packer.getBuffer();

        return sock.send(buf);
    }

    /**
     *
     */
    private static class ClientRequestFuture extends CompletableFuture<ClientMessageUnpacker> {
    }
}

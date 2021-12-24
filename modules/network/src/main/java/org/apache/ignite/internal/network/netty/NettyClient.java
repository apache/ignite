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

package org.apache.ignite.internal.network.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.stream.ChunkedWriteHandler;
import java.net.SocketAddress;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.serialization.PerSessionSerializationService;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.NetworkMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Netty client channel wrapper.
 */
public class NettyClient {
    /** A lock for start and stop operations. */
    private final Object startStopLock = new Object();

    /** Serialization service. */
    private final SerializationService serializationService;

    /** Destination address. */
    private final SocketAddress address;

    /** Future that resolves when the client finished the handshake. */
    @Nullable
    private volatile CompletableFuture<NettySender> clientFuture = null;

    /** Future that resolves when the client channel is opened. */
    private CompletableFuture<Void> channelFuture = new CompletableFuture<>();

    /** Client channel. */
    @Nullable
    private volatile Channel channel = null;

    /** Message listener. */
    private final BiConsumer<SocketAddress, NetworkMessage> messageListener;

    /** Handshake manager. */
    private final HandshakeManager handshakeManager;

    /** Flag indicating if {@link #stop()} has been called. */
    private boolean stopped = false;

    /**
     * Constructor.
     *
     * @param address               Destination address.
     * @param serializationService  Serialization service.
     * @param manager               Client handshake manager.
     * @param messageListener       Message listener.
     */
    public NettyClient(
            SocketAddress address,
            SerializationService serializationService,
            HandshakeManager manager,
            BiConsumer<SocketAddress, NetworkMessage> messageListener
    ) {
        this.address = address;
        this.serializationService = serializationService;
        this.handshakeManager = manager;
        this.messageListener = messageListener;
    }

    /**
     * Start client.
     *
     * @param bootstrapTemplate Template client bootstrap.
     * @return Future that resolves when client channel is opened.
     */
    public CompletableFuture<NettySender> start(Bootstrap bootstrapTemplate) {
        synchronized (startStopLock) {
            if (stopped) {
                throw new IgniteInternalException("Attempted to start an already stopped NettyClient");
            }

            if (clientFuture != null) {
                throw new IgniteInternalException("Attempted to start an already started NettyClient");
            }

            Bootstrap bootstrap = bootstrapTemplate.clone();

            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                /** {@inheritDoc} */
                @Override
                public void initChannel(SocketChannel ch) {
                    var sessionSerializationService = new PerSessionSerializationService(serializationService);

                    ch.pipeline().addLast(
                            new InboundDecoder(sessionSerializationService),
                            new HandshakeHandler(handshakeManager),
                            new MessageHandler(messageListener),
                            new ChunkedWriteHandler(),
                            new OutboundEncoder(sessionSerializationService),
                            new IoExceptionSuppressingHandler()
                    );
                }
            });

            clientFuture = NettyUtils.toChannelCompletableFuture(bootstrap.connect(address))
                    .handle((channel, throwable) -> {
                        synchronized (startStopLock) {
                            this.channel = channel;

                            if (throwable != null) {
                                channelFuture.completeExceptionally(throwable);
                            } else {
                                channelFuture.complete(null);
                            }

                            if (stopped) {
                                return CompletableFuture.<NettySender>failedFuture(new CancellationException("Client was stopped"));
                            } else if (throwable != null) {
                                return CompletableFuture.<NettySender>failedFuture(throwable);
                            } else {
                                return handshakeManager.handshakeFuture();
                            }
                        }
                    })
                    .thenCompose(Function.identity());

            return clientFuture;
        }
    }

    /**
     * Returns client start future.
     *
     * @return Client start future.
     */
    @Nullable
    public CompletableFuture<NettySender> sender() {
        return clientFuture;
    }

    /**
     * Stops the client.
     *
     * @return Future that is resolved when the client's channel has closed or an already completed future for a subsequent call.
     */
    public CompletableFuture<Void> stop() {
        synchronized (startStopLock) {
            if (stopped) {
                return CompletableFuture.completedFuture(null);
            }

            stopped = true;

            if (clientFuture == null) {
                return CompletableFuture.completedFuture(null);
            }

            return channelFuture
                    .handle((sender, throwable) ->
                            channel == null
                                    ? CompletableFuture.<Void>completedFuture(null) : NettyUtils.toCompletableFuture(channel.close()))
                    .thenCompose(Function.identity());
        }
    }

    /**
     * Returns {@code true} if the client has failed to connect to the remote server, {@code false} otherwise.
     *
     * @return {@code true} if the client has failed to connect to the remote server, {@code false} otherwise.
     */
    public boolean failedToConnect() {
        return clientFuture != null && clientFuture.isCompletedExceptionally();
    }

    /**
     * Returns {@code true} if the client has lost the connection or has been stopped, {@code false} otherwise.
     *
     * @return {@code true} if the client has lost the connection or has been stopped, {@code false} otherwise.
     */
    public boolean isDisconnected() {
        return (channel != null && !channel.isOpen()) || stopped;
    }
}

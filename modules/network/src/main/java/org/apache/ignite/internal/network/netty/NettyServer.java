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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.stream.ChunkedWriteHandler;
import java.net.SocketAddress;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.configuration.schemas.network.NetworkView;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.serialization.PerSessionSerializationService;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.network.NetworkMessage;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Netty server channel wrapper.
 */
public class NettyServer {
    /** A lock for start and stop operations. */
    private final Object startStopLock = new Object();

    /** Bootstrap factory. */
    private final NettyBootstrapFactory bootstrapFactory;

    /** Server socket configuration. */
    private final NetworkView configuration;

    /** Serialization service. */
    private final SerializationService serializationService;

    /** Incoming message listener. */
    private final BiConsumer<SocketAddress, NetworkMessage> messageListener;

    /** Handshake manager. */
    private final Supplier<HandshakeManager> handshakeManager;

    /** Server start future. */
    private CompletableFuture<Void> serverStartFuture;

    /** Server socket channel. */
    @Nullable
    private volatile ServerChannel channel;

    /** Server close future. */
    @Nullable
    private CompletableFuture<Void> serverCloseFuture;

    /** New connections listener. */
    private final Consumer<NettySender> newConnectionListener;

    /** Flag indicating if {@link #stop()} has been called. */
    private boolean stopped;

    /**
     * Constructor.
     *
     * @param configuration         Server configuration.
     * @param handshakeManager      Handshake manager supplier.
     * @param newConnectionListener New connections listener.
     * @param messageListener       Message listener.
     * @param serializationService  Serialization service.
     * @param bootstrapFactory      Netty bootstrap factory.
     */
    public NettyServer(
            NetworkView configuration,
            Supplier<HandshakeManager> handshakeManager,
            Consumer<NettySender> newConnectionListener,
            BiConsumer<SocketAddress, NetworkMessage> messageListener,
            SerializationService serializationService,
            NettyBootstrapFactory bootstrapFactory
    ) {
        this.configuration = configuration;
        this.handshakeManager = handshakeManager;
        this.newConnectionListener = newConnectionListener;
        this.messageListener = messageListener;
        this.serializationService = serializationService;
        this.bootstrapFactory = bootstrapFactory;
    }

    /**
     * Starts the server.
     *
     * @return Future that resolves when the server is successfully started.
     */
    public CompletableFuture<Void> start() {
        synchronized (startStopLock) {
            if (stopped) {
                throw new IgniteInternalException("Attempted to start an already stopped server");
            }

            if (serverStartFuture != null) {
                throw new IgniteInternalException("Attempted to start an already started server");
            }

            ServerBootstrap bootstrap = bootstrapFactory.createServerBootstrap();

            bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                        /** {@inheritDoc} */
                        @Override
                        public void initChannel(SocketChannel ch) {
                            var sessionSerializationService = new PerSessionSerializationService(serializationService);

                            // Get handshake manager for the new channel.
                            HandshakeManager manager = handshakeManager.get();

                            ch.pipeline().addLast(
                                    /*
                                     * Decoder that uses the MessageReader
                                     * to read chunked data.
                                     */
                                    new InboundDecoder(sessionSerializationService),
                                    // Handshake handler.
                                    new HandshakeHandler(manager),
                                    // Handles decoded NetworkMessages.
                                    new MessageHandler(messageListener),
                                    /*
                                     * Encoder that uses the MessageWriter
                                     * to write chunked data.
                                     */
                                    new ChunkedWriteHandler(),
                                    // Converts NetworkMessage to a ChunkedNetworkMessageInput
                                    new OutboundEncoder(sessionSerializationService),
                                    new IoExceptionSuppressingHandler()
                            );

                            manager.handshakeFuture().thenAccept(newConnectionListener);
                        }
                    });

            int port = configuration.port();
            int portRange = configuration.portRange();

            var bindFuture = new CompletableFuture<Channel>();

            tryBind(bootstrap, port, port + portRange, port, bindFuture);

            serverStartFuture = bindFuture
                    .handle((channel, err) -> {
                        synchronized (startStopLock) {
                            if (channel != null) {
                                serverCloseFuture = NettyUtils.toCompletableFuture(channel.closeFuture());
                            }

                            this.channel = (ServerChannel) channel;

                            if (err != null || stopped) {
                                Throwable stopErr = err != null ? err : new CancellationException("Server was stopped");

                                return CompletableFuture.<Void>failedFuture(stopErr);
                            } else {
                                return CompletableFuture.<Void>completedFuture(null);
                            }
                        }
                    })
                    .thenCompose(Function.identity());

            return serverStartFuture;
        }
    }

    /**
     * Try bind this server to a port.
     *
     * @param bootstrap Bootstrap.
     * @param port      Target port.
     * @param endPort   Last port that server can be bound to.
     * @param startPort Start port.
     * @param fut       Future.
     */
    private void tryBind(ServerBootstrap bootstrap, int port, int endPort, int startPort, CompletableFuture<Channel> fut) {
        if (port > endPort) {
            fut.completeExceptionally(new IllegalStateException("No available port in range [" + startPort + "-" + endPort + ']'));
        }

        bootstrap.bind(port).addListener((ChannelFuture future) -> {
            if (future.isSuccess()) {
                fut.complete(future.channel());
            } else if (future.isCancelled()) {
                fut.cancel(true);
            } else {
                tryBind(bootstrap, port + 1, endPort, startPort, fut);
            }
        });
    }

    /**
     * Returns gets the local address of the server.
     *
     * @return Gets the local address of the server.
     */
    public SocketAddress address() {
        return channel.localAddress();
    }

    /**
     * Stops the server.
     *
     * @return Future that is resolved when the server's channel has closed or an already completed future for a subsequent call.
     */
    public CompletableFuture<Void> stop() {
        synchronized (startStopLock) {
            if (stopped) {
                return CompletableFuture.completedFuture(null);
            }

            stopped = true;

            if (serverStartFuture == null) {
                return CompletableFuture.completedFuture(null);
            }

            var serverCloseFuture0 = serverCloseFuture;

            return serverStartFuture.handle((unused, throwable) -> {
                if (channel != null) {
                    channel.close();
                }

                return serverCloseFuture0 == null ? CompletableFuture.<Void>completedFuture(null) : serverCloseFuture0;
            }).thenCompose(Function.identity());
        }
    }

    /**
     * Returns {@code true} if the server is running, {@code false} otherwise.
     *
     * @return {@code true} if the server is running, {@code false} otherwise.
     */
    @TestOnly
    public boolean isRunning() {
        var channel0 = channel;

        return channel0 != null && channel0.isOpen();
    }
}

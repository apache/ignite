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

package org.apache.ignite.network.internal.netty;

import java.net.SocketAddress;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Netty server channel wrapper.
 */
public class NettyServer {
    /** A lock for start and stop operations. */
    private final Object startStopLock = new Object();

    /** {@link NioServerSocketChannel} bootstrapper. */
    private final ServerBootstrap bootstrap;

    /** Socket accepter event loop group. */
    private final NioEventLoopGroup bossGroup = new NioEventLoopGroup();

    /** Socket handler event loop group. */
    private final NioEventLoopGroup workerGroup = new NioEventLoopGroup();

    /** Server port. */
    private final int port;

    /** Serialization registry. */
    private final MessageSerializationRegistry serializationRegistry;

    /** Incoming message listener. */
    private final BiConsumer<SocketAddress, NetworkMessage> messageListener;

    /** Server start future. */
    private CompletableFuture<Void> serverStartFuture;

    /** Server socket channel. */
    @Nullable
    private volatile ServerChannel channel;

    /** Server close future. */
    private CompletableFuture<Void> serverCloseFuture = CompletableFuture.allOf(
        NettyUtils.toCompletableFuture(bossGroup.terminationFuture()),
        NettyUtils.toCompletableFuture(workerGroup.terminationFuture())
    );

    /** New connections listener. */
    private final Consumer<NettySender> newConnectionListener;

    /** Flag indicating if {@link #stop()} has been called. */
    private boolean stopped = false;

    /**
     * Constructor.
     *
     * @param port Server port.
     * @param newConnectionListener New connections listener.
     * @param messageListener Message listener.
     * @param serializationRegistry Serialization registry.
     */
    public NettyServer(
        int port,
        Consumer<NettySender> newConnectionListener,
        BiConsumer<SocketAddress, NetworkMessage> messageListener,
        MessageSerializationRegistry serializationRegistry
    ) {
        this(new ServerBootstrap(), port, newConnectionListener, messageListener, serializationRegistry);
    }

    /**
     * Constructor.
     *
     * @param bootstrap Server bootstrap.
     * @param port Server port.
     * @param newConnectionListener New connections listener.
     * @param messageListener Message listener.
     * @param serializationRegistry Serialization registry.
     */
    public NettyServer(
        ServerBootstrap bootstrap,
        int port,
        Consumer<NettySender> newConnectionListener,
        BiConsumer<SocketAddress, NetworkMessage> messageListener,
        MessageSerializationRegistry serializationRegistry
    ) {
        this.bootstrap = bootstrap;
        this.port = port;
        this.newConnectionListener = newConnectionListener;
        this.messageListener = messageListener;
        this.serializationRegistry = serializationRegistry;
    }

    /**
     * Starts the server.
     *
     * @return Future that resolves when the server is successfully started.
     */
    public CompletableFuture<Void> start() {
        synchronized (startStopLock) {
            if (stopped)
                throw new IgniteInternalException("Attempted to start an already stopped server");

            if (serverStartFuture != null)
                throw new IgniteInternalException("Attempted to start an already started server");

            bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    /** {@inheritDoc} */
                    @Override public void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(
                            /*
                             * Decoder that uses org.apache.ignite.network.internal.MessageReader
                             * to read chunked data.
                             */
                            new InboundDecoder(serializationRegistry),
                            // Handles decoded NetworkMessages.
                            new MessageHandler(messageListener),
                            /*
                             * Encoder that uses org.apache.ignite.network.internal.MessageWriter
                             * to write chunked data.
                             */
                            new ChunkedWriteHandler()
                        );

                        newConnectionListener.accept(new NettySender(ch, serializationRegistry));
                    }
                })
                /*
                 * The maximum queue length for incoming connection indications (a request to connect) is set
                 * to the backlog parameter. If a connection indication arrives when the queue is full,
                 * the connection is refused.
                 */
                .option(ChannelOption.SO_BACKLOG, 128)
                /*
                 * When the keepalive option is set for a TCP socket and no data has been exchanged across the socket
                 * in either direction for 2 hours (NOTE: the actual value is implementation dependent),
                 * TCP automatically sends a keepalive probe to the peer.
                 */
                .childOption(ChannelOption.SO_KEEPALIVE, true);

            serverStartFuture = NettyUtils.toChannelCompletableFuture(bootstrap.bind(port))
                .handle((channel, err) -> {
                    synchronized (startStopLock) {
                        CompletableFuture<Void> workerCloseFuture = serverCloseFuture;

                        if (channel != null) {
                            CompletableFuture<Void> channelCloseFuture = NettyUtils.toCompletableFuture(channel.closeFuture())
                                // Shutdown event loops on channel close.
                                .whenComplete((v, err0) -> shutdownEventLoopGroups());

                            serverCloseFuture = CompletableFuture.allOf(channelCloseFuture, workerCloseFuture);
                        }

                        this.channel = (ServerChannel) channel;

                        // Shutdown event loops if the server has failed to start or has been stopped.
                        if (err != null || stopped) {
                            shutdownEventLoopGroups();

                            return workerCloseFuture.handle((unused, throwable) -> {
                                Throwable stopErr = err != null ? err : new CancellationException("Server was stopped");

                                if (throwable != null)
                                    stopErr.addSuppressed(throwable);

                                return CompletableFuture.<Void>failedFuture(stopErr);
                            }).thenCompose(Function.identity());
                        }
                        else
                            return CompletableFuture.<Void>completedFuture(null);
                    }
                })
                .thenCompose(Function.identity());

            return serverStartFuture;
        }
    }

    /**
     * @return Gets the local address of the server.
     */
    public SocketAddress address() {
        return channel.localAddress();
    }

    /**
     * Stops the server.
     *
     * @return Future that is resolved when the server's channel has closed or an already completed future
     * for a subsequent call.
     */
    public CompletableFuture<Void> stop() {
        synchronized (startStopLock) {
            if (stopped)
                return CompletableFuture.completedFuture(null);

            stopped = true;

            if (serverStartFuture == null)
                return CompletableFuture.completedFuture(null);

            return serverStartFuture.handle((unused, throwable) -> {
               if (channel != null)
                   channel.close();

               return serverCloseFuture;
            }).thenCompose(Function.identity());
        }
    }

    /**
     * Shutdown event loops.
     */
    private void shutdownEventLoopGroups() {
        // TODO: IGNITE-14538 quietPeriod and timeout should be configurable.
        bossGroup.shutdownGracefully(0L, 15, TimeUnit.SECONDS);
        workerGroup.shutdownGracefully(0L, 15, TimeUnit.SECONDS);
    }

    /**
     * @return {@code true} if the server is running, {@code false} otherwise.
     */
    @TestOnly
    public boolean isRunning() {
        return channel != null && channel.isOpen() && !bossGroup.isShuttingDown() && !workerGroup.isShuttingDown();
    }

    /**
     * @return Accepter event loop group.
     */
    @TestOnly
    public NioEventLoopGroup getBossGroup() {
        return bossGroup;
    }

    /**
     * @return Worker event loop group.
     */
    @TestOnly
    public NioEventLoopGroup getWorkerGroup() {
        return workerGroup;
    }
}

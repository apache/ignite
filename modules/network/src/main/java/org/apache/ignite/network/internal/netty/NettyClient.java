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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.jetbrains.annotations.Nullable;

/**
 * Netty client channel wrapper.
 */
public class NettyClient {
    /** A lock for start and stop operations. */
    private final Object startStopLock = new Object();

    /** Serialization registry. */
    private final MessageSerializationRegistry serializationRegistry;

    /** Destination address. */
    private final SocketAddress address;

    /** Future that resolves when client channel is opened. */
    @Nullable
    private volatile CompletableFuture<NettySender> clientFuture = null;

    /** Client channel. */
    @Nullable
    private volatile Channel channel = null;

    /** Flag indicating if {@link #stop()} has been called. */
    private boolean stopped = false;

    /**
     * Constructor.
     *
     * @param host Host.
     * @param port Port.
     * @param serializationRegistry Serialization registry.
     */
    public NettyClient(
        String host,
        int port,
        MessageSerializationRegistry serializationRegistry
    ) {
        this(new InetSocketAddress(host, port), serializationRegistry);
    }

    /**
     * Constructor.
     *
     * @param address Destination address.
     * @param serializationRegistry Serialization registry.
     */
    public NettyClient(
        SocketAddress address,
        MessageSerializationRegistry serializationRegistry
    ) {
        this.address = address;
        this.serializationRegistry = serializationRegistry;
    }

    /**
     * Start client.
     *
     * @return Future that resolves when client channel is opened.
     */
    public CompletableFuture<NettySender> start(Bootstrap bootstrap) {
        synchronized (startStopLock) {
            if (stopped)
                throw new IgniteInternalException("Attempted to start an already stopped NettyClient");

            if (clientFuture != null)
                throw new IgniteInternalException("Attempted to start an already started NettyClient");

            clientFuture = NettyUtils.toChannelCompletableFuture(bootstrap.connect(address))
                .handle((channel, throwable) -> {
                    synchronized (startStopLock) {
                        this.channel = channel;

                        if (stopped)
                            return CompletableFuture.<NettySender>failedFuture(new CancellationException("Client was stopped"));
                        else if (throwable != null)
                            return CompletableFuture.<NettySender>failedFuture(throwable);
                        else
                            return CompletableFuture.completedFuture(new NettySender(channel, serializationRegistry));
                    }
                })
                .thenCompose(Function.identity());

            return clientFuture;
        }
    }

    /**
     * @return Client start future.
     */
    @Nullable
    public CompletableFuture<NettySender> sender() {
        return clientFuture;
    }

    /**
     * Stops the client.
     *
     * @return Future that is resolved when the client's channel has closed or an already completed future
     * for a subsequent call.
     */
    public CompletableFuture<Void> stop() {
        synchronized (startStopLock) {
            if (stopped)
                return CompletableFuture.completedFuture(null);

            stopped = true;

            if (clientFuture == null)
                return CompletableFuture.completedFuture(null);

            return clientFuture
                .handle((sender, throwable) ->
                    channel == null ?
                        CompletableFuture.<Void>completedFuture(null) :
                        NettyUtils.toCompletableFuture(channel.close()))
                .thenCompose(Function.identity());
        }
    }

    /**
     * @return {@code true} if the client has failed to connect to the remote server, {@code false} otherwise.
     */
    public boolean failedToConnect() {
        return clientFuture != null && clientFuture.isCompletedExceptionally();
    }

    /**
     * @return {@code true} if the client has lost the connection or has been stopped, {@code false} otherwise.
     */
    public boolean isDisconnected() {
        return (channel != null && !channel.isOpen()) || stopped;
    }
}

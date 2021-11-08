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

import io.netty.channel.Channel;
import io.netty.handler.stream.ChunkedInput;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.direct.DirectMessageWriter;
import org.apache.ignite.network.NetworkMessage;
import org.jetbrains.annotations.TestOnly;

/**
 * Wrapper for a Netty {@link Channel}, that uses {@link ChunkedInput} and {@link DirectMessageWriter} to send data.
 */
public class NettySender {
    /** Netty channel. */
    private final Channel channel;

    /** Launch id of the remote node. */
    private final String launchId;

    /** Consistent id of the remote node. */
    private final String consistentId;

    /**
     * Constructor.
     *
     * @param channel      Netty channel.
     * @param launchId     Launch id of the remote node.
     * @param consistentId Consistent id of the remote node.
     */
    public NettySender(Channel channel, String launchId, String consistentId) {
        this.channel = channel;
        this.launchId = launchId;
        this.consistentId = consistentId;
    }

    /**
     * Sends the message.
     *
     * @param msg Network message.
     * @return Future of the send operation.
     */
    public CompletableFuture<Void> send(NetworkMessage msg) {
        return NettyUtils.toCompletableFuture(channel.writeAndFlush(msg));
    }

    /**
     * Returns launch id of the remote node.
     *
     * @return Launch id of the remote node.
     */
    public String launchId() {
        return launchId;
    }

    /**
     * Returns consistent id of the remote node.
     *
     * @return Consistent id of the remote node.
     */
    public String consistentId() {
        return consistentId;
    }

    /**
     * Closes channel.
     */
    public void close() {
        this.channel.close().awaitUninterruptibly();
    }

    /**
     * Returns {@code true} if the channel is open, {@code false} otherwise.
     *
     * @return {@code true} if the channel is open, {@code false} otherwise.
     */
    public boolean isOpen() {
        return this.channel.isOpen();
    }

    /**
     * Returns channel.
     *
     * @return Channel.
     */
    @TestOnly
    public Channel channel() {
        return channel;
    }
}

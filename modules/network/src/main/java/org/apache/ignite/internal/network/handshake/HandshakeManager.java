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

package org.apache.ignite.internal.network.handshake;

import java.util.concurrent.CompletableFuture;
import io.netty.channel.Channel;
import org.apache.ignite.internal.network.netty.HandshakeHandler;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.network.NetworkMessage;

/**
 * Handshake operation manager.
 */
public interface HandshakeManager {
    /**
     * Initialize handshake manager with the channel.
     *
     * @param channel Channel.
     * @return Action to perform by {@link HandshakeHandler}.
     */
    HandshakeAction init(Channel channel);

    /**
     * Handle an event of the connection opening.
     *
     * @param channel Channel.
     * @return Action to perform by {@link HandshakeHandler}.
     */
    HandshakeAction onConnectionOpen(Channel channel);

    /**
     * Handle an incoming message.
     *
     * @param channel Channel.
     * @param message Message to handle.
     * @return Action to perform by {@link HandshakeHandler}.
     */
    HandshakeAction onMessage(Channel channel, NetworkMessage message);

    /**
     * @return Future that represents the handshake operation.
     */
    CompletableFuture<NettySender> handshakeFuture();
}

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

package org.apache.ignite.internal.network.recovery;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.handshake.HandshakeException;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.handshake.HandshakeResult;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.network.netty.NettyUtils;
import org.apache.ignite.internal.network.recovery.message.HandshakeStartMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeStartResponseMessage;
import org.apache.ignite.network.NetworkMessage;

/**
 * Recovery protocol handshake manager for a server.
 */
public class RecoveryServerHandshakeManager implements HandshakeManager {
    /** Launch id. */
    private final UUID launchId;

    /** Consistent id. */
    private final String consistentId;

    /** Handshake completion future. */
    private final CompletableFuture<NettySender> handshakeCompleteFuture = new CompletableFuture<>();

    /** Message factory. */
    private final NetworkMessagesFactory messageFactory;

    /**
     * Constructor.
     *
     * @param launchId       Launch id.
     * @param consistentId   Consistent id.
     * @param messageFactory Message factory.
     */
    public RecoveryServerHandshakeManager(
            UUID launchId, String consistentId, NetworkMessagesFactory messageFactory
    ) {
        this.launchId = launchId;
        this.consistentId = consistentId;
        this.messageFactory = messageFactory;
    }

    /** {@inheritDoc} */
    @Override
    public HandshakeResult init(Channel channel) {
        return HandshakeResult.noOp();
    }

    /** {@inheritDoc} */
    @Override
    public HandshakeResult onConnectionOpen(Channel channel) {
        HandshakeStartMessage handshakeStartMessage = messageFactory.handshakeStartMessage()
                .launchId(launchId)
                .consistentId(consistentId)
                .build();

        ChannelFuture sendFuture = channel.writeAndFlush(handshakeStartMessage);

        NettyUtils.toCompletableFuture(sendFuture).whenComplete((unused, throwable) -> {
            if (throwable != null) {
                handshakeCompleteFuture.completeExceptionally(
                        new HandshakeException("Failed to send handshake start message: " + throwable.getMessage(), throwable)
                );
            }
        });

        return HandshakeResult.noOp();
    }

    /** {@inheritDoc} */
    @Override
    public HandshakeResult onMessage(Channel channel, NetworkMessage message) {
        if (message instanceof HandshakeStartResponseMessage) {
            HandshakeStartResponseMessage msg = (HandshakeStartResponseMessage) message;

            UUID remoteLaunchId = msg.launchId();
            String remoteConsistentId = msg.consistentId();

            handshakeCompleteFuture.complete(new NettySender(channel, remoteLaunchId.toString(), remoteConsistentId));

            return HandshakeResult.removeHandler(remoteLaunchId, remoteConsistentId);
        }

        handshakeCompleteFuture.completeExceptionally(
                new HandshakeException("Unexpected message during handshake: " + message.toString())
        );

        return HandshakeResult.fail();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NettySender> handshakeFuture() {
        return handshakeCompleteFuture;
    }
}

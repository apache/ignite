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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.function.Function;
import org.apache.ignite.internal.network.handshake.HandshakeAction;
import org.apache.ignite.internal.network.handshake.HandshakeException;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.handshake.HandshakeResult;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.NetworkMessage;

/**
 * Netty handler of the handshake operation.
 */
public class HandshakeHandler extends ChannelInboundHandlerAdapter {
    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(HandshakeHandler.class);

    /** Handshake manager. */
    private final HandshakeManager manager;

    /** Function that creates a handler that substitutes this handshake handler after the handshake is done. */
    private final Function<String, ChannelInboundHandlerAdapter> newHandlerCreator;

    /**
     * Constructor.
     *
     * @param manager Handshake manager.
     */
    public HandshakeHandler(HandshakeManager manager, Function<String, ChannelInboundHandlerAdapter> newHandlerCreator) {
        this.manager = manager;
        this.newHandlerCreator = newHandlerCreator;
    }

    /** {@inheritDoc} */
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        HandshakeResult handshakeResult = manager.init(ctx.channel());

        handleHandshakeAction(handshakeResult, ctx);
    }

    /** {@inheritDoc} */
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        HandshakeResult handshakeResult = manager.onConnectionOpen(ctx.channel());

        manager.handshakeFuture().whenComplete((unused, throwable) -> {
            if (throwable != null) {
                LOG.debug("Error when performing handshake", throwable);

                ctx.close();
            }
        });

        handleHandshakeAction(handshakeResult, ctx);

        ctx.fireChannelActive();
    }

    /** {@inheritDoc} */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        HandshakeResult handshakeResult = manager.onMessage(ctx.channel(), (NetworkMessage) msg);

        handleHandshakeAction(handshakeResult, ctx);
        // No need to forward the message to the next handler as this message only matters for a handshake.
    }

    /** {@inheritDoc} */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // If this method is called that means channel has been closed before handshake has finished or handshake
        // has failed.
        manager.handshakeFuture().completeExceptionally(
                new HandshakeException("Channel has been closed before handshake has finished or handshake has failed")
        );

        ctx.fireChannelInactive();
    }

    /** {@inheritDoc} */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        manager.handshakeFuture().completeExceptionally(cause);
    }

    /**
     * Handle {@link HandshakeAction}.
     *
     * @param result Handshake result.
     * @param ctx    Netty channel context.
     */
    private void handleHandshakeAction(HandshakeResult result, ChannelHandlerContext ctx) {
        HandshakeAction action = result.action();
        switch (action) {
            case REMOVE_HANDLER:
                ChannelInboundHandlerAdapter newHandler = newHandlerCreator.apply(result.consistentId());
                String handlerName = newHandler.getClass().getName();
                ctx.pipeline().replace(this, handlerName, newHandler);
                break;

            case FAIL:
                ctx.channel().close();
                break;

            case NOOP:
                break;

            default:
                break;
        }
    }
}

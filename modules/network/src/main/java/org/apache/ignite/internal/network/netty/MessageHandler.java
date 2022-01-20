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
import java.util.function.BiConsumer;
import org.apache.ignite.network.NetworkMessage;

/**
 * Network message handler that delegates handling to {@link #messageListener}.
 */
public class MessageHandler extends ChannelInboundHandlerAdapter {
    /** Message listener. */
    private final BiConsumer<String, NetworkMessage> messageListener;

    /** Consistent id of the remote node. */
    private final String consistentId;

    /**
     * Constructor.
     *
     * @param messageListener Message listener.
     * @param consistentId Consistent id of the remote node.
     */
    public MessageHandler(BiConsumer<String, NetworkMessage> messageListener, String consistentId) {
        this.messageListener = messageListener;
        this.consistentId = consistentId;
    }

    /** {@inheritDoc} */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        NetworkMessage message = (NetworkMessage) msg;

        messageListener.accept(consistentId, message);
    }
}

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

package org.apache.ignite.spi.discovery.zk.internal;

import org.apache.ignite.internal.OperationContextMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextDispatcher;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.jetbrains.annotations.Nullable;

/**
 * <p>A holder for effective attributes of distributed {@link OperationContext}. Analogue of
 * {@link TcpDiscoveryAbstractMessage#opCtxMsg} while we do not use a base class for all the Zookeper messages.</p>
 *
 * <p>NOTE: The difference is also the limitation on message type. In {@link TcpDiscoverySpi} we transfer distributed
 * {@link OperationContext} with all the messages. In {@link ZookeeperDiscoveryImpl} with custom events only.</p>
 *
 * @see OperationContextDispatcher
 * @see GridDiscoveryManager#sendCustomEvent(DiscoveryCustomMessage)
 */
public class ZkCustomEventMessage implements Message {
    /** */
    @Order(0)
    DiscoverySpiCustomMessage originalMsg;

    /** */
    @Order(1)
    @Nullable OperationContextMessage opCtxMsg;

    /** Default constructor for {@link MessageFactory}. */
    public ZkCustomEventMessage() {
        // No-op.
    }

    /** */
    public ZkCustomEventMessage(DiscoverySpiCustomMessage msg, @Nullable OperationContextMessage opCtxMsg) {
        this.originalMsg = msg;
        this.opCtxMsg = opCtxMsg;
    }

    /**
     * Wraps {@code msg} as or casts to {@link ZkCustomEventMessage}.
     *
     * @param msg either a {@link DiscoverySpiCustomMessage} or a {@link ZkCustomEventMessage}.
     */
    public static ZkCustomEventMessage of(Message msg) {
        assert msg instanceof ZkCustomEventMessage || msg instanceof DiscoverySpiCustomMessage;

        return msg instanceof ZkCustomEventMessage
            ? (ZkCustomEventMessage)msg
            : new ZkCustomEventMessage((DiscoverySpiCustomMessage)msg, null);
    }
}

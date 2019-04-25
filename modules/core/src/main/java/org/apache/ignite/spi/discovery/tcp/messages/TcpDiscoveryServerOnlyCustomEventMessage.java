/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.discovery.tcp.messages;

import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * Wrapped for custom message that must not be delivered to the client nodes.
 */
@TcpDiscoveryEnsureDelivery
public class TcpDiscoveryServerOnlyCustomEventMessage extends TcpDiscoveryCustomEventMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param creatorNodeId Creator node id.
     * @param msg Message.
     * @param msgBytes Serialized message.
     */
    public TcpDiscoveryServerOnlyCustomEventMessage(UUID creatorNodeId, @Nullable DiscoverySpiCustomMessage msg,
        @NotNull byte[] msgBytes) {
        super(creatorNodeId, msg, msgBytes);
    }
}

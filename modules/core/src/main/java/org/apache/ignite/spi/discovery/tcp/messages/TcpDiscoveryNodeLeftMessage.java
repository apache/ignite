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

import java.util.UUID;
import org.apache.ignite.internal.processors.tracing.messages.SpanContainer;
import org.apache.ignite.internal.processors.tracing.messages.TraceableMessage;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Sent by node that is stopping to coordinator across the ring,
 * then sent by coordinator across the ring.
 */
@TcpDiscoveryEnsureDelivery
@TcpDiscoveryRedirectToClient
public class TcpDiscoveryNodeLeftMessage extends TcpDiscoveryAbstractMessage implements TraceableMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Span container. */
    private final SpanContainer spanContainer = new SpanContainer();

    /**
     * Constructor.
     *
     * @param creatorNodeId ID of the node that is about to leave the topology.
     */
    public TcpDiscoveryNodeLeftMessage(UUID creatorNodeId) {
        super(creatorNodeId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryNodeLeftMessage.class, this, "super", super.toString());
    }

    /** {@inheritDoc} */
    @Override public SpanContainer spanContainer() {
        return spanContainer;
    }
}
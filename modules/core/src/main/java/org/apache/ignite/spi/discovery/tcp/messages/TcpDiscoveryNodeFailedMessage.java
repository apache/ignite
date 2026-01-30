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

package org.apache.ignite.spi.discovery.tcp.messages;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Sent by node that has detected node failure to coordinator across the ring,
 * then sent by coordinator across the ring.
 */
@TcpDiscoveryEnsureDelivery
@TcpDiscoveryRedirectToClient
public class TcpDiscoveryNodeFailedMessage extends TcpDiscoveryAbstractTraceableMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** ID of the failed node. */
    @Order(6)
    private UUID failedNodeId;

    /** Internal order of the failed node. */
    @Order(value = 7, method = "internalOrder")
    private long order;

    /** */
    @Order(8)
    private String warning;

    /** Constructor. */
    public TcpDiscoveryNodeFailedMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNodeId ID of the node that detects nodes failure.
     * @param failedNodeId ID of the failed nodes.
     * @param internalOrder Order of the failed node.
     */
    public TcpDiscoveryNodeFailedMessage(UUID creatorNodeId, UUID failedNodeId, long internalOrder) {
        super(creatorNodeId);

        assert failedNodeId != null;
        assert internalOrder > 0;

        this.failedNodeId = failedNodeId;
        order = internalOrder;
    }

    /**
     * @param warning Warning message to be shown on all nodes.
     */
    public void warning(String warning) {
        this.warning = warning;
    }

    /**
     * @return Warning message to be shown on all nodes.
     */
    @Nullable public String warning() {
        return warning;
    }

    /**
     * Gets ID of the failed node.
     *
     * @return ID of the failed node.
     */
    public UUID failedNodeId() {
        return failedNodeId;
    }

    /**
     * @param failedNodeId ID of the failed node.
     */
    public void failedNodeId(UUID failedNodeId) {
        this.failedNodeId = failedNodeId;
    }

    /**
     * @return Internal order of the failed node.
     */
    public long internalOrder() {
        return order;
    }

    /**
     * @param order Internal order of the failed node.
     */
    public void internalOrder(long order) {
        this.order = order;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryNodeFailedMessage.class, this, "super", super.toString());
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 17;
    }
}

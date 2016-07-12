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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Sent by node that has detected node failure to coordinator across the ring,
 * then sent by coordinator across the ring.
 */
@TcpDiscoveryEnsureDelivery
@TcpDiscoveryRedirectToClient
public class TcpDiscoveryNodeFailedMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** ID of the failed node. */
    private final UUID failedNodeId;

    /** Internal order of the failed node. */
    private final long order;

    /** */
    private String warning;

    /**
     * Constructor.
     *
     * @param creatorNodeId ID of the node that detects nodes failure.
     * @param failedNodeId ID of the failed nodes.
     * @param order Order of the failed node.
     */
    public TcpDiscoveryNodeFailedMessage(UUID creatorNodeId, UUID failedNodeId, long order) {
        super(creatorNodeId);

        assert failedNodeId != null;
        assert order > 0;

        this.failedNodeId = failedNodeId;
        this.order = order;
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
     * @return Internal order of the failed node.
     */
    public long order() {
        return order;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryNodeFailedMessage.class, this, "super", super.toString());
    }
}
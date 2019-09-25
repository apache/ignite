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

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.jetbrains.annotations.Nullable;

/**
 * Message sent by node to its next to ensure that next node and
 * connection to it are alive. Receiving node should send it across the ring,
 * until message does not reach coordinator. Coordinator responds directly to node.
 * <p>
 * If a failed node id is specified then the message is sent across the ring up to the sender node
 * to ensure that the failed node is actually failed.
 */
public class TcpDiscoveryStatusCheckMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Status OK. */
    public static final int STATUS_OK = 1;

    /** Status RECONNECT. */
    public static final int STATUS_RECON = 2;

    /** Creator node. */
    private final TcpDiscoveryNode creatorNode;

    /** Creator node addresses. */
    private final Collection<InetSocketAddress> creatorNodeAddrs;

    /** Failed node id. */
    private final UUID failedNodeId;

    /** Creator node status (initialized by coordinator). */
    private int status;

    /**
     * Constructor.
     *
     * @param creatorNode Creator node.
     * @param failedNodeId Failed node id.
     */
    public TcpDiscoveryStatusCheckMessage(TcpDiscoveryNode creatorNode, UUID failedNodeId) {
        super(creatorNode.id());

        this.creatorNode = creatorNode;
        this.failedNodeId = failedNodeId;
        this.creatorNodeAddrs = null;
    }

    /**
     * Constructor.
     *
     * @param creatorNodeAddrs Addresses of creator node, used to be able not to serialize node in message.
     * @param creatorNodeId Creator node ID.
     * @param failedNodeId Failed node id.
     */
    public TcpDiscoveryStatusCheckMessage(UUID creatorNodeId, Collection<InetSocketAddress> creatorNodeAddrs, UUID failedNodeId) {
        super(creatorNodeId);

        this.creatorNodeAddrs = creatorNodeAddrs;
        this.creatorNode = null;
        this.failedNodeId = failedNodeId;
    }

    /**
     * Gets creator node.
     *
     * @return Creator node.
     */
    public @Nullable TcpDiscoveryNode creatorNode() {
        return creatorNode;
    }

    /**
     * Gets creator node addresses.
     *
     * @return Creator node addresses.
     */
    public Collection<InetSocketAddress> creatorNodeAddrs() {
        return creatorNodeAddrs;
    }

    /**
     * Gets failed node id.
     *
     * @return Failed node id.
     */
    public UUID failedNodeId() {
        return failedNodeId;
    }

    /**
     * Gets creator status.
     *
     * @return Creator node status.
     */
    public int status() {
        return status;
    }

    /**
     * Sets creator node status (should be set by coordinator).
     *
     * @param status Creator node status.
     */
    public void status(int status) {
        this.status = status;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        // NOTE!
        // Do not call super. As IDs will differ, but we can ignore this.

        if (!(obj instanceof TcpDiscoveryStatusCheckMessage))
            return false;

        TcpDiscoveryStatusCheckMessage other = (TcpDiscoveryStatusCheckMessage)obj;

        return F.eq(other.creatorNodeId(), creatorNodeId()) &&
            F.eq(other.failedNodeId, failedNodeId) &&
            status == other.status;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryStatusCheckMessage.class, this, "super", super.toString());
    }
}

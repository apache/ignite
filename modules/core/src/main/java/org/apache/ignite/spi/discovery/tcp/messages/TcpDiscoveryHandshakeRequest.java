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
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Handshake request.
 */
public class TcpDiscoveryHandshakeRequest extends TcpDiscoveryAbstractMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(value = 5, method = "previousNodeId")
    private @Nullable UUID prevNodeId;

    /** */
    @Order(6)
    private @Nullable String dcId;

    /**
     * Default constructor for {@link DiscoveryMessageFactory}.
     */
    public TcpDiscoveryHandshakeRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     */
    public TcpDiscoveryHandshakeRequest(UUID creatorNodeId) {
        super(creatorNodeId);
    }

    /**
     * Gets expected previous node ID to check.
     *
     * @return Previous node ID to check.
     */
    public @Nullable UUID previousNodeId() {
        return prevNodeId;
    }

    /**
     * Sets topology change request and previous node ID to check.<br>
     *
     * @param prevNodeId If not {@code null}, will set topology check request and node ID to check.
     */
    public void previousNodeId(@Nullable UUID prevNodeId) {
        this.prevNodeId = prevNodeId;
    }

    /** @return DataCenter id. */
    @Nullable public String dcId() {
        return dcId;
    }

    /** @param dcId DataCenter id. */
    public void dcId(String dcId) {
        this.dcId = dcId;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 8;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryHandshakeRequest.class, this, "super", super.toString(),
            "isChangeTopology", prevNodeId != null);
    }
}

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
 * Ping request.
 */
public class TcpDiscoveryClientPingResponse extends TcpDiscoveryAbstractMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Pinged client node ID. */
    @Order(5)
    private @Nullable UUID nodeToPing;

    /** */
    @Order(value = 6, method = "result")
    private boolean res;

    /** */
    public TcpDiscoveryClientPingResponse() {
        // No-op.
    }

    /**
     * @param creatorNodeId Creator node ID.
     * @param nodeToPing Pinged client node ID.
     * @param res Result of the node ping.
     */
    public TcpDiscoveryClientPingResponse(UUID creatorNodeId, @Nullable UUID nodeToPing, boolean res) {
        super(creatorNodeId);

        this.nodeToPing = nodeToPing;
        this.res = res;
    }

    /**
     * @return Pinged client node ID.
     */
    @Nullable public UUID nodeToPing() {
        return nodeToPing;
    }

    /**
     * @param nodeToPing Pinged client node ID.
     */
    public void nodeToPing(@Nullable UUID nodeToPing) {
        this.nodeToPing = nodeToPing;
    }

    /**
     * @return Result of ping.
     */
    public boolean result() {
        return res;
    }

    /**
     * @param res Result of ping.
     */
    public void result(boolean res) {
        this.res = res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryClientPingResponse.class, this, "super", super.toString());
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 4;
    }
}

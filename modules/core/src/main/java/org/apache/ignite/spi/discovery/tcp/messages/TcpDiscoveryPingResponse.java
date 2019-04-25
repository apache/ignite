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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Ping response.
 */
public class TcpDiscoveryPingResponse extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether pinged client exists. */
    private boolean clientExists;

    /**
     * @param creatorNodeId Creator node ID.
     */
    public TcpDiscoveryPingResponse(UUID creatorNodeId) {
        super(creatorNodeId);
    }

    /**
     * @param clientExists Whether pinged client exists.
     */
    public void clientExists(boolean clientExists) {
        this.clientExists = clientExists;
    }

    /**
     * @return Whether pinged client exists.
     */
    public boolean clientExists() {
        return clientExists;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryPingResponse.class, this, "super", super.toString());
    }
}
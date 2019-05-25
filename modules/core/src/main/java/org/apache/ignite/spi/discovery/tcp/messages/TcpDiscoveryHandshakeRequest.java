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

/**
 * Handshake request.
 */
public class TcpDiscoveryHandshakeRequest extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private UUID prevNodeId;

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     */
    public TcpDiscoveryHandshakeRequest(UUID creatorNodeId) {
        super(creatorNodeId);
    }

    /**
     * Gets topology change flag.<br>
     * {@code True} means node intent to fail nodes in a ring.
     *
     * @return Change topology flag.
     */
    public boolean changeTopology() {
        return getFlag(CHANGE_TOPOLOGY_FLAG_POS);
    }

    /**
     * Gets expected previous node ID to check.
     *
     * @return Previous node ID to check.
     */
    public UUID checkPreviousNodeId() {
        return prevNodeId;
    }

    /**
     * Sets topology change flag and previous node ID to check.<br>
     *
     * @param prevNodeId If not {@code null}, will set topology check flag and set node ID to check.
     */
    public void changeTopology(UUID prevNodeId) {
        setFlag(CHANGE_TOPOLOGY_FLAG_POS, prevNodeId != null);

        this.prevNodeId = prevNodeId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryHandshakeRequest.class, this, "super", super.toString(),
            "isChangeTopology", changeTopology());
    }
}
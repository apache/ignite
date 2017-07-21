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
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;

/**
 * Message telling joining node that new topology already contain
 * different node with same ID.
 */
public class TcpDiscoveryDuplicateIdMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node with duplicate ID. */
    private final TcpDiscoveryNode node;

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     * @param node Node with same ID.
     */
    public TcpDiscoveryDuplicateIdMessage(UUID creatorNodeId, TcpDiscoveryNode node) {
        super(creatorNodeId);

        assert node != null;

        this.node = node;
    }

    /**
     * @return Node with duplicate ID.
     */
    public TcpDiscoveryNode node() {
        return node;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryDuplicateIdMessage.class, this, "super", super.toString());
    }
}
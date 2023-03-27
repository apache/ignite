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

package org.apache.ignite.internal.processors.cache;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * WAL state propose message.
 */
public class WalStateProposeMessage extends WalStateAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID. */
    private final UUID nodeId;

    /** Cache names which are expected to be in the group along with their deployment IDs. */
    private Map<String, IgniteUuid> caches;

    /** Whether WAL should be enabled or disabled. */
    private final boolean enable;

    /** Whether message is being handled on cache affinity node. */
    private transient boolean affNode;

    /**
     * Constructor.
     *
     * @param opId Operation IDs.
     * @param grpId Expected group ID.
     * @param grpDepId Expected group deployment ID.
     * @param nodeId Node ID.
     * @param caches Expected cache names and their relevant deployment IDs.
     *
     * @param enable WAL state flag.
     */
    public WalStateProposeMessage(UUID opId, int grpId, IgniteUuid grpDepId, UUID nodeId,
        Map<String, IgniteUuid> caches, boolean enable) {
        super(opId, grpId, grpDepId);

        this.nodeId = nodeId;
        this.caches = caches;
        this.enable = enable;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Caches.
     */
    public Map<String, IgniteUuid> caches() {
        return caches;
    }

    /**
     * @return WAL state flag.
     */
    public boolean enable() {
        return enable;
    }

    /**
     * @return Whether message is being handled on cache affintiy node.
     */
    public boolean affinityNode() {
        return affNode;
    }

    /**
     * @param affNode Whether message is being handled on cache affintiy node.
     */
    public void affinityNode(boolean affNode) {
        this.affNode = affNode;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WalStateProposeMessage.class, this, "super", super.toString());
    }
}

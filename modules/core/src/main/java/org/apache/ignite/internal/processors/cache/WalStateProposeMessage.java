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

import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.UUID;

/**
 * WAL state propose message.
 */
public class WalStateProposeMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message ID */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Unique operation ID. */
    private final UUID opId;

    /** Node ID. */
    private final UUID nodeId;

    /** Cache names which are expected to be in the group along with their deployment IDs. */
    @GridToStringInclude
    private Map<String, IgniteUuid> caches;

    /** Group ID. */
    private final int grpId;

    /** Whether WAL should be enabled or disabled. */
    private final boolean enable;

    /** Whether message should be ignored. */
    private transient boolean ignored;

    /**
     * Constructor.
     *
     * @param opId Operation IDs.
     * @param nodeId Node ID.
     * @param caches Expected cache names and their relevant deployment IDs.
     * @param grpId Expected group ID.
     * @param enable WAL state flag.
     */
    public WalStateProposeMessage(UUID opId, UUID nodeId, Map<String, IgniteUuid> caches, int grpId, boolean enable) {
        this.opId = opId;
        this.nodeId = nodeId;
        this.caches = caches;
        this.grpId = grpId;
        this.enable = enable;
    }

    /**
     * @return Operation ID.
     */
    public UUID operationId() {
        return opId;
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
     * @return Group ID.
     */
    public int groupId() {
        return grpId;
    }

    /**
     * @return WAL state flag.
     */
    public boolean enable() {
        return enable;
    }

    /**
     * @return {@code True} if message should be ignored.
     */
    public boolean ignored() {
        return ignored;
    }

    /**
     * Mark message as ignored.
     */
    public void markIgnored() {
        this.ignored = true;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WalStateProposeMessage.class, this);
    }
}

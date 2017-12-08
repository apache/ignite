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

import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * WAL activate/deactivate request.
 */
public class WalModeDynamicChangeMessage implements DiscoveryCustomMessage {
    /** */
    private static final byte DISABLE = 0x01;

    /** */
    private static final byte PREPARE = 0x02;

    /** Overwrite. */
    private static final byte OVERRIDE = 0x04;

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    private UUID uid;

    /** Flags */
    private byte flags;

    /** Cache group ids. */
    private GridIntList grpIds;

    /** Near node ID in case if near cache is being started. */
    private UUID initiatingNodeId;

    /**
     * @param uid Uid.
     * @param disable Disable.
     * @param prepare Prepare.
     * @param grpIds Group ids.
     * @param nodeId Node id.
     */
    WalModeDynamicChangeMessage(UUID uid,
        boolean disable,
        boolean prepare,
        boolean override,
        GridIntList grpIds,
        UUID nodeId) {
        this.uid = uid;
        this.grpIds = grpIds;
        this.initiatingNodeId = nodeId;

        if (disable)
            flags |= DISABLE;

        if (prepare)
            flags |= PREPARE;

        if (override)
            flags |= OVERRIDE;
    }

    /**
     * @return Near node ID.
     */
    public UUID initiatingNodeId() {
        return initiatingNodeId;
    }

    /**
     *
     */
    public GridIntList grpIds() {
        return grpIds;
    }

    /**
     *
     */
    public boolean disable() {
        return (flags & DISABLE) != 0;
    }

    /**
     *
     */
    public boolean prepare() {
        return (flags & PREPARE) != 0;
    }

    /**
     *
     */
    public boolean override() {
        return (flags & OVERRIDE) != 0;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /**
     *
     */
    public UUID uid() {
        return uid;
    }

    /**
     *
     */
    boolean needExchange() {
        return !disable() && prepare();
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
        return mgr.createDiscoCacheOnCacheChange(topVer, discoCache);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "WalModeDynamicChangeMessage{" +
            "id=" + id +
            ", uid=" + uid +
            ", flags=" + flags +
            ", grpIds=" + grpIds +
            ", initiatingNodeId=" + initiatingNodeId +
            '}';
    }
}


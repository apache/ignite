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

import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * WAL activate/deactivate request.
 */
public class WalModeDynamicChangeRequest implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    private UUID uid;

    /** Activate/deactivate flag. */
    private boolean disableWal;

    /** Cache group ids. */
    @GridToStringExclude
    private GridIntList grpIds;

    /** Cache updates to be executed on exchange. */
    private transient ExchangeActions exchangeActions;

    /** Near node ID in case if near cache is being started. */
    private UUID initiatingNodeId;

    /**
     * @param uid Uid.
     * @param disableWal Disable WAL.
     * @param cacheNames Cache names.
     */
    public WalModeDynamicChangeRequest(UUID uid, boolean disableWal, Collection<String> cacheNames,
        GridKernalContext ctx) {
        this.uid = uid;
        this.disableWal = disableWal;

        Collection<Integer> grpSet = new HashSet<>();

        for (String cacheName : cacheNames)
            grpSet.add(ctx.cache().cacheDescriptor(cacheName).groupId());

        this.grpIds = new GridIntList(grpSet.size());

        for (Integer grp : grpSet)
            this.grpIds.add(grp);

        this.initiatingNodeId = ctx.localNodeId();
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

    public boolean disableWal() {
        return disableWal;
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
     * @return Cache updates to be executed on exchange.
     */
    public ExchangeActions exchangeActions() {
        if (exchangeActions == null)
            exchangeActions = new ExchangeActions();

        exchangeActions.changeWalMode(this);

        return exchangeActions;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        return mgr.createDiscoCacheOnCacheChange(topVer, discoCache);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "WalModeDynamicChangeRequest{" +
            "uid=" + uid +
            ", activate=" + disableWal +
            ", grpIds=" + grpIds +
            '}';
    }
}


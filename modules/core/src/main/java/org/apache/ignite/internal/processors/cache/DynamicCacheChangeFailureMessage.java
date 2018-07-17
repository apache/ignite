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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * This class represents discovery message that is used to provide information about dynamic cache start failure.
 */
public class DynamicCacheChangeFailureMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache names. */
    @GridToStringInclude
    private Collection<String> cacheNames;

    /** Custom message ID. */
    private IgniteUuid id;

    /** */
    private GridDhtPartitionExchangeId exchId;

    /** */
    @GridToStringInclude
    private IgniteCheckedException cause;

    /** Cache updates to be executed on exchange. */
    private transient ExchangeActions exchangeActions;

    /**
     * Creates new DynamicCacheChangeFailureMessage instance.
     *
     * @param locNode Local node.
     * @param exchId Exchange Id.
     * @param cause Cache start error.
     * @param cacheNames Cache names.
     */
    public DynamicCacheChangeFailureMessage(
        ClusterNode locNode,
        GridDhtPartitionExchangeId exchId,
        IgniteCheckedException cause,
        Collection<String> cacheNames)
    {
        assert exchId != null;
        assert cause != null;
        assert !F.isEmpty(cacheNames) : cacheNames;

        this.id = IgniteUuid.fromUuid(locNode.id());
        this.exchId = exchId;
        this.cause = cause;
        this.cacheNames = cacheNames;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /**
     * @return Collection of failed caches.
     */
    public Collection<String> cacheNames() {
        return cacheNames;
    }

    /**
     * @return Cache start error.
     */
    public IgniteCheckedException error() {
        return cause;
    }

    /**
     * @return Cache updates to be executed on exchange.
     */
    public ExchangeActions exchangeActions() {
        return exchangeActions;
    }

    /**
     * @param exchangeActions Cache updates to be executed on exchange.
     */
    public void exchangeActions(ExchangeActions exchangeActions) {
        assert exchangeActions != null && !exchangeActions.empty() : exchangeActions;

        this.exchangeActions = exchangeActions;
    }

    /**
     * @return Exchange version.
     */
    @Nullable public GridDhtPartitionExchangeId exchangeId() {
        return exchId;
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
    @Override public boolean stopProcess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public DiscoCache createDiscoCache(
        GridDiscoveryManager mgr,
        AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        return mgr.createDiscoCacheOnCacheChange(topVer, discoCache);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DynamicCacheChangeFailureMessage.class, this);
    }
}

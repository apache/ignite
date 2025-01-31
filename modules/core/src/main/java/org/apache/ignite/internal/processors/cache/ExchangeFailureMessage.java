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
import java.util.Map;
import java.util.UUID;
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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * This class represents discovery message that is used to provide information about dynamic cache start failure.
 */
public class ExchangeFailureMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache names. */
    @GridToStringInclude
    private final Collection<String> cacheNames;

    /** Custom message ID. */
    private final IgniteUuid id;

    /** */
    private final GridDhtPartitionExchangeId exchId;

    /** */
    @GridToStringInclude
    private final Map<UUID, Exception> exchangeErrors;

    /** Actions to be done to rollback changes done before the exchange failure. */
    private transient ExchangeActions exchangeRollbackActions;

    /**
     * Creates new DynamicCacheChangeFailureMessage instance.
     *
     * @param locNode Local node.
     * @param exchId Exchange Id.
     * @param exchangeErrors Errors that caused PME to fail.
     */
    public ExchangeFailureMessage(
        ClusterNode locNode,
        GridDhtPartitionExchangeId exchId,
        Map<UUID, Exception> exchangeErrors,
        Collection<String> cacheNames
    ) {
        assert exchId != null;
        assert !F.isEmpty(exchangeErrors);
        assert !F.isEmpty(cacheNames) : cacheNames;

        this.id = IgniteUuid.fromUuid(locNode.id());
        this.exchId = exchId;
        this.cacheNames = cacheNames;
        this.exchangeErrors = exchangeErrors;
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

    /** */
    public Map<UUID, Exception> exchangeErrors() {
        return exchangeErrors;
    }

    /**
     * @return Cache updates to be executed on exchange.
     */
    public ExchangeActions exchangeRollbackActions() {
        return exchangeRollbackActions;
    }

    /**
     * @param exchangeRollbackActions Cache updates to be executed on exchange.
     */
    public void exchangeRollbackActions(ExchangeActions exchangeRollbackActions) {
        assert exchangeRollbackActions != null && !exchangeRollbackActions.empty() : exchangeRollbackActions;

        this.exchangeRollbackActions = exchangeRollbackActions;
    }

    /**
     * Creates an IgniteCheckedException that is used as root cause of the exchange initialization failure. This method
     * aggregates all the exceptions provided from all participating nodes.
     *
     * @return Exception that represents a cause of the exchange initialization failure.
     */
    public IgniteCheckedException createFailureCompoundException() {
        IgniteCheckedException ex = new IgniteCheckedException("Failed to complete exchange process.");

        for (Map.Entry<UUID, Exception> entry : exchangeErrors.entrySet())
            U.addSuppressed(ex, entry.getValue());

        return ex;
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
    @Override public DiscoCache createDiscoCache(
        GridDiscoveryManager mgr,
        AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        return mgr.createDiscoCacheOnCacheChange(topVer, discoCache);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ExchangeFailureMessage.class, this);
    }
}

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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheAffinityChangeMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    private AffinityTopologyVersion topVer;

    /** */
    private GridDhtPartitionExchangeId exchId;

    /** */
    private Map<Integer, Map<Integer, List<UUID>>> assignmentChange;

    /** */
    private Map<Integer, IgniteUuid> cacheDeploymentIds;

    /** */
    private GridDhtPartitionsFullMessage partsMsg;

    /** */
    private transient boolean exchangeNeeded;

    /**
     * Constructor used when message is created after cache rebalance finished.
     *
     * @param topVer Topology version.
     * @param assignmentChange Assignment change.
     * @param cacheDeploymentIds Cache deployment ID.
     */
    public CacheAffinityChangeMessage(AffinityTopologyVersion topVer,
        Map<Integer, Map<Integer, List<UUID>>> assignmentChange,
        Map<Integer, IgniteUuid> cacheDeploymentIds) {
        assert !F.isEmpty(assignmentChange) : assignmentChange;

        this.topVer = topVer;
        this.assignmentChange = assignmentChange;
        this.cacheDeploymentIds = cacheDeploymentIds;
    }

    /**
     * Constructor used when message is created to finish exchange.
     *
     * @param exchId Exchange ID.
     * @param partsMsg Partitions messages.
     * @param assignmentChange Assignment change.
     */
    public CacheAffinityChangeMessage(GridDhtPartitionExchangeId exchId,
        GridDhtPartitionsFullMessage partsMsg,
        Map<Integer, Map<Integer, List<UUID>>> assignmentChange) {
        this.exchId = exchId;
        this.partsMsg = partsMsg;
        this.assignmentChange = assignmentChange;
    }

    /**
     * @return Cache deployment IDs.
     */
    public Map<Integer, IgniteUuid> cacheDeploymentIds() {
        return cacheDeploymentIds;
    }

    /**
     * @return {@code True} if request should trigger partition exchange.
     */
    public boolean exchangeNeeded() {
        return exchangeNeeded;
    }

    /**
     * @param exchangeNeeded {@code True} if request should trigger partition exchange.
     */
    public void exchangeNeeded(boolean exchangeNeeded) {
        this.exchangeNeeded = exchangeNeeded;
    }

    /**
     * @return Partitions message.
     */
    public GridDhtPartitionsFullMessage partitionsMessage() {
        return partsMsg;
    }

    /**
     * @return Affinity assignments.
     */
    @Nullable public Map<Integer, Map<Integer, List<UUID>>> assignmentChange() {
        return assignmentChange;
    }

    /**
     * @return Exchange version.
     */
    @Nullable public GridDhtPartitionExchangeId exchangeId() {
        return exchId;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
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
    @Override public boolean stopProcess() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr,
        AffinityTopologyVersion topVer, DiscoCache discoCache) {
        return discoCache.copy(topVer, null);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheAffinityChangeMessage.class, this);
    }
}

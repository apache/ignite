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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.DiscoverySpiMutableCustomMessageSupport;
import org.jetbrains.annotations.Nullable;

/**
 * CacheAffinityChangeMessage represent a message that switches to a new affinity assignmentafter rebalance is finished.
 * This message should not be mutated  in any way outside the "disco-notifier-worker" thread.
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

    /** If this flag is {@code true} then this message should lead to partition map exchnage. */
    private boolean exchangeNeeded;

    /**
     * This flag indicates that this message should not be passed to other nodes except the coordinator.
     * Instead of this message, the message which is returned by {@link #ackMessage()} will be sent.
     * See {@link DiscoveryCustomMessage#stopProcess()}.
     *
     * This flag is used when discovery SPI does not support mutable custom messages.
     * See {@link DiscoverySpiMutableCustomMessageSupport}.
     */
    private transient boolean stopProc;

    /**
     * Constructor used when message is created after cache rebalance finished.
     *
     * @param topVer Topology version.
     * @param cacheDeploymentIds Cache deployment ID.
     */
    public CacheAffinityChangeMessage(AffinityTopologyVersion topVer, Map<Integer, IgniteUuid> cacheDeploymentIds) {
        this.topVer = topVer;
        this.cacheDeploymentIds = cacheDeploymentIds;
    }

    /**
     * Constructor used when message is created to finish exchange.
     *
     * @param exchId Exchange ID.
     * @param partsMsg Partitions messages.
     * @param assignmentChange Assignment change.
     */
    public CacheAffinityChangeMessage(
        GridDhtPartitionExchangeId exchId,
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
        return partsMsg != null ? partsMsg.copy() : null;
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
        if (!stopProc)
            return null;

        // If stopProc is equal to true, then Discovery SPI does not support mutable custom messages.
        // Let's return the same message, that was muted on the coordinator node. This message will be sent to all nodes
        // instead of the original one.
        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean stopProcess() {
        return stopProc;
    }

    /**
     * Sets stop processing flag. If this flag is {@code true} then this message is not passed to other nodes after
     * the coordinator node notitied its own listner. If method {@link #ackMessage()} returns non-null ack message,
     * it is sent to all nodes.
     * This flag is used when discovery SPI does not support mutable custom messages.
     * See {@link DiscoverySpiMutableCustomMessageSupport}.
     *
     * @param stopProc If {@code true} then this message is not passed to other nodes.
     */
    public void stopProcess(boolean stopProc) {
        this.stopProc = stopProc;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoCache createDiscoCache(
        GridDiscoveryManager mgr,
        AffinityTopologyVersion topVer,
        DiscoCache discoCache
    ) {
        return discoCache.copy(topVer, null);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheAffinityChangeMessage.class, this);
    }
}

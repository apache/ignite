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
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheAffinityChangeMessage implements DiscoveryCustomMessage {
    /** */
    private IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    private AffinityTopologyVersion topVer;

    /** */
    private GridDhtPartitionExchangeId exchId;

    /** */
    private Map<Integer, Map<Integer, List<UUID>>> affChange;

    /**
     * @param topVer Topology version.
     */
    public CacheAffinityChangeMessage(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @param exchId Exchange ID.
     */
    public CacheAffinityChangeMessage(GridDhtPartitionExchangeId exchId) {
        this.exchId = exchId;
    }

    /**
     * @param affChange Affinity change.
     */
    public void affinityChange(Map<Integer, Map<Integer, List<UUID>>> affChange) {
        this.affChange = affChange;
    }

    /**
     * @return Affinity assignment.
     */
    @Nullable public Map<Integer, Map<Integer, List<UUID>>> affinityChange() {
        return affChange;
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
    @Override public String toString() {
        return S.toString(CacheAffinityChangeMessage.class, this);
    }
}

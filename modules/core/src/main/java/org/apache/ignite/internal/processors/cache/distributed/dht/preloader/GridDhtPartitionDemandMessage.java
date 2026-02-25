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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Partition demand request.
 */
public class GridDhtPartitionDemandMessage extends GridCacheGroupIdMessage {
    /** Rebalance id. */
    @Order(4)
    long rebalanceId;

    /** Partitions map. */
    @Order(5)
    IgniteDhtDemandedPartitionsMap parts;

    /** Timeout. */
    @Order(6)
    long timeout;

    /** Worker ID. */
    @Order(7)
    int workerId = -1;

    /** Topology version. */
    @Order(8)
    AffinityTopologyVersion topVer;

    /**
     * @param rebalanceId Rebalance id for this node.
     * @param topVer Topology version.
     * @param grpId Cache group ID.
     */
    GridDhtPartitionDemandMessage(long rebalanceId, @NotNull AffinityTopologyVersion topVer, int grpId) {
        this(rebalanceId, topVer, grpId, new IgniteDhtDemandedPartitionsMap());
    }

    /**
     * @param rebalanceId Rebalance id for this node.
     * @param topVer Topology version.
     * @param grpId Cache group ID.
     * @param parts Demand partiton map.
     */
    GridDhtPartitionDemandMessage(long rebalanceId, @NotNull AffinityTopologyVersion topVer, int grpId,
        IgniteDhtDemandedPartitionsMap parts) {
        this.grpId = grpId;
        this.rebalanceId = rebalanceId;
        this.topVer = topVer;
        this.parts = parts;
    }

    /**
     * Empty constructor.
     */
    public GridDhtPartitionDemandMessage() {
        // No-op.
    }

    /**
     * Creates copy of this message with new partitions map.
     *
     * @param parts New partitions map.
     * @return Copy of message with new partitions map.
     */
    public GridDhtPartitionDemandMessage withNewPartitionsMap(@NotNull IgniteDhtDemandedPartitionsMap parts) {
        GridDhtPartitionDemandMessage cp = new GridDhtPartitionDemandMessage();
        cp.grpId = grpId;
        cp.rebalanceId = rebalanceId;
        cp.timeout = timeout;
        cp.workerId = workerId;
        cp.topVer = topVer;
        cp.parts = parts;
        return cp;
    }

    /**
     * @return Partitions.
     */
    public IgniteDhtDemandedPartitionsMap partitions() {
        return parts;
    }

    /**
     * @param updateSeq Update sequence.
     */
    public void rebalanceId(long updateSeq) {
        rebalanceId = updateSeq;
    }

    /**
     * @return Unique rebalance session id.
     */
    public long rebalanceId() {
        return rebalanceId;
    }

    /**
     * @return Reply message timeout.
     */
    public long timeout() {
        return timeout;
    }

    /**
     * @param timeout Timeout.
     */
    public void timeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * @return Topology version for which demand message is sent.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 45;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionDemandMessage.class, this,
            "partCnt", parts != null ? parts.size() : 0,
            "super", super.toString());
    }
}

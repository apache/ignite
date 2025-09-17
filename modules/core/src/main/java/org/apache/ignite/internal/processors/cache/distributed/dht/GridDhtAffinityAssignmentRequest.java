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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Affinity assignment request.
 */
public class GridDhtAffinityAssignmentRequest extends GridCacheGroupIdMessage {
    /** */
    private static final int SND_PART_STATE_MASK = 0x01;

    /** */
    @Order(value = 4, method = "flags")
    private byte flags;

    /** */
    @Order(value = 5, method = "futureId")
    private long futId;

    /** Topology version being queried. */
    @Order(value = 6, method = "topologyVersion")
    private AffinityTopologyVersion topVer;

    /**
     * Empty constructor.
     */
    public GridDhtAffinityAssignmentRequest() {
        // No-op.
    }

    /**
     * @param futId Future ID.
     * @param grpId Cache group ID.
     * @param topVer Topology version.
     * @param sndPartMap {@code True} if need send in response cache partitions state.
     */
    public GridDhtAffinityAssignmentRequest(
        long futId,
        int grpId,
        AffinityTopologyVersion topVer,
        boolean sndPartMap) {
        assert topVer != null;

        this.futId = futId;
        this.grpId = grpId;
        this.topVer = topVer;

        if (sndPartMap)
            flags |= SND_PART_STATE_MASK;
    }

    /**
     * @return Flags.
     */
    public byte flags() {
        return flags;
    }

    /**
     * @param flags Flags.
     */
    public void flags(byte flags) {
        this.flags = flags;
    }

    /**
     * @return {@code True} if need send in response cache partitions state.
     */
    public boolean sendPartitionsState() {
        return (flags & SND_PART_STATE_MASK) != 0;
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /**
     * @param futId Future ID.
     */
    public void futureId(long futId) {
        this.futId = futId;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean partitionExchangeMessage() {
        return true;
    }

    /**
     * @return Requested topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer Requested topology version.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 28;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAffinityAssignmentRequest.class, this, super.toString());
    }
}

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

package org.apache.ignite.internal.processors.cluster;

import java.io.Serializable;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Discovery data related to cluster state.
 */
public class DiscoveryDataClusterState implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final boolean active;

    /** */
    @Nullable private final BaselineTopology baselineTop;

    /** */
    private final UUID transitionReqId;

    /** Topology version for state change exchange. */
    @GridToStringInclude
    private final AffinityTopologyVersion transitionTopVer;

    /** Nodes participating in state change exchange. */
    @GridToStringExclude
    private final Set<UUID> transitionNodes;

    /** Local flag for state transition result (global state is updated asynchronously by custom message). */
    private transient volatile Boolean transitionRes;

    /**
     * @param active Current status.
     * @return State instance.
     */
    static DiscoveryDataClusterState createState(boolean active, @Nullable BaselineTopology baselineTopology) {
        return new DiscoveryDataClusterState(active, baselineTopology, null, null, null);
    }

    /**
     * @param active New status.
     * @param transitionReqId State change request ID.
     * @param transitionTopVer State change topology version.
     * @param transitionNodes Nodes participating in state change exchange.
     * @return State instance.
     */
    static DiscoveryDataClusterState createTransitionState(boolean active,
        @Nullable BaselineTopology baselineTop,
        UUID transitionReqId,
        AffinityTopologyVersion transitionTopVer,
        Set<UUID> transitionNodes) {
        assert transitionReqId != null;
        assert transitionTopVer != null;
        assert !F.isEmpty(transitionNodes) : transitionNodes;

        return new DiscoveryDataClusterState(active, baselineTop, transitionReqId, transitionTopVer, transitionNodes);
    }

    /**
     * @param active New state.
     * @param transitionReqId State change request ID.
     * @param transitionTopVer State change topology version.
     * @param transitionNodes Nodes participating in state change exchange.
     */
    private DiscoveryDataClusterState(boolean active,
        @Nullable BaselineTopology baselineTop,
        @Nullable UUID transitionReqId,
        @Nullable AffinityTopologyVersion transitionTopVer,
        @Nullable Set<UUID> transitionNodes) {
        this.active = active;
        this.baselineTop = baselineTop;
        this.transitionReqId = transitionReqId;
        this.transitionTopVer = transitionTopVer;
        this.transitionNodes = transitionNodes;
    }

    /**
     * @return Local flag for state transition result (global state is updated asynchronously by custom message).
     */
    @Nullable public Boolean transitionResult() {
        return transitionRes;
    }

    /**
     * Discovery cluster state is changed asynchronously by discovery message, this methods changes local status
     * for public API calls.
     *
     * @param reqId Request ID.
     * @param active New cluster state.
     */
    public void setTransitionResult(UUID reqId, boolean active) {
        if (reqId.equals(transitionReqId)) {
            assert transitionRes == null : this;

            transitionRes = active;
        }
    }

    /**
     * @return State change request ID.
     */
    public UUID transitionRequestId() {
        return transitionReqId;
    }

    /**
     * @return {@code True} if state change is in progress.
     */
    public boolean transition() {
        return transitionReqId != null;
    }

    /**
     * @return State change exchange version.
     */
    public AffinityTopologyVersion transitionTopologyVersion() {
        return transitionTopVer;
    }

    /**
     * @return Current cluster state (or new state in case when transition is in progress).
     */
    public boolean active() {
        return active;
    }

    /**
     * @return Baseline topology.
     */
    @Nullable public BaselineTopology baselineTopology() {
        return baselineTop;
    }

    /**
     * @return Nodes participating in state change exchange.
     */
    public Set<UUID> transitionNodes() {
        return transitionNodes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DiscoveryDataClusterState.class, this);
    }
}

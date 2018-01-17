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
    @Nullable private final BaselineTopology baselineTopology;

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

    /** */
    private transient DiscoveryDataClusterState prevState;

    /** */
    private transient volatile Exception transitionError;

    /**
     * @param active Current status.
     * @return State instance.
     */
    static DiscoveryDataClusterState createState(boolean active, @Nullable BaselineTopology baselineTopology) {
        return new DiscoveryDataClusterState(null, active, baselineTopology, null, null, null);
    }

    /**
     * @param active New status.
     * @param transitionReqId State change request ID.
     * @param transitionTopVer State change topology version.
     * @param transitionNodes Nodes participating in state change exchange.
     * @return State instance.
     */
    static DiscoveryDataClusterState createTransitionState(
        DiscoveryDataClusterState prevState,
        boolean active,
        @Nullable BaselineTopology baselineTopology,
        UUID transitionReqId,
        AffinityTopologyVersion transitionTopVer,
        Set<UUID> transitionNodes
    ) {
        assert transitionReqId != null;
        assert transitionTopVer != null;
        assert !F.isEmpty(transitionNodes) : transitionNodes;

        return new DiscoveryDataClusterState(
            prevState,
            active,
            baselineTopology,
            transitionReqId,
            transitionTopVer,
            transitionNodes);
    }

    /**
     * @param prevState Previous state. May be non-null only for transitional states.
     * @param active New state.
     * @param transitionReqId State change request ID.
     * @param transitionTopVer State change topology version.
     * @param transitionNodes Nodes participating in state change exchange.
     */
    private DiscoveryDataClusterState(
        DiscoveryDataClusterState prevState,
        boolean active,
        @Nullable BaselineTopology baselineTopology,
        @Nullable UUID transitionReqId,
        @Nullable AffinityTopologyVersion transitionTopVer,
        @Nullable Set<UUID> transitionNodes
    ) {
        this.prevState = prevState;
        this.active = active;
        this.baselineTopology = baselineTopology;
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
        if (reqId.equals(transitionReqId))
            transitionRes = active;
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
        return baselineTopology;
    }

    /**
     * @return Nodes participating in state change exchange.
     */
    public Set<UUID> transitionNodes() {
        return transitionNodes;
    }

    /**
     * @return Transition error.
     */
    @Nullable public Exception transitionError() {
        return transitionError;
    }

    /**
     * @param ex Exception
     */
    public void transitionError(Exception ex) {
        transitionError = ex;
    }

    /**
     * @param success Transition success status.
     * @return Cluster state that finished transition.
     */
    public DiscoveryDataClusterState finish(boolean success) {
        return success ?
            new DiscoveryDataClusterState(
                null,
                active,
                baselineTopology,
                null,
                null,
                null
            ) :
            prevState;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DiscoveryDataClusterState.class, this);
    }
}

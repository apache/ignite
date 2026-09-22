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

import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.Marshalled;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cluster.ClusterState.INACTIVE;

/**
 * A pojo-object representing current cluster global state. The state includes cluster active flag and cluster
 * baseline topology.
 * <p>
 * This object also captures a transitional cluster state, when one or more fields are changing. In this case,
 * a {@code transitionReqId} field is set to a non-null value and {@code previousBaselineTopology} captures previous cluster state.
 * A joining node catching the cluster in an intermediate state will observe {@code transitionReqId} field to be
 * non-null, however the {@code previousBaselineTopology} will not be sent to the joining node.
 * <p>
 * TODO https://issues.apache.org/jira/browse/IGNITE-7640 This class must be immutable, transitionRes must be set by calling finish().
 */
public class DiscoveryDataClusterState implements Message {
    /** Current cluster state. */
    @Order(0)
    ClusterState state;

    /** Time of last cluster state change. */
    @Order(1)
    long lastStateChangeTime;

    /** Current cluster baseline topology. */
    @Marshalled("baselineTopBytes")
    @Nullable BaselineTopology baselineTop;

    /** */
    @Order(2)
    byte[] baselineTopBytes;

    /**
     * Transition request ID. Set to a non-null value if the cluster is changing it's state.
     * The ID is assigned on the initiating node.
     */
    @Order(3)
    UUID transitionReqId;

    /** Previous cluster state. May not null only if cluster in transition. */
    @Order(4)
    ClusterState prevClusterState;

    /** Nodes participating in state change exchange. */
    @Order(5)
    @GridToStringExclude
    Set<UUID> transitionNodes;

    /**
     * Local flag for state transition active state result (global state is updated asynchronously by custom message),
     * {@code null} means that state change is not completed yet.
     */
    private volatile ClusterState transitionRes;

    /**
     * Previous cluster state if this state is a transition state and it was not received by a joining node.
     */
    private DiscoveryDataClusterState prevState;

    /** Local baseline autoadjustment flag. */
    private volatile boolean locBaselineAutoAdjustment;

    /** Default constructor for {@link MessageFactory}. */
    public DiscoveryDataClusterState() {
        // No-op.
    }

    /**
     * @param state Current cluster state.
     * @param baselineTop Baseline topology associated with this state.
     * @return State instance.
     */
    static DiscoveryDataClusterState createState(ClusterState state, @Nullable BaselineTopology baselineTop) {
        return new DiscoveryDataClusterState(null, state, baselineTop, null, null, null);
    }

    /**
     * @param state New cluster state.
     * @param prevState Previous state.
     * @param baselineTop Baseline topology for new cluster state.
     * @param transitionReqId State change request ID.
     * @param transitionNodes Nodes participating in state change exchange.
     * @return Discovery cluster state instance.
     */
    static DiscoveryDataClusterState createTransitionState(
        ClusterState state,
        DiscoveryDataClusterState prevState,
        @Nullable BaselineTopology baselineTop,
        UUID transitionReqId,
        Set<UUID> transitionNodes
    ) {
        assert transitionReqId != null;
        assert !F.isEmpty(transitionNodes) : transitionNodes;
        assert prevState != null;

        return new DiscoveryDataClusterState(
            prevState,
            state,
            baselineTop,
            transitionReqId,
            transitionNodes,
            prevState.state
        );
    }

    /**
     * @param prevState Previous state. May be non-null only for transitional states.
     * @param state New cluster state.
     * @param baselineTop Baseline topology for new cluster state.
     * @param transitionReqId State change request ID.
     * @param transitionNodes Nodes participating in state change exchange.
     * @param prevClusterState Nodes participating in state change exchange.
     */
    private DiscoveryDataClusterState(
        DiscoveryDataClusterState prevState,
        ClusterState state,
        @Nullable BaselineTopology baselineTop,
        @Nullable UUID transitionReqId,
        @Nullable Set<UUID> transitionNodes,
        @Nullable ClusterState prevClusterState
    ) {
        assert state != null;

        this.prevState = prevState;
        this.state = state;
        lastStateChangeTime = U.currentTimeMillis();
        this.baselineTop = baselineTop;
        this.transitionReqId = transitionReqId;
        this.transitionNodes = transitionNodes;
        this.prevClusterState = prevClusterState;
    }

    /** */
    public boolean isBaselineChangeInProgress() {
        if (!transition())
            return false;

        if (previouslyActive() == state().active())
            return true;

        // Or it is the first activation.
        return state() != ClusterState.INACTIVE
            && !previouslyActive()
            && previousBaselineTopology() == null;
    }

    /** @return Cluster state before transition if cluster in transition and current cluster state otherwise. */
    public ClusterState lastState() {
        return transition() ? prevClusterState : state;
    }

    /** @return Local flag for state transition result (global state is updated asynchronously by custom message). */
    @Nullable public ClusterState transitionResult() {
        return transitionRes;
    }

    /**
     * Discovery cluster state is changed asynchronously by discovery message, this methods changes local status
     * for public API calls.
     *
     * @param reqId Request ID.
     * @param state New cluster state.
     */
    public void setTransitionResult(UUID reqId, ClusterState state) {
        if (reqId.equals(transitionReqId))
            transitionRes = state;
    }

    /** @return State change request ID. */
    public UUID transitionRequestId() {
        return transitionReqId;
    }

    /** @return {@code True} if any cluster state change is in progress (e.g. active state change, baseline change). */
    public boolean transition() {
        return transitionReqId != null;
    }

    /** @return Previous "active" flag value during transition. */
    public boolean previouslyActive() {
        if (prevState != null)
            return prevState.state != INACTIVE;

        return state == INACTIVE;
    }

    /**
     * @return Current cluster state (or new state in case when transition is in progress).
     * @deprecated Use {@link #state()} instead.
     */
    @Deprecated
    public boolean active() {
        return state.active();
    }

    /** @return Current cluster state (or new state in case when transition is in progress). */
    public ClusterState state() {
        return state;
    }

    /** @return Time of last cluster state change. */
    public long lastStateChangeTime() {
        return lastStateChangeTime;
    }

    /** @return Baseline topology. */
    @Nullable public BaselineTopology baselineTopology() {
        return baselineTop;
    }

    /** @return Previous Baseline topology. */
    @Nullable public BaselineTopology previousBaselineTopology() {
        return prevState != null ? prevState.baselineTopology() : null;
    }

    /** @return {@code True} if baseline topology is set in the cluster. {@code False} otherwise. */
    public boolean hasBaselineTopology() {
        return baselineTop != null;
    }

    /** @return Nodes participating in state change exchange. */
    public Set<UUID> transitionNodes() {
        return transitionNodes;
    }

    /**
     * @return {@code true} if current state was created as a result of local baseline autoadjustment with zero timeout
     *      on in-memory cluster.
     */
    public boolean localBaselineAutoAdjustment() {
        return locBaselineAutoAdjustment;
    }

    /**
     * Set local baseline autoadjustment flag.
     *
     * @param adjusted Flag value.
     * @see #localBaselineAutoAdjustment()
     */
    public void localBaselineAutoAdjustment(boolean adjusted) {
        locBaselineAutoAdjustment = adjusted;
    }

    /**
     * Creates a non-transitional cluster state. This method effectively cleans all fields identifying the
     * state as transitional and creates a new state with the state transition result.
     *
     * @param success Transition success status.
     * @return Cluster state that finished transition.
     */
    public DiscoveryDataClusterState finish(boolean success) {
        if (success)
            return createState(state, baselineTop);
        else
            return prevState != null ? prevState : createState(INACTIVE, null);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DiscoveryDataClusterState.class, this);
    }
}

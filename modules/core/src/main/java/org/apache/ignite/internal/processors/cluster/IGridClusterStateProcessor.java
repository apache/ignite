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
 *
 */

package org.apache.ignite.internal.processors.cluster;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.StateChangeRequest;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface IGridClusterStateProcessor extends GridProcessor {
    /**
     * @return Cluster state to be used on public API.
     * @deprecated Use {@link #publicApiState(boolean)} instead.
     */
    @Deprecated
    boolean publicApiActiveState(boolean waitForTransition);

    /**
     * @return Cluster state to be used on public API.
     * @deprecated Use {@link #publicApiStateAsync(boolean)} instead.
     */
    @Deprecated
    IgniteFuture<Boolean> publicApiActiveStateAsync(boolean waitForTransition);

    /**
     * @param waitForTransition Wait end of transition or not.
     * @return Current cluster state to be used on public API.
     */
    ClusterState publicApiState(boolean waitForTransition);

    /**
     * @param waitForTransition Wait end of transition or not.
     * @return Current cluster state to be used on public API.
     */
    IgniteFuture<ClusterState> publicApiStateAsync(boolean waitForTransition);

    /**
     * @return Time of last cluster state change to be used on public API.
     */
    long lastStateChangeTime();

    /**
     * @param discoCache Discovery data cache.
     * @return If transition is in progress returns future which is completed when transition finishes.
     */
    @Nullable IgniteInternalFuture<Boolean> onLocalJoin(DiscoCache discoCache);

    /**
     * @param node Failed node.
     * @return Message if cluster state changed.
     */
    @Nullable ChangeGlobalStateFinishMessage onNodeLeft(ClusterNode node);

    /**
     * @param msg Message.
     */
    void onStateFinishMessage(ChangeGlobalStateFinishMessage msg);

    /**
     * @param topVer Current topology version.
     * @param msg Message.
     * @param discoCache Current nodes.
     * @return {@code True} if need start state change process.
     */
    boolean onStateChangeMessage(AffinityTopologyVersion topVer,
        ChangeGlobalStateMessage msg,
        DiscoCache discoCache);

    /**
     * @return Current cluster state, should be called only from discovery thread.
     */
    DiscoveryDataClusterState clusterState();

    /**
     * @return Pending cluster state which will be used when state transition is finished.
     */
    DiscoveryDataClusterState pendingState(ChangeGlobalStateMessage stateMsg);

    /** */
    void cacheProcessorStarted();

    /**
     * @param state New cluster state.
     * @param forceDeactivation If {@code true}, cluster deactivation will be forced.
     * @param baselineNodes New baseline nodes.
     * @param forceChangeBaselineTopology Force change baseline topology.
     * @return State change future.
     * @see ClusterState#INACTIVE
     */
    IgniteInternalFuture<?> changeGlobalState(
        ClusterState state,
        boolean forceDeactivation,
        Collection<? extends BaselineNode> baselineNodes,
        boolean forceChangeBaselineTopology
    );

    /**
     * @param errs Errors.
     * @param req State change request.
     */
    void onStateChangeError(Map<UUID, Exception> errs, StateChangeRequest req);

    /**
     * @param req State change request.
     */
    void onStateChangeExchangeDone(StateChangeRequest req);

    /**
     * @param blt New baseline topology.
     * @param prevBltHistItem Previous baseline history item.
     */
    void onBaselineTopologyChanged(BaselineTopology blt, BaselineTopologyHistoryItem prevBltHistItem) throws IgniteCheckedException;

    /**
     * @param exchangeFuture Exchange future.
     * @param hasMovingPartitions {@code True} if there are moving partitions.
     */
    void onExchangeFinishedOnCoordinator(IgniteInternalFuture exchangeFuture, boolean hasMovingPartitions);

    /**
     * @return {@code True} if partition evictions are allowed in current state.
     */
    boolean evictionsAllowed();
}

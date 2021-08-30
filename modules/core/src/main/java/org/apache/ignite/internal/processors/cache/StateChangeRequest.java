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

import java.util.UUID;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.processors.cluster.BaselineTopologyHistoryItem;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class StateChangeRequest {
    /** */
    private final ChangeGlobalStateMessage msg;

    /** */
    private final BaselineTopologyHistoryItem prevBltHistItem;

    /** */
    private ClusterState prevState;

    /** */
    private final AffinityTopologyVersion topVer;

    /**
     * @param msg Message.
     * @param bltHistItem Baseline history item.
     * @param prevState Previous cluster state.
     * @param topVer State change topology versoin.
     */
    public StateChangeRequest(
        ChangeGlobalStateMessage msg,
        BaselineTopologyHistoryItem bltHistItem,
        ClusterState prevState,
        AffinityTopologyVersion topVer
    ) {
        this.msg = msg;
        prevBltHistItem = bltHistItem;
        this.prevState = prevState;
        this.topVer = topVer;
    }

    /**
     * @return State change exchange version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return State change message ID.
     */
    public IgniteUuid id() {
        return msg.id();
    }

    /**
     * @return State change request ID.
     */
    public UUID requestId() {
        return msg.requestId();
    }

    /**
     * @return New state.
     * @deprecated Use {@link #state()} instead.
     */
    @Deprecated
    public boolean activate() {
        return msg.activate();
    }

    /**
     * @return New cluster state.
     */
    public ClusterState state() {
        return msg.state();
    }

    /**
     * @return Previous cluster state.
     */
    public ClusterState prevState() {
        return prevState;
    }

    /**
     * @return {@code True} if active state was changed.
     */
    public boolean activeChanged() {
        return prevState.active() != msg.state().active();
    }

    /**
     * @return Previous baseline topology.
     */
    @Nullable public BaselineTopologyHistoryItem prevBaselineTopologyHistoryItem() {
        return prevBltHistItem;
    }

    /**
     * @return Baseline topology.
     */
    @Nullable public BaselineTopology baselineTopology() {
        return msg.baselineTopology();
    }

    /**
     * @return Node initiated state change process.
     */
    public UUID initiatorNodeId() {
        return msg.initiatorNodeId();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StateChangeRequest.class, this);
    }
}

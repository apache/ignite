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

import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

/**
 * Carries cluster state and recent baseline topology history to joining nodes.
 *
 * <p>Instances are sent through discovery data collection during node join.
 * A joining node receives this message, extracts the current {@link DiscoveryDataClusterState},
 * and replays the {@link BaselineTopologyHistory} items into its local history.</p>
 */
public class BaselineStateAndHistoryData implements Message {
    /** Current cluster state (active/inactive, baseline topology, transition info). */
    @Order(0)
    DiscoveryDataClusterState globalState;

    /** Recent baseline topology history items for replay on the joining node. */
    @Order(1)
    BaselineTopologyHistory recentHistory;

    /** Default constructor for {@link MessageFactory}. */
    public BaselineStateAndHistoryData() {
        // No-op.
    }

    /**
     * @param globalState Current cluster state.
     * @param recentHistory  Recent baseline topology history to transfer.
     */
    BaselineStateAndHistoryData(DiscoveryDataClusterState globalState, BaselineTopologyHistory recentHistory) {
        this.globalState = globalState;
        this.recentHistory = recentHistory;
    }
}

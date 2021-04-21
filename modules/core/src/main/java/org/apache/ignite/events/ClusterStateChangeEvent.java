/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.events;

import java.util.Collection;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CLUSTER_STATE_CHANGED;

/**
 * Cluster state change event.
 * <p>
 * Grid events are used for notification about what happens within the grid. Note that by
 * design Ignite keeps all events generated on the local node locally and it provides
 * APIs for performing a distributed queries across multiple nodes:
 * <ul>
 *      <li>
 *          {@link IgniteEvents#remoteQuery(IgnitePredicate, long, int...)} -
 *          asynchronously querying events occurred on the nodes specified, including remote nodes.
 *      </li>
 *      <li>
 *          {@link IgniteEvents#localQuery(IgnitePredicate, int...)} -
 *          querying only local events stored on this local node.
 *      </li>
 *      <li>
 *          {@link IgniteEvents#localListen(IgnitePredicate, int...)} -
 *          listening to local grid events (events from remote nodes not included).
 *      </li>
 * </ul>
 * User can also wait for events using method {@link IgniteEvents#waitForLocal(IgnitePredicate, int...)}.
 * <h1 class="header">Events and Performance</h1>
 * Note that by default all events in Ignite are enabled and therefore generated and stored
 * by whatever event storage SPI is configured. Ignite can and often does generate thousands events per seconds
 * under the load and therefore it creates a significant additional load on the system. If these events are
 * not needed by the application this load is unnecessary and leads to significant performance degradation.
 * <p>
 * It is <b>highly recommended</b> to enable only those events that your application logic requires
 * by using {@link IgniteConfiguration#getIncludeEventTypes()} method in Ignite configuration. Note that certain
 * events are required for Ignite's internal operations and such events will still be generated but not stored by
 * event storage SPI if they are disabled in Ignite configuration.
 * @see EventType#EVT_CLUSTER_STATE_CHANGED
 */
public class ClusterStateChangeEvent extends EventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Previous cluster state. */
    private final ClusterState prevState;

    /** Current cluster state. */
    private final ClusterState state;

    /** Baseline nodes of current baseline topology. */
    private final Collection<? extends BaselineNode> baselineNodes;

    /**
     * @param prevState Previous cluster state.
     * @param state Current cluster state.
     * @param baselineNodes Collection of baseline nodes. (Optional for in-memory case)
     * @param node Node.
     * @param msg Optional event message.
     */
    public ClusterStateChangeEvent(
        ClusterState prevState,
        ClusterState state,
        @Nullable Collection<? extends BaselineNode> baselineNodes,
        ClusterNode node,
        String msg
    ) {
        super(node, msg, EVT_CLUSTER_STATE_CHANGED);

        A.notNull(prevState, "prevState");
        A.notNull(state, "state");

        this.state = state;
        this.prevState = prevState;
        this.baselineNodes = baselineNodes;
    }

    /**
     * @return Previous cluster state.
     */
    public ClusterState previousState() {
        return prevState;
    }

    /**
     * @return Current cluster state.
     */
    public ClusterState state() {
        return state;
    }

    /**
     * Gets baseline nodes.
     *
     * @return Baseline nodes.
     */
    public @Nullable Collection<? extends BaselineNode> baselineNodes() {
        return baselineNodes;
    }
}

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

package org.apache.ignite.events;

import java.util.Collection;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Baseline changed event.
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
 * @see EventType#EVT_BASELINE_CHANGED
 */
public class BaselineChangedEvent extends EventAdapter {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Baseline nodes. */
    private final Collection<BaselineNode> baselineNodes;

    /**
     * Creates baseline changed event with given parameters.
     * @param node Node.
     * @param msg Optional event message.
     * @param type Event type.
     * @param baselineNodes Collection of new baseline nodes.
     */
    public BaselineChangedEvent(
        ClusterNode node,
        String msg,
        int type,
        Collection<BaselineNode> baselineNodes
    ) {
        super(node, msg, type);

        //noinspection AssignmentOrReturnOfFieldWithMutableType
        this.baselineNodes = baselineNodes;
    }

    /** New baseline nodes. */
    public Collection<BaselineNode> baselineNodes() {
        //noinspection AssignmentOrReturnOfFieldWithMutableType
        return baselineNodes;
    }
}

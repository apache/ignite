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

import java.io.Serializable;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Grid events are used for notification about what happens within the grid. Note that by
 * design Ignite keeps all events generated on the local node locally and it provides
 * APIs for performing a distributed queries across multiple nodes:
 * <ul>
 *      <li>
 *          {@link org.apache.ignite.IgniteEvents#remoteQuery(org.apache.ignite.lang.IgnitePredicate, long, int...)} - querying
 *          events occurred on the nodes specified, including remote nodes.
 *      </li>
 *      <li>
 *          {@link org.apache.ignite.IgniteEvents#localQuery(org.apache.ignite.lang.IgnitePredicate, int...)} - querying only local
 *          events stored on this local node.
 *      </li>
 *      <li>
 *          {@link org.apache.ignite.IgniteEvents#localListen(org.apache.ignite.lang.IgnitePredicate, int...)} - listening
 *          to local grid events (events from remote nodes not included).
 *      </li>
 * </ul>
 * <h1 class="header">Events and Performance</h1>
 * Note that by default all events in Ignite are enabled and therefore generated and stored
 * by whatever event storage SPI is configured. Ignite can and often does generate thousands events per seconds
 * under the load and therefore it creates a significant additional load on the system. If these events are
 * not needed by the application this load is unnecessary and leads to significant performance degradation.
 * <p>
 * It is <b>highly recommended</b> to enable only those events that your application logic requires
 * by using either {@link org.apache.ignite.configuration.IgniteConfiguration#getIncludeEventTypes()} method in Ignite configuration. Note that certain
 * events are required for Ignite's internal operations and such events will still be generated but not stored by
 * event storage SPI if they are disabled in Ignite configuration.
 * <h1 class="header">Internal and Hidden Events</h1>
 * Also note that some events are considered to be internally used or hidden.
 * <p>
 * Internally used events are always "recordable" for notification purposes (regardless of whether they were
 * enabled or disabled). But won't be sent down to SPI level if user specifically excluded them.
 * <p>
 * All discovery events are internal:
 * <ul>
 *     <li>{@link EventType#EVT_NODE_FAILED}</li>
 *     <li>{@link EventType#EVT_NODE_LEFT}</li>
 *     <li>{@link EventType#EVT_NODE_JOINED}</li>
 *     <li>{@link EventType#EVT_NODE_METRICS_UPDATED}</li>
 *     <li>{@link EventType#EVT_NODE_SEGMENTED}</li>
 * </ul>
 * <p>
 * Hidden events are NEVER sent to SPI level. They serve purpose of local
 * notification for the local node.
 * <p>
 * Hidden events:
 * <ul>
 *     <li>{@link EventType#EVT_NODE_METRICS_UPDATED}</li>
 * </ul>
 * @see JobEvent
 * @see CacheEvent
 * @see CacheRebalancingEvent
 * @see SwapSpaceEvent
 * @see CheckpointEvent
 * @see DeploymentEvent
 * @see DiscoveryEvent
 * @see TaskEvent
 * @see org.apache.ignite.IgniteEvents#waitForLocal(org.apache.ignite.lang.IgnitePredicate, int...)
 */
public interface Event extends Comparable<Event>, Serializable {
    /**
     * Gets globally unique ID of this event.
     *
     * @return Globally unique ID of this event.
     * @see #localOrder()
     */
    public IgniteUuid id();

    /**
     * Gets locally unique ID that is atomically incremented for each event. Unlike
     * global {@link #id} this local ID can be used for ordering events on this node.
     * <p>
     * Note that for performance considerations Ignite doesn't order events globally.
     *
     * @return Locally unique ID that is atomically incremented for each new event.
     * @see #id()
     */
    public long localOrder();

    /**
     * Node where event occurred and was recorded
     *
     * @return node where event occurred and was recorded.
     */
    public ClusterNode node();

    /**
     * Gets optional message for this event.
     *
     * @return Optional (can be {@code null}) message for this event.
     */
    @Nullable public String message();

    /**
     * Gets type of this event. All system event types are defined in
     * {@link EventType}.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @return Event's type.
     * @see EventType
     */
    public int type();

    /**
     * Gets name of this event. All events are defined in {@link EventType} class.
     *
     * @return Name of this event.
     */
    public String name();

    /**
     * Gets event timestamp. Timestamp is local to the node on which this
     * event was produced. Note that more than one event can be generated
     * with the same timestamp. For ordering purposes use {@link #localOrder()} instead.
     *
     * @return Event timestamp.
     */
    public long timestamp();

    /**
     * Gets a shortened version of {@code toString()} result. Suitable for humans to read.
     *
     * @return Shortened version of {@code toString()} result.
     */
    public String shortDisplay();
}
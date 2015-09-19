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

import org.apache.ignite.IgniteEvents;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * In-memory database (cache) rebalancing event. Rebalance event happens every time there is a change
 * in grid topology, which means that a node has either joined or left the grid.
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
 * @see EventType#EVT_CACHE_REBALANCE_PART_LOADED
 * @see EventType#EVT_CACHE_REBALANCE_PART_UNLOADED
 * @see EventType#EVT_CACHE_REBALANCE_STARTED
 * @see EventType#EVT_CACHE_REBALANCE_STOPPED
 * @see EventType#EVT_CACHE_REBALANCE_PART_DATA_LOST
 */
public class CacheRebalancingEvent extends EventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String cacheName;

    /** Partition for the event. */
    private int part;

    /** Discovery node. */
    private ClusterNode discoNode;

    /** Discovery event type. */
    private int discoEvtType;

    /** Discovery event time. */
    private long discoTs;

    /**
     * Constructs cache event.
     *
     * @param cacheName Cache name.
     * @param node Event node.
     * @param msg Event message.
     * @param type Event type.
     * @param part Partition for the event (usually the partition the key belongs to).
     * @param discoNode Node that triggered this rebalancing event.
     * @param discoEvtType Discovery event type that triggered this rebalancing event.
     * @param discoTs Timestamp of discovery event that triggered this rebalancing event.
     */
    public CacheRebalancingEvent(
        String cacheName,
        ClusterNode node,
        String msg,
        int type,
        int part,
        ClusterNode discoNode,
        int discoEvtType,
        long discoTs
    ) {
        super(node, msg, type);
        this.cacheName = cacheName;
        this.part = part;
        this.discoNode = discoNode;
        this.discoEvtType = discoEvtType;
        this.discoTs = discoTs;
    }

    /**
     * Gets cache name.
     *
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * Gets partition for the event.
     *
     * @return Partition for the event.
     */
    public int partition() {
        return part;
    }

    /**
     * Gets shadow of the node that triggered this rebalancing event.
     *
     * @return Shadow of the node that triggered this rebalancing event.
     */
    public ClusterNode discoveryNode() {
        return discoNode;
    }

    /**
     * Gets type of discovery event that triggered this rebalancing event.
     *
     * @return Type of discovery event that triggered this rebalancing event.
     * @see DiscoveryEvent#type()
     */
    public int discoveryEventType() {
        return discoEvtType;
    }

    /**
     * Gets name of discovery event that triggered this rebalancing event.
     *
     * @return Name of discovery event that triggered this rebalancing event.
     * @see DiscoveryEvent#name()
     */
    public String discoveryEventName() {
        return U.gridEventName(discoEvtType);
    }

    /**
     * Gets timestamp of discovery event that caused this rebalancing event.
     *
     * @return Timestamp of discovery event that caused this rebalancing event.
     */
    public long discoveryTimestamp() {
        return discoTs;
    }

    /** {@inheritDoc} */
    @Override public String shortDisplay() {
        return name() + ": cache=" + CU.mask(cacheName) + ", cause=" +
            discoveryEventName();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheRebalancingEvent.class, this,
            "discoEvtName", discoveryEventName(),
            "nodeId8", U.id8(node().id()),
            "msg", message(),
            "type", name(),
            "tstamp", timestamp());
    }
}
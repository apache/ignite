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

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Grid checkpoint event.
 * <p>
 * Grid events are used for notification about what happens within the grid. Note that by
 * design Ignite keeps all events generated on the local node locally and it provides
 * APIs for performing a distributed queries across multiple nodes:
 * <ul>
 *      <li>
 *          {@link org.apache.ignite.IgniteEvents#remoteQuery(org.apache.ignite.lang.IgnitePredicate, long, int...)} -
 *          asynchronously querying events occurred on the nodes specified, including remote nodes.
 *      </li>
 *      <li>
 *          {@link org.apache.ignite.IgniteEvents#localQuery(org.apache.ignite.lang.IgnitePredicate, int...)} -
 *          querying only local events stored on this local node.
 *      </li>
 *      <li>
 *          {@link org.apache.ignite.IgniteEvents#localListen(org.apache.ignite.lang.IgnitePredicate, int...)} -
 *          listening to local grid events (events from remote nodes not included).
 *      </li>
 * </ul>
 * User can also wait for events using method
 * {@link org.apache.ignite.IgniteEvents#waitForLocal(org.apache.ignite.lang.IgnitePredicate, int...)}.
 * <h1 class="header">Events and Performance</h1>
 * Note that by default all events in Ignite are enabled and therefore generated and stored
 * by whatever event storage SPI is configured. Ignite can and often does generate thousands events per seconds
 * under the load and therefore it creates a significant additional load on the system. If these events are
 * not needed by the application this load is unnecessary and leads to significant performance degradation.
 * <p>
 * It is <b>highly recommended</b> to enable only those events that your application logic requires
 * by using {@link org.apache.ignite.configuration.IgniteConfiguration#getIncludeEventTypes()} method in Ignite configuration.
 * Note that certain events are required for Ignite's internal operations and such events will still be generated but not stored by
 * event storage SPI if they are disabled in Ignite configuration.
 * @see EventType#EVT_CHECKPOINT_LOADED
 * @see EventType#EVT_CHECKPOINT_REMOVED
 * @see EventType#EVT_CHECKPOINT_SAVED
 * @see EventType#EVTS_CHECKPOINT
 */
public class CheckpointEvent extends EventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String cpKey;

    /**
     * No-arg constructor.
     */
    public CheckpointEvent() {
        // No-op.
    }

    /**
     * Creates new checkpoint event with given parameters.
     *
     * @param node Local node.
     * @param msg Optional event message.
     * @param type Event type.
     * @param cpKey Checkpoint key associated with this event.
     */
    public CheckpointEvent(ClusterNode node, String msg, int type, String cpKey) {
        super(node, msg, type);

        this.cpKey = cpKey;
    }

    /** {@inheritDoc} */
    @Override public String shortDisplay() {
        return name() + ": cpKey=" + cpKey;
    }

    /**
     * Gets checkpoint key associated with this event.
     *
     * @return Checkpoint key associated with this event.
     */
    public String key() {
        assert cpKey != null;

        return cpKey;
    }

    /**
     * Sets checkpoint key.
     *
     * @param cpKey Checkpoint key to set.
     */
    public void key(String cpKey) {
        assert cpKey != null;

        this.cpKey = cpKey;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CheckpointEvent.class, this,
            "nodeId8", U.id8(node().id()),
            "msg", message(),
            "type", name(),
            "tstamp", timestamp());
    }
}

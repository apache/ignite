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

import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_SQL_QUERY_EXECUTION;

/**
 * Query execution event.
 * This event is triggered after a corresponding SQL query validated and before it is executed.
 * Unlike {@link EventType#EVT_CACHE_QUERY_EXECUTED}, {@link EventType#EVT_SQL_QUERY_EXECUTION} is fired only once for a request
 * and does not relate to a specific cache.
 *
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
 * User can also wait for events using method {@link org.apache.ignite.IgniteEvents#waitForLocal(org.apache.ignite.lang.IgnitePredicate, int...)}.
 * <h1 class="header">Events and Performance</h1>
 * Note that by default all events in Ignite are enabled and therefore generated and stored
 * by whatever event storage SPI is configured. Ignite can and often does generate thousands events per seconds
 * under the load and therefore it creates a significant additional load on the system. If these events are
 * not needed by the application this load is unnecessary and leads to significant performance degradation.
 * <p>
 * It is <b>highly recommended</b> to enable only those events that your application logic requires
 * by using {@link org.apache.ignite.configuration.IgniteConfiguration#getIncludeEventTypes()} method in Ignite configuration. Note that certain
 * events are required for Ignite's internal operations and such events will still be generated but not stored by
 * event storage SPI if they are disabled in Ignite configuration.
 *
 * @see EventType#EVT_SQL_QUERY_EXECUTION
 */
public class SqlQueryExecutionEvent extends EventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query text. */
    private final String text;

    /** Query arguments. */
    @GridToStringInclude(sensitive = true)
    private final Object[] args;

    /** Security subject ID. */
    private final UUID subjId;

    /**
     * @param node Node where event was fired.
     * @param msg Event message.
     * @param text Query text.
     * @param args Query arguments.
     * @param subjId Security subject ID.
     */
    public SqlQueryExecutionEvent(
        ClusterNode node,
        String msg,
        @Nullable String text,
        @Nullable Object[] args,
        @Nullable UUID subjId
    ) {
        super(node, msg, EVT_SQL_QUERY_EXECUTION);

        this.text = text;
        this.args = args;
        this.subjId = subjId;
    }

    /**
     * Gets query text.
     * <p>
     * Applicable for {@code SQL}, {@code SQL fields} queries.
     *
     * @return Query text.
     */
    @Nullable public String text() {
        return text;
    }

    /**
     * Gets query arguments.
     * <p>
     * Applicable for {@code SQL} and {@code SQL fields} queries.
     *
     * @return Query arguments.
     */
    @Nullable public Object[] arguments() {
        return args.clone();
    }

    /**
     * Gets security subject ID.
     *
     * @return Security subject ID.
     */
    @Nullable public UUID subjectId() {
        return subjId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlQueryExecutionEvent.class, this,
            "nodeId8", U.id8(node().id()),
            "msg", message(),
            "type", name(),
            "tstamp", timestamp());
    }
}


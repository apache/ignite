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
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.jetbrains.annotations.Nullable;

/**
 * Cache query read event.
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
 * @see EventType#EVT_CACHE_QUERY_OBJECT_READ
 * @see EventType#EVTS_CACHE_QUERY
 */
public class CacheQueryReadEvent<K, V> extends EventAdapter {
    /** */
    private static final long serialVersionUID = -1984731272984397445L;

    /** Query type. */
    private final String qryType;

    /** Cache name. */
    private final String cacheName;

    /** Class name. */
    private final String clsName;

    /** Clause. */
    private final String clause;

    /** Scan query filter. */
    @GridToStringInclude
    private final IgniteBiPredicate<K, V> scanQryFilter;

    /** Continuous query filter. */
    @GridToStringInclude
    private final CacheEntryEventSerializableFilter<K, V> contQryFilter;

    /** Query arguments. */
    @GridToStringInclude
    private final Object[] args;

    /** Security subject ID. */
    private final UUID subjId;

    /** Task name. */
    private final String taskName;

    /** Key. */
    @GridToStringInclude(sensitive = true)
    private final K key;

    /** Value. */
    @GridToStringInclude(sensitive = true)
    private final V val;

    /** Old value. */
    @GridToStringInclude(sensitive = true)
    private final V oldVal;

    /** Result row. */
    @GridToStringInclude(sensitive = true)
    private final Object row;

    /**
     * @param node Node where event was fired.
     * @param msg Event message.
     * @param type Event type.
     * @param cacheName Cache name.
     * @param clsName Class name.
     * @param clause Clause.
     * @param scanQryFilter Scan query filter.
     * @param args Query arguments.
     * @param subjId Security subject ID.
     * @param key Key.
     * @param val Value.
     * @param oldVal Old value.
     */
    public CacheQueryReadEvent(
        ClusterNode node,
        String msg,
        int type,
        String qryType,
        @Nullable String cacheName,
        @Nullable String clsName,
        @Nullable String clause,
        @Nullable IgniteBiPredicate<K, V> scanQryFilter,
        @Nullable CacheEntryEventSerializableFilter<K, V> contQryFilter,
        @Nullable Object[] args,
        @Nullable UUID subjId,
        @Nullable String taskName,
        @Nullable K key,
        @Nullable V val,
        @Nullable V oldVal,
        @Nullable Object row) {
        super(node, msg, type);

        assert qryType != null;

        this.qryType = qryType;
        this.cacheName = cacheName;
        this.clsName = clsName;
        this.clause = clause;
        this.scanQryFilter = scanQryFilter;
        this.contQryFilter = contQryFilter;
        this.args = args;
        this.subjId = subjId;
        this.taskName = taskName;
        this.key = key;
        this.val = val;
        this.oldVal = oldVal;
        this.row = row;
    }

    /**
     * Gets query type.
     *
     * @return Query type. Can be {@code "SQL"}, {@code "SQL_FIELDS"}, {@code "FULL_TEXT"}, {@code "SCAN"},
     * {@code "CONTINUOUS"} or {@code "SPI"}.
     */
    public String queryType() {
        return qryType;
    }

    /**
     * Gets cache name on which query was executed.
     *
     * @return Cache name.
     */
    @Nullable public String cacheName() {
        return cacheName;
    }

    /**
     * Gets queried class name.
     * <p>
     * Applicable for {@code SQL} and @{code full text} queries.
     *
     * @return Queried class name.
     */
    @Nullable public String className() {
        return clsName;
    }

    /**
     * Gets query clause.
     * <p>
     * Applicable for {@code SQL}, {@code SQL fields} and @{code full text} queries.
     *
     * @return Query clause.
     */
    @Nullable public String clause() {
        return clause;
    }

    /**
     * Gets scan query filter.
     * <p>
     * Applicable for {@code scan} queries.
     *
     * @return Scan query filter.
     */
    @Nullable public IgniteBiPredicate<K, V> scanQueryFilter() {
        return scanQryFilter;
    }

    /**
     * Gets continuous query filter.
     * <p>
     * Applicable for {@code continuous} queries.
     *
     * @return Continuous query filter.
     */
    @Nullable public CacheEntryEventSerializableFilter<K, V> continuousQueryFilter() {
        return contQryFilter;
    }

    /**
     * Gets query arguments.
     * <p>
     * Applicable for {@code SQL} and {@code SQL fields} queries.
     *
     * @return Query arguments.
     */
    @Nullable public Object[] arguments() {
        return args;
    }

    /**
     * Gets security subject ID.
     *
     * @return Security subject ID.
     */
    @Nullable public UUID subjectId() {
        return subjId;
    }

    /**
     * Gets the name of the task that executed the query (if any).
     *
     * @return Task name.
     */
    @Nullable public String taskName() {
        return taskName;
    }

    /**
     * Gets read entry key.
     *
     * @return Key.
     */
    @Nullable public K key() {
        return key;
    }

    /**
     * Gets read entry value.
     *
     * @return Value.
     */
    @Nullable public V value() {
        return val;
    }

    /**
     * Gets read entry old value (applicable for continuous queries).
     *
     * @return Old value.
     */
    @Nullable public V oldValue() {
        return oldVal;
    }

    /**
     * Gets read results set row.
     *
     * @return Result row.
     */
    @Nullable public Object row() {
        return row;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheQueryReadEvent.class, this,
            "nodeId8", U.id8(node().id()),
            "msg", message(),
            "type", name(),
            "tstamp", timestamp());
    }
}
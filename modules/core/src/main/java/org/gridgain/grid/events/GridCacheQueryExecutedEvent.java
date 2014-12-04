/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.events;

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Cache query execution event.
 * <p>
 * Grid events are used for notification about what happens within the grid. Note that by
 * design GridGain keeps all events generated on the local node locally and it provides
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
 * Note that by default all events in GridGain are enabled and therefore generated and stored
 * by whatever event storage SPI is configured. GridGain can and often does generate thousands events per seconds
 * under the load and therefore it creates a significant additional load on the system. If these events are
 * not needed by the application this load is unnecessary and leads to significant performance degradation.
 * <p>
 * It is <b>highly recommended</b> to enable only those events that your application logic requires
 * by using {@link org.apache.ignite.configuration.IgniteConfiguration#getIncludeEventTypes()} method in GridGain configuration. Note that certain
 * events are required for GridGain's internal operations and such events will still be generated but not stored by
 * event storage SPI if they are disabled in GridGain configuration.
 *
 * @see GridEventType#EVT_CACHE_QUERY_EXECUTED
 * @see GridEventType#EVTS_CACHE_QUERY
 */
public class GridCacheQueryExecutedEvent<K, V> extends GridEventAdapter {
    /** */
    private static final long serialVersionUID = 3738753361235304496L;

    /** Query type. */
    private final GridCacheQueryType qryType;

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
    private final IgnitePredicate<GridCacheContinuousQueryEntry<K, V>> contQryFilter;

    /** Query arguments. */
    @GridToStringInclude
    private final Object[] args;

    /** Security subject ID. */
    private final UUID subjId;

    /** Task name. */
    private final String taskName;

    /**
     * @param node Node where event was fired.
     * @param msg Event message.
     * @param type Event type.
     * @param qryType Query type.
     * @param cacheName Cache name.
     * @param clsName Class name.
     * @param clause Clause.
     * @param scanQryFilter Scan query filter.
     * @param args Query arguments.
     * @param subjId Security subject ID.
     */
    public GridCacheQueryExecutedEvent(
        ClusterNode node,
        String msg,
        int type,
        GridCacheQueryType qryType,
        @Nullable String cacheName,
        @Nullable String clsName,
        @Nullable String clause,
        @Nullable IgniteBiPredicate<K, V> scanQryFilter,
        @Nullable IgnitePredicate<GridCacheContinuousQueryEntry<K, V>> contQryFilter,
        @Nullable Object[] args,
        @Nullable UUID subjId,
        @Nullable String taskName) {
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
    }

    /**
     * Gets query type.
     *
     * @return Query type.
     */
    public GridCacheQueryType queryType() {
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
    @Nullable public IgnitePredicate<GridCacheContinuousQueryEntry<K, V>> continuousQueryFilter() {
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryExecutedEvent.class, this,
            "nodeId8", U.id8(node().id()),
            "msg", message(),
            "type", name(),
            "tstamp", timestamp());
    }
}

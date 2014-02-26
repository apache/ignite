// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.cache.query.*;
import org.jetbrains.annotations.*;

/**
 * Cache query metrics used to obtain statistics on query. You can get query metrics via
 * {@link GridCacheQueries#queryMetrics()} method which will provide metrics for all queries
 * executed on cache.
 * <p>
 * Note that in addition to query metrics, you can also enable query tracing by setting
 * {@code "org.gridgain.cache.queries"} logging category to {@code DEBUG} level.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridCacheQueryMetrics {
    /**
     * Gets time of the first query execution.
     *
     * @return First execution time.
     */
    public long firstRunTime();

    /**
     * Gets time of the last query execution.
     *
     * @return Last execution time.
     */
    public long lastRunTime();

    /**
     * Gets minimum execution time of query.
     *
     * @return Minimum execution time of query.
     */
    public long minimumTime();

    /**
     * Gets maximum execution time of query.
     *
     * @return Maximum execution time of query.
     */
    public long maximumTime();

    /**
     * Gets average execution time of query.
     *
     * @return Average execution time of query.
     */
    public double averageTime();

    /**
     * Gets total number execution of query.
     *
     * @return Number of executions.
     */
    public int executions();

    /**
     * Gets total number of times a query execution failed.
     *
     * @return total number of times a query execution failed.
     */
    public int fails();

    /**
     * Gets query clause.
     *
     * @return Query clause.
     */
    // TODO: remove
    @Nullable public String clause();

    /**
     * Gets query type.
     *
     * @return type Query type.
     */
    // TODO: remove
    public GridCacheQueryType type();

    /**
     * Gets Java class name of the values selected by the query.
     *
     * @return Java class name of the values selected by the query.
     */
    // TODO: remove
    @Nullable public String className();
}

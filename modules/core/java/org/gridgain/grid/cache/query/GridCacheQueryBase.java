// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Base API for all supported types of cache queries. Specifically for:
 * {@link GridCacheQuery}, {@link GridCacheReduceQuery}, and {@link GridCacheTransformQuery}.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 * @author @java.author
 * @version @java.version
 */
public interface GridCacheQueryBase<K, V, T extends GridCacheQueryBase> extends Closeable {
    /** Default query page size. */
    public static final int DFLT_PAGE_SIZE = 1024;

    /**
     * Gets query id.
     *
     * @return Query id.
     */
    public int id();

    /**
     * Gets query type ({@code null} for fields queries).
     *
     * @return Query type.
     */
    @Nullable public GridCacheQueryType type();

    /**
     * Sets query clause.
     * Can be {@code null} for {@link GridCacheQueryType#SCAN} queries.
     *
     * @param clause Query clause.
     */
    public void clause(@Nullable String clause);

    /**
     * Gets query clause.
     *
     * @return Query clause.
     */
    @Nullable public String clause();

    /**
     * Sets Java class name of the values selected by the query.
     * Can be {@code null} for {@link GridCacheQueryType#SCAN} queries.
     *
     * @param clsName Java class name of the values selected by the query.
     */
    public void className(@Nullable String clsName);

    /**
     * Gets Java class name of the values selected by the query.
     *
     * @return Java class name of the values selected by the query.
     */
    @Nullable public String className();

    /**
     * Sets result page size. If not provided, {@link #DFLT_PAGE_SIZE} will be used.
     * Results are returned from queried nodes one page at a tme.
     *
     * @param  pageSize Page size.
     */
    public void pageSize(int pageSize);

    /**
     * Gets query result page size.
     *
     * @return Query page size.
     */
    public int pageSize();

    /**
     * Sets query timeout. {@code 0} means there is no timeout. Default value
     * is {@code 30} seconds.
     *
     * @param timeout Query timeout.
     */
    public void timeout(long timeout);

    /**
     * Gets query timeout.
     *
     * @return Query timeout.
     */
    public long timeout();

    /**
     * Sets whether or not to keep all query results local. If not - only the current page
     * is kept locally. Default value is {@code true}.
     *
     * @param keepAll Keep results or not.
     */
    public void keepAll(boolean keepAll);

    /**
     * Gets query {@code keepAll} flag.
     *
     * @return Query {@code keepAll} flag.
     */
    public boolean keepAll();

    /**
     * Sets whether or not to include backup entries into query result. This flag
     * is {@code false} by default.
     *
     * @param incBackups Query {@code includeBackups} flag.
     */
    public void includeBackups(boolean incBackups);

    /**
     * Gets query {@code includeBackups} flag.
     *
     * @return Query {@code includeBackups} flag.
     */
    public boolean includeBackups();

    /**
     * Sets whether or not to deduplicate query result set. If this flag is {@code true}
     * then query result will not contain some key more than once even if several nodes
     * returned entries with the same keys. Default value is {@code false}.
     *
     * @param dedup Query {@code enableDedup} flag.
     */
    public void enableDedup(boolean dedup);

    /**
     * Gets query {@code enableDedup} flag.
     *
     * @return Query {@code enableDedup} flag.
     */
    public boolean enableDedup();

    /**
     * Sets optional grid projection to execute this query on.
     *
     * @param prj Projection.
     * @return New query object.
     */
    public T projection(GridProjection prj);

    /**
     * Gets grid projection on which this query will be executed.
     *
     * @return Grid projection.
     */
    @Nullable public GridProjection projection();

    /**
     * Key filter to be used on queried nodes prior to visiting or returning key-value pairs
     * to user.
     * <p>
     * If filter is set, then it will be used for every query execution. Only keys that
     * pass the filter will be included in query result.
     *
     * @param keyFilter Key filter.
     * @return New query with remote key filter set.
     */
    public T remoteKeyFilter(GridPredicate<K> keyFilter);

    /**
     * Value filter to be used on queried nodes prior to visiting or returning key-value pairs
     * to user.
     * <p>
     * If filter is set, then it will be used for every query execution. Only values that
     * pass the filter will be included in query result.
     *
     * @param valFilter Value filter.
     * @return New query with remote value filter set.
     */
    public T remoteValueFilter(GridPredicate<V> valFilter);

    /**
     * Optional callback to be used on queried nodes that will be called before query execution.
     * <p>
     * If callback is set, then it will be used for every query execution.
     *
     * @param beforeCb Callback closure.
     * @return New query with before callback set.
     */
    public T beforeExecution(Runnable beforeCb);

    /**
     * Optional callback to be used on queried nodes that will be called after query execution.
     * <p>
     * If callback is set, then it will be used for every query execution.
     *
     * @param afterCb Callback closure.
     * @return New query with after callback set.
     */
    public T afterExecution(Runnable afterCb);

    /**
     * Gets query metrics.
     *
     * @return Query metrics.
     */
    public GridCacheQueryMetrics metrics();
}

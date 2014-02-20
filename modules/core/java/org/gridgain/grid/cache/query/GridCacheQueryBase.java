// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.query;

import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.lang.*;
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
public interface GridCacheQueryBase<K, V> extends Closeable {
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
     * Optional filter factory to be used on queried nodes to create key filters prior
     * to visiting or returning key-value pairs to user. The factory is a closure that accepts
     * array of objects provided by {@link GridCacheQuery#closureArguments(Object...)} or
     * {@link GridCacheReduceQuery#closureArguments(Object...)} or
     * {@link GridCacheTransformQuery#closureArguments(Object...)} methods as a parameter
     * and returns predicate filter for keys.
     * <p>
     * If factory is set, then it will be invoked for every query execution. Only keys that
     * pass the filter will be included in query result. If state of the filter changes after
     * each query execution, then factory should return a new filter for every execution.
     *
     * @param factory Optional factory closure to create key filters.
     */
    public void remoteKeyFilter(@Nullable GridClosure<Object[], GridPredicate<? super K>> factory);

    /**
     * Optional filter factory to be used on queried nodes to create value filters prior
     * to visiting or returning key-value pairs to user. The factory is a closure that accepts
     * array of objects provided by {@link GridCacheQuery#closureArguments(Object...)} or
     * {@link GridCacheReduceQuery#closureArguments(Object...)} or
     * {@link GridCacheTransformQuery#closureArguments(Object...)} methods as a parameter
     * and returns predicate filter for values.
     * <p>
     * If factory is set, then it will be invoked for every query execution. Only values that
     * pass the filter will be included in query result. If state of the filter changes after
     * each query execution, then factory should return a new filter for every execution.
     *
     * @param factory Optional factory closure to create value filters.
     */
    public void remoteValueFilter(@Nullable GridClosure<Object[], GridPredicate<? super V>> factory);

    /**
     * Optional callback factory to be used on queried nodes to create callbacks that will be
     * called before query execution. The factory is a closure that accepts
     * array of objects provided by {@link GridCacheQuery#closureArguments(Object...)} or
     * {@link GridCacheReduceQuery#closureArguments(Object...)} or
     * {@link GridCacheTransformQuery#closureArguments(Object...)} methods as a parameter
     * and returns callback closure.
     * <p>
     * If factory is set, then it will be invoked for every query execution.
     *
     * @param factory Optional factory closure to create callbacks.
     */
    public void beforeCallback(@Nullable GridClosure<Object[], GridAbsClosure> factory);

    /**
     * Optional callback factory to be used on queried nodes to create callbacks that will be
     * called after query execution. The factory is a closure that accepts
     * array of objects provided by {@link GridCacheQuery#closureArguments(Object...)} or
     * {@link GridCacheReduceQuery#closureArguments(Object...)} or
     * {@link GridCacheTransformQuery#closureArguments(Object...)} methods as a parameter
     * and returns callback closure.
     * <p>
     * If factory is set, then it will be invoked for every query execution.
     *
     * @param factory Optional factory closure to create callbacks.
     */
    public void afterCallback(@Nullable GridClosure<Object[], GridAbsClosure> factory);

    /**
     * Gets query metrics.
     *
     * @return Query metrics.
     */
    public GridCacheQueryMetrics metrics();
}

// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.query;

import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * TODO: Add interface description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridCacheQueries<K, V> {
    /**
     * Gets metrics (statistics) for all queries executed in this cache. Metrics
     * are grouped by query clause (e.g. SQL clause), query type, and return value.
     * <p>
     * Note that only the last {@code 1000} query metrics are kept. This should be
     * enough for majority of the applications, as generally applications have
     * significantly less than {@code 1000} different queries that are executed.
     * <p>
     * Note that in addition to query metrics, you can also enable query tracing by setting
     * {@code "org.gridgain.cache.queries"} logging category to {@code DEBUG} level.
     *
     * @return Queries metrics or {@code null} if a query manager is not provided.
     */
    @Nullable public Collection<GridCacheQueryMetrics> queryMetrics();

    /**
     * Creates user's query for given query type. For more information refer to
     * {@link GridCacheQuery} documentation.
     *
     * @param type Query type.
     * @return Created query.
     */
    public GridCacheQuery<K, V> createQuery(GridCacheQueryType type);

    /**
     * Creates user's query for given query type, queried class, and query clause which is generally
     * a where clause. For more information refer to {@link GridCacheQuery} documentation.
     *
     * @param type Query type.
     * @param cls Query class (may be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @param clause Query clause (should be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @return Created query.
     */
    public GridCacheQuery<K, V> createQuery(GridCacheQueryType type, @Nullable Class<?> cls,
        @Nullable String clause);

    /**
     * Creates user's query for given query type, queried class, and query clause which is generally
     * a where clause. For more information refer to {@link GridCacheQuery} documentation.
     *
     * @param type Query type.
     * @param clsName Query class name (may be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @param clause Query clause (should be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @return Created query.
     */
    public GridCacheQuery<K, V> createQuery(GridCacheQueryType type, @Nullable String clsName,
        @Nullable String clause);

    /**
     * Creates user's transform query. For more information refer to {@link GridCacheQuery} documentation.
     *
     * @return Created query.
     */
    public <T> GridCacheTransformQuery<K, V, T> createTransformQuery();

    /**
     * Creates user's transform query. For more information refer to {@link GridCacheQuery} documentation.
     *
     * @param type Query type.
     * @return Created query.
     */
    public <T> GridCacheTransformQuery<K, V, T> createTransformQuery(GridCacheQueryType type);

    /**
     * Creates user's transform query. For more information refer to {@link GridCacheQuery} documentation.
     *
     * @param type Query type.
     * @param cls Query class (may be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @param clause Query clause (should be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @return Created query.
     */
    public <T> GridCacheTransformQuery<K, V, T> createTransformQuery(GridCacheQueryType type,
        @Nullable Class<?> cls, @Nullable String clause);

    /**
     * Creates user's transform query. For more information refer to {@link GridCacheQuery} documentation.
     *
     * @param type Query type.
     * @param clsName Query class name (may be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @param clause Query clause (should be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @return Created query.
     */
    public <T> GridCacheTransformQuery<K, V, T> createTransformQuery(GridCacheQueryType type, @Nullable String clsName,
        @Nullable String clause);

    /**
     * Creates user's reduce query. For more information refer to {@link GridCacheQuery} documentation.
     *
     * @return Created query.
     */
    public <R1, R2> GridCacheReduceQuery<K, V, R1, R2> createReduceQuery();

    /**
     * Creates user's reduce query. For more information refer to {@link GridCacheQuery} documentation.
     *
     * @param type Query type.
     * @return Created query.
     */
    public <R1, R2> GridCacheReduceQuery<K, V, R1, R2> createReduceQuery(GridCacheQueryType type);

    /**
     * Creates user's reduce query. For more information refer to {@link GridCacheQuery} documentation.
     *
     * @param type Query type.
     * @param cls Query class (may be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @param clause Query clause (should be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @return Created query.
     */
    public <R1, R2> GridCacheReduceQuery<K, V, R1, R2> createReduceQuery(GridCacheQueryType type,
        @Nullable Class<?> cls, @Nullable String clause);

    /**
     * Creates user's reduce query. For more information refer to {@link GridCacheQuery} documentation.
     *
     * @param type Query type.
     * @param clsName Query class name (may be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @param clause Query clause (should be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @return Created query.
     */
    public <R1, R2> GridCacheReduceQuery<K, V, R1, R2> createReduceQuery(GridCacheQueryType type,
        @Nullable String clsName, @Nullable String clause);

    /**
     * Creates user's fields query for given clause. For more information refer to
     * {@link GridCacheFieldsQuery} documentation.
     *
     * @param clause Query clause.
     * @return Created query.
     */
    public GridCacheFieldsQuery<K, V> createFieldsQuery(String clause);

    /**
     * Creates user's reduce fields query for given clause. For more information refer to
     * {@link GridCacheReduceFieldsQuery} documentation.
     *
     * @param clause Query clause.
     * @return Created query.
     */
    public <R1, R2> GridCacheReduceFieldsQuery<K, V, R1, R2> createReduceFieldsQuery(String clause);

    /**
     * Creates new continuous query.
     * <p>
     * For more information refer to {@link GridCacheContinuousQuery} documentation.
     *
     * @return Created continuous query.
     * @see GridCacheContinuousQuery
     */
    public GridCacheContinuousQuery<K, V> createContinuousQuery();

    /**
     * Forces this cache to rebuild all search indexes of given value type. Sometimes indexes
     * may hold references to objects that have already been removed from cache. Although
     * not affecting query results, these objects may consume extra memory. Rebuilding
     * indexes will remove any redundant references that may have temporarily got stuck
     * inside in-memory index.
     *
     * @param cls Value type to rebuild indexes for.
     *
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    public GridFuture<?> rebuildIndexes(Class<?> cls);

    /**
     * Forces this cache to rebuild search indexes of all types. Sometimes indexes
     * may hold references to objects that have already been removed from cache. Although
     * not affecting query results, these objects may consume extra memory. Rebuilding
     * indexes will remove any redundant references that may have temporarily got stuck
     * inside in-memory index.
     *
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    public GridFuture<?> rebuildAllIndexes();

    /**
     * Gets cache metadata that can be used for SQL queries.
     * First item in collection is metadata for current cache,
     * rest are for caches configured with the same indexing SPI.
     * <p>
     * See {@link GridCacheSqlMetadata} javadoc for more information.
     *
     * @return Cache metadata.
     * @throws GridException If operation failed.
     * @see GridCacheSqlMetadata
     * @see GridCacheQuery
     * @see GridCacheFieldsQuery
     */
    public Collection<GridCacheSqlMetadata> sqlMetadata() throws GridException;
}

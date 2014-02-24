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

import java.util.*;

/**
 * TODO: Add interface description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridCacheQueries<K, V> {
    /**
     * Creates user's SQL query, queried class, and query clause which is generally
     * a where clause. For more information refer to {@link GridCacheQuery} documentation.
     *
     * @param cls Query class.
     * @param clause Query clause.
     * @return Created query.
     */
    public GridCacheQuery<Map.Entry<K, V>> createSqlQuery(Class<?> cls, String clause);

    /**
     * Creates user's SQL fields query for given clause. For more information refer to
     * {@link GridCacheQuery} documentation.
     *
     * @param query Query.
     * @return Created query.
     */
    public GridCacheQuery<List<?>> createSqlFieldsQuery(String query);

    /**
     * Creates user's full text query, queried class, and query clause.
     * For more information refer to {@link GridCacheQuery} documentation.
     *
     * @param cls Query class.
     * @param search Search clause.
     * @return Created query.
     */
    public GridCacheQuery<Map.Entry<K, V>> createFulltextQuery(Class<?> cls, String search);

    /**
     * Creates user's predicate based scan query.
     *
     * @param filter Filter.
     * @return Created query.
     */
    public GridCacheQuery<Map.Entry<K, V>> createScanQuery(@Nullable GridBiPredicate<K, V> filter);

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
}

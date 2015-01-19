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

package org.gridgain.grid.cache.query;

import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Facade for creating distributed queries. It contains various {@code 'createXxxQuery(..)'}
 * methods for {@code SQL}, {@code TEXT}, and {@code SCAN} query creation (see {@link GridCacheQuery}
 * for more information).
 * <p>
 * Instance of {@code GridCacheQueries} is obtained from cache projection as follows:
 * <pre name="code" class="java">
 * GridCacheQueries q = GridGain.grid().cache("myCache").queries();
 * </pre>
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
     * Creates user's SQL query, queried class, and query clause which is generally
     * a where clause. For more information refer to {@link GridCacheQuery} documentation.
     *
     * @param clsName Query class name.
     * @param clause Query clause.
     * @return Created query.
     */
    public GridCacheQuery<Map.Entry<K, V>> createSqlQuery(String clsName, String clause);

    /**
     * Creates user's SQL fields query for given clause. For more information refer to
     * {@link GridCacheQuery} documentation.
     *
     * @param qry Query.
     * @return Created query.
     */
    public GridCacheQuery<List<?>> createSqlFieldsQuery(String qry);

    /**
     * Creates user's full text query, queried class, and query clause.
     * For more information refer to {@link GridCacheQuery} documentation.
     *
     * @param clsName Query class name.
     * @param search Search clause.
     * @return Created query.
     */
    public GridCacheQuery<Map.Entry<K, V>> createFullTextQuery(String clsName, String search);

    /**
     * Creates user's full text query, queried class, and query clause.
     * For more information refer to {@link GridCacheQuery} documentation.
     *
     * @param cls Query class.
     * @param search Search clause.
     * @return Created query.
     */
    public GridCacheQuery<Map.Entry<K, V>> createFullTextQuery(Class<?> cls, String search);

    /**
     * Creates user's predicate based scan query.
     *
     * @param filter Scan filter.
     * @return Created query.
     */
    public GridCacheQuery<Map.Entry<K, V>> createScanQuery(@Nullable IgniteBiPredicate<K, V> filter);

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
    public IgniteFuture<?> rebuildIndexes(Class<?> cls);

    /**
     * Forces this cache to rebuild all search indexes of given value type. Sometimes indexes
     * may hold references to objects that have already been removed from cache. Although
     * not affecting query results, these objects may consume extra memory. Rebuilding
     * indexes will remove any redundant references that may have temporarily got stuck
     * inside in-memory index.
     *
     * @param typeName Value type name to rebuild indexes for.
     *
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    public IgniteFuture<?> rebuildIndexes(String typeName);

    /**
     * Forces this cache to rebuild search indexes of all types. Sometimes indexes
     * may hold references to objects that have already been removed from cache. Although
     * not affecting query results, these objects may consume extra memory. Rebuilding
     * indexes will remove any redundant references that may have temporarily got stuck
     * inside in-memory index.
     *
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    public IgniteFuture<?> rebuildAllIndexes();

    /**
     * Accumulated metrics for all queries executed for this cache.
     *
     * @return Cache query metrics.
     */
    public GridCacheQueryMetrics metrics();

    /**
     * Resets accumulated metrics.
     */
    public void resetMetrics();
}

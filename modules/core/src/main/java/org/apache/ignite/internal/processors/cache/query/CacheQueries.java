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

package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.cache.query.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Facade for creating distributed queries. It contains various {@code 'createXxxQuery(..)'}
 * methods for {@code SQL}, {@code TEXT}, and {@code SCAN} query creation (see {@link CacheQuery}
 * for more information).
 * <p>
 * Instance of {@code CacheQueries} is obtained from cache projection as follows:
 * <pre name="code" class="java">
 * CacheQueries q = Ignition.ignite().cache("myCache").queries();
 * </pre>
 */
public interface CacheQueries<K, V> {
    /**
     * Creates user's SQL fields query for given clause. For more information refer to
     * {@link CacheQuery} documentation.
     *
     * @param qry Query.
     * @return Created query.
     */
    public CacheQuery<List<?>> createSqlFieldsQuery(String qry);

    /**
     * Creates user's full text query, queried class, and query clause.
     * For more information refer to {@link CacheQuery} documentation.
     *
     * @param clsName Query class name.
     * @param search Search clause.
     * @return Created query.
     */
    public CacheQuery<Map.Entry<K, V>> createFullTextQuery(String clsName, String search);

    /**
     * Creates user's predicate based scan query.
     *
     * @param filter Scan filter.
     * @return Created query.
     */
    public CacheQuery<Map.Entry<K, V>> createScanQuery(@Nullable IgniteBiPredicate<K, V> filter);

    /**
     * Accumulated metrics for all queries executed for this cache.
     *
     * @return Cache query metrics.
     */
    public QueryMetrics metrics();
}

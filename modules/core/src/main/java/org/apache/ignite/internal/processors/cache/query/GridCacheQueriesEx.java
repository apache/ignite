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

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;

import java.util.*;

/**
 * Extended queries interface.
 */
public interface GridCacheQueriesEx<K, V> extends CacheQueries<K, V> {
    /**
     * Gets SQL metadata.
     *
     * @return SQL metadata.
     * @throws IgniteCheckedException In case of error.
     */
    public Collection<GridCacheSqlMetadata> sqlMetadata() throws IgniteCheckedException;

    /**
     * Creates SQL fields query which will include results metadata if needed.
     *
     * @param qry SQL query.
     * @param incMeta Whether to include results metadata.
     * @return Created query.
     */
    public CacheQuery<List<?>> createSqlFieldsQuery(String qry, boolean incMeta);

    /**
     * Creates SPI query.
     *
     * @return Query.
     */
    public <R> CacheQuery<R> createSpiQuery();
}

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

package org.apache.ignite.internal.cache.query.index.sorted;

import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;

/**
 * Schema for sorted index.
 */
public interface SortedIndexSchema {
    /**
     * Describe all index keys.
     */
    public IndexKeyDefinition[] getKeyDefinitions();

    /**
     * @param idx index of key within index schema.
     * @param row original cache data row.
     * @return index key
     */
    public Object getIndexKey(int idx, CacheDataRow row);

    /**
     * @return parition for specified row.
     */
    public int partition(CacheDataRow row);

    /**
     * @param row Cache row.
     * @return Cache key.
     */
    public Object getCacheKey(CacheDataRow row);

    /**
     * @param row Cache row.
     * @return Cache value.
     */
    public Object getCacheValue(CacheDataRow row);
}

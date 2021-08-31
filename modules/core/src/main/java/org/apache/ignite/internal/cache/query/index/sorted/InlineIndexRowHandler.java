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

import java.util.List;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;

/**
 * Handles InlineIndexRow. Stores information about inlined keys, and rules how to convert CacheDataRow to IndexRow.
 */
public interface InlineIndexRowHandler {
    /**
     * Returns index key by specified idx.
     *
     * @param idx Index of key within index schema.
     * @param row Original cache data row.
     * @return Index key.
     */
    public IndexKey indexKey(int idx, CacheDataRow row);

    /**
     * @return List of key types for inlined index keys.
     */
    public List<InlineIndexKeyType> inlineIndexKeyTypes();

    /**
     * @return List of index key definitions.
     */
    public List<IndexKeyDefinition> indexKeyDefinitions();

    /**
     * @return Index key type settings.
     */
    public IndexKeyTypeSettings indexKeyTypeSettings();

    /**
     * @return Parition for specified row.
     */
    public int partition(CacheDataRow row);

    /**
     * @param row Cache row.
     * @return Cache key.
     */
    public Object cacheKey(CacheDataRow row);

    /**
     * @param row Cache row.
     * @return Cache value.
     */
    public Object cacheValue(CacheDataRow row);
}

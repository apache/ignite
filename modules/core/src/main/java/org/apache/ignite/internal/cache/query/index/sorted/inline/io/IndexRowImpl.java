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

package org.apache.ignite.internal.cache.query.index.sorted.inline.io;

import org.apache.ignite.cache.query.index.sorted.SortedIndex;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexSchema;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;

/**
 * This class represents a row in {@link SortedIndex}.
 */
public class IndexRowImpl implements IndexSearchRow {
    /** Object that contains info about original IgniteCache row. */
    private final CacheDataRow cacheRow;

    /** Cache for index row keys. To avoid hit underlying cache for every comparation. */
    private final Object[] keyCache;

    /** Schema of an index. */
    private final SortedIndexSchema schema;

    /** Constructor. */
    public IndexRowImpl(SortedIndexSchema schema, CacheDataRow row) {
        cacheRow = row;
        keyCache = new Object[schema.getKeyDefinitions().length];
        this.schema = schema;
    }

    /**
     * Get indexed value.
     */
    public CacheObject value() {
        return cacheRow.value();
    }

    /** {@inheritDoc} */
    @Override public Object getKey(int idx) {
        // TODO: how to handle when null is a valid value?
        if (keyCache[idx] != null)
            return keyCache[idx];

        Object key = schema.getIndexKey(idx, cacheRow);

        keyCache[idx] = key;

        return key;
    }

    /** {@inheritDoc} */
    @Override public long getLink() {
        return cacheRow.link();
    }

    /** {@inheritDoc} */
    @Override public SortedIndexSchema getSchema() {
        return schema;
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow getCacheDataRow() {
        return cacheRow;
    }

    /** {@inheritDoc} */
    // This method there due to BPlusTree work with the most wide interface.
    @Override public int getSearchKeysCount() {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    // This method there due to BPlusTree work with the most wide interface.
    @Override public boolean isFullSchemaSearch() {
        throw new IllegalStateException();
    }

    // TODO: MVCC
}

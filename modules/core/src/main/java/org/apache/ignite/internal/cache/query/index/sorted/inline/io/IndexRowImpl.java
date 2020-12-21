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
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE;

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
        this(schema, row, new Object[schema.getKeyDefinitions().length]);
    }

    /**
     * Constructor with prefilling of keys cache.
     */
    public IndexRowImpl(SortedIndexSchema schema, CacheDataRow row, Object[] keys) {
        this.schema = schema;
        cacheRow = row;
        keyCache = keys;
    }

    /**
     * Get indexed value.
     */
    public CacheObject value() {
        return cacheRow.value();
    }

    /**
     * Get cached keys.
     */
    public Object[] keys() {
        return keyCache;
    }

    /** {@inheritDoc} */
    @Override public Object getKey(int idx) {
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
    @Override public int getSearchKeysCount() {
        return schema.getKeyDefinitions().length;
    }

    /** {@inheritDoc} */
    @Override public boolean isFullSchemaSearch() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        SB sb = new SB("Row@");

        sb.a(Integer.toHexString(System.identityHashCode(this)));

        Object v = schema.getCacheKey(cacheRow);

        sb.a("[ key: ").a(v == null ? "nil" : v.toString());

        v = schema.getCacheValue(cacheRow);
        sb.a(", val: ").a(v == null ? "nil" : (S.includeSensitive() ? v.toString() :
            "Data hidden due to " + IGNITE_TO_STRING_INCLUDE_SENSITIVE + " flag."));

        sb.a(" ][ ");

        if (v != null) {
            for (int i = QueryUtils.DEFAULT_COLUMNS_COUNT, cnt = schema.getKeyDefinitions().length; i < cnt; i++) {
                if (i != QueryUtils.DEFAULT_COLUMNS_COUNT)
                    sb.a(", ");

                try {
                    v = getKey(i);

                    sb.a(v == null ? "nil" : (S.includeSensitive() ? v.toString() : "data hidden"));
                }
                catch (Exception e) {
                    sb.a("<value skipped on error: " + e.getMessage() + '>');
                }
            }
        }

        sb.a(" ]");

        return sb.toString();
    }
}

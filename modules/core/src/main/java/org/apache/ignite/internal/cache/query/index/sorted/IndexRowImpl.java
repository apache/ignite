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

import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE;

/**
 * This class represents a row in {@link SortedSegmentedIndex}.
 */
public class IndexRowImpl implements IndexRow {
    /** Object that contains info about original IgniteCache row. */
    private final CacheDataRow cacheRow;

    /** Cache for index row keys. To avoid hit underlying cache for every comparation. */
    private IndexKey[] keyCache;

    /** Schema of an index. */
    private final InlineIndexRowHandler rowHnd;

    /** Constructor. */
    public IndexRowImpl(InlineIndexRowHandler rowHnd, CacheDataRow row) {
        this(rowHnd, row, null);
    }

    /**
     * Constructor with prefilling of keys cache.
     */
    public IndexRowImpl(InlineIndexRowHandler rowHnd, CacheDataRow row, IndexKey[] keys) {
        this.rowHnd = rowHnd;
        cacheRow = row;
        keyCache = keys;
    }

    /**
     * @return Indexed value.
     */
    public CacheObject value() {
        return cacheRow.value();
    }

    /** {@inheritDoc} */
    @Override public IndexKey key(int idx) {
        if (keyCache != null && keyCache[idx] != null)
            return keyCache[idx];

        IndexKey key = rowHnd.indexKey(idx, cacheRow);

        if (keyCache != null)
            keyCache[idx] = key;

        return key;
    }

    /** {@inheritDoc} */
    @Override public int keysCount() {
        return rowHnd.indexKeyDefinitions().size();
    }

    /** {@inheritDoc} */
    @Override public long link() {
        return cacheRow.link();
    }

    /** {@inheritDoc} */
    @Override public InlineIndexRowHandler rowHandler() {
        return rowHnd;
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow cacheDataRow() {
        return cacheRow;
    }

    /**
     * @return Cache ID or {@code 0} if cache ID is not defined.
     */
    public int cacheId() {
        return cacheDataRow().cacheId();
    }

    /** Initialize a cache for index keys. Useful for inserting rows as there are a lot of comparisons. */
    public void prepareCache() {
        keyCache = new IndexKey[rowHnd.indexKeyDefinitions().size()];
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        SB sb = new SB("Row@");

        sb.a(Integer.toHexString(System.identityHashCode(this)));

        Object v = rowHnd.cacheKey(cacheRow);

        sb.a("[ key: ").a(v == null ? "nil" : v.toString());

        v = rowHnd.cacheValue(cacheRow);
        sb.a(", val: ").a(v == null ? "nil" : (S.includeSensitive() ? v.toString() :
            "Data hidden due to " + IGNITE_TO_STRING_INCLUDE_SENSITIVE + " flag."));

        sb.a(" ][ ");

        if (v != null) {
            for (int i = 0, cnt = rowHnd.indexKeyDefinitions().size(); i < cnt; i++) {
                if (i != 0)
                    sb.a(", ");

                try {
                    v = key(i);

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

    /** {@inheritDoc} */
    @Override public long mvccCoordinatorVersion() {
        return cacheRow.mvccCoordinatorVersion();
    }

    /** {@inheritDoc} */
    @Override public long mvccCounter() {
        return cacheRow.mvccCounter();
    }

    /** {@inheritDoc} */
    @Override public int mvccOperationCounter() {
        return cacheRow.mvccOperationCounter();
    }

    /** {@inheritDoc} */
    @Override public byte mvccTxState() {
        return cacheRow.mvccTxState();
    }

    /** {@inheritDoc} */
    @Override public boolean indexSearchRow() {
        return false;
    }
}

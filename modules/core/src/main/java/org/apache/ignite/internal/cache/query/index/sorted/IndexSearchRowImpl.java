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
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Represents a search row that used to find a place in a tree.
 */
public class IndexSearchRowImpl implements IndexRow {
    /** */
    private final IndexKey[] keys;

    /** */
    private final InlineIndexRowHandler rowHnd;

    /** Constructor. */
    public IndexSearchRowImpl(IndexKey[] idxKeys, InlineIndexRowHandler rowHnd) {
        keys = idxKeys;
        this.rowHnd = rowHnd;
    }

    /** {@inheritDoc} */
    @Override public IndexKey key(int idx) {
        return keys[idx];
    }

    /** {@inheritDoc} */
    @Override public int keysCount() {
        return keys.length;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IndexSearchRowImpl.class, this);
    }

    /** {@inheritDoc} */
    @Override public long link() {
        assert false : "Should not get link by IndexSearchRowImpl";

        return 0;
    }

    /** {@inheritDoc} */
    @Override public InlineIndexRowHandler rowHandler() {
        return rowHnd;
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow cacheDataRow() {
        assert false : "Should not cache data row by IndexSearchRowImpl";

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean indexSearchRow() {
        return true;
    }
}

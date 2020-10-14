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

import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexSchema;

/**
 * Represents a search row that used to find a place in a tree.
 */
public class IndexSearchRowImpl extends IndexRowImpl {
    /** Keys of search row. */
    private final Object[] idxKeys;

    /**
     * TODO
     * If {@code true} then length of {@link #idxKeys} must be equal to length of schema, so use full
     * schema to search. If {@code false} then it's possible to use only part of schema for search. Only first
     */
    private final boolean fullSchemaSearch;

    /** Constructor. */
    public IndexSearchRowImpl(Object[] idxKeys, SortedIndexSchema schema, boolean fullSchemaSearch) {
        super(schema, null);

        this.idxKeys = idxKeys;
        this.fullSchemaSearch = fullSchemaSearch;
    }

    /** {@inheritDoc} */
    @Override public Object getKey(int idx) {
        return idxKeys[idx];
    }

    /** {@inheritDoc} */
    @Override public int getSearchKeysCount() {
        return idxKeys.length;
    }

    /** {@inheritDoc} */
    @Override public boolean isFullSchemaSearch() {
        return fullSchemaSearch;
    }

    // TODO: MVCC
}

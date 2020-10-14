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

import org.apache.ignite.cache.query.index.IndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexRowComparator;
import org.apache.ignite.internal.processors.cache.GridCacheContext;

/**
 * Represents a definition of sorted index.
 */
public class SortedIndexDefinition implements IndexDefinition {
    /** Cache context index is created for. */
    private GridCacheContext ctx;

    /** Schema of index. */
    private final SortedIndexSchema schema;

    /** Row comparator. */
    private final IndexRowComparator rowComparator;

    /** Unique index name. */
    private final String idxName;

    /** Configured inline size. */
    private final int inlineSize;

    /** Segments. */
    private final int segments;

    /** Constructor. */
    public SortedIndexDefinition(GridCacheContext ctx, String idxName, int segments,
        SortedIndexSchema schema, IndexRowComparator rowComparator, int inlineSize) {

        this.ctx = ctx;
        this.idxName = idxName;
        this.segments = segments;
        this.schema = schema;
        this.rowComparator = rowComparator;
        this.inlineSize = inlineSize;
    }

    /** {@inheritDoc} */
    @Override public GridCacheContext getContext() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override public String getIdxName() {
        return idxName;
    }

    /** */
    public String getTreeName() {
        return idxName;
    }

    /** */
    public int getSegments() {
        return segments;
    }

    /** */
    public SortedIndexSchema getSchema() {
        return schema;
    }

    /** */
    public IndexRowComparator getRowComparator() {
        return rowComparator;
    }

    /** */
    public int getInlineSize() {
        return inlineSize;
    }
}

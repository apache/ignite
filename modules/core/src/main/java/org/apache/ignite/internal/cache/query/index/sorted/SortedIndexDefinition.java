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
import org.apache.ignite.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexRowComparator;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;

/**
 * Represents a definition of a sorted index.
 */
public class SortedIndexDefinition implements IndexDefinition {
    /** Schema of an index. */
    private final SortedIndexSchema schema;

    /** Index row comparator. */
    private final IndexRowComparator rowComparator;

    /** Index name. */
    private final IndexName idxName;

    /** Configured inline size. */
    private final int inlineSize;

    /** Segments. */
    private final int segments;

    /** Whether this index is primary key (unique) or not. */
    private final boolean isPrimary;

    /**
     * Whether this index is affinity key index or not.
     */
    private final boolean isAffinity;

    /** Constructor. */
    public SortedIndexDefinition(
        IndexName idxName,
        boolean isPrimary,
        boolean isAffinity,
        SortedIndexSchema schema,
        int segments,
        int inlineSize,
        IndexRowComparator rowComparator) {

        this.idxName = idxName;
        this.segments = segments;
        this.schema = schema;
        this.rowComparator = rowComparator;
        this.inlineSize = inlineSize;
        this.isPrimary = isPrimary;
        this.isAffinity = isAffinity;
    }

    /** {@inheritDoc} */
    @Override public IndexName getIdxName() {
        return idxName;
    }

    /** Represents an index tree name. */
    public String getTreeName() {
        return BPlusTree.treeName(getIdxName().idxName(), "sorted");
    }

    /** Amount of index tree segments.*/
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

    /**
     * For backward compatibility.
     * <p>
     * Prior some Ignite version complex key column was handled as regular object. Currently complex column is splitted
     * on multiple keys (if it possible) and then every key is handled separately. Information how to work with such
     * column is stored in the tree meta page info.
     */
    public void setUseUnwrappedPk(boolean useUnwrappedPk) {
        // No-op.
    }

    /** */
    public boolean isPrimary() {
        return isPrimary;
    }

    /** */
    public boolean isAffinity() {
        return isAffinity;
    }
}

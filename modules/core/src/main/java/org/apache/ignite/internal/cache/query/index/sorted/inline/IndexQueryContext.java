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

package org.apache.ignite.internal.cache.query.index.sorted.inline;

import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;

/** */
public class IndexQueryContext {
    /** Cache filter. */
    private final IndexingQueryFilter cacheFilter;

    /** Index rows filter. */
    private final BPlusTree.TreeRowClosure<IndexRow, IndexRow> rowFilter;

    /** Index row factory. */
    private final BPlusTree.TreeRowFactory<IndexRow, IndexRow> rowFactory;

    /** */
    private final MvccSnapshot mvccSnapshot;

    /** */
    public IndexQueryContext(
        IndexingQueryFilter cacheFilter,
        BPlusTree.TreeRowClosure<IndexRow, IndexRow> rowFilter,
        MvccSnapshot mvccSnapshot
    ) {
        this(cacheFilter, rowFilter, null, mvccSnapshot);
    }

    /** */
    public IndexQueryContext(
        IndexingQueryFilter cacheFilter,
        BPlusTree.TreeRowClosure<IndexRow, IndexRow> rowFilter,
        BPlusTree.TreeRowFactory<IndexRow, IndexRow> rowFactory,
        MvccSnapshot mvccSnapshot
    ) {
        this.cacheFilter = cacheFilter;
        this.rowFilter = rowFilter;
        this.rowFactory = rowFactory;
        this.mvccSnapshot = mvccSnapshot;
    }

    /**
     * @return Mvcc snapshot.
     */
    public MvccSnapshot mvccSnapshot() {
        return mvccSnapshot;
    }

    /**
     * @return Cache filter.
     */
    public IndexingQueryFilter cacheFilter() {
        return cacheFilter;
    }

    /**
     * @return Index row filter.
     */
    public BPlusTree.TreeRowClosure<IndexRow, IndexRow> rowFilter() {
        return rowFilter;
    }

    /**
     * @return Index row factory.
     */
    public BPlusTree.TreeRowFactory<IndexRow, IndexRow> rowFactory() {
        return rowFactory;
    }
}

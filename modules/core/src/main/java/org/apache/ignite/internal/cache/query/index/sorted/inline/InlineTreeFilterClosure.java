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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexSearchRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InlineIO;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;

import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;

// TODO: H2TreeFilterClosure, MVCC

/**
 * Reopresents filter that allow query only primary partitions.
 */
public class InlineTreeFilterClosure implements BPlusTree.TreeRowClosure<IndexSearchRow, IndexSearchRow> {
    /** */
    private final IndexingQueryCacheFilter filter;

    public InlineTreeFilterClosure(IndexingQueryCacheFilter filter) {
        this.filter = filter;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(BPlusTree<IndexSearchRow, IndexSearchRow> tree,
        BPlusIO<IndexSearchRow> io, long pageAddr, int idx) throws IgniteCheckedException {

        InlineIO inlineIO = (InlineIO) io;

        return filter.applyPartition(PageIdUtils.partId(pageId(inlineIO.getLink(pageAddr, idx))));
    }
}

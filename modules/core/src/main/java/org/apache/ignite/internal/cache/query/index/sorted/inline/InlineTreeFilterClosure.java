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
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InlineIO;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;

/**
 * Represents filter that allow query only primary partitions.
 */
public class InlineTreeFilterClosure implements BPlusTree.TreeRowClosure<IndexRow, IndexRow> {
    /** */
    private final IndexingQueryCacheFilter cacheFilter;

    /** */
    private final BPlusTree.TreeRowClosure<IndexRow, IndexRow> rowFilter;

    /** Last index row analyzed by {@link #rowFilter}. */
    private @Nullable IndexRow lastRow;

    /** Constructor. */
    public InlineTreeFilterClosure(IndexingQueryCacheFilter cacheFilter,
        BPlusTree.TreeRowClosure<IndexRow, IndexRow> rowFilter) {
        assert cacheFilter != null || rowFilter != null;

        this.cacheFilter = cacheFilter;
        this.rowFilter = rowFilter;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(BPlusTree<IndexRow, IndexRow> tree, BPlusIO<IndexRow> io,
        long pageAddr, int idx) throws IgniteCheckedException {

        boolean val = cacheFilter == null || applyFilter((InlineIO)io, pageAddr, idx);

        if (val && rowFilter != null) {
            val = rowFilter.apply(tree, io, pageAddr, idx);

            lastRow = rowFilter.lastRow();
        }
        else
            lastRow = null;

        return val;
    }

    /** {@inheritDoc} */
    @Override public @Nullable IndexRow lastRow() {
        return lastRow;
    }

    /**
     * @param io Row IO.
     * @param pageAddr Page address.
     * @param idx Item index.
     * @return {@code True} if row passes the filter.
     */
    private boolean applyFilter(InlineIO io, long pageAddr, int idx) {
        assert cacheFilter != null;

        return cacheFilter.applyPartition(PageIdUtils.partId(pageId(io.link(pageAddr, idx))));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(InlineTreeFilterClosure.class, this);
    }
}

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

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.InlineIndexColumnFactory;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2CacheRow;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;

/**
 * We need indexes on an not affinity nodes. The index shouldn't contains any data.
 */
public class H2TreeClientIndex extends H2TreeIndexBase {
    /** */
    private final int inlineSize;

    /**
     * @param tbl Table.
     * @param name Index name.
     * @param cols Index columns.
     * @param idxType Index type.
     * @param inlineSize Inline size.
     */
    private H2TreeClientIndex(GridH2Table tbl, String name, IndexColumn[] cols, IndexType idxType, int inlineSize) {
        super(tbl, name, cols, idxType);

        this.inlineSize = inlineSize;
    }

    /**
     * @param tbl Table.
     * @param idxName Index name.
     * @param pk Primary key.
     * @param colsList Indexed columns.
     * @param inlineSize Inline size.
     * @param log Logger.
     * @return Index.
     */
    public static H2TreeClientIndex createIndex(
        GridH2Table tbl,
        String idxName,
        boolean pk,
        List<IndexColumn> colsList,
        int inlineSize,
        IgniteLogger log
    ) {
        IndexColumn[] cols = GridH2IndexBase.columnsArray(tbl, colsList);

        IndexType idxType = pk ? IndexType.createPrimaryKey(false, false) :
            IndexType.createNonUnique(false, false, false);

        CacheConfiguration ccfg = tbl.cacheInfo().config();

        List<InlineIndexColumn> inlineCols = getAvailableInlineColumns(false, ccfg.getName(),
            idxName, log, pk, tbl, cols, new InlineIndexColumnFactory(tbl.getCompareMode()), true);

        inlineSize = computeInlineSize(inlineCols, inlineSize, ccfg.getSqlIndexMaxInlineSize());

        return new H2TreeClientIndex(tbl, idxName, cols, idxType, inlineSize);
    }

    /** {@inheritDoc} */
    @Override public int inlineSize() {
        return inlineSize;
    }

    /** {@inheritDoc} */
    @Override public void refreshColumnIds() {
        // Do nothing.
    }

    /** {@inheritDoc} */
    @Override public long totalRowCount(IndexingQueryCacheFilter partsFilter) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public int segmentsCount() {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow lower, SearchRow upper) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public H2CacheRow put(H2CacheRow row) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean putx(H2CacheRow row) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean removex(SearchRow row) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session session, boolean first) {
        throw unsupported();
    }

    /**
     * @return Exception about unsupported operation.
     */
    private static IgniteException unsupported() {
        return new IgniteSQLException("Shouldn't be invoked on non-affinity node.");
    }
}

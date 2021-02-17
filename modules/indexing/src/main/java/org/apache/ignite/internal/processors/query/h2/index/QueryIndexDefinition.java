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

package org.apache.ignite.internal.processors.query.h2.index;

import java.util.List;
import org.apache.ignite.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandlerFactory;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexRowComparator;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.table.IndexColumn;

/**
 * Define H2 query index.
 */
public class QueryIndexDefinition implements SortedIndexDefinition {
    /** Wrapped key definitions. */
    private List<IndexKeyDefinition> keyDefs;

    /** List of unwrapped index columns. */
    List<IndexColumn> h2UnwrappedCols;

    /** List of wrapped index columns. */
    List<IndexColumn> h2WrappedCols;

    /** Cache context. */
    private final GridCacheContext cctx;

    /** H2 table. */
    private final GridH2Table table;

    /** Index name. */
    private final IndexName idxName;

    /** Configured inline size. */
    private final int inlineSize;

    /** Segments. */
    private final int segments;

    /** Whether this index is primary key (unique) or not. */
    private final boolean isPrimary;

    /** Whether this index is affinity key index or not. */
    private final boolean isAffinity;

    /** Index row comparator. */
    private H2RowComparator rowComparator;

    /** Row handler factory. */
    private final QueryRowHandlerFactory rowHndFactory = new QueryRowHandlerFactory();

    /** */
    public QueryIndexDefinition(GridH2Table tbl, String idxName, boolean isPrimary, boolean isAffinity,
        List<IndexColumn> h2UnwrappedCols, List<IndexColumn> h2WrappedCols, int cfgInlineSize) {

        this.idxName = new IndexName(tbl.cacheName(), tbl.getSchema().getName(), tbl.getName(), idxName);
        this.segments = tbl.rowDescriptor().context().config().getQueryParallelism();
        this.inlineSize = cfgInlineSize;
        this.isPrimary = isPrimary;
        this.isAffinity = isAffinity;

        cctx = tbl.cacheContext();

        table = tbl;

        this.h2WrappedCols = h2WrappedCols;
        this.h2UnwrappedCols = h2UnwrappedCols;
    }

    /** {@inheritDoc} */
    @Override public String getTreeName() {
        GridH2RowDescriptor rowDesc = table.rowDescriptor();

        String typeIdStr = "";

        if (rowDesc != null) {
            GridQueryTypeDescriptor typeDesc = rowDesc.type();

            int typeId = cctx.binaryMarshaller() ? typeDesc.typeId() : typeDesc.valueClass().hashCode();

            typeIdStr = typeId + "_";
        }

        // Legacy in treeName from H2Tree.
        return BPlusTree.treeName(typeIdStr + getIdxName().idxName(), "H2Tree");
    }

    /** {@inheritDoc} */
    @Override public List<IndexKeyDefinition> getIndexKeyDefinitions() {
        if (keyDefs == null)
            throw new IllegalStateException("Index key definitions is not initialized yet.");

        return keyDefs;
    }

    /** {@inheritDoc} */
    @Override public IndexRowComparator getRowComparator() {
        if (rowComparator == null)
            throw new IllegalStateException("Index key definitions is not initialized yet.");

        return rowComparator;
    }

    /** {@inheritDoc} */
    @Override public int getSegments() {
        return segments;
    }

    /** {@inheritDoc} */
    @Override public int getInlineSize() {
        return inlineSize;
    }

    /** {@inheritDoc} */
    @Override public boolean isPrimary() {
        return isPrimary;
    }

    /** {@inheritDoc} */
    @Override public boolean isAffinity() {
        return isAffinity;
    }

    /** {@inheritDoc} */
    @Override public InlineIndexRowHandlerFactory getRowHandlerFactory() {
        return rowHndFactory;
    }

    /** {@inheritDoc} */
    @Override public IndexName getIdxName() {
        return idxName;
    }

    /** */
    public GridH2Table getTable() {
        return table;
    }

    /**
     * This method should be invoked from row handler to finally configure definition.
     * In case of multiple segments within signle index it affects only once.
     */
    public void setUpFlags(boolean useUnWrapPK, boolean inlineObjHash) {
        if (keyDefs == null) {
            if (useUnWrapPK)
                keyDefs = new QueryIndexKeyDefinitionProvider(table, h2UnwrappedCols).get();
            else
                keyDefs = new QueryIndexKeyDefinitionProvider(table, h2WrappedCols).get();

            rowComparator = new H2RowComparator(table, inlineObjHash);
        }
    }
}

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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowCache;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowComparator;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandlerFactory;
import org.apache.ignite.internal.cache.query.index.sorted.MetaPageInfo;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.table.IndexColumn;
import org.h2.value.CompareMode;

/**
 * Define H2 query index.
 */
public class QueryIndexDefinition implements SortedIndexDefinition {
    /** Wrapped key definitions. */
    private LinkedHashMap<String, IndexKeyDefinition> keyDefs;

    /** List of unwrapped index columns. */
    private List<IndexColumn> h2UnwrappedCols;

    /** List of wrapped index columns. */
    private List<IndexColumn> h2WrappedCols;

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
    private final H2RowComparator rowComparator;

    /** Index key type settings. */
    private final IndexKeyTypeSettings keyTypeSettings;

    /** Index rows cache. */
    private final IndexRowCache idxRowCache;

    /** Row handler factory. */
    private final QueryRowHandlerFactory rowHndFactory = new QueryRowHandlerFactory();

    /** */
    public QueryIndexDefinition(GridH2Table tbl, String idxName, IndexRowCache idxRowCache,
        boolean isPrimary, boolean isAffinity, List<IndexColumn> h2UnwrappedCols, List<IndexColumn> h2WrappedCols,
        int cfgInlineSize) {

        this.idxName = new IndexName(tbl.cacheName(), tbl.getSchema().getName(), tbl.getName(), idxName);
        this.idxRowCache = idxRowCache;
        this.segments = tbl.rowDescriptor().context().config().getQueryParallelism();
        this.inlineSize = cfgInlineSize;
        this.isPrimary = isPrimary;
        this.isAffinity = isAffinity;

        cctx = tbl.cacheContext();

        table = tbl;

        this.h2WrappedCols = h2WrappedCols;
        this.h2UnwrappedCols = h2UnwrappedCols;

        keyTypeSettings = new IndexKeyTypeSettings()
            .stringOptimizedCompare(CompareMode.OFF.equals(table.getCompareMode().getName()))
            .binaryUnsigned(table.getCompareMode().isBinaryUnsigned());

        rowComparator = new H2RowComparator(table, keyTypeSettings);
    }

    /** {@inheritDoc} */
    @Override public String treeName() {
        GridH2RowDescriptor rowDesc = table.rowDescriptor();

        String typeIdStr = "";

        if (rowDesc != null) {
            GridQueryTypeDescriptor typeDesc = rowDesc.type();

            int typeId = cctx.binaryMarshaller() ? typeDesc.typeId() : typeDesc.valueClass().hashCode();

            typeIdStr = typeId + "_";
        }

        // Legacy in treeName from H2Tree.
        return BPlusTree.treeName(typeIdStr + idxName().idxName(), "H2Tree");
    }

    /** {@inheritDoc} */
    @Override public LinkedHashMap<String, IndexKeyDefinition> indexKeyDefinitions() {
        if (keyDefs == null)
            throw new IllegalStateException("Index key definitions is not initialized yet.");

        return keyDefs;
    }

    /** {@inheritDoc} */
    @Override public IndexRowComparator rowComparator() {
        if (rowComparator == null)
            throw new IllegalStateException("Index key definitions is not initialized yet.");

        return rowComparator;
    }

    /** {@inheritDoc} */
    @Override public int segments() {
        return segments;
    }

    /** {@inheritDoc} */
    @Override public int inlineSize() {
        return inlineSize;
    }

    /** {@inheritDoc} */
    @Override public boolean primary() {
        return isPrimary;
    }

    /** {@inheritDoc} */
    @Override public boolean affinity() {
        return isAffinity;
    }

    /** {@inheritDoc} */
    @Override public void initByMeta(boolean created, MetaPageInfo metaPageInfo) {
        if (keyDefs == null) {
            if (created || metaPageInfo.useUnwrappedPk()) {
                h2WrappedCols = null;
                keyDefs = new QueryIndexKeyDefinitionProvider(table, h2UnwrappedCols).keyDefinitions();
            }
            else {
                h2UnwrappedCols = null;
                keyDefs = new QueryIndexKeyDefinitionProvider(table, h2WrappedCols).keyDefinitions();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public InlineIndexRowHandlerFactory rowHandlerFactory() {
        return rowHndFactory;
    }

    /** {@inheritDoc} */
    @Override public IndexKeyTypeSettings keyTypeSettings() {
        return keyTypeSettings;
    }

    /** {@inheritDoc} */
    @Override public IndexRowCache idxRowCache() {
        return idxRowCache;
    }

    /** {@inheritDoc} */
    @Override public IndexName idxName() {
        return idxName;
    }

    /** */
    public GridH2Table getTable() {
        return table;
    }

    /** */
    public List<IndexColumn> getColumns() {
        return h2UnwrappedCols != null ? h2UnwrappedCols : h2WrappedCols;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        String flds = getColumns().stream().map(c -> c.columnName).collect(Collectors.joining(", "));

        return "QueryIndex[name=" + idxName.idxName() + ", fields=" + flds + "]";
    }
}

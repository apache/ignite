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

import org.apache.ignite.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexSchema;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;

/**
 * Define H2 query index.
 */
public class QueryIndexDefinition extends SortedIndexDefinition {
    /** Array of possible schemas with wrapped or unwrapped PK. Discover final schema when InlineIndexTree is creating. */
    private volatile QueryIndexSchema[] possibleSchemas;

    /** H2 query index schema. */
    private volatile QueryIndexSchema schema;

    /** Cache context. */
    private final GridCacheContext cctx;

    /** H2 table. */
    private final GridH2Table table;

    /** */
    public QueryIndexDefinition(GridH2Table tbl, String idxName, boolean isPrimary, QueryIndexSchema unwrappedSchema,
        QueryIndexSchema wrappedSchema, int cfgInlineSize) {
        super(
            new IndexName(tbl.cacheName(), tbl.getSchema().getName(), tbl.getName(), idxName),
            isPrimary, null, tbl.rowDescriptor().context().config().getQueryParallelism(),
            cfgInlineSize, new H2RowComparator(unwrappedSchema.getTable()));

        cctx = tbl.cacheContext();

        table = tbl;

        possibleSchemas = new QueryIndexSchema[2];
        possibleSchemas[0] = unwrappedSchema;
        possibleSchemas[1] = wrappedSchema;
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
    @Override public SortedIndexSchema getSchema() {
        assert schema != null : "Must not get schema before initializing it with setUseUnwrappedPk.";

        return schema;
    }

    /** {@inheritDoc} */
    @Override public void setUseUnwrappedPk(boolean useUnwrappedPk) {
        synchronized (this) {
            if (schema != null)
                return;

            if (useUnwrappedPk)
                schema = possibleSchemas[0];
            else
                schema = possibleSchemas[1];

            possibleSchemas = null;
        }
    }
}

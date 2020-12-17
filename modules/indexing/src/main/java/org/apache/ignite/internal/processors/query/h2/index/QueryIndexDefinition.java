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
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;

/**
 * Define H2 query index.
 */
public class QueryIndexDefinition extends SortedIndexDefinition {
    /** H2 query index schema. */
    private final QueryIndexSchema schema;

    /** Cache context. */
    private final GridCacheContext cctx;

    /** */
    public QueryIndexDefinition(GridH2Table tbl, String idxName, QueryIndexSchema schema, int cfgInlineSize) {
        super(
            new IndexName(tbl.getSchema().getName(), tbl.getName(), idxName),
            schema,
            tbl.rowDescriptor().context().config().getQueryParallelism(),
            cfgInlineSize,
            new H2RowComparator(schema.getTable()));

        cctx = tbl.cacheContext();
        this.schema = schema;
    }

    /** {@inheritDoc} */
    @Override public String getTreeName() {
        GridQueryTypeDescriptor typeDesc = schema.getTable().rowDescriptor().type();

        int typeId = cctx.binaryMarshaller() ? typeDesc.typeId() : typeDesc.valueClass().hashCode();

        // Legacy in treeName from H2Tree.
        return BPlusTree.treeName((schema.getTable().rowDescriptor() == null ? "" : typeId + "_") +
                getIdxName().idxName(), "H2Tree");
    }
}

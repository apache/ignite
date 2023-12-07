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

package org.apache.ignite.internal.processors.query.h2;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cache.query.index.SortOrder;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeClientIndex;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ProxyIndex;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ProxySpatialIndex;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.schema.management.IndexDescriptor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.table.Column;
import org.h2.table.IndexColumn;

/**
 * Index factory for H2 indexes.
 */
class H2IndexFactory {
    /** */
    private final IgniteLogger log;

    /** */
    H2IndexFactory(GridKernalContext ctx) {
        log = ctx.log(H2IndexFactory.class);
    }

    /**
     * Create H2 index.
     */
    Index createIndex(GridH2Table tbl, IndexDescriptor idxDesc) {
        GridCacheContextInfo<?, ?> cacheInfo = tbl.cacheInfo();

        if (log.isDebugEnabled())
            log.debug("Creating H2 index [cacheId=" + cacheInfo.cacheId() + ", idxName=" + idxDesc.name() + ']');

        // Convert key definitions to list of IndexColumns.
        LinkedHashMap<String, IndexKeyDefinition> keyDefs = idxDesc.keyDefinitions();
        List<IndexColumn> idxCols = new ArrayList<>(keyDefs.size());

        for (Map.Entry<String, IndexKeyDefinition> keyDef : keyDefs.entrySet()) {
            Column col = tbl.getColumn(keyDef.getKey());

            idxCols.add(tbl.indexColumn(col.getColumnId(),
                keyDef.getValue().order().sortOrder() == SortOrder.ASC ? org.h2.result.SortOrder.ASCENDING
                    : org.h2.result.SortOrder.DESCENDING));
        }

        IndexColumn[] idxColsArr = idxCols.toArray(new IndexColumn[idxCols.size()]);

        if (idxDesc.type() == QueryIndexType.SORTED) {
            if (idxDesc.isProxy()) {
                Index targetIdx = tbl.getIndex(idxDesc.targetIdx().name());

                assert targetIdx != null;

                return new GridH2ProxyIndex(tbl, idxDesc.name(), idxCols, targetIdx);
            }

            if (cacheInfo.affinityNode()) {
                InlineIndexImpl qryIdx = idxDesc.index().unwrap(InlineIndexImpl.class);

                return new H2TreeIndex(qryIdx, tbl, idxColsArr, idxDesc.isPk(), log);
            }
            else {
                InlineIndex qryIdx = idxDesc.index().unwrap(InlineIndex.class);

                IndexType idxType = idxDesc.isPk() ? IndexType.createPrimaryKey(false, false) :
                    IndexType.createNonUnique(false, false, false);

                return new H2TreeClientIndex(qryIdx, tbl, idxDesc.name(), idxColsArr, idxType);
            }
        }
        else if (idxDesc.type() == QueryIndexType.GEOSPATIAL) {
            if (idxDesc.isProxy()) {
                Index targetIdx = tbl.getIndex(idxDesc.targetIdx().name());

                assert targetIdx != null;

                return new GridH2ProxySpatialIndex(tbl, idxDesc.name(), idxCols, targetIdx);
            }

            return H2Utils.createSpatialIndex(tbl, idxDesc, idxCols);
        }

        throw new IllegalStateException("Index type: " + idxDesc.type());
    }
}

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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Closure to rebuild some cache indexes.
 */
public class IndexRebuildPartialClosure implements SchemaIndexCacheVisitorClosure {
    /** Indexes. */
    private final Map<GridH2Table, Collection<GridH2IndexBase>> tblIdxs = new IdentityHashMap<>();

    /** {@inheritDoc} */
    @Override public void apply(CacheDataRow row) throws IgniteCheckedException {
        assert hasIndexes();

        for (Map.Entry<GridH2Table, Collection<GridH2IndexBase>> tblIdxEntry : tblIdxs.entrySet()) {
            GridH2Table tbl = tblIdxEntry.getKey();

            GridH2Row row0 = tbl.rowDescriptor().createRow(row);

            for (GridH2IndexBase idx : tblIdxEntry.getValue())
                idx.putx(row0);
        }
    }

    /**
     * @param idx Index to be rebuilt.
     */
    public void addIndex(GridH2Table tbl, GridH2IndexBase idx) {
        Collection<GridH2IndexBase> idxs = tblIdxs.get(tbl);

        if (idxs == null) {
            idxs = Collections.newSetFromMap(new IdentityHashMap<>());

            idxs.add(idx);

            tblIdxs.put(tbl, idxs);
        }

        idxs.add(idx);
    }

    /**
     * @return {@code True} if there is at least one index to rebuild.
     */
    public boolean hasIndexes() {
        return !tblIdxs.isEmpty();
    }
}

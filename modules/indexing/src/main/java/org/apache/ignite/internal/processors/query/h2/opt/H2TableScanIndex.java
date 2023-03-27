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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.HashSet;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.h2.engine.Session;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.TableFilter;
import org.jetbrains.annotations.Nullable;

/**
 * Scan index for {@link GridH2Table}. Delegates to {@link CacheDataTree} when either index rebuild is in progress,
 * or when direct scan over data pages is enabled.
 */
public class H2TableScanIndex extends H2ScanIndex<GridH2IndexBase> {
    /** */
    public static final String SCAN_INDEX_NAME_SUFFIX = "__SCAN_";

    /** Parent table. */
    private final GridH2Table tbl;

    /** */
    private final GridH2IndexBase hashIdx;

    /**
     * Constructor.
     *
     * @param tbl Table.
     * @param treeIdx Tree index.
     * @param hashIdx Hash index.
     */
    H2TableScanIndex(GridH2Table tbl, GridH2IndexBase treeIdx, @Nullable GridH2IndexBase hashIdx) {
        super(treeIdx);

        this.tbl = tbl;
        this.hashIdx = hashIdx;
    }

    /** {@inheritDoc} */
    @Override protected GridH2IndexBase delegate() {
        boolean rebuildFromHashInProgress = tbl.rebuildFromHashInProgress();

        if (hashIdx != null) {
            return rebuildFromHashInProgress || CacheDataTree.isDataPageScanEnabled() ?
                hashIdx : super.delegate();
        }
        else {
            assert !rebuildFromHashInProgress;

            return super.delegate();
        }
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter,
        SortOrder sortOrder, HashSet<Column> allColumnsSet) {
        double baseCost = super.getCost(ses, masks, filters, filter, sortOrder, allColumnsSet);

        int mul = delegate().getDistributedMultiplier(ses, filters, filter);

        return mul * baseCost;
    }

    /** {@inheritDoc} */
    @Override public String getPlanSQL() {
        return delegate().getTable().getSQL() + "." + SCAN_INDEX_NAME_SUFFIX;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return delegate().getName() + SCAN_INDEX_NAME_SUFFIX;
    }
}

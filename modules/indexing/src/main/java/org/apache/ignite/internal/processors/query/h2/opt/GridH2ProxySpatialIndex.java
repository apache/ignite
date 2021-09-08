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
import java.util.List;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.SpatialIndex;
import org.h2.index.SpatialTreeIndex;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;

/**
 * Allows to have 'free' spatial index for alias columns
 * Delegates the calls to underlying normal index
 */
public class GridH2ProxySpatialIndex extends GridH2ProxyIndex implements SpatialIndex {
    /**
     *
     * @param tbl Table.
     * @param name Name of the proxy index.
     * @param colsList Column list for the proxy index.
     * @param idx Target index.
     */
    public GridH2ProxySpatialIndex(GridH2Table tbl,
                                   String name,
                                   List<IndexColumn> colsList,
                                   Index idx) {
        super(tbl, name, colsList, idx);
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter,
        SortOrder sortOrder, HashSet<Column> cols) {
        return SpatialTreeIndex.getCostRangeIndex(masks, columns) / 10;
    }

    /** {@inheritDoc} */
    @Override public Cursor findByGeometry(TableFilter filter, SearchRow first, SearchRow last, SearchRow intersection) {
        GridH2RowDescriptor desc = ((GridH2Table)idx.getTable()).rowDescriptor();

        return ((SpatialIndex)idx).findByGeometry(filter,
                desc.prepareProxyIndexRow(first),
                desc.prepareProxyIndexRow(last),
                desc.prepareProxyIndexRow(intersection));
    }
}

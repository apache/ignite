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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.HashSet;
import org.apache.ignite.internal.GridKernalContext;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * H2 {@link Index} adapter for {@link SortedReducer}.
 */
public final class SortedReduceIndexAdapter extends AbstractReduceIndexAdapter {
    /** */
    private static final IndexType TYPE = IndexType.createNonUnique(false);

    /** */
    private final SortedReducer delegate;

    /**
     * @param ctx Kernal context.
     * @param tbl Table.
     * @param name Index name,
     * @param cols Columns.
     */
    public SortedReduceIndexAdapter(
        GridKernalContext ctx,
        ReduceTable tbl,
        String name,
        IndexColumn[] cols
    ) {
        super(ctx, tbl, name, TYPE, cols);

        delegate = new SortedReducer(ctx, this::compareRows);
    }

    /** {@inheritDoc} */
    @Override protected SortedReducer reducer() {
        return delegate;
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder,
        HashSet<Column> allColumnsSet) {
        return getCostRangeIndex(masks, getRowCountApproximation(), filters, filter, sortOrder, false, allColumnsSet);
    }

    /** {@inheritDoc} */
    @Override public int compareRows(SearchRow rowData, SearchRow compare) {
        if (rowData == compare)
            return 0;

        for (int i = 0, len = indexColumns.length; i < len; i++) {
            int index = columnIds[i];
            int sortType = indexColumns[i].sortType;
            Value v1 = rowData.getValue(index);
            Value v2 = compare.getValue(index);

            if (v1 == null || v2 == null)
                return 0;

            else if (v1 == v2) continue;

            else if (v1 == ValueNull.INSTANCE || v2 == ValueNull.INSTANCE) {
                if ((sortType & SortOrder.NULLS_FIRST) != 0)
                    return v1 == ValueNull.INSTANCE ? -1 : 1;
                else if ((sortType & SortOrder.NULLS_LAST) != 0)
                    return v1 == ValueNull.INSTANCE ? 1 : -1;
            }

            int comp = table.compareTypeSafe(v1, v2);
            if ((sortType & SortOrder.DESCENDING) != 0)
                comp = -comp;

            if (comp != 0)
                return comp;
        }
        return 0;
    }
}

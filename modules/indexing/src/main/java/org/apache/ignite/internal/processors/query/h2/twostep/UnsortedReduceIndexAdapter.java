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
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;

/**
 * H2 {@link Index} adapter for {@link UnsortedReducer}.
 */
public final class UnsortedReduceIndexAdapter extends AbstractReduceIndexAdapter {
    /** Index type. */
    private static final IndexType TYPE = IndexType.createScan(false);

    /** */
    private final UnsortedReducer delegate;

    /**
     * @param ctx Context.
     * @param tbl Table.
     * @param name Index name.
     */
    public UnsortedReduceIndexAdapter(GridKernalContext ctx, ReduceTable tbl, String name) {
        super(ctx, tbl, name, TYPE, IndexColumn.wrap(tbl.getColumns()));

        delegate = new UnsortedReducer(ctx);
    }

    /** {@inheritDoc} */
    @Override protected UnsortedReducer reducer() {
        return delegate;
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder,
        HashSet<Column> allColumnsSet) {
        return getCostRangeIndex(masks, getRowCountApproximation(), filters, filter, sortOrder, true, allColumnsSet);
    }
}

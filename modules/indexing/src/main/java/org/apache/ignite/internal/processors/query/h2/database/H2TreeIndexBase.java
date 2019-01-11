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
 *
 */

package org.apache.ignite.internal.processors.query.h2.database;

import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.h2.command.dml.AllColumnsForPlan;
import org.h2.engine.Session;
import org.h2.index.IndexType;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;

/**
 * H2 tree index base.
 */
public abstract class H2TreeIndexBase extends GridH2IndexBase {
    /**
     * Initialize the base index.
     *
     * @param newTable the table.
     * @param id the object id.
     * @param name the index name.
     * @param newIndexColumns the columns that are indexed or null if this is not yet known.
     * @param newIndexType the index type.
     */
    protected H2TreeIndexBase(Table newTable, int id, String name, IndexColumn[] newIndexColumns,
        IndexType newIndexType) {
        super(newTable, id, name, newIndexColumns, newIndexType);
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter,
        SortOrder sortOrder, AllColumnsForPlan allColsSet) {
        long rowCnt = getRowCountApproximation();

        double baseCost = getCostRangeIndex(masks, rowCnt, filters, filter, sortOrder, false, allColsSet);

        int mul = getDistributedMultiplier(ses, filters, filter);

        return mul * baseCost;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return 10_000; // TODO
    }
}

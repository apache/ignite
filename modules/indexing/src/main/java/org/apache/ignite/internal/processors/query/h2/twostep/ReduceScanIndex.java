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

import org.apache.ignite.internal.processors.query.h2.opt.GridH2ScanIndex;
import org.h2.engine.Session;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.TableFilter;

import java.util.HashSet;

/**
 * Scan index wrapper.
 */
public class ReduceScanIndex extends GridH2ScanIndex<ReduceIndex> {
    /**
     * @param delegate Delegate.
     */
    public ReduceScanIndex(ReduceIndex delegate) {
        super(delegate);
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session session, int[] masks, TableFilter[] filters, int filter,
        SortOrder sortOrder, HashSet<Column> allColumnsSet) {
        long rows = getRowCountApproximation();

        return getCostRangeIndex(masks, rows, filters, filter, sortOrder, true, allColumnsSet);
    }
}

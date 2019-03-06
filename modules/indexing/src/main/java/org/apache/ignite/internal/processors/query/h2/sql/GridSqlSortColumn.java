/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2.sql;

import java.util.List;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;

/**
 * Sort order for ORDER BY clause.
 */
public class GridSqlSortColumn {
    /** */
    private final int col;

    /** */
    private final boolean asc;

    /** */
    private final boolean nullsFirst;

    /** */
    private final boolean nullsLast;

    /**
     * @param col Column index.
     * @param asc Ascending.
     * @param nullsFirst Nulls go first.
     * @param nullsLast Nulls go last.
     */
    public GridSqlSortColumn(int col, boolean asc, boolean nullsFirst, boolean nullsLast) {
        this.col = col;
        this.asc = asc;
        this.nullsFirst = nullsFirst;
        this.nullsLast = nullsLast;
    }

    /**
     * @param tbl Table.
     * @param sortCols Sort columns.
     * @return Index columns.
     */
    public static IndexColumn[] toIndexColumns(Table tbl, List<GridSqlSortColumn> sortCols) {
        assert !F.isEmpty(sortCols);

        IndexColumn[] res = new IndexColumn[sortCols.size()];

        for (int i = 0; i < res.length; i++) {
            GridSqlSortColumn sc = sortCols.get(i);

            Column col = tbl.getColumn(sc.column());

            IndexColumn c = new IndexColumn();

            c.column = col;
            c.columnName = col.getName();

            c.sortType = sc.asc ? SortOrder.ASCENDING : SortOrder.DESCENDING;

            if (sc.nullsFirst)
                c.sortType |= SortOrder.NULLS_FIRST;

            if (sc.nullsLast)
                c.sortType |= SortOrder.NULLS_LAST;

            res[i] = c;
        }

        return res;
    }

    /**
     * @return Column index.
     */
    public int column() {
        return col;
    }

    /**
     * @return {@code true} For ASC order.
     */
    public boolean asc() {
        return asc;
    }

    /**
     * @return {@code true} If {@code null}s must go first.
     */
    public boolean nullsFirst() {
        return nullsFirst;
    }

    /**
     * @return {@code true} If {@code null}s must go last.
     */
    public boolean nullsLast() {
        return nullsLast;
    }
}
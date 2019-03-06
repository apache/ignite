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
import org.h2.util.StatementBuilder;

/** */
public class GridSqlInsert extends GridSqlStatement {
    /** */
    private GridSqlElement into;

    /** */
    private GridSqlColumn[] cols;

    /** */
    private List<GridSqlElement[]> rows;

    /** Insert subquery. */
    private GridSqlQuery qry;

    /**
     * Not supported, introduced for clarity and correct SQL generation.
     * @see org.h2.command.dml.Insert#insertFromSelect
     */
    private boolean direct;

    /**
     * Not supported, introduced for clarity and correct SQL generation.
     * @see org.h2.command.dml.Insert#sortedInsertMode
     */
    private boolean sorted;

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StatementBuilder buff = new StatementBuilder(explain() ? "EXPLAIN " : "");
        buff.append("INSERT")
            .append("\nINTO ")
            .append(into.getSQL())
            .append('(');

        for (GridSqlColumn col : cols) {
            buff.appendExceptFirst(", ");
            buff.append('\n')
                .append(col.getSQL());
        }
        buff.append("\n)\n");

        if (direct)
            buff.append("DIRECT ");

        if (sorted)
            buff.append("SORTED ");

        if (!rows.isEmpty()) {
            buff.append("VALUES\n");
            StatementBuilder valuesBuff = new StatementBuilder();

            for (GridSqlElement[] row : rows()) {
                valuesBuff.appendExceptFirst(",\n");
                StatementBuilder rowBuff = new StatementBuilder("(");
                for (GridSqlElement e : row) {
                    rowBuff.appendExceptFirst(", ");
                    rowBuff.append(e != null ? e.getSQL() : "DEFAULT");
                }
                rowBuff.append(')');
                valuesBuff.append(rowBuff.toString());
            }
            buff.append(valuesBuff.toString());
        }
        else
            buff.append('\n')
                .append(qry.getSQL());

        return buff.toString();
    }

    /** */
    public GridSqlElement into() {
        return into;
    }

    /** */
    public GridSqlInsert into(GridSqlElement from) {
        this.into = from;
        return this;
    }

    /** */
    public List<GridSqlElement[]> rows() {
        return rows;
    }

    /** */
    public GridSqlInsert rows(List<GridSqlElement[]> rows) {
        assert rows != null;
        this.rows = rows;
        return this;
    }

    /** */
    public GridSqlQuery query() {
        return qry;
    }

    /** */
    public GridSqlInsert query(GridSqlQuery qry) {
        this.qry = qry;
        return this;
    }

    /** */
    public GridSqlColumn[] columns() {
        return cols;
    }

    /** */
    public GridSqlInsert columns(GridSqlColumn[] cols) {
        this.cols = cols;
        return this;
    }

    /** */
    public GridSqlInsert direct(boolean direct) {
        this.direct = direct;
        return this;
    }

    /** */
    public GridSqlInsert sorted(boolean sorted) {
        this.sorted = sorted;
        return this;
    }
}

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

package org.apache.ignite.internal.processors.query.h2.sql;

import java.util.List;
import org.h2.util.StatementBuilder;

/** */
public class GridSqlMerge extends GridSqlStatement {
    /** */
    private GridSqlElement into;

    /** */
    private GridSqlColumn[] cols;

    /** */
    private List<GridSqlElement[]> rows;

    /** Insert subquery. */
    private GridSqlQuery qry;

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StatementBuilder buff = new StatementBuilder(explain() ? "EXPLAIN " : "");
        buff.append("MERGE INTO ")
            .append(into.getSQL())
            .append("(");

        for (GridSqlColumn col : cols) {
            buff.appendExceptFirst(", ");
            buff.append('\n')
                .append(col.getSQL());
        }
        buff.append("\n)\n");

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
    public GridSqlMerge into(GridSqlElement from) {
        this.into = from;
        return this;
    }

    /** */
    public List<GridSqlElement[]> rows() {
        return rows;
    }

    /** */
    public GridSqlMerge rows(List<GridSqlElement[]> rows) {
        assert rows != null;
        this.rows = rows;
        return this;
    }

    /** */
    public GridSqlQuery query() {
        return qry;
    }

    /** */
    public GridSqlMerge query(GridSqlQuery qry) {
        this.qry = qry;
        return this;
    }

    /** */
    public GridSqlColumn[] columns() {
        return cols;
    }

    /** */
    public GridSqlMerge columns(GridSqlColumn[] cols) {
        this.cols = cols;
        return this;
    }
}

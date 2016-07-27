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

import java.util.ArrayList;
import java.util.HashMap;
import org.h2.command.Parser;
import org.h2.expression.ValueExpression;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.value.ValueString;

/** */
public class GridSqlUpdate extends GridSqlStatement {
    /** */
    private GridSqlElement target;

    /** */
    private ArrayList<GridSqlColumn> cols;

    /** */
    private HashMap<GridSqlColumn, GridSqlElement> set;

    /** */
    private GridSqlElement where;

    /** */
    public GridSqlUpdate target(GridSqlElement target) {
        this.target = target;
        return this;
    }

    /** */
    public GridSqlElement target() {
        return target;
    }

    /** */
    public GridSqlUpdate cols(ArrayList<GridSqlColumn> cols) {
        this.cols = cols;
        return this;
    }

    /** */
    public GridSqlUpdate set(HashMap<GridSqlColumn, GridSqlElement> set) {
        this.set = set;
        return this;
    }

    /** */
    public GridSqlUpdate where(GridSqlElement where) {
        this.where = where;
        return this;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StatementBuilder buff = new StatementBuilder(explain() ? "EXPLAIN " : "");
        buff.append("UPDATE ")
            .append(target.getSQL())
            .append("\nSET\n");

        for (GridSqlColumn c : cols) {
            GridSqlElement e = set.get(c);
            buff.appendExceptFirst(",\n    ");
            buff.append(c.columnName()).append(" = ").append(e != null ? e.getSQL() : "DEFAULT");
        }

        if (where != null)
            buff.append("\nWHERE ").append(StringUtils.unEnclose(where.getSQL()));

        if (limit != null)
            buff.append("\nLIMIT (").append(StringUtils.unEnclose(limit.getSQL())).append(')');

        return buff.toString();
    }
}

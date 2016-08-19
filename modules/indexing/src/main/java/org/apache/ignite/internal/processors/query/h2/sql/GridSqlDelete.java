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

import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;

/** */
public class GridSqlDelete extends GridSqlStatement {
    /** */
    private GridSqlElement from;

    /** */
    private GridSqlElement where;

    /** */
    public GridSqlDelete from(GridSqlElement from) {
        this.from = from;
        return this;
    }

    /** */
    public GridSqlElement from() {
        return from;
    }

    /** */
    public GridSqlDelete where(GridSqlElement where) {
        this.where = where;
        return this;
    }

    /** */
    public GridSqlElement where() {
        return where;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StatementBuilder buff = new StatementBuilder(explain() ? "EXPLAIN " : "");
        buff.append("DELETE")
            .append("\nFROM ")
            .append(from.getSQL());

        if (where != null)
            buff.append("\nWHERE ").append(StringUtils.unEnclose(where.getSQL()));

        if (limit != null)
            buff.append("\nLIMIT (").append(StringUtils.unEnclose(limit.getSQL())).append(')');

        return buff.toString();
    }
}

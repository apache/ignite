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

import org.h2.table.Column;
import org.h2.table.TableFilter;

/**
 * Column.
 */
public class GridSqlColumnExpression extends GridSqlColumn {
    /** Column resolver. */
    private final  TableFilter colResolver;

    /**
     * @param col Column.
     * @param from From.
     * @param schema Schema name.
     * @param tblAlias Table alias.
     * @param colName Column name.
     * @param colResolver Column resolver.
     */
    public GridSqlColumnExpression(
        Column col,
        GridSqlAst from,
        String schema,
        String tblAlias,
        String colName,
        TableFilter colResolver) {
        super(col, from, schema, tblAlias, colName);

        this.colResolver = colResolver;
    }

    /**
     * @return Column name.
     */
    public TableFilter columnResolver() {
        return colResolver;
    }
}

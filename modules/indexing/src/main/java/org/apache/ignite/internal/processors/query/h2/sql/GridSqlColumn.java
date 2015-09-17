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

import java.util.Collections;

/**
 * Column.
 */
public class GridSqlColumn extends GridSqlElement implements GridSqlValue {
    /** */
    private final GridSqlElement expressionInFrom;

    /** */
    private final String colName;

    /** SQL from original query. May be qualified or unqualified column name. */
    private final String sqlText;

    /**
     * @param from From.
     * @param name Name.
     * @param sqlText Text.
     */
    public GridSqlColumn(GridSqlElement from, String name, String sqlText) {
        super(Collections.<GridSqlElement>emptyList());

        assert sqlText != null;

        expressionInFrom = from;
        colName = name;
        this.sqlText = sqlText;
    }

    /**
     * @return Simple unqualified column with only name.
     */
    public GridSqlColumn simplify() {
        return new GridSqlColumn(null, colName, colName);
    }

    /**
     * @return Column name.
     */
    public String columnName() {
        return colName;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return sqlText;
    }

    /**
     * @return Expression in from.
     */
    public GridSqlElement expressionInFrom() {
        return expressionInFrom;
    }
}
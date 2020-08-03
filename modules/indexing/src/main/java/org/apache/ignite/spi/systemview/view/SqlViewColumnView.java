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

package org.apache.ignite.spi.systemview.view;

import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemView;
import org.h2.table.Column;
import org.h2.value.DataType;

/**
 * Sql view column representation for a {@link SystemView}.
 */
public class SqlViewColumnView {
    /** System view. */
    private final SqlSystemView view;

    /** Column. */
    private final Column col;

    /**
     * @param view View.
     * @param col Column.
     */
    public SqlViewColumnView(SqlSystemView view, Column col) {
        this.view = view;
        this.col = col;
    }

    /** @return Column name. */
    @Order
    public String columnName() {
        return col.getName();
    }

    /** @return Schema name. */
    @Order(2)
    public String schemaName() {
        return QueryUtils.SCHEMA_SYS;
    }

    /** @return View name. */
    @Order(1)
    public String viewName() {
        return view.getTableName();
    }

    /** @return Field data type. */
    public String type() {
        return DataType.getTypeClassName(col.getType());
    }

    /** @return Field default. */
    public String defaultValue() {
        return String.valueOf(col.getDefaultExpression());
    }

    /** @return Precision. */
    public long precision() {
        return col.getPrecision();
    }

    /** @return Scale. */
    public int scale() {
        return col.getScale();
    }

    /** @return {@code True} if nullable field. */
    public boolean nullable() {
        return col.isNullable();
    }
}

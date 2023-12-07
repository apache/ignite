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

package org.apache.ignite.spi.systemview.view.sql;

import java.util.Map;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.spi.systemview.view.SystemView;

/**
 * Sql view column representation for a {@link SystemView}.
 */
public class SqlViewColumnView {
    /** System view. */
    private final SystemView<?> view;

    /** Column. */
    private final Map.Entry<String, Class<?>> col;

    /**
     * @param view View.
     * @param col Column.
     */
    public SqlViewColumnView(SystemView<?> view, Map.Entry<String, Class<?>> col) {
        this.view = view;
        this.col = col;
    }

    /** @return Column name. */
    @Order
    public String columnName() {
        return MetricUtils.toSqlName(col.getKey());
    }

    /** @return Schema name. */
    @Order(2)
    public String schemaName() {
        return QueryUtils.SCHEMA_SYS;
    }

    /** @return View name. */
    @Order(1)
    public String viewName() {
        return MetricUtils.toSqlName(view.name());
    }

    /** @return Field data type. */
    public String type() {
        return col.getValue().getName();
    }

    /** @return Field default. */
    public String defaultValue() {
        return null;
    }

    /** @return Precision. */
    public int precision() {
        return -1;
    }

    /** @return Scale. */
    public int scale() {
        return -1;
    }

    /** @return {@code True} if nullable field. */
    public boolean nullable() {
        return !col.getValue().isPrimitive();
    }
}

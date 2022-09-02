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

import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.schema.management.TableDescriptor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.systemview.view.SystemView;

/**
 * Sql table column representation for a {@link SystemView}.
 */
public class SqlTableColumnView {
    /** Table. */
    private final TableDescriptor tbl;

    /** Query property. */
    private final GridQueryProperty prop;

    /**
     * @param tbl Table.
     * @param prop Column.
     */
    public SqlTableColumnView(TableDescriptor tbl, GridQueryProperty prop) {
        this.tbl = tbl;
        this.prop = prop;
    }

    /** @return Column name. */
    @Order
    public String columnName() {
        return prop.name();
    }

    /** @return Schema name. */
    @Order(2)
    public String schemaName() {
        return tbl.type().schemaName();
    }

    /** @return Table name. */
    @Order(1)
    public String tableName() {
        return tbl.type().tableName();
    }

    /** @return Field data type. */
    public Class<?> type() {
        return prop.type();
    }

    /** @return Field default. */
    public String defaultValue() {
        return prop.defaultValue() == null ? null : prop.defaultValue().toString();
    }

    /** @return Precision. */
    public int precision() {
        return prop.precision();
    }

    /** @return Scale. */
    public int scale() {
        return prop.scale();
    }

    /** @return {@code True} if nullable field. */
    public boolean nullable() {
        return !prop.notNull();
    }

    /** @return {@code True} if primary key. */
    public boolean pk() {
        return F.eq(prop.name(), tbl.type().keyFieldName()) || prop.key();
    }

    /** @return {@code True} if autoincremented field. */
    public boolean autoIncrement() {
        return false;
    }

    /** @return {@code True} if affinity column. */
    public boolean affinityColumn() {
        return !tbl.type().customAffinityKeyMapper() &&
            (F.eq(prop.name(), tbl.type().affinityKey()) || (F.isEmpty(tbl.type().affinityKey()) && pk()));
    }
}

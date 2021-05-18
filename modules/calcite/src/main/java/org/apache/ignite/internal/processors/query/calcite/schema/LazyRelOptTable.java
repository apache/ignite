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

package org.apache.ignite.internal.processors.query.calcite.schema;

import java.util.function.Supplier;
import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Wrapper;

/**
 * Lazy {@link RelOptTable}, which can supply {@link IgniteTable} or {@link TableDescriptor} via {@link #unwrap(Class)}
 * method when this table becomes available. It can be used, for example, in CREATE TABLE AS SELECT statement, where
 * the query should be planned to insert data into the table (plan requires {@link RelOptTable} instance), but
 * {@link IgniteTable} instance is not exists at the moment of planning and will be created only after execution of the
 * first part of the query.
 */
public class LazyRelOptTable extends RelOptAbstractTable {
    /** Table supplier. */
    private final Supplier<Table> tableSupplier;

    /** Table. */
    private volatile Table table;

    /** */
    public LazyRelOptTable(Supplier<Table> tableSupplier, RelDataType rowType, String tableName) {
        super(null, tableName, rowType);

        this.tableSupplier = tableSupplier;
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        if (table == null)
            table = tableSupplier.get();

        if (clazz.isInstance(this))
            return clazz.cast(this);

        if (clazz.isInstance(table))
            return clazz.cast(table);

        if (table instanceof Wrapper) {
            final T t = ((Wrapper) table).unwrap(clazz);

            if (t != null)
                return t;
        }

        return super.unwrap(clazz);
    }
}

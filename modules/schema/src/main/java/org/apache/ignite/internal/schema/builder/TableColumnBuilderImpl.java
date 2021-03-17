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

package org.apache.ignite.internal.schema.builder;

import java.util.Map;
import org.apache.ignite.internal.schema.ColumnImpl;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.builder.TableColumnBuilder;

/**
 * Column builder.
 */
public class TableColumnBuilderImpl implements TableColumnBuilder {
    /** Column name. */
    private final String colName;

    /** Column type. */
    private final ColumnType colType;

    /** Nullable flag. */
    private boolean nullable;

    /** Default value. */
    private Object defValue;

    /**
     * Constructor.
     * @param colName Column name.
     * @param colType Column type.
     */
    public TableColumnBuilderImpl(String colName, ColumnType colType) {
        this.colName = colName;
        this.colType = colType;
    }

    /** {@inheritDoc} */
    @Override public TableColumnBuilderImpl asNullable() {
        nullable = true;

        return this;
    }

    /** {@inheritDoc} */
    @Override public TableColumnBuilderImpl asNonNull() {
        nullable = false;

        return this;
    }

    /** {@inheritDoc} */
    @Override public TableColumnBuilderImpl withDefaultValue(Object defValue) {
        this.defValue = defValue;

        return this;
    }

    /** {@inheritDoc} */
    @Override public TableColumnBuilderImpl withHints(Map<String, String> hints) {
        // No op.

        return this;
    }

    /** {@inheritDoc} */
    @Override public Column build() {
        return new ColumnImpl(colName, colType, nullable, defValue);
    }
}

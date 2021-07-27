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

package org.apache.ignite.internal.table;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.Objects;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjects;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleBuilder;
import org.jetbrains.annotations.NotNull;

/**
 * Buildable tuple.
 */
public class TupleBuilderImpl implements TupleBuilder, Tuple, SchemaAware {
    /** Columns values. */
    protected Map<String, Object> map;

    /** Current schema descriptor. */
    private SchemaDescriptor schemaDesc;

    /**
     * Creates tuple builder.
     *
     * @param schemaDesc Schema descriptor.
     */
    public TupleBuilderImpl(SchemaDescriptor schemaDesc) {
        Objects.requireNonNull(schemaDesc);

        this.schemaDesc = schemaDesc;
        map = new HashMap<>(schemaDesc.length());
    }

    /** {@inheritDoc} */
    @Override public TupleBuilder set(String columnName, Object val) {
        getColumnOrThrow(columnName).validate(val);

        map.put(columnName, val);

        return this;
    }

    /**
     * Sets column value.
     *
     * @param col Column.
     * @param value Value to set.
     * @return {@code this} for chaining.
     */
    public TupleBuilder set(Column col, Object value) {
        assert col != null;

        col.validate(value);

        map.put(col.name(), value);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Tuple build() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public String columnName(int columnIndex) {
        return schemaDesc.column(columnIndex).name();
    }

    /** {@inheritDoc} */
    @Override public Integer columnIndex(String columnName) {
        var col = schemaDesc.column(columnName);

        return col == null ? null : col.schemaIndex();
    }

    /** {@inheritDoc} */
    @Override public int columnCount() {
        return schemaDesc.length();
    }

    /** {@inheritDoc} */
    @Override public <T> T valueOrDefault(String columnName, T def) {
        return (T)map.getOrDefault(columnName, def);
    }

    /** {@inheritDoc} */
    @Override public <T> T value(String columnName) {
        getColumnOrThrow(columnName);

        return (T)map.get(columnName);
    }

    /** {@inheritDoc} */
    @Override public <T> T value(int columnIndex) {
        Column col = schemaDesc.column(columnIndex);

        return (T)map.get(col.name());
    }

    /** {@inheritDoc} */
    @Override public BinaryObject binaryObjectValue(String columnName) {
        byte[] data = value(columnName);

        return BinaryObjects.wrap(data);
    }

    /** {@inheritDoc} */
    @Override public BinaryObject binaryObjectValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public short shortValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public short shortValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public int intValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public int intValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public long longValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public long longValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public float floatValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public float floatValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public double doubleValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public double doubleValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public String stringValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public String stringValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public UUID uuidValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public UUID uuidValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public BitSet bitmaskValue(String columnName) {
        return value(columnName);
    }

    /** {@inheritDoc} */
    @Override public BitSet bitmaskValue(int columnIndex) {
        return value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public SchemaDescriptor schema() {
        return schemaDesc;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<Object> iterator() {
        return new Iterator<>() {
            /** Current column index. */
            private int cur;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return cur < schemaDesc.length();
            }

            /** {@inheritDoc} */
            @Override public Object next() {
                return hasNext() ? value(cur++) : null;
            }
        };
    }

    /**
     * @param schemaDesc New current schema descriptor.
     */
    protected void schema(SchemaDescriptor schemaDesc) {
        this.schemaDesc = schemaDesc;
    }

    /**
     * Gets column by name or throws an exception when not found.
     *
     * @param columnName Column name.
     * @return Column.
     * @throws ColumnNotFoundException when not found.
     */
    @NotNull private Column getColumnOrThrow(String columnName) {
        Column col = schema().column(columnName);

        if (col == null)
            throw new ColumnNotFoundException("Column not found [col=" + columnName + "schema=" + schemaDesc + ']');

        return col;
    }
}

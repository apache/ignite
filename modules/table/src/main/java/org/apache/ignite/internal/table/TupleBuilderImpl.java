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
    @Override public TupleBuilder set(String colName, Object val) {
        Column col = schema().column(colName);

        if (col == null)
            throw new ColumnNotFoundException("Column not found [col=" + colName + "schema=" + schemaDesc + ']');

        col.validate(val);

        map.put(colName, val);

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
    @Override public <T> T valueOrDefault(String colName, T def) {
        return (T)map.getOrDefault(colName, def);
    }

    /** {@inheritDoc} */
    @Override public <T> T value(String colName) {
        return (T)map.get(colName);
    }

    /** {@inheritDoc} */
    @Override public BinaryObject binaryObjectField(String colName) {
        byte[] data = value(colName);

        return BinaryObjects.wrap(data);
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public short shortValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public int intValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public long longValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public float floatValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public double doubleValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public String stringValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public UUID uuidValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public BitSet bitmaskValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public SchemaDescriptor schema() {
        return schemaDesc;
    }

    /**
     * @param schemaDesc New current schema descriptor.
     */
    protected void schema(SchemaDescriptor schemaDesc) {
        this.schemaDesc = schemaDesc;
    }
}

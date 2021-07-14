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
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleBuilder;

/**
 * Buildable tuple.
 */
public class TupleBuilderImpl implements TupleBuilder, Tuple {
    /** Columns values. */
    protected Map<String, Object> map;

    /** Current schema descriptor. */
    private SchemaDescriptor schemaDesc;

    /**
     * Constructor.
     */
    public TupleBuilderImpl(SchemaDescriptor schemaDesc) {
        Objects.requireNonNull(schemaDesc);

        this.schemaDesc = schemaDesc;
        map = new HashMap<>();
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

    /** {@inheritDoc} */
    @Override public Tuple build() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(String colName) {
        return map.containsKey(colName);
    }

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

    /**
     * Get schema descriptor.
     *
     * @return Schema descriptor.
     */
    public SchemaDescriptor schema() {
        return schemaDesc;
    }

    /**
     * @param schemaDesc New current schema descriptor.
     */
    protected void schema(SchemaDescriptor schemaDesc) {
        this.schemaDesc = schemaDesc;
    }
}

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

package org.apache.ignite.internal.schema.registry;

import java.math.BigDecimal;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaException;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.row.Row;

/**
 * Adapter for row of older schema.
 */
class UpgradingRowAdapter extends Row {
    /** Column mapper. */
    private final ColumnMapper mapper;

    /** Adapter schema. */
    private final SchemaDescriptor schema;

    /**
     * @param schema Row adapter schema descriptor.
     * @param rowSchema Row schema descriptor.
     * @param row Row.
     * @param mapper Column mapper.
     */
    UpgradingRowAdapter(SchemaDescriptor schema, SchemaDescriptor rowSchema, BinaryRow row, ColumnMapper mapper) {
        super(rowSchema, row);

        this.schema = schema;
        this.mapper = mapper;
    }

    /** {@inheritDoc} */
    @Override public SchemaDescriptor rowSchema() {
        return schema;
    }

    /** {@inheritDoc} */
    @Override public int schemaVersion() {
        return schema.version();
    }

    /**
     * Map column.
     *
     * @param colIdx Column index in source schema.
     * @return Column index in targer schema.
     */
    private int mapColumn(int colIdx) throws InvalidTypeException {
        return mapper.map(colIdx);
    }

    /**
     * Reads value for specified column.
     *
     * @param colIdx Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    @Override public byte byteValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : super.rowSchema().column(mappedId);

        if (NativeTypeSpec.INT8 != column.type().spec())
            throw new SchemaException("Type conversion is not supported yet.");

        return mappedId < 0 ? (byte)column.defaultValue() : super.byteValue(mappedId);
    }

    /**
     * Reads value for specified column.
     *
     * @param colIdx Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    @Override public Byte byteValueBoxed(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : super.rowSchema().column(mappedId);

        if (NativeTypeSpec.INT8 != column.type().spec())
            throw new SchemaException("Type conversion is not supported yet.");

        return mappedId < 0 ? (Byte)column.defaultValue() : super.byteValueBoxed(mappedId);
    }

    /**
     * Reads value for specified column.
     *
     * @param colIdx Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    @Override public short shortValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : super.rowSchema().column(mappedId);

        if (NativeTypeSpec.INT16 != column.type().spec())
            throw new SchemaException("Type conversion is not supported yet.");

        return mappedId < 0 ? (short)column.defaultValue() : super.shortValue(mappedId);
    }

    /**
     * Reads value for specified column.
     *
     * @param colIdx Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    @Override public Short shortValueBoxed(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : super.rowSchema().column(mappedId);

        if (NativeTypeSpec.INT16 != column.type().spec())
            throw new SchemaException("Type conversion is not supported yet.");

        return mappedId < 0 ? (Short)column.defaultValue() : super.shortValueBoxed(mappedId);
    }

    /**
     * Reads value for specified column.
     *
     * @param colIdx Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    @Override public int intValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : super.rowSchema().column(mappedId);

        if (NativeTypeSpec.INT32 != column.type().spec())
            throw new SchemaException("Type conversion is not supported yet.");

        return mappedId < 0 ? (int)column.defaultValue() : super.intValue(mappedId);
    }

    /**
     * Reads value for specified column.
     *
     * @param colIdx Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    @Override public Integer intValueBoxed(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : super.rowSchema().column(mappedId);

        if (NativeTypeSpec.INT32 != column.type().spec())
            throw new SchemaException("Type conversion is not supported yet.");

        return mappedId < 0 ? (Integer)column.defaultValue() : super.intValueBoxed(mappedId);
    }

    /**
     * Reads value for specified column.
     *
     * @param colIdx Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    @Override public long longValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : super.rowSchema().column(mappedId);

        if (NativeTypeSpec.INT64 != column.type().spec())
            throw new SchemaException("Type conversion is not supported yet.");

        return mappedId < 0 ? (long)column.defaultValue() : super.longValue(mappedId);
    }

    /**
     * Reads value for specified column.
     *
     * @param colIdx Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    @Override public Long longValueBoxed(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : super.rowSchema().column(mappedId);

        if (NativeTypeSpec.INT64 != column.type().spec())
            throw new SchemaException("Type conversion is not supported yet.");

        return mappedId < 0 ? (Long)column.defaultValue() : super.longValueBoxed(mappedId);
    }

    /**
     * Reads value for specified column.
     *
     * @param colIdx Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    @Override public float floatValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : super.rowSchema().column(mappedId);

        if (NativeTypeSpec.FLOAT != column.type().spec())
            throw new SchemaException("Type conversion is not supported yet.");

        return mappedId < 0 ? (float)column.defaultValue() : super.floatValue(mappedId);
    }

    /**
     * Reads value for specified column.
     *
     * @param colIdx Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    @Override public Float floatValueBoxed(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : super.rowSchema().column(mappedId);

        if (NativeTypeSpec.FLOAT != column.type().spec())
            throw new SchemaException("Type conversion is not supported yet.");

        return mappedId < 0 ? (Float)column.defaultValue() : super.floatValueBoxed(mappedId);
    }

    /**
     * Reads value for specified column.
     *
     * @param colIdx Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    @Override public double doubleValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : super.rowSchema().column(mappedId);

        if (NativeTypeSpec.DOUBLE != column.type().spec())
            throw new SchemaException("Type conversion is not supported yet.");

        return mappedId < 0 ? (double)column.defaultValue() : super.doubleValue(mappedId);
    }

    /**
     * Reads value for specified column.
     *
     * @param colIdx Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    @Override public Double doubleValueBoxed(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : super.rowSchema().column(mappedId);

        if (NativeTypeSpec.DOUBLE != column.type().spec())
            throw new SchemaException("Type conversion is not supported yet.");

        return mappedId < 0 ? (Double)column.defaultValue() : super.doubleValueBoxed(mappedId);
    }

    /**
     * Reads value from specified column.
     *
     * @param colIdx Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    @Override public BigDecimal decimalValue(int colIdx) throws InvalidTypeException {
        // TODO: IGNITE-13668 decimal support
        return null;
    }

    /**
     * Reads value for specified column.
     *
     * @param colIdx Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    @Override public String stringValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : super.rowSchema().column(mappedId);

        if (NativeTypeSpec.STRING != column.type().spec())
            throw new SchemaException("Type conversion is not supported yet.");

        return mappedId < 0 ? (String)column.defaultValue() : super.stringValue(mappedId);
    }

    /**
     * Reads value for specified column.
     *
     * @param colIdx Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    @Override public byte[] bytesValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : super.rowSchema().column(mappedId);

        if (NativeTypeSpec.STRING != column.type().spec())
            throw new SchemaException("Type conversion is not supported yet.");

        return mappedId < 0 ? (byte[])column.defaultValue() : super.bytesValue(mappedId);
    }

    /**
     * Reads value for specified column.
     *
     * @param colIdx Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    @Override public UUID uuidValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : super.rowSchema().column(mappedId);

        if (NativeTypeSpec.UUID != column.type().spec())
            throw new SchemaException("Type conversion is not supported yet.");

        return mappedId < 0 ? (UUID)column.defaultValue() : super.uuidValue(mappedId);
    }

    /**
     * Reads value for specified column.
     *
     * @param colIdx Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    @Override public BitSet bitmaskValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : super.rowSchema().column(mappedId);

        if (NativeTypeSpec.BITMASK != column.type().spec())
            throw new SchemaException("Type conversion is not supported yet.");

        return mappedId < 0 ? (BitSet)column.defaultValue() : super.bitmaskValue(mappedId);
    }
}

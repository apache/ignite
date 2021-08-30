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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjects;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract row tuple adapter.
 */
public abstract class AbstractRowTupleAdapter implements Tuple, SchemaAware {
    /** Underlying row. */
    protected Row row;

    /**
     * Creates Tuple adapter for row.
     *
     * @param row Row.
     */
    public AbstractRowTupleAdapter(@NotNull Row row) {
        this.row = row;
    }

    /** {@inheritDoc} */
    @Override @Nullable public SchemaDescriptor schema() {
        return row == null ? null : row.schema();
    }

    /** {@inheritDoc} */
    @Override public int columnCount() {
        return row.schema().length();
    }

    /** {@inheritDoc} */
    @Override public String columnName(int columnIndex) {
        return rowColumnByIndex(columnIndex).name();
    }

    /** {@inheritDoc} */
    @Override public int columnIndex(@NotNull String columnName) {
        Objects.requireNonNull(columnName);

        var col = row.schema().column(columnName);

        return col == null ? -1 : col.schemaIndex();
    }

    /** {@inheritDoc} */
    @Override public <T> T valueOrDefault(@NotNull String columnName, T defaultValue) {
        Objects.requireNonNull(columnName);

        final Column col = row.schema().column(columnName);

        return col == null ? defaultValue : (T) col.type().spec().objectValue(row, col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public <T> T value(@NotNull String columnName) {
        final Column col = rowColumnByName(columnName);

        return (T) col.type().spec().objectValue(row, col.schemaIndex());
    }

    @Override public <T> T value(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return (T) col.type().spec().objectValue(row, col.schemaIndex());
    }


    /** {@inheritDoc} */
    @Override public BinaryObject binaryObjectValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return BinaryObjects.wrap(row.bytesValue(col.schemaIndex()));
    }

    /** {@inheritDoc} */
    @Override public BinaryObject binaryObjectValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return BinaryObjects.wrap(row.bytesValue(col.schemaIndex()));
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.byteValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.byteValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public short shortValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.shortValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public short shortValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.shortValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public int intValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.intValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public int intValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.intValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public long longValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.longValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public long longValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.longValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public float floatValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.floatValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public float floatValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.floatValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public double doubleValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.doubleValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public double doubleValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.doubleValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public String stringValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.stringValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public String stringValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.stringValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public UUID uuidValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.uuidValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public UUID uuidValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.uuidValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public BitSet bitmaskValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.bitmaskValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public BitSet bitmaskValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.bitmaskValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public LocalDate dateValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.dateValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public LocalDate dateValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.dateValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public LocalTime timeValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.timeValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public LocalTime timeValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.timeValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public LocalDateTime datetimeValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.dateTimeValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public LocalDateTime datetimeValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.dateTimeValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public Instant timestampValue(String columnName) {
        Column col = rowColumnByName(columnName);

        return row.timestampValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public Instant timestampValue(int columnIndex) {
        Column col = rowColumnByIndex(columnIndex);

        return row.timestampValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<Object> iterator() {
        return new Iterator<>() {
            /** Current column index. */
            private int cur;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return cur < row.schema().length();
            }

            /** {@inheritDoc} */
            @Override public Object next() {
                return hasNext() ? value(cur++) : null;
            }
        };
    }

    /**
     * Returns row column for name.
     *
     * @param columnName Column name.
     * @return Column.
     */
    protected Column rowColumnByName(@NotNull String columnName) {
        Objects.requireNonNull(columnName);

        final Column col = row.schema().column(columnName);

        if (col == null)
            throw new IllegalArgumentException("Invalid column name: columnName=" + columnName);

        return col;
    }

    /**
     * Returns row column for index.
     * @param columnIndex Column index.
     * @return Column.
     */
    protected Column rowColumnByIndex(int columnIndex) {
        Objects.checkIndex(columnIndex, row.schema().length());

        return row.schema().column(columnIndex);
    }

}

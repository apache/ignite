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
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleImpl;
import org.jetbrains.annotations.NotNull;

/**
 * Mutable tuple adapter for a row.
 */
public class MutableRowTupleAdapter extends AbstractRowTupleAdapter {
    /** Tuple with overwritten data. */
    protected TupleImpl tuple;

    /**
     * Creates mutable wrapper over row.
     *
     * @param row Row.
     */
    public MutableRowTupleAdapter(@NotNull Row row) {
        super(row);
    }

    /** {@inheritDoc} */
    @Override public int columnCount() {
        return tuple != null ? tuple.columnCount() : super.columnCount();
    }

    /** {@inheritDoc} */
    @Override public String columnName(int columnIndex) {
        return tuple != null ? tuple.columnName(columnIndex) : super.columnName(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public int columnIndex(@NotNull String columnName) {
        return tuple != null ? tuple.columnIndex(columnName) : super.columnIndex(columnName);
    }

    /** {@inheritDoc} */
    @Override public <T> T valueOrDefault(@NotNull String columnName, T defaultValue) {
        return tuple != null ? tuple.valueOrDefault(columnName, defaultValue) : super.valueOrDefault(columnName, defaultValue);
    }

    /** {@inheritDoc} */
    @Override public <T> T value(@NotNull String columnName) {
        return tuple != null ? tuple.value(columnName) : super.value(columnName);
    }

    /** {@inheritDoc} */
    @Override public <T> T value(int columnIndex) {
        return tuple != null ? tuple.value(columnIndex) : super.value(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public BinaryObject binaryObjectValue(String columnName) {
        return tuple != null ? tuple.binaryObjectValue(columnName) : super.binaryObjectValue(columnName);
    }

    /** {@inheritDoc} */
    @Override public BinaryObject binaryObjectValue(int columnIndex) {
        return super.binaryObjectValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(String columnName) {
        return tuple != null ? tuple.byteValue(columnName) : super.byteValue(columnName);
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(int columnIndex) {
        return tuple != null ? tuple.byteValue(columnIndex) : super.byteValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public short shortValue(String columnName) {
        return tuple != null ? tuple.shortValue(columnName) : super.shortValue(columnName);
    }

    /** {@inheritDoc} */
    @Override public short shortValue(int columnIndex) {
        return tuple != null ? tuple.shortValue(columnIndex) : super.shortValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public int intValue(String columnName) {
        return tuple != null ? tuple.intValue(columnName) : super.intValue(columnName);
    }

    /** {@inheritDoc} */
    @Override public int intValue(int columnIndex) {
        return tuple != null ? tuple.intValue(columnIndex) : super.intValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public long longValue(String columnName) {
        return tuple != null ? tuple.longValue(columnName) : super.longValue(columnName);
    }

    /** {@inheritDoc} */
    @Override public long longValue(int columnIndex) {
        return tuple != null ? tuple.longValue(columnIndex) : super.longValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public float floatValue(String columnName) {
        return tuple != null ? tuple.floatValue(columnName) : super.floatValue(columnName);
    }

    /** {@inheritDoc} */
    @Override public float floatValue(int columnIndex) {
        return tuple != null ? tuple.floatValue(columnIndex) : super.floatValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public double doubleValue(String columnName) {
        return tuple != null ? tuple.doubleValue(columnName) : super.doubleValue(columnName);
    }

    /** {@inheritDoc} */
    @Override public double doubleValue(int columnIndex) {
        return tuple != null ? tuple.doubleValue(columnIndex) : super.doubleValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public String stringValue(String columnName) {
        return tuple != null ? tuple.stringValue(columnName) : super.stringValue(columnName);
    }

    /** {@inheritDoc} */
    @Override public String stringValue(int columnIndex) {
        return tuple != null ? tuple.stringValue(columnIndex) : super.stringValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public UUID uuidValue(String columnName) {
        return tuple != null ? tuple.uuidValue(columnName) : super.uuidValue(columnName);
    }

    /** {@inheritDoc} */
    @Override public UUID uuidValue(int columnIndex) {
        return tuple != null ? tuple.uuidValue(columnIndex) : super.uuidValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public BitSet bitmaskValue(String columnName) {
        return tuple != null ? tuple.bitmaskValue(columnName) : super.bitmaskValue(columnName);
    }

    /** {@inheritDoc} */
    @Override public BitSet bitmaskValue(int columnIndex) {
        return tuple != null ? tuple.bitmaskValue(columnIndex) : super.bitmaskValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public LocalDate dateValue(String columnName) {
        return tuple != null ? tuple.dateValue(columnName) : super.dateValue(columnName);
    }

    /** {@inheritDoc} */
    @Override public LocalDate dateValue(int columnIndex) {
        return tuple != null ? tuple.dateValue(columnIndex) : super.dateValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public LocalTime timeValue(String columnName) {
        return tuple != null ? tuple.timeValue(columnName) : super.timeValue(columnName);
    }

    /** {@inheritDoc} */
    @Override public LocalTime timeValue(int columnIndex) {
        return tuple != null ? tuple.timeValue(columnIndex) : super.timeValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public LocalDateTime datetimeValue(String columnName) {
        return tuple != null ? tuple.datetimeValue(columnName) : super.datetimeValue(columnName);
    }

    /** {@inheritDoc} */
    @Override public LocalDateTime datetimeValue(int columnIndex) {
        return tuple != null ? tuple.datetimeValue(columnIndex) : super.datetimeValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public Instant timestampValue(String columnName) {
        return tuple != null ? tuple.timestampValue(columnName) : super.timestampValue(columnName);
    }

    /** {@inheritDoc} */
    @Override public Instant timestampValue(int columnIndex) {
        return tuple != null ? tuple.timestampValue(columnIndex) : super.timestampValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public @NotNull Iterator<Object> iterator() {
        return tuple != null ? tuple.iterator() : super.iterator();
    }

    /** {@inheritDoc} */
    @Override public Tuple set(@NotNull String columnName, Object value) {
        if (tuple == null) {
            TupleImpl tuple0 = new TupleImpl(this);

            tuple = tuple0;
            row = null;
        }

        tuple.set(columnName, value);

        return this;
    }
}

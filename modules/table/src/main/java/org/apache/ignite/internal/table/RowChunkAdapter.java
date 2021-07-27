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
import java.util.Iterator;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjects;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;

/**
 * Row to RowChunk adapter.
 */
public abstract class RowChunkAdapter implements Tuple, SchemaAware {
    /**
     * @param colName Column name.
     * @return Column.
     * @throws ColumnNotFoundException If column wasn't found.
     */
    @NotNull protected abstract Column columnByName(@NotNull String colName);

    /**
     * @return Underlying row.
     */
    protected abstract Row row();

    /** {@inheritDoc} */
    @Override public int columnCount() {
        return schema().length();
    }

    /** {@inheritDoc} */
    @Override public String columnName(int columnIndex) {
        return schema().column(columnIndex).name();
    }

    /** {@inheritDoc} */
    @Override public Integer columnIndex(String columnName) {
        var col = schema().column(columnName);

        return col == null ? null : col.schemaIndex();
    }

    /** {@inheritDoc} */
    @Override public <T> T valueOrDefault(String columnName, T def) {
        try {
            return value(columnName);
        }
        catch (ColumnNotFoundException ex) {
            return def;
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T value(String columnName) {
        final Column col = columnByName(columnName);

        return (T)col.type().spec().objectValue(row(), col.schemaIndex());
    }

    @Override public <T> T value(int columnIndex) {
        final Column col = schema().column(columnIndex);

        return (T)col.type().spec().objectValue(row(), col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public BinaryObject binaryObjectValue(String columnName) {
        Column col = columnByName(columnName);

        return BinaryObjects.wrap(row().bytesValue(col.schemaIndex()));
    }

    /** {@inheritDoc} */
    @Override public BinaryObject binaryObjectValue(int columnIndex) {
        schema().validateColumnIndex(columnIndex);

        return BinaryObjects.wrap(row().bytesValue(columnIndex));
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(String columnName) {
        Column col = columnByName(columnName);

        return row().byteValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(int columnIndex) {
        schema().validateColumnIndex(columnIndex);

        return row().byteValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public short shortValue(String columnName) {
        Column col = columnByName(columnName);

        return row().shortValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public short shortValue(int columnIndex) {
        schema().validateColumnIndex(columnIndex);

        return row().shortValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public int intValue(String columnName) {
        Column col = columnByName(columnName);

        return row().intValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public int intValue(int columnIndex) {
        schema().validateColumnIndex(columnIndex);

        return row().intValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public long longValue(String columnName) {
        Column col = columnByName(columnName);

        return row().longValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public long longValue(int columnIndex) {
        schema().validateColumnIndex(columnIndex);

        return row().longValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public float floatValue(String columnName) {
        Column col = columnByName(columnName);

        return row().floatValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public float floatValue(int columnIndex) {
        schema().validateColumnIndex(columnIndex);

        return row().floatValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public double doubleValue(String columnName) {
        Column col = columnByName(columnName);

        return row().doubleValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public double doubleValue(int columnIndex) {
        schema().validateColumnIndex(columnIndex);

        return row().doubleValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public String stringValue(String columnName) {
        Column col = columnByName(columnName);

        return row().stringValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public String stringValue(int columnIndex) {
        schema().validateColumnIndex(columnIndex);

        return row().stringValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public UUID uuidValue(String columnName) {
        Column col = columnByName(columnName);

        return row().uuidValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public UUID uuidValue(int columnIndex) {
        schema().validateColumnIndex(columnIndex);

        return row().uuidValue(columnIndex);
    }

    /** {@inheritDoc} */
    @Override public BitSet bitmaskValue(String columnName) {
        Column col = columnByName(columnName);

        return row().bitmaskValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public BitSet bitmaskValue(int columnIndex) {
        schema().validateColumnIndex(columnIndex);

        return row().bitmaskValue(columnIndex);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<Object> iterator() {
        return new Iterator<>() {
            /** Current column index. */
            private int cur;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return cur < schema().length();
            }

            /** {@inheritDoc} */
            @Override public Object next() {
                return hasNext() ? value(cur++) : null;
            }
        };
    }
}

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

import java.util.Objects;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Row to Tuple adapter.
 * <p>
 * Provides methods to access columns values by column names.
 */
public class TableRow extends MutableRowTupleAdapter {
    /**
     * Returns tuple for row key chunk.
     *
     * @param row Row.
     * @return Tuple.
     */
    public static Tuple keyTuple(Row row) {
        return new KeyRowChunk(row);
    }

    /**
     * Returns tuple for row value chunk.
     *
     * @param row Row.
     * @return Tuple.
     */
    public static @Nullable Tuple valueTuple(@Nullable Row row) {
        return row != null && row.hasValue() ? new ValueRowChunk(row) : null;
    }

    /**
     * Returns tuple for whole row.
     *
     * @param row Row.
     * @return Tuple.
     */
    public static @Nullable Tuple tuple(Row row) {
        return new TableRow(row);
    }

    /**
     * Constructor.
     *
     * @param row Row.
     */
    private TableRow(Row row) {
        super(row);
    }

    /**
     * Key column chunk.
     */
    private static class KeyRowChunk extends MutableRowTupleAdapter {
        /**
         * Creates tuple for key chunk.
         *
         * @param row Row
         */
        KeyRowChunk(@NotNull Row row) {
            super(row);
        }

        /** {@inheritDoc} */
        @Override public int columnCount() {
            return tuple != null ? tuple.columnCount() : schema().keyColumns().length();
        }

        /** {@inheritDoc} */
        @Override public int columnIndex(@NotNull String columnName) {
            if (tuple != null)
                return tuple.columnIndex(columnName);

            Objects.requireNonNull(columnName);

            var col = schema().column(columnName);

            return col == null || !schema().isKeyColumn(col.schemaIndex()) ? -1 : col.schemaIndex();
        }

        /** {@inheritDoc} */
        @Override protected Column rowColumnByName(@NotNull String columnName) {
            final Column col = super.rowColumnByName(columnName);

            if (!schema().isKeyColumn(col.schemaIndex()))
                throw new IllegalArgumentException("Invalid column name: columnName=" + columnName);

            return col;
        }

        /** {@inheritDoc} */
        @Override protected Column rowColumnByIndex(@NotNull int columnIndex) {
            Objects.checkIndex(columnIndex, schema().keyColumns().length());

            return schema().column(columnIndex);
        }
    }

    /**
     * Value column chunk.
     */
    private static class ValueRowChunk extends MutableRowTupleAdapter {
        /**
         * Creates tuple for value chunk.
         *
         * @param row Row.
         */
        ValueRowChunk(@NotNull Row row) {
            super(row);
        }

        /** {@inheritDoc} */
        @Override public int columnCount() {
            return tuple != null ? tuple.columnCount() : schema().valueColumns().length();
        }

        /** {@inheritDoc} */
        @Override public int columnIndex(@NotNull String columnName) {
            if (tuple != null)
                return tuple.columnIndex(columnName);

            Objects.requireNonNull(columnName);

            var col = schema().column(columnName);

            return col == null || schema().isKeyColumn(col.schemaIndex()) ? -1 :
                    col.schemaIndex() - schema().keyColumns().length();
        }

        /** {@inheritDoc} */
        @Override protected Column rowColumnByName(@NotNull String columnName) {
            final Column col = super.rowColumnByName(columnName);

            if (schema().isKeyColumn(col.schemaIndex()))
                throw new IllegalArgumentException("Invalid column name: columnName=" + columnName);

            return col;
        }

        /** {@inheritDoc} */
        @Override protected Column rowColumnByIndex(@NotNull int columnIndex) {
            Objects.checkIndex(columnIndex, schema().valueColumns().length());

            return schema().column(columnIndex + schema().keyColumns().length());
        }
    }
}

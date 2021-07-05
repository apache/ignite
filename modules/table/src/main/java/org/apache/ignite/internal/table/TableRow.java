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
import org.apache.ignite.internal.schema.Row;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Row to Tuple adapter.
 * <p>
 * Provides methods to access columns values by column names.
 */
public class TableRow extends RowChunkAdapter {
    /** Schema. */
    private final SchemaDescriptor schema;

    /** Row. */
    private final Row row;

    /**
     * Constructor.
     *
     * @param schema Schema descriptor.
     * @param row Row.
     */
    public TableRow(SchemaDescriptor schema, Row row) {
        assert schema.version() == row.schemaVersion();

        this.schema = schema;
        this.row = row;
    }

    /** {@inheritDoc} */
    @Override @NotNull protected final Column columnByName(@NotNull String colName) {
        Objects.requireNonNull(colName);

        final Column col = schema.column(colName);

        if (col == null)
            throw new ColumnNotFoundException("Invalid column name: columnName=" + colName + ", schemaVersion=" + schema.version());

        return col;
    }

    /**
     * @return Key chunk.
     */
    public Tuple keyChunk() {
        return new KeyRowChunk();
    }

    /**
     * @return Value chunk.
     */
    public @Nullable Tuple valueChunk() {
        return row.hasValue() ? new ValueRowChunk() : null;
    }

    /** {@inheritDoc} */
    @Override protected Row row() {
        return row;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(String colName) {
        return schema.column(colName) != null;
    }

    /** Key column chunk. */
    private class KeyRowChunk extends RowChunkAdapter {
        /** {@inheritDoc} */
        @Override protected Row row() {
            return row;
        }

        /** {@inheritDoc} */
        @Override protected @NotNull Column columnByName(@NotNull String colName) {
            Objects.requireNonNull(colName);

            final Column col = schema.column(colName);

            if (col == null || !schema.isKeyColumn(col.schemaIndex()))
                throw new ColumnNotFoundException("Invalid key column name: columnName=" + colName + ", schemaVersion=" + schema.version());

            return col;
        }

        /** {@inheritDoc} */
        @Override public boolean contains(String colName) {
            return schema.column(colName) != null;
        }
    }

    /** Value column chunk. */
    private class ValueRowChunk extends RowChunkAdapter {
        /** {@inheritDoc} */
        @Override protected Row row() {
            return row;
        }

        /** {@inheritDoc} */
        @Override protected @NotNull Column columnByName(@NotNull String colName) {
            Objects.requireNonNull(colName);

            final Column col = schema.column(colName);

            if (col == null || schema.isKeyColumn(col.schemaIndex()))
                throw new ColumnNotFoundException("Invalid value column name: columnName=" + colName + ", schemaVersion=" + schema.version());

            return col;
        }

        /** {@inheritDoc} */
        @Override public boolean contains(String colName) {
            return schema.column(colName) != null;
        }
    }
}

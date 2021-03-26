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
import org.jetbrains.annotations.NotNull;

/**
 * Table row adapter for Row.
 */
public class TableRowAdapter extends RowChunkAdapter implements TableRow {
    /** Schema. */
    private final SchemaDescriptor schema;

    /** Row. */
    private final Row row;

    /** Key chunk projection. */
    private final RowChunk keyChunk;

    /** Value chunk projection. */
    private final RowChunk valChunk;

    /**
     * Constructor.
     *
     * @param row Row.
     * @param schema Schema descriptor.
     */
    public TableRowAdapter(Row row, SchemaDescriptor schema) {
        this.schema = schema;
        this.row = row;

        keyChunk = new KeyRowChunk();
        valChunk = row.hasValue() ? new ValueRowChunk() : null;
    }

    /** {@inheritDoc} */
    @Override public long schemaVersion() {
        return schema.version();
    }

    /** {@inheritDoc} */
    @Override @NotNull protected final Column columnByName(@NotNull String colName) {
        Objects.requireNonNull(colName);

        final Column col = schema.column(colName);

        if (col == null)
            throw new IllegalArgumentException("Invalid column name: columnName=" + colName + ", schemaVersion=" + schema.version());

        return col;
    }

    /** {@inheritDoc} */
    @Override public byte[] toBytes() {
        return row.rowBytes();
    }

    /** {@inheritDoc} */
    @Override public RowChunk keyChunk() {
        return keyChunk;
    }

    /** {@inheritDoc} */
    @Override public RowChunk valueChunk() {
        return valChunk;
    }

    /** {@inheritDoc} */
    @Override protected Row row() {
        return row;
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

            if (col == null || !schema.keyColumn(col.schemaIndex()))
                throw new IllegalArgumentException("Invalid key column name: columnName=" + colName + ", schemaVersion=" + schema.version());

            return col;
        }

        /** {@inheritDoc} */
        @Override public byte[] toBytes() {
            return row.keyChunkBytes();
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

            if (col == null || schema.keyColumn(col.schemaIndex()))
                throw new IllegalArgumentException("Invalid key column name: columnName=" + colName + ", schemaVersion=" + schema.version());

            return col;
        }

        /** {@inheritDoc} */
        @Override public byte[] toBytes() {
            return row.valueChunkBytes();
        }
    }
}

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
    @Override public <T> T valueOrDefault(String colName, T def) {
        try {
            return value(colName);
        }
        catch (ColumnNotFoundException ex) {
            return def;
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T value(String colName) {
        final Column col = columnByName(colName);

        return (T)col.type().spec().objectValue(row(), col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public BinaryObject binaryObjectField(String colName) {
        Column col = columnByName(colName);

        return BinaryObjects.wrap(row().bytesValue(col.schemaIndex()));
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(String colName) {
        Column col = columnByName(colName);

        return row().byteValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public short shortValue(String colName) {
        Column col = columnByName(colName);

        return row().shortValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public int intValue(String colName) {
        Column col = columnByName(colName);

        return row().intValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public long longValue(String colName) {
        Column col = columnByName(colName);

        return row().longValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public float floatValue(String colName) {
        Column col = columnByName(colName);

        return row().floatValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public double doubleValue(String colName) {
        Column col = columnByName(colName);

        return row().doubleValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public String stringValue(String colName) {
        Column col = columnByName(colName);

        return row().stringValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public UUID uuidValue(String colName) {
        Column col = columnByName(colName);

        return row().uuidValue(col.schemaIndex());
    }

    /** {@inheritDoc} */
    @Override public BitSet bitmaskValue(String colName) {
        Column col = columnByName(colName);

        return row().bitmaskValue(col.schemaIndex());
    }
}

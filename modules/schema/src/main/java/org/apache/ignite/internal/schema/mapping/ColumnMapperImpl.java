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

package org.apache.ignite.internal.schema.mapping;

import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Column mapper implementation.
 */
class ColumnMapperImpl implements ColumnMapper {
    /** Mapping. */
    private final int[] mapping;

    /** Mapped columns. */
    private final Column[] cols;

    /**
     * @param schema Schema descriptor.
     */
    ColumnMapperImpl(SchemaDescriptor schema) {
        mapping = new int[schema.length()];
        cols = new Column[schema.length()];

        for (int i = 0; i < mapping.length; i++)
            mapping[i] = i;
    }

    /** {@inheritDoc} */
    @Override public ColumnMapperImpl add(@NotNull Column col) {
        add0(col.schemaIndex(), -1, col);

        return this;
    }

    /** {@inheritDoc} */
    @Override public ColumnMapperImpl add(int from, int to) {
        add0(from, to, null);

        return this;
    }

    /**
     * @param from Source column index.
     * @param to Target column index.
     * @param col Target column descriptor.
     */
    void add0(int from, int to, @Nullable Column col) {
        mapping[from] = to;
        cols[from] = col;
    }

    /** {@inheritDoc} */
    @Override public int map(int idx) {
        return idx < mapping.length ? mapping[idx] : -1;
    }

    /** {@inheritDoc} */
    @Override public Column mappedColumn(int idx) {
        return idx < cols.length ? cols[idx] : null;
    }
}

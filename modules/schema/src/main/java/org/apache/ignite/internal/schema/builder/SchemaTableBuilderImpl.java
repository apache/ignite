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

package org.apache.ignite.internal.schema.builder;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.ignite.internal.schema.SchemaTableImpl;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.ColumnarIndex;
import org.apache.ignite.schema.IndexColumn;
import org.apache.ignite.schema.PrimaryIndex;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.schema.SortedIndex;
import org.apache.ignite.schema.TableIndex;
import org.apache.ignite.schema.builder.SchemaTableBuilder;

import static org.apache.ignite.schema.PrimaryIndex.PRIMARY_KEY_INDEX_NAME;

/**
 * Table builder.
 */
public class SchemaTableBuilderImpl implements SchemaTableBuilder {
    /** Schema name. */
    private final String schemaName;

    /** Table name. */
    private final String tableName;

    /** Columns. */
    private final LinkedHashMap<String, Column> columns = new LinkedHashMap<>();

    /** Indices. */
    private final Map<String, TableIndex> indices = new HashMap<>();

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param tableName Table name.
     */
    public SchemaTableBuilderImpl(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    /** {@inheritDoc} */
    @Override public SchemaTableBuilderImpl columns(Column... columns) {
        for (int i = 0; i < columns.length; i++) {
            if (this.columns.put(columns[i].name(), columns[i]) != null)
                throw new IllegalArgumentException("Column with same name already exists: columnName=" + columns[i].name());
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public SchemaTableBuilder withIndex(TableIndex index) {
        if (index instanceof PrimaryIndex) {
            if (!PRIMARY_KEY_INDEX_NAME.equals(index.name()))
                throw new IllegalArgumentException("Not valid index name for a primary index: " + index.name());
        }
        else if (PRIMARY_KEY_INDEX_NAME.equals(index.name()))
            throw new IllegalArgumentException("Not valid index name for a secondary index: " + index.name());

        if (indices.put(index.name(), index) != null)
            throw new IllegalArgumentException("Index with same name already exists: " + index.name());

        return this;
    }

    /** {@inheritDoc} */
    @Override public SchemaTableBuilder withPrimaryKey(String colName) {
        withIndex(SchemaBuilders.pkIndex().addIndexColumn(colName).done().withAffinityColumns(colName).build());

        return this;
    }

    /** {@inheritDoc} */
    @Override public SchemaTableBuilder withHints(Map<String, String> hints) {
        // No op.
        return this;
    }

    /** {@inheritDoc} */
    @Override public SchemaTable build() {
        assert schemaName != null : "Table name was not specified.";

        validateIndices(indices.values(), columns.values());

        assert columns.size() > ((SortedIndex)indices.get(PRIMARY_KEY_INDEX_NAME)).columns().size() : "Key or/and value columns was not defined.";

        return new SchemaTableImpl(
            schemaName,
            tableName,
            columns,
            Collections.unmodifiableMap(indices)
        );
    }

    /**
     * Validate indices.
     *
     * @param indices Table indices.
     * @param columns Table columns.
     */
    public static void validateIndices(Collection<TableIndex> indices, Collection<Column> columns) {
        Set<String> colNames = columns.stream().map(Column::name).collect(Collectors.toSet());

        assert indices.stream()
            .filter(ColumnarIndex.class::isInstance)
            .map(ColumnarIndex.class::cast)
            .flatMap(idx -> idx.columns().stream())
            .map(IndexColumn::name)
            .allMatch(colNames::contains) : "Index column doesn't exists in schema.";

        TableIndex pkIdx = indices.stream().filter(idx -> PRIMARY_KEY_INDEX_NAME.equals(idx.name())).findAny().orElse(null);

        assert pkIdx != null : "Primary key index is not configured.";
        assert !((PrimaryIndex)pkIdx).affinityColumns().isEmpty() : "Primary key must have one affinity column at least.";

        // Note: E.g. functional index is not columnar index as it index an expression result only.
        assert indices.stream().allMatch(ColumnarIndex.class::isInstance) : "Columnar indices are supported only.";
    }
}

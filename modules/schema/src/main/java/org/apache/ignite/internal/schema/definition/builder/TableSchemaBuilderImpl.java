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

package org.apache.ignite.internal.schema.definition.builder;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.definition.TableDefinitionImpl;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.PrimaryKeyDefinition;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.schema.definition.builder.TableSchemaBuilder;
import org.apache.ignite.schema.definition.index.ColumnarIndexDefinition;
import org.apache.ignite.schema.definition.index.IndexColumnDefinition;
import org.apache.ignite.schema.definition.index.IndexDefinition;

/**
 * Table builder.
 */
public class TableSchemaBuilderImpl implements TableSchemaBuilder {
    /** Schema name. */
    private final String schemaName;

    /** Table name. */
    private final String tableName;

    /** Columns. */
    private final LinkedHashMap<String, ColumnDefinition> columns = new LinkedHashMap<>();

    /** Indices. */
    private final Map<String, IndexDefinition> indices = new HashMap<>();

    /** Table primary key. */
    private PrimaryKeyDefinition primaryKeyDefinition;

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param tableName Table name.
     */
    public TableSchemaBuilderImpl(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    /** {@inheritDoc} */
    @Override public TableSchemaBuilderImpl columns(ColumnDefinition... columns) {
        for (ColumnDefinition column : columns) {
            if (this.columns.put(column.name(), column) != null)
                throw new IllegalArgumentException("Column with same name already exists: columnName=" + column.name());
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public TableSchemaBuilder withIndex(IndexDefinition indexDefinition) {
        if (indices.put(indexDefinition.name(), indexDefinition) != null)
            throw new IllegalArgumentException("Index with same name already exists: " + indexDefinition.name());

        return this;
    }

    /** {@inheritDoc} */
    @Override public TableSchemaBuilder withPrimaryKey(String colName) {
        primaryKeyDefinition = SchemaBuilders.primaryKey().withColumns(colName).build();

        return this;
    }

    /** {@inheritDoc} */
    @Override public TableSchemaBuilder withPrimaryKey(PrimaryKeyDefinition primaryKeyDefinition) {
        this.primaryKeyDefinition = primaryKeyDefinition;

        return this;
    }

    /** {@inheritDoc} */
    @Override public TableSchemaBuilder withHints(Map<String, String> hints) {
        // No op.
        return this;
    }

    /** {@inheritDoc} */
    @Override public TableDefinition build() {
        assert schemaName != null : "Database schema name must be specified.";

        assert columns.size() > primaryKeyDefinition.columns().size() : "Key or/and value columns must be defined.";
        assert primaryKeyDefinition != null : "Primary key index must be configured.";

        validateIndices(indices.values(), columns.values(), primaryKeyDefinition.affinityColumns());

        return new TableDefinitionImpl(
            schemaName,
            tableName,
            columns,
            primaryKeyDefinition,
            Collections.unmodifiableMap(indices)
        );
    }

    /**
     * Validate indices.
     *
     * @param indices Table indices.
     * @param cols Table columns.
     * @param affColNames Affinity columns names.
     */
    public static void validateIndices(Collection<IndexDefinition> indices, Collection<ColumnDefinition> cols, Set<String> affColNames) {
        Set<String> colNames = cols.stream().map(ColumnDefinition::name).collect(Collectors.toSet());

        for (IndexDefinition idx : indices) {
            assert idx instanceof ColumnarIndexDefinition : "Only columnar indices are supported.";
            // Note: E.g. functional index is not columnar index as it index an expression result only.

            ColumnarIndexDefinition idx0 = (ColumnarIndexDefinition)idx;

            if (!idx0.columns().stream().map(IndexColumnDefinition::name).allMatch(colNames::contains))
                throw new IllegalStateException("Index column must exist in the schema.");

            if (idx0.unique() &&
                    !(idx0.columns().stream().map(IndexColumnDefinition::name).allMatch(affColNames::contains)))
                throw new IllegalStateException("Unique index must contains all affinity columns.");
        }
    }
}

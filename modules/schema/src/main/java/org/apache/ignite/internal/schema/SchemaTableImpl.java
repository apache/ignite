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

package org.apache.ignite.internal.schema;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.modification.TableModificationBuilderImpl;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.IndexColumn;
import org.apache.ignite.schema.PrimaryIndex;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.schema.TableIndex;
import org.apache.ignite.schema.modification.TableModificationBuilder;

/**
 * Table.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class SchemaTableImpl extends AbstractSchemaObject implements SchemaTable {
    /** Schema name. */
    private final String schemaName;

    /** Key columns. */
    private final LinkedHashMap<String, Column> cols;

    /** Indices. */
    private final Map<String, TableIndex> indices;

    /** Cached key columns. */
    private final List<Column> keyCols;

    /** Cached key affinity columns. */
    private final List<Column> affCols;

    /** Cached value columns. */
    private final List<Column> valCols;

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param tableName Table name.
     * @param cols Columns.
     * @param indices Indices.
     */
    public SchemaTableImpl(
        String schemaName,
        String tableName,
        final LinkedHashMap<String, Column> cols,
        final Map<String, TableIndex> indices
    ) {
        super(tableName);

        this.schemaName = schemaName;
        this.cols = cols;
        this.indices = indices;

        final PrimaryIndex pkIndex = (PrimaryIndex)indices.get(PrimaryIndex.PRIMARY_KEY_INDEX_NAME);
        final Set<String> pkColNames = pkIndex.columns().stream().map(IndexColumn::name).collect(Collectors.toSet());

        assert pkIndex != null;

        keyCols = pkIndex.columns().stream().map(c -> cols.get(c.name())).collect(Collectors.toUnmodifiableList());
        affCols = pkIndex.affinityColumns().stream().map(cols::get).collect(Collectors.toUnmodifiableList());
        valCols = cols.values().stream().filter(c -> !pkColNames.contains(c.name())).collect(Collectors.toUnmodifiableList());

    }

    /** {@inheritDoc} */
    @Override public Collection<Column> keyColumns() {
        return keyCols;
    }

    /** {@inheritDoc} */
    @Override public Collection<Column> affinityColumns() {
        return affCols;
    }

    /** {@inheritDoc} */
    @Override public Collection<Column> valueColumns() {
        return valCols;
    }

    /** {@inheritDoc} */
    @Override public String canonicalName() {
        return schemaName + '.' + name();
    }

    /** {@inheritDoc} */
    @Override public Collection<TableIndex> indices() {
        return Collections.unmodifiableCollection(indices.values());
    }

    /** {@inheritDoc} */
    @Override public TableModificationBuilder toBuilder() {
        return new TableModificationBuilderImpl(this);
    }

    /**
     * @param name Column name.
     * @return {@code True} if column with given name already exists, {@code false} otherwise.
     */
    public boolean hasColumn(String name) {
        return cols.containsKey(name);
    }

    /**
     * @param name Column name.
     * @return {@code True} if key column with given name already exists, {@code false} otherwise.
     */
    public boolean hasKeyColumn(String name) {
        return keyCols.stream().anyMatch(c -> c.name().equals(name));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaTableImpl.class, this);
    }
}

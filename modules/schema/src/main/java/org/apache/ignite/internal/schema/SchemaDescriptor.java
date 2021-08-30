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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.mapping.ColumnMapping;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.ArrayUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Full schema descriptor containing key columns chunk, value columns chunk, and schema version.
 */
public class SchemaDescriptor implements Serializable {
    /** Table identifier. */
    private final UUID tableId;

    /** Schema version. Incremented on each schema modification. */
    private final int ver;

    /** Key columns in serialization order. */
    private final Columns keyCols;

    /** Value columns in serialization order. */
    private final Columns valCols;

    /** Affinity columns. */
    private final Column[] affCols;

    /** Mapping 'Column name' -&gt; Column. */
    private final Map<String, Column> colMap;

    /** Column mapper. */
    private ColumnMapper colMapper = ColumnMapping.identityMapping();

    /**
     * @param tableId Table id.
     * @param ver Schema version.
     * @param keyCols Key columns.
     * @param valCols Value columns.
     */
    public SchemaDescriptor(UUID tableId, int ver, Column[] keyCols, Column[] valCols) {
        this(tableId, ver, keyCols, null, valCols);
    }

    /**
     * @param tableId Table id.
     * @param ver Schema version.
     * @param keyCols Key columns.
     * @param affCols Affinity column names.
     * @param valCols Value columns.
     */
    public SchemaDescriptor(UUID tableId, int ver, Column[] keyCols, @Nullable String[] affCols, Column[] valCols) {
        assert keyCols.length > 0 : "No key columns are configured.";
        assert valCols.length > 0 : "No value columns are configured.";

        this.tableId = tableId;
        this.ver = ver;
        this.keyCols = new Columns(0, keyCols);
        this.valCols = new Columns(keyCols.length, valCols);

        colMap = new HashMap<>(keyCols.length + valCols.length);

        Arrays.stream(this.keyCols.columns()).forEach(c -> colMap.put(c.name(), c));
        Arrays.stream(this.valCols.columns()).forEach(c -> colMap.put(c.name(), c));

        // Preserving key chunk column order is not actually required.
        // It is sufficient to has same column order for all nodes.
        this.affCols = (ArrayUtils.nullOrEmpty(affCols)) ? keyCols :
            Arrays.stream(affCols).map(colMap::get).toArray(Column[]::new);
    }

    /**
     * @return Table identifier.
     */
    public UUID tableId() {
        return tableId;
    }

    /**
     * @return Schema version.
     */
    public int version() {
        return ver;
    }

    /**
     * @param idx Column index to check.
     * @return {@code true} if the column belongs to the key chunk, {@code false} otherwise.
     */
    public boolean isKeyColumn(int idx) {
        return idx < keyCols.length();
    }

    /**
     * @param colIdx Column index.
     * @return Column instance.
     */
    public Column column(int colIdx) {
        validateColumnIndex(colIdx);

        return colIdx < keyCols.length() ? keyCols.column(colIdx) : valCols.column(colIdx - keyCols.length());
    }

    /**
     * Validates the column index.
     *
     * @param colIdx Column index.
     */
    public void validateColumnIndex(int colIdx) {
        Objects.checkIndex(colIdx, length());
    }

    /**
     * Gets columns names.
     *
     * @return Columns names.
     */
    public Collection<String> columnNames() {
        return colMap.keySet();
    }

    /**
     * @return Key columns chunk.
     */
    public Columns keyColumns() {
        return keyCols;
    }

    /**
     * @return Key affinity columns chunk.
     */
    public Column[] affinityColumns() {
        return affCols;
    }

    /**
     * @return Value columns chunk.
     */
    public Columns valueColumns() {
        return valCols;
    }

    /**
     * @return Total number of columns in schema.
     */
    public int length() {
        return keyCols.length() + valCols.length();
    }

    /**
     * @param name Column name.
     * @return Column.
     */
    public @Nullable Column column(@NotNull String name) {
        return colMap.get(name);
    }

    /**
     * Sets column mapper for previous schema version.
     *
     * @param colMapper Column mapper.
     */
    public void columnMapping(ColumnMapper colMapper) {
        this.colMapper = colMapper;
    }

    /**
     * @return Column mapper.
     */
    public ColumnMapper columnMapping() {
        return colMapper;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaDescriptor.class, this);
    }
}

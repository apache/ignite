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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Full schema descriptor containing key columns chunk, value columns chunk, and schema version.
 */
public class SchemaDescriptor {
    /** Schema version. Incremented on each schema modification. */
    private final int ver;

    /** Key columns in serialization order. */
    private final Columns keyCols;

    /** Value columns in serialization order. */
    private final Columns valCols;

    /** Mapping 'Column name' -> Column. */
    private final Map<String, Column> colMap;

    /**
     * @param ver Schema version.
     * @param keyCols Key columns.
     * @param valCols Value columns.
     */
    public SchemaDescriptor(int ver, Column[] keyCols, Column[] valCols) {
        this(ver, keyCols, null, valCols);
    }

    /**
     * @param ver Schema version.
     * @param keyCols Key columns.
     * @param affCols Affinity column names.
     * @param valCols Value columns.
     */
    public SchemaDescriptor(int ver, Column[] keyCols, @Nullable String[] affCols, Column[] valCols) {
        assert keyCols.length > 0 : "No key columns are conigured.";
        assert valCols.length > 0 : "No value columns are conigured.";

        this.ver = ver;
        this.keyCols = new Columns(0, keyCols);
        this.valCols = new Columns(keyCols.length, valCols);

        colMap = new HashMap<>(keyCols.length + valCols.length);

        Arrays.stream(this.keyCols.columns()).forEach(c -> colMap.put(c.name(), c));
        Arrays.stream(this.valCols.columns()).forEach(c -> colMap.put(c.name(), c));

        //TODO: https://issues.apache.org/jira/browse/IGNITE-14388 Add affinity columns support.
    }

    /**
     * @return Schema version.
     */
    public int version() {
        return ver;
    }

    /**
     * @param idx Index to check.
     * @return {@code true} if the column belongs to the key chunk.
     */
    public boolean isKeyColumn(int idx) {
        return idx < keyCols.length();
    }

    /**
     * @param colIdx Column index.
     * @return Column instance.
     */
    public Column column(int colIdx) {
        return colIdx < keyCols.length() ? keyCols.column(colIdx) : valCols.column(colIdx - keyCols.length());
    }

    /**
     * @return Key columns chunk.
     */
    public Columns keyColumns() {
        return keyCols;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaDescriptor.class, this);
    }
}

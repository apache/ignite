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

package org.apache.ignite.internal.schema.registry;

import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.Row;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.jetbrains.annotations.Nullable;

/**
 * Caching registry of actual schema descriptors for a table.
 */
public class SchemaRegistryImpl implements SchemaRegistry {
    /** Initial schema version. */
    public static final int INITIAL_SCHEMA_VERSION = -1;

    /** Cached schemas. */
    private final ConcurrentSkipListMap<Integer, SchemaDescriptor> schemaCache = new ConcurrentSkipListMap<>();

    /** Last registered version. */
    private volatile int lastVer;

    /** Schema store. */
    private final Function<Integer, SchemaDescriptor> history;

    /**
     * Default constructor.
     *
     * @param history Schema history.
     */
    public SchemaRegistryImpl(Function<Integer, SchemaDescriptor> history) {
        lastVer = INITIAL_SCHEMA_VERSION;
        this.history = history;
    }

    /**
     * Constructor.
     *
     * @param initialVer Initial version.
     * @param history Schema history.
     */
    public SchemaRegistryImpl(int initialVer, Function<Integer, SchemaDescriptor> history) {
        lastVer = initialVer;
        this.history = history;
    }

    /** {@inheritDoc} */
    @Override public SchemaDescriptor schema(int ver) {
        SchemaDescriptor desc = schemaCache.get(ver);

        if (desc != null)
            return desc;

        desc = history.apply(ver);

        if (desc != null) {
            schemaCache.putIfAbsent(ver, desc);

            return desc;
        }

        if (lastVer < ver || ver <= 0)
            throw new SchemaRegistryException("Incorrect schema version requested: ver=" + ver);
        else
            throw new SchemaRegistryException("Failed to find schema: ver=" + ver);
    }

    /** {@inheritDoc} */
    @Override public @Nullable SchemaDescriptor schema() {
        final int lastVer0 = lastVer;

        if (lastVer0 == INITIAL_SCHEMA_VERSION)
            return null;

        return schema(lastVer0);
    }

    /** {@inheritDoc} */
    @Override public int lastSchemaVersion() {
        return lastVer;
    }

    /** {@inheritDoc} */
    @Override public Row resolve(BinaryRow row) {
        final SchemaDescriptor rowSchema = schema(row.schemaVersion());
        final SchemaDescriptor curSchema = schema();

        if (curSchema.version() == rowSchema.version())
            return new Row(rowSchema, row);

        return new UpgradingRowAdapter(curSchema, row, columnMapper(curSchema, rowSchema));
    }

    /**
     * Create column mapping for schemas.
     *
     * @param src Source schema of newer version.
     * @param dst Target schema of older version.
     * @return Column mapping.
     */
    private ColumnMapping columnMapper(SchemaDescriptor src, SchemaDescriptor dst) {
        assert src.version() > dst.version();
        assert src.version() == dst.version() + 1; // TODO: IGNITE-14863 implement merged mapper for arbitraty schema versions.

        final Columns srcCols = src.valueColumns();
        final Columns dstCols = dst.valueColumns();

        final ColumnMapping mapping = new ColumnMapping(src);

        for (int i = 0; i < srcCols.columns().length; i++) {
            final Column col = srcCols.column(i);

            try {
                final int idx = dstCols.columnIndex(col.name());

                if (!col.equals(dstCols.column(idx)))
                    throw new InvalidTypeException("Column of incompatible type: [colIdx=" + col.schemaIndex() + ", schemaVer=" + src.version());

                mapping.add(col.schemaIndex(), dst.keyColumns().length() + idx);
            }
            catch (NoSuchElementException ex) {
                mapping.add(col.schemaIndex(), -1);
            }
        }

        return mapping;
    }

    /**
     * Registers new schema.
     *
     * @param desc Schema descriptor.
     * @throws SchemaRegistrationConflictException If schema of provided version was already registered.
     * @throws SchemaRegistryException If schema of incorrect version provided.
     */
    public void onSchemaRegistered(SchemaDescriptor desc) {
        if (lastVer == INITIAL_SCHEMA_VERSION) {
            if (desc.version() != 1)
                throw new SchemaRegistryException("Try to register schema of wrong version: ver=" + desc.version() + ", lastVer=" + lastVer);
        }
        else if (desc.version() != lastVer + 1) {
            if (desc.version() > 0 && desc.version() <= lastVer)
                throw new SchemaRegistrationConflictException("Schema with given version has been already registered: " + desc.version());

            throw new SchemaRegistryException("Try to register schema of wrong version: ver=" + desc.version() + ", lastVer=" + lastVer);
        }

        schemaCache.put(desc.version(), desc);

        lastVer = desc.version();
    }

    /**
     * Cleanup given schema version from history.
     *
     * @param ver Schema version to remove.
     * @throws SchemaRegistryException If incorrect schema version provided.
     */
    public void onSchemaDropped(int ver) {
        if (ver >= lastVer || ver <= 0 || schemaCache.keySet().first() < ver)
            throw new SchemaRegistryException("Incorrect schema version to clean up to: " + ver);

        schemaCache.remove(ver);
    }
}

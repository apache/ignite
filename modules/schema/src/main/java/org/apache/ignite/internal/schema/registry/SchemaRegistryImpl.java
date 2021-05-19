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

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
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
     * @param history Schema history.
     */
    public SchemaRegistryImpl(int initialVer, Function<Integer, SchemaDescriptor> history) {
        lastVer = initialVer;
        this.history = history;
    }

    /**
     * Gets schema descriptor for given version.
     *
     * @param ver Schema version to get descriptor for.
     * @return Schema descriptor.
     * @throws SchemaRegistryException If no schema found for given version.
     */
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

    /**
     * Gets schema descriptor for the latest version if initialized.
     *
     * @return Schema descriptor if initialized, {@code null} otherwise.
     * @throws SchemaRegistryException If failed.
     */
    @Override public @Nullable SchemaDescriptor schema() {
        final int lastVer0 = lastVer;

        if (lastVer0 == INITIAL_SCHEMA_VERSION)
            return null;

       return schema(lastVer0);
    }

    /**
     * @return Last known schema version.
     */
    public int lastSchemaVersion() {
        return lastVer;
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

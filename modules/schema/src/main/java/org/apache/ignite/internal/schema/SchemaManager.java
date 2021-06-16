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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Conditions;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.metastorage.client.EntryEvent;
import org.apache.ignite.internal.metastorage.client.Operations;
import org.apache.ignite.internal.metastorage.client.WatchEvent;
import org.apache.ignite.internal.metastorage.client.WatchListener;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.configuration.SchemaDescriptorConverter;
import org.apache.ignite.internal.schema.event.SchemaEvent;
import org.apache.ignite.internal.schema.event.SchemaEventParameters;
import org.apache.ignite.internal.schema.registry.SchemaRegistryException;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.schema.SchemaTable;
import org.jetbrains.annotations.NotNull;

/**
 * Schema Manager.
 *
 * Schemas MUST be registered in a version ascending order incrementing by {@code 1} with NO gaps,
 * otherwise an exception will be thrown. The version numbering starts from the {@code 1}.
 * <p>
 * After some table maintenance process some first versions may become outdated and can be safely cleaned up
 * if the process guarantees the table no longer has a data of these versions.
 *
 * @implSpec The changes in between two arbitrary actual versions MUST NOT be lost.
 * Thus, schema versions can only be removed from the beginning.
 * @implSpec Initial schema history MAY be registered without the first outdated versions
 * that could be cleaned up earlier.
 */
public class SchemaManager extends Producer<SchemaEvent, SchemaEventParameters> {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(SchemaManager.class);

    /** Internal prefix for the metasorage. */
    private static final String INTERNAL_PREFIX = "internal.tables.schema.";

    /** Schema history item key suffix. */
    private static final String INTERNAL_VER_SUFFIX = ".ver.";

    /** Configuration manager in order to handle and listen schema specific configuration. */
    private final ConfigurationManager configurationMgr;

    /** Metastorage manager. */
    private final MetaStorageManager metaStorageMgr;

    /** Vault manager. */
    private final VaultManager vaultMgr;

    /** Schema registries (tableId -> SchemaRegistry). */
    private final Map<UUID, SchemaRegistryImpl> schemaRegs = new ConcurrentHashMap<>();

    /**
     * The constructor.
     *
     * @param configurationMgr Configuration manager.
     * @param metaStorageMgr Metastorage manager.
     * @param vaultMgr Vault manager.
     */
    public SchemaManager(
        ConfigurationManager configurationMgr,
        MetaStorageManager metaStorageMgr,
        VaultManager vaultMgr
    ) {
        this.configurationMgr = configurationMgr;
        this.metaStorageMgr = metaStorageMgr;
        this.vaultMgr = vaultMgr;

        metaStorageMgr.registerWatchByPrefix(new ByteArray(INTERNAL_PREFIX), new WatchListener() {
            @Override public boolean onUpdate(@NotNull WatchEvent events) {
                for (EntryEvent evt : events.entryEvents()) {
                    String keyTail = evt.newEntry().key().toString().substring(INTERNAL_PREFIX.length());

                    int verPos = keyTail.indexOf(INTERNAL_VER_SUFFIX);

                    if (verPos == -1) {
                        final UUID tblId = UUID.fromString(keyTail);

                        SchemaRegistry reg = schemaRegistryForTable(tblId);

                        assert reg != null : "Table schema was not initialized or table has been dropped: " + tblId;

                        if (evt.oldEntry().empty() || evt.oldEntry().tombstone())
                            onEvent(SchemaEvent.INITIALIZED, new SchemaEventParameters(tblId, reg), null);
                        else if (evt.newEntry().empty() || evt.newEntry().tombstone()) {
                            schemaRegs.remove(tblId);

                            onEvent(SchemaEvent.DROPPED, new SchemaEventParameters(tblId, null), null);
                        }

                        return true; // Ignore last table schema version.
                    }
                    else {
                        UUID tblId = UUID.fromString(keyTail.substring(0, verPos));

                        SchemaRegistryImpl reg = schemaRegs.get(tblId);

                        if (reg == null)
                            schemaRegs.put(tblId, (reg = new SchemaRegistryImpl(v -> tableSchema(tblId, v))));

                        if (evt.oldEntry().empty() || evt.oldEntry().tombstone())
                            reg.onSchemaRegistered((SchemaDescriptor)ByteUtils.fromBytes(evt.newEntry().value()));
                        else if (evt.newEntry().empty() || evt.newEntry().tombstone()) {
                            int ver = Integer.parseInt(keyTail.substring(verPos + INTERNAL_VER_SUFFIX.length()));

                            reg.onSchemaDropped(ver);
                        }
                        else
                            throw new SchemaRegistryException("Schema of concrete version can't be changed.");
                    }
                }

                return true;
            }

            @Override public void onError(@NotNull Throwable e) {
                LOG.error("Metastorage listener issue", e);
            }
        });
    }

    /**
     * Creates schema registry for the table with existed schema or
     * registers initial schema from configuration.
     *
     * @param tblId Table id.
     * @param tblName Table name.
     * @return Operation future.
     */
    public CompletableFuture<Boolean> initSchemaForTable(final UUID tblId, String tblName) {
        return vaultMgr.get(ByteArray.fromString(INTERNAL_PREFIX + tblId)).
            thenCompose(entry -> {
                TableConfiguration tblConfig = configurationMgr.configurationRegistry().
                    getConfiguration(TablesConfiguration.KEY).tables().get(tblName);

                assert entry.empty();

                final int schemaVer = 1;

                final ByteArray lastVerKey = new ByteArray(INTERNAL_PREFIX + tblId);
                final ByteArray schemaKey = new ByteArray(INTERNAL_PREFIX + tblId + INTERNAL_VER_SUFFIX + schemaVer);

                SchemaTable schemaTable = SchemaConfigurationConverter.convert(tblConfig);
                final SchemaDescriptor desc = SchemaDescriptorConverter.convert(tblId, schemaVer, schemaTable);

                return metaStorageMgr.invoke(Conditions.notExists(schemaKey),
                    Operations.put(schemaKey, ByteUtils.toBytes(desc)),
                    Operations.noop())
                    //TODO: IGNITE-14679 Serialize schema.
                    .thenCompose(res -> metaStorageMgr.invoke(Conditions.notExists(lastVerKey),
                        Operations.put(lastVerKey, ByteUtils.longToBytes(schemaVer)),
                        Operations.noop()));
            });
    }

    /**
     * Return table schema of certain version from history.
     *
     * @param tblId Table id.
     * @param schemaVer Schema version.
     * @return Schema descriptor.
     */
    private SchemaDescriptor tableSchema(UUID tblId, int schemaVer) {
        try {
            return vaultMgr.get(ByteArray.fromString(INTERNAL_PREFIX + tblId + INTERNAL_VER_SUFFIX + schemaVer))
                .thenApply(e -> e.empty() ? null : (SchemaDescriptor)ByteUtils.fromBytes(e.value())).get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new SchemaRegistryException("Can't read schema from vault: ver=" + schemaVer, e);
        }
    }

    /**
     * Compares schemas.
     *
     * @param expected Expected schema.
     * @param actual Actual schema.
     * @return {@code True} if schemas are equal, {@code false} otherwise.
     */
    public static boolean equalSchemas(SchemaDescriptor expected, SchemaDescriptor actual) {
        if (expected.keyColumns().length() != actual.keyColumns().length() ||
            expected.valueColumns().length() != actual.valueColumns().length())
            return false;

        for (int i = 0; i < expected.length(); i++) {
            if (!expected.column(i).equals(actual.column(i)))
                return false;
        }

        return true;
    }

    /**
     * @param tableId Table id.
     * @return Schema registry for the table.
     */
    private SchemaRegistry schemaRegistryForTable(UUID tableId) {
        final SchemaRegistry reg = schemaRegs.get(tableId);

        if (reg == null)
            throw new SchemaRegistryException("No schema was ever registered for the table: " + tableId);

        return reg;
    }

    /**
     * Unregistered all schemas associated with a table identifier.
     *
     * @param tableId Table identifier.
     * @return Future which will complete when all versions of schema will be unregistered.
     */
    public CompletableFuture<Boolean> unregisterSchemas(UUID tableId) {
        CompletableFuture<Void> fut = metaStorageMgr.remove(new ByteArray(INTERNAL_PREFIX + tableId));

        String schemaPrefix = INTERNAL_PREFIX + tableId + INTERNAL_VER_SUFFIX;

        Set<ByteArray> keys = new HashSet<>();

        IgniteBiTuple<ByteArray, ByteArray> range = toRange(new ByteArray(schemaPrefix));

        try (Cursor<Entry> cursor = metaStorageMgr.range(range.get1(), range.get2())) {
            cursor.forEach(entry -> keys.add(entry.key()));
        }
        catch (Exception e) {
            LOG.error("Can't remove schemas for the table [tblId=" + tableId + ']', e);
        }

        return fut.thenCompose(r -> metaStorageMgr.removeAll(keys)).thenApply(v -> true);
    }

    /**
     * Transforms a prefix bytes to range.
     * This method should be replaced to direct call of range by prefix
     * in Meta storage manager when it will be implemented.
     * TODO: IGNITE-14799
     *
     * @param prefixKey Prefix bytes.
     * @return Tuple with left and right borders for range.
     */
    private IgniteBiTuple<ByteArray, ByteArray> toRange(ByteArray prefixKey) {
        var bytes = Arrays.copyOf(prefixKey.bytes(), prefixKey.bytes().length);

        if (bytes[bytes.length - 1] != Byte.MAX_VALUE)
            bytes[bytes.length - 1]++;
        else
            bytes = Arrays.copyOf(bytes, bytes.length + 1);

        return new IgniteBiTuple<>(prefixKey, new ByteArray(bytes));
    }
}

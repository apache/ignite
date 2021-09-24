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

package org.apache.ignite.internal.table.distributed;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.configuration.schema.ExtendedTableChange;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfiguration;
import org.apache.ignite.internal.configuration.schema.ExtendedTableView;
import org.apache.ignite.internal.configuration.schema.SchemaView;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaUtils;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorage;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.table.event.TableEventParameters;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.IgniteUuidGenerator;
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.manager.IgniteTables;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Table manager.
 */
public class TableManager extends Producer<TableEvent, TableEventParameters> implements IgniteTables, IgniteTablesInternal, IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(TableManager.class);

    /** */
    private static final int INITIAL_SCHEMA_VERSION = 1;

    /** Public prefix for metastorage. */
    // TODO: IGNITE-15412 Remove after implementation. Configuration manager will be used to retrieve distributed values
    // TODO: instead of metastorage manager.
    private static final String PUBLIC_PREFIX = "dst-cfg.table.tables.";

    /** */
    private static final IgniteUuidGenerator TABLE_ID_GENERATOR = new IgniteUuidGenerator(UUID.randomUUID(), 0);

    /** Tables configuration. */
    private final TablesConfiguration tablesCfg;

    /** Raft manager. */
    private final Loza raftMgr;

    /** Baseline manager. */
    private final BaselineManager baselineMgr;

    // TODO: IGNITE-15412 Remove after implementation. Configuration manager will be used to retrieve distributed values
    // TODO: instead of metastorage manager.
    /** Meta storage service. */
    private final MetaStorageManager metaStorageMgr;

    /** Partitions store directory. */
    private final Path partitionsStoreDir;

    /** Tables. */
    private final Map<String, TableImpl> tables = new ConcurrentHashMap<>();

    /** Tables. */
    private final Map<IgniteUuid, TableImpl> tablesById = new ConcurrentHashMap<>();

    /**
     * Creates a new table manager.
     *
     * @param tablesCfg Tables configuration.
     * @param raftMgr Raft manager.
     * @param baselineMgr Baseline manager.
     * @param metaStorageMgr Meta storage manager.
     * @param partitionsStoreDir Partitions store directory.
     */
    public TableManager(
        TablesConfiguration tablesCfg,
        Loza raftMgr,
        BaselineManager baselineMgr,
        MetaStorageManager metaStorageMgr,
        Path partitionsStoreDir
    ) {
        this.tablesCfg = tablesCfg;
        this.raftMgr = raftMgr;
        this.baselineMgr = baselineMgr;
        this.metaStorageMgr = metaStorageMgr;
        this.partitionsStoreDir = partitionsStoreDir;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        tablesCfg.tables().
            listenElements(new ConfigurationNamedListListener<TableView>() {
            @Override
            public @NotNull CompletableFuture<?> onCreate(@NotNull ConfigurationNotificationEvent<TableView> ctx) {
                // Empty assignments might be a valid case if tables are created from within cluster init HOCON
                // configuration, which is not supported now.
                assert ((ExtendedTableView)ctx.newValue()).assignments() != null :
                    "Table =[" + ctx.newValue().name() + "] has empty assignments.";

                final IgniteUuid tblId = IgniteUuid.fromString(((ExtendedTableView)ctx.newValue()).id());

                // TODO: IGNITE-15409 Listener with any placeholder should be used instead.
                ((ExtendedTableConfiguration)tablesCfg.tables().get(ctx.newValue().name())).schemas().
                    listenElements(new ConfigurationNamedListListener<>() {
                        @Override public @NotNull CompletableFuture<?> onCreate(
                            @NotNull ConfigurationNotificationEvent<SchemaView> schemasCtx) {
                            try {
                                ((SchemaRegistryImpl)tables.get(ctx.newValue().name()).schemaView()).
                                    onSchemaRegistered((SchemaDescriptor)ByteUtils.
                                        fromBytes(schemasCtx.newValue().schema()));

                                fireEvent(TableEvent.ALTER, new TableEventParameters(tablesById.get(tblId)), null);
                            }
                            catch (Exception e) {
                                fireEvent(TableEvent.ALTER, new TableEventParameters(tblId, ctx.newValue().name()), e);
                            }

                            return CompletableFuture.completedFuture(null);
                        }

                        @Override
                        public @NotNull CompletableFuture<?> onRename(@NotNull String oldName, @NotNull String newName,
                            @NotNull ConfigurationNotificationEvent<SchemaView> ctx) {
                            return CompletableFuture.completedFuture(null);
                        }

                        @Override public @NotNull CompletableFuture<?> onDelete(
                            @NotNull ConfigurationNotificationEvent<SchemaView> ctx) {
                            return CompletableFuture.completedFuture(null);
                        }

                        @Override public @NotNull CompletableFuture<?> onUpdate(
                            @NotNull ConfigurationNotificationEvent<SchemaView> ctx) {
                            return CompletableFuture.completedFuture(null);
                        }
                    });

                createTableLocally(
                    ctx.newValue().name(),
                    IgniteUuid.fromString(((ExtendedTableView)ctx.newValue()).id()),
                    (List<List<ClusterNode>>)ByteUtils.fromBytes(((ExtendedTableView)ctx.newValue()).assignments()),
                    (SchemaDescriptor)ByteUtils.fromBytes(((ExtendedTableView)ctx.newValue()).schemas().
                        get(String.valueOf(INITIAL_SCHEMA_VERSION)).schema())
                );

                return CompletableFuture.completedFuture(null);
            }

            @Override public @NotNull CompletableFuture<?> onRename(@NotNull String oldName, @NotNull String newName,
                @NotNull ConfigurationNotificationEvent<TableView> ctx) {
                // TODO: IGNITE-15485 Support table rename operation.

                return CompletableFuture.completedFuture(null);
            }

            @Override public @NotNull CompletableFuture<?> onDelete(
                @NotNull ConfigurationNotificationEvent<TableView> ctx
            ) {
                dropTableLocally(
                    ctx.oldValue().name(),
                    IgniteUuid.fromString(((ExtendedTableView)ctx.oldValue()).id()),
                    (List<List<ClusterNode>>)ByteUtils.fromBytes(((ExtendedTableView)ctx.oldValue()).assignments())
                );

                return CompletableFuture.completedFuture(null);
            }

            @Override
            public @NotNull CompletableFuture<?> onUpdate(@NotNull ConfigurationNotificationEvent<TableView> ctx) {
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // TODO: IGNITE-15161 Implement component's stop.
    }

    /**
     * Creates local structures for a table.
     *
     * @param name Table name.
     * @param tblId Table id.
     * @param assignment Affinity assignment.
     */
    private void createTableLocally(
        String name,
        IgniteUuid tblId,
        List<List<ClusterNode>> assignment,
        SchemaDescriptor schemaDesc
    ) {
        int partitions = assignment.size();

        var partitionsGroupsFutures = new ArrayList<CompletableFuture<RaftGroupService>>();

        IntStream.range(0, partitions).forEach(p ->
            partitionsGroupsFutures.add(
                raftMgr.prepareRaftGroup(
                    raftGroupName(tblId, p),
                    assignment.get(p),
                    () -> {
                        Path storageDir = partitionsStoreDir.resolve(name);

                        try {
                            Files.createDirectories(storageDir);
                        }
                        catch (IOException e) {
                            throw new IgniteInternalException(
                                "Failed to create partitions store directory for " + name + ": " + e.getMessage(),
                                e
                            );
                        }

                        return new PartitionListener(
                            new RocksDbStorage(
                                storageDir.resolve(String.valueOf(p)),
                                ByteBuffer::compareTo
                            )
                        );
                    }
                )
            )
        );

        CompletableFuture.allOf(partitionsGroupsFutures.toArray(CompletableFuture[]::new)).thenRun(() -> {
            try {
                HashMap<Integer, RaftGroupService> partitionMap = new HashMap<>(partitions);

                for (int p = 0; p < partitions; p++) {
                    CompletableFuture<RaftGroupService> future = partitionsGroupsFutures.get(p);

                    assert future.isDone();

                    RaftGroupService service = future.join();

                    partitionMap.put(p, service);
                }

                InternalTableImpl internalTable = new InternalTableImpl(name, tblId, partitionMap, partitions);

                var schemaRegistry = new SchemaRegistryImpl(v -> schemaDesc);

                schemaRegistry.onSchemaRegistered(schemaDesc);

                var table = new TableImpl(
                    internalTable,
                    schemaRegistry,
                    TableManager.this
                );

                tables.put(name, table);
                tablesById.put(tblId, table);

                fireEvent(TableEvent.CREATE, new TableEventParameters(table), null);
            }
            catch (Exception e) {
                fireEvent(TableEvent.CREATE, new TableEventParameters(tblId, name), e);
            }
        });
    }

    /**
     * Drops local structures for a table.
     *
     * @param name Table name.
     * @param tblId Table id.
     * @param assignment Affinity assignment.
     */
    private void dropTableLocally(String name, IgniteUuid tblId, List<List<ClusterNode>> assignment) {
        try {
            int partitions = assignment.size();

            for (int p = 0; p < partitions; p++)
                raftMgr.stopRaftGroup(raftGroupName(tblId, p), assignment.get(p));

            TableImpl table = tables.get(name);

            assert table != null : "There is no table with the name specified [name=" + name + ']';

            tables.remove(name);
            tablesById.remove(table.tableId());

            fireEvent(TableEvent.DROP, new TableEventParameters(table), null);
        }
        catch (Exception e) {
            fireEvent(TableEvent.DROP, new TableEventParameters(tblId, name), e);
        }
    }

    /**
     * Compounds a RAFT group unique name.
     *
     * @param tblId Table identifier.
     * @param partition Number of table partitions.
     * @return A RAFT group name.
     */
    @NotNull private String raftGroupName(IgniteUuid tblId, int partition) {
        return tblId + "_part_" + partition;
    }

    /** {@inheritDoc} */
    @Override public Table createTable(String name, Consumer<TableChange> tableInitChange) {
        return createTableAsync(name, tableInitChange).join();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Table> createTableAsync(String name, Consumer<TableChange> tableInitChange) {
        return createTableAsync(name, tableInitChange, true);
    }

    /** {@inheritDoc} */
    @Override public Table createTableIfNotExists(String name, Consumer<TableChange> tableInitChange) {
        return createTableIfNotExistsAsync(name, tableInitChange).join();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Table> createTableIfNotExistsAsync(String name, Consumer<TableChange> tableInitChange) {
        return createTableAsync(name, tableInitChange, false);
    }

    /**
     * Creates a new table with the specified name or returns an existing table with the same name.
     *
     * @param name Table name.
     * @param tableInitChange Table configuration.
     * @param exceptionWhenExist If the value is {@code true}, an exception will be thrown when the table already exists,
     * {@code false} means the existing table will be returned.
     * @return A table instance.
     */
    private CompletableFuture<Table> createTableAsync(
        String name,
        Consumer<TableChange> tableInitChange,
        boolean exceptionWhenExist
    ) {
        CompletableFuture<Table> tblFut = new CompletableFuture<>();

        tableAsync(name, true).thenAccept(tbl -> {
            if (tbl != null) {
                if (exceptionWhenExist) {
                    tblFut.completeExceptionally(new IgniteInternalCheckedException(
                        LoggerMessageHelper.format("Table already exists [name={}]", name)));
                }
                else
                    tblFut.complete(tbl);
            }
            else {
                IgniteUuid tblId = TABLE_ID_GENERATOR.randomUuid();

                EventListener<TableEventParameters> clo = new EventListener<>() {
                    @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable e) {
                        IgniteUuid notificationTblId = parameters.tableId();

                        if (!tblId.equals(notificationTblId))
                            return false;

                        if (e == null)
                            tblFut.complete(parameters.table());
                        else
                            tblFut.completeExceptionally(e);

                        return true;
                    }

                    @Override public void remove(@NotNull Throwable e) {
                        tblFut.completeExceptionally(e);
                    }
                };

                listen(TableEvent.CREATE, clo);

                tablesCfg.tables()
                    .change(
                        change -> change.create(
                            name,
                            (ch) -> {
                                tableInitChange.accept(ch);
                                ((ExtendedTableChange)ch).
                                    // Table id specification.
                                        changeId(tblId.toString()).
                                    // Affinity assignments calculation.
                                        changeAssignments(
                                        ByteUtils.toBytes(
                                            AffinityUtils.calculateAssignments(
                                                baselineMgr.nodes(),
                                                ch.partitions(),
                                                ch.replicas()
                                            )
                                        )
                                    ).
                                    // Table schema preparation.
                                        changeSchemas(
                                        schemasCh -> schemasCh.create(
                                            String.valueOf(INITIAL_SCHEMA_VERSION),
                                            schemaCh -> schemaCh.changeSchema(
                                                ByteUtils.toBytes(
                                                    SchemaUtils.prepareSchemaDescriptor(
                                                        ((ExtendedTableView)ch).schemas().size(),
                                                        ch
                                                    )
                                                )
                                            )
                                        )
                                    );
                            }
                        )
                    )
                    .exceptionally(t -> {
                        LOG.error(LoggerMessageHelper.format("Table wasn't created [name={}]", name), t);

                        removeListener(TableEvent.CREATE, clo, new IgniteInternalCheckedException(t));

                        return null;
                    });
            }
        });

        return tblFut;
    }

    /** {@inheritDoc} */
    @Override public void alterTable(String name, Consumer<TableChange> tableChange) {
        alterTableAsync(name, tableChange).join();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> alterTableAsync(String name, Consumer<TableChange> tableChange) {
        CompletableFuture<Void> tblFut = new CompletableFuture<>();

        tableAsync(name, true).thenAccept(tbl -> {
            if (tbl == null) {
                tblFut.completeExceptionally(new IgniteException(
                    LoggerMessageHelper.format("Table [name={}] does not exist and cannot be altered", name)));
            }
            else {
                IgniteUuid tblId = ((TableImpl) tbl).tableId();

                EventListener<TableEventParameters> clo = new EventListener<>() {
                    @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable e) {
                        IgniteUuid notificationTblId = parameters.tableId();

                        if (!tblId.equals(notificationTblId))
                            return false;

                        if (e == null)
                            tblFut.complete(null);
                        else
                            tblFut.completeExceptionally(e);

                        return true;
                    }

                    @Override public void remove(@NotNull Throwable e) {
                        tblFut.completeExceptionally(e);
                    }
                };

                listen(TableEvent.ALTER, clo);

                tablesCfg.tables()
                    .change(ch -> {
                        ch.createOrUpdate(name, tableChange);
                        ch.createOrUpdate(name, tblCh ->
                            ((ExtendedTableChange)tblCh).changeSchemas(
                                schemasCh ->
                                    schemasCh.createOrUpdate(
                                        String.valueOf(schemasCh.size() + 1),
                                        schemaCh -> {
                                            ExtendedTableView currTableView = (ExtendedTableView)tablesCfg.tables().get(name).value();

                                            SchemaDescriptor descriptor = SchemaUtils.prepareSchemaDescriptor(
                                                ((ExtendedTableView)tblCh).schemas().size(),
                                                tblCh
                                            );

                                            descriptor.columnMapping(SchemaUtils.columnMapper(
                                                tablesById.get(tblId).schemaView().schema(currTableView.schemas().size()),
                                                currTableView,
                                                descriptor,
                                                tblCh
                                            ));

                                            schemaCh.changeSchema(ByteUtils.toBytes(descriptor));
                                        }
                                    )
                            ));
                    })
                    .exceptionally(t -> {
                        LOG.error(LoggerMessageHelper.format("Table wasn't altered [name={}]", name), t);

                        removeListener(TableEvent.ALTER, clo, new IgniteInternalCheckedException(t));

                        return null;
                    });
            }
        });

        return tblFut;
    }

    /** {@inheritDoc} */
    @Override public void dropTable(String name) {
        dropTableAsync(name).join();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> dropTableAsync(String name) {
        CompletableFuture<Void> dropTblFut = new CompletableFuture<>();

        tableAsync(name, true).thenAccept(tbl -> {
            // In case of drop it's an optimization that allows not to fire drop-change-closure if there's no such
            // distributed table and the local config has lagged behind.
            if (tbl == null)
                dropTblFut.complete(null);
            else {
                EventListener<TableEventParameters> clo = new EventListener<>() {
                    @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable e) {
                        String tableName = parameters.tableName();

                        if (!name.equals(tableName))
                            return false;

                        if (e == null)
                            dropTblFut.complete(null);
                        else
                            dropTblFut.completeExceptionally(e);

                        return true;
                    }

                    @Override public void remove(@NotNull Throwable e) {
                        dropTblFut.completeExceptionally(e);
                    }
                };

                listen(TableEvent.DROP, clo);

                tablesCfg
                    .tables()
                    .change(change -> change.delete(name))
                    .exceptionally(t -> {
                        LOG.error(LoggerMessageHelper.format("Table wasn't dropped [name={}]", name), t);

                        removeListener(TableEvent.DROP, clo, new IgniteInternalCheckedException(t));

                        return null;
                    });
            }
        });

        return dropTblFut;
    }

    /** {@inheritDoc} */
    @Override public List<Table> tables() {
        return tablesAsync().join();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<List<Table>> tablesAsync() {
        var tableNames = tableNamesConfigured();
        var tableFuts = new CompletableFuture[tableNames.size()];
        var i = 0;

        for (String tblName : tableNames)
            tableFuts[i++] = tableAsync(tblName, false);

        return CompletableFuture.allOf(tableFuts).thenApply(unused -> {
            var tables = new ArrayList<Table>(tableNames.size());

            try {
                for (var fut : tableFuts) {
                    var table = fut.get();

                    if (table != null)
                        tables.add((Table) table);
                }
            } catch (Throwable t) {
                throw new CompletionException(t);
            }

            return tables;
        });
    }

    /**
     * Collects a set of table names from the distributed configuration storage.
     *
     * @return A set of table names.
     */
    // TODO: IGNITE-15412 Configuration manager will be used to retrieve distributed values
    // TODO: instead of metastorage manager. That will automatically resolve several bugs of current implementation.
    private Set<String> tableNamesConfigured() {
        IgniteBiTuple<ByteArray, ByteArray> range = toRange(new ByteArray(PUBLIC_PREFIX));

        Set<String> tableNames = new HashSet<>();

        try (Cursor<Entry> cursor = metaStorageMgr.range(range.get1(), range.get2())) {
            while (cursor.hasNext()) {
                Entry entry = cursor.next();

                List<String> keySplit = ConfigurationUtil.split(entry.key().toString());

                if (keySplit.size() == 5 && NamedListNode.NAME.equals(keySplit.get(4))) {
                    @Nullable byte[] value = entry.value();
                    if (value != null)
                        tableNames.add(ByteUtils.fromBytes(value).toString());
                }
            }
        }
        catch (Exception e) {
            LOG.error("Can't get table names.", e);
        }

        return tableNames;
    }

    /**
     * Transforms a prefix bytes to range.
     * This method should be replaced to direct call of range by prefix
     * in Meta storage manager when it will be implemented.
     *
     * @param prefixKey Prefix bytes.
     * @return Tuple with left and right borders for range.
     */
    // TODO: IGNITE-15412 Remove after implementation. Configuration manager will be used to retrieve distributed values
    // TODO: instead of metastorage manager.
    private IgniteBiTuple<ByteArray, ByteArray> toRange(ByteArray prefixKey) {
        var bytes = Arrays.copyOf(prefixKey.bytes(), prefixKey.bytes().length);

        if (bytes[bytes.length - 1] != Byte.MAX_VALUE)
            bytes[bytes.length - 1]++;
        else
            bytes = Arrays.copyOf(bytes, bytes.length + 1);

        return new IgniteBiTuple<>(prefixKey, new ByteArray(bytes));
    }

    /** {@inheritDoc} */
    @Override public Table table(String name) {
        return tableAsync(name).join();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Table> tableAsync(String name) {
        return tableAsync(name, true);
    }

    /**
     * Gets a table if it exists or {@code null} if it was not created or was removed before.
     *
     * @param id Table ID.
     * @return A table or {@code null} if table does not exist.
     */
    @Override public TableImpl table(IgniteUuid id) {
        var tbl = tablesById.get(id);

        if (tbl != null)
            return tbl;

        CompletableFuture<TableImpl> getTblFut = new CompletableFuture<>();

        EventListener<TableEventParameters> clo = new EventListener<>() {
            @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable e) {
                if (!id.equals(parameters.tableId()))
                    return false;

                if (e == null)
                    getTblFut.complete(parameters.table());
                else
                    getTblFut.completeExceptionally(e);

                return true;
            }

            @Override public void remove(@NotNull Throwable e) {
                getTblFut.completeExceptionally(e);
            }
        };

        listen(TableEvent.CREATE, clo);

        tbl = tablesById.get(id);

        if (tbl != null && getTblFut.complete(tbl) || getTblFut.complete(null))
            removeListener(TableEvent.CREATE, clo, null);

        return getTblFut.join();
    }

    /**
     * Gets a table if it exists or {@code null} if it was not created or was removed before.
     *
     * @param checkConfiguration True when the method checks a configuration before tries to get a table,
     * false otherwise.
     * @return A table or {@code null} if table does not exist.
     */
    private CompletableFuture<Table> tableAsync(String name, boolean checkConfiguration) {
        if (checkConfiguration && !isTableConfigured(name))
            return CompletableFuture.completedFuture(null);

        Table tbl = tables.get(name);

        if (tbl != null)
            return CompletableFuture.completedFuture(tbl);

        CompletableFuture<Table> getTblFut = new CompletableFuture<>();

        EventListener<TableEventParameters> clo = new EventListener<>() {
            @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable e) {
                String tableName = parameters.tableName();

                if (!name.equals(tableName))
                    return false;

                if (e == null)
                    getTblFut.complete(parameters.table());
                else
                    getTblFut.completeExceptionally(e);

                return true;
            }

            @Override public void remove(@NotNull Throwable e) {
                getTblFut.completeExceptionally(e);
            }
        };

        listen(TableEvent.CREATE, clo);

        tbl = tables.get(name);

        if (tbl != null && getTblFut.complete(tbl) ||
            !isTableConfigured(name) && getTblFut.complete(null))
            removeListener(TableEvent.CREATE, clo, null);

        return getTblFut;
    }

    /**
     * Checks that the table is configured.
     *
     * @param name Table name.
     * @return True if table configured, false otherwise.
     */
    private boolean isTableConfigured(String name) {
        return tableNamesConfigured().contains(name);
    }
}

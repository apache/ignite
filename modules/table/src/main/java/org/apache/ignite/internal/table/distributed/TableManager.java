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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DirectConfigurationProperty;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
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
import org.apache.ignite.internal.schema.marshaller.schema.SchemaSerializerImpl;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.storage.engine.DataRegion;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
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
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.manager.IgniteTables;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.configuration.schemas.store.DataStorageConfigurationSchema.DEFAULT_DATA_REGION_NAME;

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

    /** Data storage configuration. */
    private final DataStorageConfiguration dataStorageCfg;

    /** Raft manager. */
    private final Loza raftMgr;

    /** Baseline manager. */
    private final BaselineManager baselineMgr;

    // TODO: IGNITE-15412 Remove after implementation. Configuration manager will be used to retrieve distributed values
    // TODO: instead of metastorage manager.
    /** Meta storage service. */
    private final MetaStorageManager metaStorageMgr;

    /** Storage engine instance. Only one type is available right now, which is the {@link RocksDbStorageEngine}. */
    private final StorageEngine engine;

    /** Partitions store directory. */
    private final Path partitionsStoreDir;

    /** Tables. */
    private final Map<String, TableImpl> tables = new ConcurrentHashMap<>();

    /** Tables. */
    private final Map<IgniteUuid, TableImpl> tablesById = new ConcurrentHashMap<>();

    /** Resolver that resolves a network address to node id. */
    private final Function<NetworkAddress, String> netAddrResolver;

    /** Data region instances. */
    private final Map<String, DataRegion> dataRegions = new ConcurrentHashMap<>();

    //TODO: IGNITE-15161 These should go into TableImpl instances.
    /** Instances of table storages that need to be stopped on component stop. */
    private final Map<IgniteUuid, TableStorage> tableStorages = new ConcurrentHashMap<>();

    /**
     * Creates a new table manager.
     *
     * @param tablesCfg Tables configuration.
     * @param dataStorageCfg Data storage configuration.
     * @param raftMgr Raft manager.
     * @param baselineMgr Baseline manager.
     * @param metaStorageMgr Meta storage manager.
     * @param partitionsStoreDir Partitions store directory.
     */
    public TableManager(
        TablesConfiguration tablesCfg,
        DataStorageConfiguration dataStorageCfg,
        Loza raftMgr,
        BaselineManager baselineMgr,
        TopologyService topologyService,
        MetaStorageManager metaStorageMgr,
        Path partitionsStoreDir
    ) {
        this.tablesCfg = tablesCfg;
        this.dataStorageCfg = dataStorageCfg;
        this.raftMgr = raftMgr;
        this.baselineMgr = baselineMgr;
        this.metaStorageMgr = metaStorageMgr;
        this.partitionsStoreDir = partitionsStoreDir;

        netAddrResolver = addr -> {
            ClusterNode node = topologyService.getByAddress(addr);

            if (node == null)
                throw new IllegalStateException("Can't resolve ClusterNode by its networkAddress=" + addr);

            return node.id();
        };

        engine = new RocksDbStorageEngine();
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
                                    onSchemaRegistered(
                                        SchemaSerializerImpl.INSTANCE.deserialize((schemasCtx.newValue().schema()))
                                    );

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

                ((ExtendedTableConfiguration)tablesCfg.tables().get(ctx.newValue().name())).assignments().
                    listen(assignmentsCtx -> {
                        List<List<ClusterNode>> oldAssignments =
                            (List<List<ClusterNode>>)ByteUtils.fromBytes(assignmentsCtx.oldValue());

                        List<List<ClusterNode>> newAssignments =
                            (List<List<ClusterNode>>)ByteUtils.fromBytes(assignmentsCtx.newValue());

                        CompletableFuture<?>[] futures = new CompletableFuture<?>[oldAssignments.size()];

                        // TODO: IGNITE-15554 Add logic for assignment recalculation in case of partitions or replicas changes
                        // TODO: Until IGNITE-15554 is implemented it's safe to iterate over partitions and replicas cause there will
                        // TODO: be exact same amount of partitions and replicas for both old and new assignments
                        for (int i = 0; i < oldAssignments.size(); i++) {
                            final int p = i;

                            List<ClusterNode> oldPartitionAssignment = oldAssignments.get(p);
                            List<ClusterNode> newPartitionAssignment = newAssignments.get(p);

                            var toAdd = new HashSet<>(newPartitionAssignment);
                            var toRemove = new HashSet<>(oldPartitionAssignment);

                            toAdd.removeAll(oldPartitionAssignment);
                            toRemove.removeAll(newPartitionAssignment);

                            // Create new raft nodes according to new assignments.
                            futures[i] = raftMgr.updateRaftGroup(
                                raftGroupName(tblId, p),
                                newPartitionAssignment,
                                toAdd,
                                () -> new PartitionListener(tableStorages.get(tblId).getOrCreatePartition(p))
                            )
                                .thenAccept(
                                    updatedRaftGroupService -> tables.get(ctx.newValue().name()).updateInternalTableRaftGroupService(p, updatedRaftGroupService)
                                ).thenRun(() -> {
                                    if (raftMgr.stopRaftGroup(raftGroupName(tblId, p), new ArrayList<>(toRemove)))
                                        tableStorages.get(tblId).dropPartition(p);
                                }).exceptionally(th -> {
                                        LOG.error("Failed to update raft groups one the node", th);
                                        return null;
                                    }
                                );
                        }

                        return CompletableFuture.allOf(futures);
                    });

                createTableLocally(
                    ctx.newValue().name(),
                    IgniteUuid.fromString(((ExtendedTableView)ctx.newValue()).id()),
                    (List<List<ClusterNode>>)ByteUtils.fromBytes(((ExtendedTableView)ctx.newValue()).assignments()),
                    SchemaSerializerImpl.INSTANCE.deserialize(((ExtendedTableView)ctx.newValue()).schemas().
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

        DataRegion defaultDataRegion = engine.createDataRegion(dataStorageCfg.defaultRegion());

        dataRegions.put(DEFAULT_DATA_REGION_NAME, defaultDataRegion);

        defaultDataRegion.start();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // Get view for table configurations to get assignments from.
        NamedListView<TableView> tablesView = tablesCfg.tables().value();

        // Iterate through all existing tables.
        for (Map.Entry<String, TableImpl> entry : tables.entrySet()) {
            String tblName = entry.getKey();
            TableImpl table = entry.getValue();

            byte[] assignmentsBytes = ((ExtendedTableView)tablesView.get(tblName)).assignments();

            var assignment = (List<List<ClusterNode>>)ByteUtils.fromBytes(assignmentsBytes);

            // Stop all currently running RAFT groups.
            for (int p = 0; p < assignment.size(); p++)
                raftMgr.stopRaftGroup(raftGroupName(table.tableId(), p), assignment.get(p));
        }

        // Stop all table storages when all RAFT groups are already stopped.
        for (TableStorage tableStorage : tableStorages.values()) {
            try {
                tableStorage.stop();
            }
            catch (Exception e) {
                LOG.error("Failed to stop table storage " + tableStorage, e);
            }
        }

        // Stop all data regions when all table storages are stopped.
        for (Map.Entry<String, DataRegion> entry : dataRegions.entrySet()) {
            try {
                entry.getValue().stop();
            }
            catch (Exception e) {
                LOG.error("Failed to stop data region " + entry.getKey(), e);
            }
        }
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

        TableConfiguration tableCfg = tablesCfg.tables().get(name);

        DataRegion dataRegion = dataRegions.computeIfAbsent(tableCfg.dataRegion().value(), dataRegionName -> {
            DataRegion newDataRegion = engine.createDataRegion(dataStorageCfg.regions().get(dataRegionName));

            try {
                newDataRegion.start();
            }
            catch (Exception e) {
                try {
                    newDataRegion.stop();
                }
                catch (Exception stopException) {
                    e.addSuppressed(stopException);
                }

                throw e;
            }

            return newDataRegion;
        });

        TableStorage tableStorage = engine.createTable(
            storageDir,
            tableCfg,
            dataRegion,
            (tableCfgView, indexName) -> {
                throw new UnsupportedOperationException("Not implemented yet.");
            }
        );

        tableStorage.start();

        tableStorages.put(tblId, tableStorage);

        for (int p = 0; p < partitions; p++) {
            int partId = p;

            partitionsGroupsFutures.add(
                raftMgr.prepareRaftGroup(
                    raftGroupName(tblId, p),
                    assignment.get(p),
                    () -> new PartitionListener(tableStorage.getOrCreatePartition(partId))
                )
            );
        }

        CompletableFuture.allOf(partitionsGroupsFutures.toArray(CompletableFuture[]::new)).thenRun(() -> {
            try {
                HashMap<Integer, RaftGroupService> partitionMap = new HashMap<>(partitions);

                for (int p = 0; p < partitions; p++) {
                    CompletableFuture<RaftGroupService> future = partitionsGroupsFutures.get(p);

                    assert future.isDone();

                    RaftGroupService service = future.join();

                    partitionMap.put(p, service);
                }

                InternalTableImpl internalTable = new InternalTableImpl(name, tblId, partitionMap, partitions, netAddrResolver);

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
            tablesById.remove(tblId);

            TableStorage tableStorage = tableStorages.get(tblId);

            tableStorage.destroy();

            tableStorages.remove(tblId);

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
                                                SchemaSerializerImpl.INSTANCE.serialize(
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

                                            schemaCh.changeSchema(SchemaSerializerImpl.INSTANCE.serialize(descriptor));
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

    /** {@inheritDoc} */
    @Override public TableImpl table(IgniteUuid id) {
        return tableAsync(id).join();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<TableImpl> tableAsync(IgniteUuid id) {
        if (!isTableConfigured(id))
            return CompletableFuture.completedFuture(null);

        var tbl = tablesById.get(id);

        if (tbl != null)
            return CompletableFuture.completedFuture(tbl);

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

        if (tbl != null && getTblFut.complete(tbl) ||
            !isTableConfigured(id) && getTblFut.complete(null))
            removeListener(TableEvent.CREATE, clo, null);

        return getTblFut;
    }

    /**
     * Checks that the table is configured with specific id.
     *
     * @param id Table id.
     * @return True when the table is configured into cluster, false otherwise.
     */
    private boolean isTableConfigured(IgniteUuid id) {
        NamedListView<TableView> directTablesCfg = ((DirectConfigurationProperty<NamedListView<TableView>>)tablesCfg.tables()).directValue();

        // TODO: IGNITE-15721 Need to review this approach after the ticket would be fixed.
        // Probably, it won't be required getting configuration of all tables from Metastor.
        for (String name : directTablesCfg.namedListKeys()) {
            ExtendedTableView tView = (ExtendedTableView)directTablesCfg.get(name);

            if (tView != null && id.equals(IgniteUuid.fromString(tView.id())))
                return true;
        }

        return false;
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

    /**
     * @see Ignite#setBaseline(Set)
     */
    public void setBaseline(Set<String> nodes) {
        if (nodes == null || nodes.isEmpty())
            throw new IgniteException("New baseline can't be null or empty");

        var currClusterMembers = new HashSet<>(baselineMgr.nodes());

        var currClusterMemberNames =
            currClusterMembers.stream().map(ClusterNode::name).collect(Collectors.toSet());

        for (String nodeName: nodes) {
            if (!currClusterMemberNames.contains(nodeName))
                throw new IgniteException("Node '" + nodeName + "' not in current network cluster membership. " +
                    " Adding not alive nodes is not supported yet.");
        }

        var newBaseline = currClusterMembers
            .stream().filter(n -> nodes.contains(n.name())).collect(Collectors.toSet());

        updateAssignments(currClusterMembers);

        if (!newBaseline.equals(currClusterMembers))
            updateAssignments(newBaseline);
    }

    /**
     * Update assignments for all current tables according to input nodes list.
     * These approach has known issues {@link Ignite#setBaseline(Set)}.
     *
     * @param clusterNodes Set of nodes for assignment.
     */
    private void updateAssignments(Set<ClusterNode> clusterNodes) {
        var setBaselineFut = new CompletableFuture<>();

        var changePeersQueue = new ArrayList<Supplier<CompletableFuture<Void>>>();

        tablesCfg.tables().change(
            tbls -> {
                changePeersQueue.clear();
                for (int i = 0; i < tbls.size(); i++) {
                    tbls.createOrUpdate(tbls.get(i).name(), changeX -> {
                        ExtendedTableChange change = (ExtendedTableChange)changeX;
                        byte[] currAssignments = change.assignments();

                        List<List<ClusterNode>> recalculatedAssignments = AffinityUtils.calculateAssignments(
                            clusterNodes,
                            change.partitions(),
                            change.replicas());

                        if (!recalculatedAssignments.equals(ByteUtils.fromBytes(currAssignments))) {
                            change.changeAssignments(ByteUtils.toBytes(recalculatedAssignments));
                            changePeersQueue.add(() ->
                                updateRaftTopology(
                                    (List<List<ClusterNode>>)ByteUtils.fromBytes(currAssignments),
                                    recalculatedAssignments,
                                    IgniteUuid.fromString(change.id())));
                        }
                    });
                }
            }).thenCompose((v) -> {
                CompletableFuture<?>[] changePeersFutures = new CompletableFuture<?>[changePeersQueue.size()];

                int i = 0;
                for (Supplier<CompletableFuture<Void>> task: changePeersQueue) {
                    changePeersFutures[i++] = task.get();
                }

                return CompletableFuture.allOf(changePeersFutures);
            }).whenComplete((res, th) -> {
                if (th != null)
                    setBaselineFut.completeExceptionally(th);
                else
                    setBaselineFut.complete(null);
            });

        setBaselineFut.join();
    }

    /**
     * Update raft groups of table partitions to new peers list.
     *
     * @param oldAssignments Old assignment.
     * @param newAssignments New assignment.
     * @param tblId Table ID.
     * @return Future, which completes, when update finished.
     */
    private CompletableFuture<Void> updateRaftTopology(
        List<List<ClusterNode>> oldAssignments,
        List<List<ClusterNode>> newAssignments,
        IgniteUuid tblId) {
        CompletableFuture<?>[] futures = new CompletableFuture<?>[oldAssignments.size()];

        // TODO: IGNITE-15554 Add logic for assignment recalculation in case of partitions or replicas changes
        // TODO: Until IGNITE-15554 is implemented it's safe to iterate over partitions and replicas cause there will
        // TODO: be exact same amount of partitions and replicas for both old and new assignments
        for (int i = 0; i < oldAssignments.size(); i++) {
            final int p = i;

            List<ClusterNode> oldPartitionAssignment = oldAssignments.get(p);
            List<ClusterNode> newPartitionAssignment = newAssignments.get(p);

            var toAdd = new HashSet<>(newPartitionAssignment);

            toAdd.removeAll(oldPartitionAssignment);

            futures[i] = raftMgr.prepareRaftGroup(
                raftGroupName(tblId, p),
                oldPartitionAssignment,
                () -> new RaftGroupListener() {
                    @Override public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {

                    }

                    @Override public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {

                    }

                    @Override public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {

                    }

                    @Override public boolean onSnapshotLoad(Path path) {
                        return false;
                    }

                    @Override public void onShutdown() {

                    }
                },
                60000,
                10000
            )
                .thenCompose(
                    updatedRaftGroupService -> {
                        return
                            updatedRaftGroupService.
                                changePeers(
                                    newPartitionAssignment.stream().map(n -> new Peer(n.address())).collect(Collectors.toList()));
                    }
                ).exceptionally(th -> {
                        LOG.error("Failed to update raft peers for group " + raftGroupName(tblId, p) +
                            "from " + oldPartitionAssignment + " to " + newPartitionAssignment, th);
                        return null;
                    }
                );
        }

        return CompletableFuture.allOf(futures);
    }
}

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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.affinity.AffinityManager;
import org.apache.ignite.internal.affinity.event.AffinityEvent;
import org.apache.ignite.internal.affinity.event.AffinityEventParameters;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Conditions;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.metastorage.client.Operations;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaModificationException;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.event.SchemaEvent;
import org.apache.ignite.internal.schema.event.SchemaEventParameters;
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
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.schema.PrimaryIndex;
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

    /** Internal prefix for the metasorage. */
    private static final String INTERNAL_PREFIX = "internal.tables.";

    /** Public prefix for metastorage. */
    private static final String PUBLIC_PREFIX = "dst-cfg.table.tables.";

    /** Meta storage service. */
    private final MetaStorageManager metaStorageMgr;

    /** Configuration manager. */
    private final ConfigurationManager configurationMgr;

    /** Raft manager. */
    private final Loza raftMgr;

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /** Affinity manager. */
    private final AffinityManager affMgr;

    /** Partitions store directory. */
    private final Path partitionsStoreDir;

    /** Tables. */
    private final Map<String, TableImpl> tables = new ConcurrentHashMap<>();

    /** Tables. */
    private final Map<UUID, TableImpl> tablesById = new ConcurrentHashMap<>();

    /**
     * Creates a new table manager.
     *
     * @param configurationMgr Configuration manager.
     * @param metaStorageMgr Meta storage manager.
     * @param schemaMgr Schema manager.
     * @param affMgr Affinity manager.
     * @param raftMgr Raft manager.
     * @param partitionsStoreDir Partitions store directory.
     */
    public TableManager(
        ConfigurationManager configurationMgr,
        MetaStorageManager metaStorageMgr,
        SchemaManager schemaMgr,
        AffinityManager affMgr,
        Loza raftMgr,
        Path partitionsStoreDir
    ) {
        this.configurationMgr = configurationMgr;
        this.metaStorageMgr = metaStorageMgr;
        this.affMgr = affMgr;
        this.raftMgr = raftMgr;
        this.schemaMgr = schemaMgr;
        this.partitionsStoreDir = partitionsStoreDir;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        //TODO: IGNITE-14652 Change a metastorage update in listeners to multi-invoke
        configurationMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables().listen(ctx -> {
            return onConfigurationChanged(ctx.storageRevision(), ctx.oldValue(), ctx.newValue());
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
     * @param schemaReg Schema registry for the table.
     */
    private void createTableLocally(
        String name,
        UUID tblId,
        List<List<ClusterNode>> assignment,
        SchemaRegistry schemaReg
    ) {
        int partitions = assignment.size();

        var partitionsGroupsFutures = new ArrayList<CompletableFuture<RaftGroupService>>();

        Path storageDir = partitionsStoreDir.resolve(name);

        try {
            Files.createDirectories(storageDir);
        } catch (IOException e) {
            throw new IgniteInternalException(
                "Failed to create partitions store directory for " + name + ": " + e.getMessage(),
                e
            );
        }

        for (int p = 0; p < partitions; p++) {
            RocksDbStorage storage = new RocksDbStorage(
                storageDir.resolve(String.valueOf(p)),
                ByteBuffer::compareTo
            );

            partitionsGroupsFutures.add(raftMgr.prepareRaftGroup(
                raftGroupName(tblId, p),
                assignment.get(p),
                new PartitionListener(storage)
            ));
        }

        CompletableFuture.allOf(partitionsGroupsFutures.toArray(CompletableFuture[]::new)).thenRun(() -> {
            HashMap<Integer, RaftGroupService> partitionMap = new HashMap<>(partitions);

            for (int p = 0; p < partitions; p++) {
                CompletableFuture<RaftGroupService> future = partitionsGroupsFutures.get(p);

                assert future.isDone();

                RaftGroupService service = future.join();

                partitionMap.put(p, service);
            }

            InternalTableImpl internalTable = new InternalTableImpl(name, tblId, partitionMap, partitions);

            var table = new TableImpl(internalTable, schemaReg, this, null);

            tables.put(name, table);
            tablesById.put(table.tableId(), table);

            onEvent(TableEvent.CREATE, new TableEventParameters(table), null);
        });
    }

    /**
     * Drops local structures for a table.
     *
     * @param name Table name.
     * @param tblId Table id.
     * @param assignment Affinity assignment.
     */
    private void dropTableLocally(String name, UUID tblId, List<List<ClusterNode>> assignment) {
        int partitions = assignment.size();

        for (int p = 0; p < partitions; p++)
            raftMgr.stopRaftGroup(raftGroupName(tblId, p), assignment.get(p));

        TableImpl table = tables.get(name);

        assert table != null : "There is no table with the name specified [name=" + name + ']';

        onEvent(TableEvent.DROP, new TableEventParameters(table), null);
    }

    /**
     * Compounds a RAFT group unique name.
     *
     * @param tableId Table identifier.
     * @param partition Number of table partitions.
     * @return A RAFT group name.
     */
    @NotNull private String raftGroupName(UUID tableId, int partition) {
        return tableId + "_part_" + partition;
    }

    /**
     * Table configuration changed callback.
     *
     * @param rev Storage revision.
     * @param oldCfg Old configuration.
     * @param newCfg New configuration.
     * @return Operation future.
     */
    @NotNull private CompletableFuture<?> onConfigurationChanged(
        long rev,
        @Nullable NamedListView<TableView> oldCfg,
        @Nullable NamedListView<TableView> newCfg
    ) {
        Set<String> tablesToStart = (newCfg == null || newCfg.namedListKeys() == null) ?
            Collections.emptySet() :
            newCfg.namedListKeys().stream().filter(t -> !oldCfg.namedListKeys().contains(t)).collect(Collectors.toSet());

        Set<String> tablesToStop = (oldCfg == null || oldCfg.namedListKeys() == null) ?
            Collections.emptySet() :
            oldCfg.namedListKeys().stream().filter(t -> !newCfg.namedListKeys().contains(t)).collect(Collectors.toSet());

        List<CompletableFuture<Boolean>> futs = new ArrayList<>();

        final Set<String> schemaChanged =
            (oldCfg != null && oldCfg.namedListKeys() != null && newCfg != null && newCfg.namedListKeys() != null) ?
                oldCfg.namedListKeys().stream()
                    .filter(tblName -> newCfg.namedListKeys().contains(tblName)) // Filter changed tables.
                    .filter(tblName -> {
                        final TableView newTbl = newCfg.get(tblName);
                        final TableView oldTbl = oldCfg.get(tblName);

                        assert newTbl.columns().namedListKeys() != null && oldTbl.columns().namedListKeys() != null;

                        if (!newTbl.columns().namedListKeys().equals(oldTbl.columns().namedListKeys()))
                            return true;

                        return newTbl.columns().namedListKeys().stream().anyMatch(k -> {
                            final ColumnView newCol = newTbl.columns().get(k);
                            final ColumnView oldCol = oldTbl.columns().get(k);

                            assert oldCol != null;

                            if (!Objects.equals(newCol.type(), oldCol.type()))
                                throw new SchemaModificationException("Columns type change is not supported.");

                            if (!Objects.equals(newCol.nullable(), oldCol.nullable()))
                                throw new SchemaModificationException("Column nullability change is not supported");

                            if (!Objects.equals(newCol.name(), oldCol.name()) &&
                                oldTbl.indices().namedListKeys().stream()
                                    .map(n -> oldTbl.indices().get(n))
                                    .filter(idx -> PrimaryIndex.PRIMARY_KEY_INDEX_NAME.equals(idx.name()))
                                    .anyMatch(idx -> idx.columns().namedListKeys().stream()
                                        .anyMatch(c -> idx.columns().get(c).name().equals(oldCol.name()))
                                    ))
                                throw new SchemaModificationException("Key column rename is not supported");

                            return !Objects.equals(newCol.name(), oldCol.name()) ||
                                !Objects.equals(newCol.defaultValue(), oldCol.defaultValue());
                        });
                    }).collect(Collectors.toSet()) :
                Collections.emptySet();

        if (!tablesToStart.isEmpty())
            futs.addAll(startTables(tablesToStart, rev, newCfg));

        if (!schemaChanged.isEmpty())
            futs.addAll(changeSchema(schemaChanged, oldCfg, newCfg));

        if (!tablesToStop.isEmpty())
            futs.addAll(stopTables(tablesToStop));

        return CompletableFuture.allOf(futs.toArray(CompletableFuture[]::new));
    }

    /**
     * Start tables routine.
     *
     * @param tbls Tables to start.
     * @param rev Metastore revision.
     * @param cfgs Table configurations.
     * @return Table creation futures.
     */
    private List<CompletableFuture<Boolean>> startTables(Set<String> tbls, long rev, NamedListView<TableView> cfgs) {
        boolean hasMetastorageLocally = metaStorageMgr.hasMetastorageLocally(configurationMgr);

        List<CompletableFuture<Boolean>> futs = new ArrayList<>();

        for (String tblName : tbls) {
            TableView tableView = cfgs.get(tblName);

            UUID tblId = new UUID(rev, 0L);

            if (hasMetastorageLocally) {
                var key = new ByteArray(INTERNAL_PREFIX + tblId);
                futs.add(metaStorageMgr.invoke(
                    Conditions.notExists(key),
                    Operations.put(key, tableView.name().getBytes(StandardCharsets.UTF_8)),
                    Operations.noop())
                    .thenCompose(res -> schemaMgr.initSchemaForTable(tblId, tableView.name()))
                    .thenCompose(res -> affMgr.calculateAssignments(tblId, tableView.name())));
            }

            final CompletableFuture<AffinityEventParameters> affinityReadyFut = new CompletableFuture<>();
            final CompletableFuture<SchemaEventParameters> schemaReadyFut = new CompletableFuture<>();

            CompletableFuture.allOf(affinityReadyFut, schemaReadyFut)
                .exceptionally(e -> {
                    LOG.error("Failed to create a new table [name=" + tblName + ", id=" + tblId + ']', e);

                    onEvent(TableEvent.CREATE, new TableEventParameters(tblId, tblName), e);

                    return null;
                })
                .thenRun(() -> createTableLocally(
                    tblName,
                    tblId,
                    affinityReadyFut.join().assignment(),
                    schemaReadyFut.join().schemaRegistry()
                ));

            affMgr.listen(AffinityEvent.CALCULATED, new EventListener<>() {
                @Override public boolean notify(@NotNull AffinityEventParameters parameters, @Nullable Throwable e) {
                    if (!tblId.equals(parameters.tableId()))
                        return false;

                    if (e == null)
                        affinityReadyFut.complete(parameters);
                    else
                        affinityReadyFut.completeExceptionally(e);

                    return true;
                }

                @Override public void remove(@NotNull Throwable e) {
                    affinityReadyFut.completeExceptionally(e);
                }
            });

            schemaMgr.listen(SchemaEvent.INITIALIZED, new EventListener<>() {
                @Override public boolean notify(@NotNull SchemaEventParameters parameters, @Nullable Throwable e) {
                    if (!tblId.equals(parameters.tableId()) && parameters.schemaRegistry().lastSchemaVersion() >= 1)
                        return false;

                    if (e == null)
                        schemaReadyFut.complete(parameters);
                    else
                        schemaReadyFut.completeExceptionally(e);

                    return true;
                }

                @Override public void remove(@NotNull Throwable e) {
                    schemaReadyFut.completeExceptionally(e);
                }
            });
        }

        return futs;
    }

    /**
     * Drop tables routine.
     *
     * @param tbls Tables to drop.
     * @return Table drop futures.
     */
    private List<CompletableFuture<Boolean>> stopTables(Set<String> tbls) {
        boolean hasMetastorageLocally = metaStorageMgr.hasMetastorageLocally(configurationMgr);

        List<CompletableFuture<Boolean>> futs = new ArrayList<>();

        for (String tblName : tbls) {
            TableImpl t = tables.get(tblName);

            UUID tblId = t.tableId();

            if (hasMetastorageLocally) {
                var key = new ByteArray(INTERNAL_PREFIX + tblId);

                futs.add(affMgr.removeAssignment(tblId)
                    .thenCompose(res -> schemaMgr.unregisterSchemas(tblId))
                    .thenCompose(res ->
                        metaStorageMgr.invoke(Conditions.exists(key),
                            Operations.remove(key),
                            Operations.noop())));
            }

            affMgr.listen(AffinityEvent.REMOVED, new EventListener<>() {
                @Override public boolean notify(@NotNull AffinityEventParameters parameters, @Nullable Throwable e) {
                    if (!tblId.equals(parameters.tableId()))
                        return false;

                    if (e == null)
                        dropTableLocally(tblName, tblId, parameters.assignment());
                    else
                        onEvent(TableEvent.DROP, new TableEventParameters(tblId, tblName), e);

                    return true;
                }

                @Override public void remove(@NotNull Throwable e) {
                    onEvent(TableEvent.DROP, new TableEventParameters(tblId, tblName), e);
                }
            });
        }

        return futs;
    }

    /**
     * Change tables schemas.
     *
     * @param tbls Tables.
     * @param oldCfg Old configuration.
     * @param newCfg New configuration.
     * @return Schema change futures.
     */
    private List<CompletableFuture<Boolean>> changeSchema(
        Set<String> tbls,
        @NotNull NamedListView<TableView> oldCfg,
        @NotNull NamedListView<TableView> newCfg
    ) {
        boolean hasMetastorageLocally = metaStorageMgr.hasMetastorageLocally(configurationMgr);

        List<CompletableFuture<Boolean>> futs = new ArrayList<>();

        for (String tblName : tbls) {
            TableImpl tbl = tables.get(tblName);

            UUID tblId = tbl.tableId();

            final int ver = tbl.schemaView().lastSchemaVersion() + 1;

            if (hasMetastorageLocally)
                futs.add(schemaMgr.updateSchemaForTable(tblId, oldCfg.get(tblName), newCfg.get(tblName)));

            final CompletableFuture<SchemaEventParameters> schemaReadyFut = new CompletableFuture<>();

            CompletableFuture.allOf(schemaReadyFut)
                .exceptionally(e -> {
                    LOG.error("Failed to upgrade schema for a table [name=" + tblName + ", id=" + tblId + ']', e);

                    onEvent(TableEvent.ALTER, new TableEventParameters(tblId, tblName), e);

                    return null;
                })
                .thenRun(() ->
                    onEvent(TableEvent.ALTER, new TableEventParameters(tblId, tblName), null)
                );

            schemaMgr.listen(SchemaEvent.CHANGED, new EventListener<>() {
                @Override public boolean notify(@NotNull SchemaEventParameters parameters, @Nullable Throwable e) {
                    if (!tblId.equals(parameters.tableId()) && parameters.schemaRegistry().lastSchemaVersion() < ver)
                        return false;

                    if (e == null)
                        schemaReadyFut.complete(parameters);
                    else
                        schemaReadyFut.completeExceptionally(e);

                    return true;
                }

                @Override public void remove(@NotNull Throwable e) {
                    schemaReadyFut.completeExceptionally(e);
                }
            });
        }

        return futs;
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
    @Override public Table getOrCreateTable(String name, Consumer<TableChange> tableInitChange) {
        return getOrCreateTableAsync(name, tableInitChange).join();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Table> getOrCreateTableAsync(String name, Consumer<TableChange> tableInitChange) {
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
    private CompletableFuture<Table> createTableAsync(String name, Consumer<TableChange> tableInitChange, boolean exceptionWhenExist) {
        CompletableFuture<Table> tblFut = new CompletableFuture<>();

        EventListener<TableEventParameters> clo = new EventListener<>() {
            @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable e) {
                String tableName = parameters.tableName();

                if (!name.equals(tableName))
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

        tableAsync(name, true).thenAccept(tbl -> {
            if (tbl != null) {
                if (exceptionWhenExist) {
                    removeListener(TableEvent.CREATE, clo, new IgniteInternalCheckedException(
                            LoggerMessageHelper.format("Table already exists [name={}]", name)));
                } else if (tblFut.complete(tbl))
                    removeListener(TableEvent.CREATE, clo);
            } else {
                try {
                    configurationMgr
                            .configurationRegistry()
                            .getConfiguration(TablesConfiguration.KEY)
                            .tables()
                            .change(change -> change.create(name, tableInitChange))
                            .get();
                } catch (InterruptedException | ExecutionException e) {
                    LOG.error("Table wasn't created [name=" + name + ']', e);

                    removeListener(TableEvent.CREATE, clo, new IgniteInternalCheckedException(e));
                }
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

        listen(TableEvent.ALTER, new EventListener<>() {
            @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable e) {
                String tableName = parameters.tableName();

                if (!name.equals(tableName))
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
        });

        try {
            configurationMgr.configurationRegistry()
                    .getConfiguration(TablesConfiguration.KEY).tables().change(change ->
                    change.createOrUpdate(name, tableChange)).get();
        }
        catch (InterruptedException | ExecutionException e) {
            LOG.error("Table wasn't created [name=" + name + ']', e);

            tblFut.completeExceptionally(e);
        }

        return tblFut;
    }

    /** {@inheritDoc} */
    @Override public void dropTable(String name) {
        dropTableAsync(name).join();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> dropTableAsync(String name) {
        CompletableFuture<Void> dropTblFut = new CompletableFuture<>();

        EventListener<TableEventParameters> clo = new EventListener<>() {
            @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable e) {
                String tableName = parameters.tableName();

                if (!name.equals(tableName))
                    return false;

                if (e == null) {
                    TableImpl droppedTable = tables.remove(tableName);

                    assert droppedTable != null;

                    tablesById.remove(droppedTable.tableId());

                    dropTblFut.complete(null);
                }
                else
                    dropTblFut.completeExceptionally(e);

                return true;
            }

            @Override public void remove(@NotNull Throwable e) {
                dropTblFut.completeExceptionally(e);
            }
        };

        listen(TableEvent.DROP, clo);

        if (!isTableConfigured(name)) {
            if (dropTblFut.complete(null))
                removeListener(TableEvent.DROP, clo, null);
        }
        else {
            try {
                configurationMgr
                        .configurationRegistry()
                        .getConfiguration(TablesConfiguration.KEY)
                        .tables()
                        .change(change -> change.delete(name))
                        .get();
            }
            catch (InterruptedException | ExecutionException e) {
                LOG.error("Table wasn't dropped [name=" + name + ']', e);

                removeListener(TableEvent.DROP, clo, new IgniteInternalCheckedException(e));
            }
        }

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
    //TODO This is an egregious violation of encapsulation. Current approach has to be revisited.
    private Set<String> tableNamesConfigured() {
        IgniteBiTuple<ByteArray, ByteArray> range = toRange(new ByteArray(PUBLIC_PREFIX));

        Set<String> tableNames = new HashSet<>();

        try (Cursor<Entry> cursor = metaStorageMgr.range(range.get1(), range.get2())) {
            while (cursor.hasNext()) {
                Entry entry = cursor.next();

                List<String> keySplit = ConfigurationUtil.split(entry.key().toString());

                if (keySplit.size() == 5 && NamedListNode.NAME.equals(keySplit.get(4)))
                    tableNames.add(ByteUtils.fromBytes(entry.value()).toString());
            }
        }
        catch (Exception e) {
            LOG.error("Can't get table names.", e);
        }

        return tableNames;
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
     * false otherwise.
     * @return A table or {@code null} if table does not exist.
     */
    @Override public TableImpl table(UUID id) {
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

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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.affinity.AffinityManager;
import org.apache.ignite.internal.affinity.event.AffinityEvent;
import org.apache.ignite.internal.affinity.event.AffinityEventParameters;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.event.SchemaEvent;
import org.apache.ignite.internal.schema.event.SchemaEventParameters;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.raft.PartitionCommandListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.table.event.TableEventParameters;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.metastorage.client.Conditions;
import org.apache.ignite.metastorage.client.Operations;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.manager.IgniteTables;
import org.jetbrains.annotations.NotNull;

/**
 * Table manager.
 */
public class TableManager extends Producer<TableEvent, TableEventParameters> implements IgniteTables {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(TableManager.class);

    /** Internal prefix for the metasorage. */
    private static final String INTERNAL_PREFIX = "internal.tables.";

    /** Meta storage service. */
    private final MetaStorageManager metaStorageMgr;

    /** Configuration manager. */
    private final ConfigurationManager configurationMgr;

    /** Raft manmager. */
    private final Loza raftMgr;

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /** Affinity manager. */
    private final AffinityManager affMgr;

    /** Tables. */
    private final Map<String, TableImpl> tables = new ConcurrentHashMap<>();

    /**
     * Creates a new table manager.
     *
     * @param configurationMgr Configuration manager.
     * @param metaStorageMgr Meta storage manager.
     * @param schemaMgr Schema manager.
     * @param affMgr Affinity manager.
     * @param raftMgr Raft manager.
     * @param vaultManager Vault manager.
     */
    public TableManager(
        ConfigurationManager configurationMgr,
        MetaStorageManager metaStorageMgr,
        SchemaManager schemaMgr,
        AffinityManager affMgr,
        Loza raftMgr,
        VaultManager vaultManager
    ) {
        this.configurationMgr = configurationMgr;
        this.metaStorageMgr = metaStorageMgr;
        this.affMgr = affMgr;
        this.raftMgr = raftMgr;
        this.schemaMgr = schemaMgr;

        listenForTableChange();
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

        HashMap<Integer, RaftGroupService> partitionMap = new HashMap<>(partitions);

        for (int p = 0; p < partitions; p++) {
            partitionMap.put(p, raftMgr.startRaftGroup(
                raftGroupName(tblId, p),
                assignment.get(p),
                new PartitionCommandListener()
            ));
        }

        onEvent(TableEvent.CREATE, new TableEventParameters(
            tblId,
            name,
            schemaReg,
            new InternalTableImpl(tblId, partitionMap, partitions)
        ), null);
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

        onEvent(TableEvent.DROP, new TableEventParameters(
            tblId,
            name,
            table.schemaView(),
            table.internalTable()
        ), null);
    }

    /**
     * Compounds a RAFT group unique name.
     *
     * @param tableId Table identifier.
     * @param partition Muber of table partition.
     * @return A RAFT group name.
     */
    @NotNull private String raftGroupName(UUID tableId, int partition) {
        return tableId + "_part_" + partition;
    }

    /**
     * Listens on a drop or create table.
     */
    private void listenForTableChange() {
        //TODO: IGNITE-14652 Change a metastorage update in listeners to multi-invoke
        configurationMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables().listen(ctx -> {
            Set<String> tablesToStart = (ctx.newValue() == null || ctx.newValue().namedListKeys() == null) ?
                Collections.emptySet() : new HashSet<>(ctx.newValue().namedListKeys());

            tablesToStart.removeAll(ctx.oldValue().namedListKeys());

            long revision = ctx.storageRevision();

            List<CompletableFuture<Boolean>> futs = new ArrayList<>();

            boolean hasMetastorageLocally = MetaStorageManager.hasMetastorageLocally(configurationMgr);

            for (String tblName : tablesToStart) {
                TableView tableView = ctx.newValue().get(tblName);

                UUID tblId = new UUID(revision, 0L);

                if (hasMetastorageLocally) {
                    var key = new ByteArray(INTERNAL_PREFIX + tblId);
                    futs.add(metaStorageMgr.invoke(
                        Conditions.notExists(key),
                        Operations.put(key, tableView.name().getBytes(StandardCharsets.UTF_8)),
                        Operations.noop())
                        .thenCompose(res -> schemaMgr.initSchemaForTable(tblId, tableView.name()))
                        .thenCompose(res -> affMgr.calculateAssignments(tblId)));
                }

                final CompletableFuture<AffinityEventParameters> affinityReadyFut = new CompletableFuture<>();
                final CompletableFuture<SchemaEventParameters> schemaReadyFut = new CompletableFuture<>();

                CompletableFuture.allOf(affinityReadyFut, schemaReadyFut)
                    .exceptionally(e -> {
                        LOG.error("Failed to create a new table [name=" + tblName + ", id=" + tblId + ']', e);

                        onEvent(TableEvent.CREATE, new TableEventParameters(
                            tblId,
                            tblName,
                            null,
                            null
                        ), e);

                        return null;
                    })
                    .thenRun(() -> createTableLocally(
                        tblName,
                        tblId,
                        affinityReadyFut.join().assignment(),
                        schemaReadyFut.join().schemaRegistry()
                    ));

                affMgr.listen(AffinityEvent.CALCULATED, (parameters, e) -> {
                    if (!tblId.equals(parameters.tableId()))
                        return false;

                    if (e == null)
                        affinityReadyFut.complete(parameters);
                    else
                        affinityReadyFut.completeExceptionally(e);

                    return true;
                });

                schemaMgr.listen(SchemaEvent.INITIALIZED, (parameters, e) -> {
                    if (!tblId.equals(parameters.tableId()))
                        return false;

                    if (e == null)
                        schemaReadyFut.complete(parameters);
                    else
                        schemaReadyFut.completeExceptionally(e);

                    return true;
                });
            }

            Set<String> tablesToStop = (ctx.oldValue() == null || ctx.oldValue().namedListKeys() == null) ?
                Collections.emptySet() : new HashSet<>(ctx.oldValue().namedListKeys());

            tablesToStop.removeAll(ctx.newValue().namedListKeys());

            for (String tblName : tablesToStop) {
                TableImpl t = tables.get(tblName);

                UUID tblId = t.internalTable().tableId();

                if (hasMetastorageLocally) {
                    var key = new ByteArray(INTERNAL_PREFIX + tblId);

                    futs.add(affMgr.removeAssignment(tblId)
                        .thenCompose(res -> schemaMgr.unregisterSchemas(tblId))
                        .thenCompose(res ->
                            metaStorageMgr.invoke(Conditions.exists(key),
                                Operations.remove(key),
                                Operations.noop())));
                }

                affMgr.listen(AffinityEvent.REMOVED, (parameters, e) -> {
                    if (!tblId.equals(parameters.tableId()))
                        return false;

                    if (e == null)
                        dropTableLocally(tblName, tblId, parameters.assignment());
                    else {
                        LOG.error("Failed to drop a table [name=" + tblName + ", id=" + tblId + ']', e);

                        onEvent(TableEvent.DROP, new TableEventParameters(
                            tblId,
                            tblName,
                            null,
                            null
                        ), e);
                    }

                    return true;
                });
            }

            return CompletableFuture.allOf(futs.toArray(CompletableFuture[]::new));
        });
    }

    /** {@inheritDoc} */
    @Override public Table createTable(String name, Consumer<TableChange> tableInitChange) {
        CompletableFuture<Table> tblFut = new CompletableFuture<>();

        listen(TableEvent.CREATE, (params, e) -> {
            String tableName = params.tableName();

            if (!name.equals(tableName))
                return false;

            if (e == null) {
                tblFut.complete(tables.compute(tableName, (key, val) ->
                    new TableImpl(params.internalTable(), params.tableSchemaView())));
            }
            else
                tblFut.completeExceptionally(e);

            return true;
        });

        try {
            configurationMgr.configurationRegistry()
                .getConfiguration(TablesConfiguration.KEY).tables().change(change ->
                change.create(name, tableInitChange)).get();
        }
        catch (InterruptedException | ExecutionException e) {
            LOG.error("Table wasn't created [name=" + name + ']', e);

            tblFut.completeExceptionally(e);
        }

        return tblFut.join();
    }

    /** {@inheritDoc} */
    @Override public void dropTable(String name) {
        CompletableFuture<Void> dropTblFut = new CompletableFuture<>();

        listen(TableEvent.DROP, new BiPredicate<>() {
            @Override public boolean test(TableEventParameters params, Throwable e) {
                String tableName = params.tableName();

                if (!name.equals(tableName))
                    return false;

                if (e == null) {
                    Table droppedTable = tables.remove(tableName);

                    assert droppedTable != null;

                    dropTblFut.complete(null);
                }
                else
                    dropTblFut.completeExceptionally(e);

                return true;
            }
        });

        try {
            configurationMgr
                .configurationRegistry()
                .getConfiguration(TablesConfiguration.KEY)
                .tables()
                .change(change -> change.delete(name)).get();
        }
        catch (InterruptedException | ExecutionException e) {
            LOG.error("Table wasn't dropped [name=" + name + ']', e);

            dropTblFut.completeExceptionally(e);
        }

        dropTblFut.join();
    }

    /** {@inheritDoc} */
    @Override public List<Table> tables() {
        return new ArrayList<>(tables.values());
    }

    /** {@inheritDoc} */
    @Override public Table table(String name) {
        return tables.get(name);
    }
}

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
import org.apache.ignite.configuration.schemas.runner.ClusterConfiguration;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableSchemaViewImpl;
import org.apache.ignite.internal.table.distributed.raft.PartitionCommandListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.table.event.TableEventParameters;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.metastorage.common.Conditions;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.Operations;
import org.apache.ignite.metastorage.common.WatchEvent;
import org.apache.ignite.metastorage.common.WatchListener;
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

    /** Table creation subscription future. */
    private CompletableFuture<Long> tableCreationSubscriptionFut;

    /** Tables. */
    private Map<String, TableImpl> tables = new ConcurrentHashMap<>();

    /*
     * @param configurationMgr Configuration manager.
     * @param metaStorageMgr Meta storage manager.
     * @param schemaManager Schema manager.
     * @param raftMgr Raft manager.
     * @param vaultManager Vault manager.
     */
    public TableManager(
        ConfigurationManager configurationMgr,
        MetaStorageManager metaStorageMgr,
        SchemaManager schemaManager,
        Loza raftMgr,
        VaultManager vaultManager
    ) {
        this.configurationMgr = configurationMgr;
        this.metaStorageMgr = metaStorageMgr;

        String localNodeName = configurationMgr.configurationRegistry().getConfiguration(NodeConfiguration.KEY)
            .name().value();

        configurationMgr.configurationRegistry().getConfiguration(ClusterConfiguration.KEY)
            .metastorageNodes().listen(ctx -> {
            if (ctx.newValue() != null) {
                if (hasMetastorageLocally(localNodeName, ctx.newValue()))
                    subscribeForTableCreation();
                else
                    unsubscribeForTableCreation();
            }
            return CompletableFuture.completedFuture(null);

        });

        String[] metastorageMembers = configurationMgr.configurationRegistry().getConfiguration(NodeConfiguration.KEY)
            .metastorageNodes().value();

        if (hasMetastorageLocally(localNodeName, metastorageMembers))
            subscribeForTableCreation();

        String tableInternalPrefix = INTERNAL_PREFIX + "assignment.";

        tableCreationSubscriptionFut = metaStorageMgr.registerWatchByPrefix(new Key(tableInternalPrefix), new WatchListener() {
            @Override public boolean onUpdate(@NotNull Iterable<WatchEvent> events) {
                for (WatchEvent evt : events) {
                    String placeholderValue = evt.newEntry().key().toString().substring(tableInternalPrefix.length() - 1);

                    String name = new String(vaultManager.get(ByteArray.fromString(INTERNAL_PREFIX + placeholderValue))
                        .join().value(), StandardCharsets.UTF_8);

                    UUID tblId = UUID.fromString(placeholderValue);

                    if (evt.newEntry().value() == null) {
                        assert evt.oldEntry().value() != null : "Previous assignment is unknown";

                        List<List<ClusterNode>> assignment = (List<List<ClusterNode>>)ByteUtils.fromBytes(
                            evt.oldEntry().value());

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
                    else if (evt.newEntry().value().length > 0) {
                        List<List<ClusterNode>> assignment = (List<List<ClusterNode>>)ByteUtils.fromBytes(
                            evt.newEntry().value());

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
                            new TableSchemaViewImpl(tblId, schemaManager),
                            new InternalTableImpl(tblId, partitionMap, partitions)
                        ), null);
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
     * Checks whether the local node hosts Metastorage.
     *
     * @param localNodeName Local node uniq name.
     * @param metastorageMembers Metastorage members names.
     * @return True if the node has Metastorage, false otherwise.
     */
    private boolean hasMetastorageLocally(String localNodeName, String[] metastorageMembers) {
        boolean isLocalNodeHasMetasorage = false;

        for (String name : metastorageMembers) {
            if (name.equals(localNodeName)) {
                isLocalNodeHasMetasorage = true;

                break;
            }
        }
        return isLocalNodeHasMetasorage;
    }

    /**
     * Subscribes on table create.
     */
    private void subscribeForTableCreation() {
        //TODO: IGNITE-14652 Change a metastorage update in listeners to multi-invoke
        configurationMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY)
            .tables().listen(ctx -> {
            Set<String> tablesToStart = ctx.newValue().namedListKeys() == null ?
                Collections.EMPTY_SET : ctx.newValue().namedListKeys();

            tablesToStart.removeAll(ctx.oldValue().namedListKeys());

            long revision = ctx.storageRevision();

            List<CompletableFuture<Boolean>> futs = new ArrayList<>();

            for (String tblName : tablesToStart) {
                TableView tableView = ctx.newValue().get(tblName);
                long update = 0;

                UUID tblId = new UUID(revision, update);

                var key = new Key(INTERNAL_PREFIX + tblId.toString());
                futs.add(metaStorageMgr.invoke(
                    Conditions.key(key).value().eq(null),
                    Operations.put(key, tableView.name().getBytes(StandardCharsets.UTF_8)),
                    Operations.noop()).thenCompose(res ->
                    res ? metaStorageMgr.put(new Key(INTERNAL_PREFIX + "assignment." + tblId.toString()), new byte[0])
                        .thenApply(v -> true)
                        : CompletableFuture.completedFuture(false)));
            }

            Set<String> tablesToStop = ctx.oldValue().namedListKeys() == null ?
                Collections.EMPTY_SET : ctx.oldValue().namedListKeys();

            tablesToStop.removeAll(ctx.newValue().namedListKeys());

            for (String tblName : tablesToStop) {
                TableImpl t = tables.get(tblName);

                UUID tblId = t.internalTable().tableId();

                var key = new Key(INTERNAL_PREFIX + "assignment." + tblId.toString());
                futs.add(metaStorageMgr.invoke(
                    Conditions.key(key).value().ne(null),
                    Operations.remove(key),
                    Operations.noop()).thenCompose(res ->
                    res ? metaStorageMgr.remove(new Key(INTERNAL_PREFIX + tblId.toString()))
                        .thenApply(v -> true)
                        : CompletableFuture.completedFuture(false)));
            }

            return CompletableFuture.allOf(futs.toArray(CompletableFuture[]::new));
        });
    }

    /**
     * Unsubscribe from table creation.
     */
    private void unsubscribeForTableCreation() {
        if (tableCreationSubscriptionFut == null)
            return;

        try {
            Long subscriptionId = tableCreationSubscriptionFut.get();

            metaStorageMgr.unregisterWatch(subscriptionId);

            tableCreationSubscriptionFut = null;
        }
        catch (InterruptedException | ExecutionException e) {
            LOG.error("Couldn't unsubscribe from table creation", e);
        }
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

        configurationMgr.configurationRegistry()
            .getConfiguration(TablesConfiguration.KEY).tables().change(change ->
            change.create(name, tableInitChange));

        return tblFut.join();
    }

    /** {@inheritDoc} */
    @Override public void dropTable(String name) {
        CompletableFuture<Void> dropTblFut = new CompletableFuture<>();

        listen(TableEvent.DROP, new BiPredicate<TableEventParameters, Exception>() {
            @Override public boolean test(TableEventParameters params, Exception e) {
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

        configurationMgr.configurationRegistry()
            .getConfiguration(TablesConfiguration.KEY).tables().change(change -> {
            change.delete(name);
        });

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

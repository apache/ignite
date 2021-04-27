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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.configuration.schemas.runner.ClusterConfiguration;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableSchemaView;
import org.apache.ignite.internal.table.distributed.raft.PartitionCommandListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
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
public class TableManager implements IgniteTables {
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
    private Map<String, Table> tables;

    /**
     * @param configurationMgr Configuration manager.
     * @param metaStorageMgr Meta storage manager.
     * @param schemaManager Schema manager.
     * @param raftMgr Raft manager.
     */
    public TableManager(
        ConfigurationManager configurationMgr,
        MetaStorageManager metaStorageMgr,
        SchemaManager schemaManager,
        Loza raftMgr,
        VaultManager vaultManager
    ) {
        tables = new HashMap<>();

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

        String tableInternalPrefix = INTERNAL_PREFIX + "assignment.#";

        tableCreationSubscriptionFut = metaStorageMgr.registerWatch(new Key(tableInternalPrefix), new WatchListener() {
            @Override public boolean onUpdate(@NotNull Iterable<WatchEvent> events) {
                for (WatchEvent evt : events) {
                    if (!ArrayUtils.empty(evt.newEntry().value())) {
                        String keyTail = evt.newEntry().key().toString().substring(INTERNAL_PREFIX.length());

                        String placeholderValue = keyTail.substring(0, keyTail.indexOf('.'));

                        UUID tblId = UUID.fromString(placeholderValue);

                        try {
                            String name = new String(vaultManager.get((INTERNAL_PREFIX + tblId.toString())
                                .getBytes(StandardCharsets.UTF_8)).get().value(), StandardCharsets.UTF_8);

                            int partitions = configurationMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY)
                                .tables().get(name).partitions().value();

                            List<List<ClusterNode>> assignment = (List<List<ClusterNode>>)IgniteUtils.fromBytes(
                                evt.newEntry().value());

                            HashMap<Integer, RaftGroupService> partitionMap = new HashMap<>(partitions);

                            for (int p = 0; p < partitions; p++) {
                                partitionMap.put(p, raftMgr.startRaftGroup(
                                    name + "_part_" + p,
                                    assignment.get(p),
                                    new PartitionCommandListener()
                                ));
                            }

                            tables.put(name, new TableImpl(
                                new InternalTableImpl(
                                    tblId,
                                    partitionMap,
                                    partitions
                                ),
                                new TableSchemaView() {
                                    @Override public SchemaDescriptor schema() {
                                        return schemaManager.schema(tblId);
                                    }

                                    @Override public SchemaDescriptor schema(int ver) {
                                        return schemaManager.schema(tblId, ver);
                                    }
                                }));
                        }
                        catch (InterruptedException | ExecutionException e) {
                            LOG.error("Failed to start table [key={}]",
                                evt.newEntry().key(), e);
                        }
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
            HashSet<String> tblNamesToStart = new HashSet<>(ctx.newValue().namedListKeys());

            long revision = ctx.storageRevision();

            if (ctx.oldValue() != null)
                tblNamesToStart.removeAll(ctx.oldValue().namedListKeys());

            for (String tblName : tblNamesToStart) {
                TableView tableView = ctx.newValue().get(tblName);
                long update = 0;

                UUID tblId = new UUID(revision, update);

                CompletableFuture<Boolean> fut = metaStorageMgr.invoke(
                    new Key(INTERNAL_PREFIX + tblId.toString()),
                    Conditions.value().eq(null),
                    Operations.put(tableView.name().getBytes(StandardCharsets.UTF_8)),
                    Operations.noop());

                try {
                    if (fut.get()) {
                        metaStorageMgr.put(new Key(INTERNAL_PREFIX + "assignment." + tblId.toString()), new byte[0]);

                        LOG.info("Table manager created a table [name={}, revision={}]",
                            tableView.name(), revision);
                    }
                }
                catch (InterruptedException | ExecutionException e) {
                    LOG.error("Table was not fully initialized [name={}, revision={}]",
                        tableView.name(), revision, e);
                }
            }
            return CompletableFuture.completedFuture(null);
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
        configurationMgr.configurationRegistry()
            .getConfiguration(TablesConfiguration.KEY).tables().change(change ->
            change.create(name, tableInitChange));

//        this.createTable("tbl1", change -> {
//            change.initReplicas(2);
//            change.initName("tbl1");
//            change.initPartitions(1_000);
//        });

        //TODO: IGNITE-14646 Support asynchronous table creation
        Table tbl = null;

        while (tbl == null) {
            try {
                Thread.sleep(50);

                tbl = table(name);
            }
            catch (InterruptedException e) {
                LOG.error("Waiting of creation of table was interrupted.", e);
            }
        }

        return tbl;
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

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

package org.apache.ignite.internal.processors.query.h2.maintenance;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexFactory;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexTarget;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.aware.IndexBuildStatusStorage;
import org.apache.ignite.internal.processors.query.h2.H2TableDescriptor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.schema.IndexRebuildCancelToken;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorImpl;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexUtils.INDEX_REBUILD_MNTC_TASK_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;

/**
 * Maintenance action that handles index rebuilding.
 */
public class RebuildIndexAction implements MaintenanceAction<Boolean> {
    /** Indexes to rebuild. */
    private final List<MaintenanceRebuildIndexTarget> indexesToRebuild;

    /** Ignite indexing. */
    private final IgniteH2Indexing indexing;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param indexesToRebuild Indexes to rebuild.
     * @param indexing Indexing.
     * @param log Logger.
     */
    public RebuildIndexAction(List<MaintenanceRebuildIndexTarget> indexesToRebuild, IgniteH2Indexing indexing, IgniteLogger log) {
        this.indexesToRebuild = indexesToRebuild;
        this.indexing = indexing;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public Boolean execute() {
        GridKernalContext kernalContext = indexing.kernalContext();

        GridCacheDatabaseSharedManager database = (GridCacheDatabaseSharedManager)kernalContext.cache()
            .context().database();

        CheckpointManager manager = database.getCheckpointManager();

        IndexBuildStatusStorage storage = getIndexBuildStatusStorage(kernalContext);

        try {
            prepareForRebuild(database, manager, storage);

            for (MaintenanceRebuildIndexTarget params : indexesToRebuild) {
                int cacheId = params.cacheId();
                String idxName = params.idxName();

                try {
                    execute0(cacheId, idxName, kernalContext, storage, manager);
                }
                catch (Exception e) {
                    log.error("Rebuilding index " + idxName + " for cache " + cacheId + " failed", e);

                    return false;
                }
            }
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to prepare for the rebuild of indexes", e);
            return false;
        }
        finally {
            cleanUpAfterRebuild(manager, storage);
        }

        unregisterMaintenanceTask(kernalContext);

        return true;
    }

    /**
     * Executes index rebuild.
     *
     * @param cacheId Cache id.
     * @param idxName Name of the index.
     * @param kernalContext Context.
     * @param storage Index build status storage.
     * @param manager Checkpoint manager.
     * @throws Exception If failed to execute rebuild.
     */
    private void execute0(
        int cacheId,
        String idxName,
        GridKernalContext kernalContext,
        IndexBuildStatusStorage storage,
        CheckpointManager manager
    ) throws Exception {
        GridCacheContext<?, ?> context = kernalContext.cache().context().cacheContext(cacheId);

        String cacheName = context.name();

        SchemaManager schemaManager = indexing.schemaManager();

        H2TreeIndex targetIndex = findIndex(cacheName, idxName, schemaManager);

        if (targetIndex == null) {
            // Our job here is already done.
            return;
        }

        GridH2Table targetTable = targetIndex.getTable();

        destroyOldIndex(targetIndex, targetTable);

        recreateIndex(targetIndex, context, cacheName, storage, schemaManager, targetTable);

        manager.forceCheckpoint("afterIndexRebuild", null).futureFor(FINISHED).get();
    }

    /**
     * Creates new index from the old one and builds it.
     *
     * @param oldIndex Old index.
     * @param context Cache context.
     * @param cacheName Cache name.
     * @param storage Index build status storage.
     * @param schemaManager Schema manager.
     * @param targetTable Table for the index.
     * @throws IgniteCheckedException If failed to recreate an index.
     */
    private void recreateIndex(
        H2TreeIndex oldIndex,
        GridCacheContext<?, ?> context,
        String cacheName,
        IndexBuildStatusStorage storage,
        SchemaManager schemaManager,
        GridH2Table targetTable
    ) throws IgniteCheckedException {
        GridFutureAdapter<Void> createIdxFut = new GridFutureAdapter<>();

        IndexRebuildCancelToken token = new IndexRebuildCancelToken();

        SchemaIndexCacheVisitorImpl visitor = new SchemaIndexCacheVisitorImpl(context, token, createIdxFut) {
            /** {@inheritDoc} */
            @Override public void visit(SchemaIndexCacheVisitorClosure clo) {
                // Rebuild index after it is created.
                storage.onStartRebuildIndexes(context);

                try {
                    super.visit(clo);

                    buildIdxFut.get();
                }
                catch (Exception e) {
                    throw new IgniteException(e);
                }
                finally {
                    storage.onFinishRebuildIndexes(cacheName);
                }
            }
        };

        IndexProcessor indexProcessor = context.kernalContext().indexProcessor();

        SortedIndexDefinition definition = oldIndex.index().indexDefinition();

        org.apache.ignite.internal.cache.query.index.Index newIndex = indexProcessor.createIndexDynamically(
            targetTable.cacheContext(), InlineIndexFactory.INSTANCE, definition, visitor);

        InlineIndexImpl queryIndex = newIndex.unwrap(InlineIndexImpl.class);

        H2TreeIndex newIdx = oldIndex.createCopy(queryIndex, definition);

        schemaManager.createIndex(
            targetTable.getSchema().getName(),
            targetTable.getName(),
            newIdx,
            true
        );

        // This future must be already finished by the schema index cache visitor above
        assert createIdxFut.isDone();
    }

    /**
     * Destroys old index.
     *
     * @param index Index.
     * @param table Table.
     * @throws IgniteCheckedException If failed to destroy index.
     */
    private void destroyOldIndex(H2TreeIndex index, GridH2Table table) throws IgniteCheckedException {
        index.destroyImmediately();

        Session session = table.getDatabase().getSystemSession();

        table.removeIndex(session, index);
    }

    /**
     * Finds index for the cache by the name.
     *
     * @param cacheName Cache name.
     * @param idxName Index name.
     * @param schemaManager Schema manager.
     * @return Index or {@code null} if index was not found.
     */
    @Nullable private H2TreeIndex findIndex(String cacheName, String idxName, SchemaManager schemaManager) {
        H2TreeIndex targetIndex = null;

        for (H2TableDescriptor tblDesc : schemaManager.tablesForCache(cacheName)) {
            GridH2Table tbl = tblDesc.table();

            assert tbl != null;

            Index index = tbl.getIndex(idxName);

            if (index != null) {
                assert index instanceof H2TreeIndex;

                targetIndex = (H2TreeIndex)index;
                break;
            }
        }

        return targetIndex;
    }

    /**
     * Prepares system for the rebuild.
     *
     * @param database Database manager.
     * @param manager Checkpoint manager.
     * @param storage Index build status storage.
     * @throws IgniteCheckedException If failed.
     */
    private void prepareForRebuild(
        GridCacheDatabaseSharedManager database,
        CheckpointManager manager,
        IndexBuildStatusStorage storage
    ) throws IgniteCheckedException {
        // Enable WAL
        database.resumeWalLogging();

        // Enable checkpointer
        database.onStateRestored(null);

        // IndexBuildStatusStorage listens for checkpoint to update the status of the rebuild in the metastorage.
        // We need to set up the listener manually here, because it's maintenance mode.
        manager.addCheckpointListener(storage, null);
    }

    /**
     * Cleans up after the rebuild.
     *
     * @param manager Checkpoint manager.
     * @param storage Index build status storage.
     */
    private void cleanUpAfterRebuild(CheckpointManager manager, IndexBuildStatusStorage storage) {
        // Remove the checkpoint listener
        manager.removeCheckpointListener(storage);
    }

    /**
     * Removes maintenance task.
     *
     * @param kernalContext Kernal context.
     */
    private void unregisterMaintenanceTask(GridKernalContext kernalContext) {
        kernalContext.maintenanceRegistry().unregisterMaintenanceTask(INDEX_REBUILD_MNTC_TASK_NAME);
    }

    /**
     * Gets index build status storage.
     *
     * @param kernalContext Kernal context.
     * @return Index build status storage.
     */
    private IndexBuildStatusStorage getIndexBuildStatusStorage(GridKernalContext kernalContext) {
        GridQueryProcessor query = kernalContext.query();

        return query.getIdxBuildStatusStorage();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "rebuild";
    }

    /** {@inheritDoc} */
    @Nullable @Override public String description() {
        return "Rebuilding indexes";
    }
}

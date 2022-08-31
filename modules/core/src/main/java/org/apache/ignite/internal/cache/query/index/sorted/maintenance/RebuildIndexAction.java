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

package org.apache.ignite.internal.cache.query.index.sorted.maintenance;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.IndexDefinition;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexFactory;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.processors.query.aware.IndexBuildStatusStorage;
import org.apache.ignite.internal.processors.query.schema.IndexRebuildCancelToken;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorImpl;
import org.apache.ignite.internal.processors.query.schema.management.IndexDescriptor;
import org.apache.ignite.internal.processors.query.schema.management.SchemaManager;
import org.apache.ignite.internal.processors.query.schema.management.TableDescriptor;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexUtils.INDEX_REBUILD_MNTC_TASK_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;

/**
 * Maintenance action that handles index rebuilding.
 */
public class RebuildIndexAction implements MaintenanceAction<Boolean> {
    /** Indexes to rebuild. */
    private final List<MaintenanceRebuildIndexTarget> indexesToRebuild;

    /** Context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param indexesToRebuild Indexes to rebuild.
     * @param ctx Context.
     */
    public RebuildIndexAction(List<MaintenanceRebuildIndexTarget> indexesToRebuild, GridKernalContext ctx) {
        this.indexesToRebuild = indexesToRebuild;
        this.ctx = ctx;

        log = ctx.log(RebuildIndexAction.class);
    }

    /** {@inheritDoc} */
    @Override public Boolean execute() {
        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ctx.cache().context().database();

        CheckpointManager cpMgr = db.getCheckpointManager();

        IndexBuildStatusStorage storage = ctx.query().getIdxBuildStatusStorage();

        try {
            prepareForRebuild(db, cpMgr, storage);

            for (MaintenanceRebuildIndexTarget params : indexesToRebuild) {
                int cacheId = params.cacheId();
                String idxName = params.idxName();

                try {
                    execute0(cacheId, idxName, storage, cpMgr);
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
            cleanUpAfterRebuild(cpMgr, storage);
        }

        unregisterMaintenanceTask();

        return true;
    }

    /**
     * Executes index rebuild.
     *
     * @param cacheId Cache id.
     * @param idxName Name of the index.
     * @param storage Index build status storage.
     * @param cpMgr Checkpoint manager.
     * @throws Exception If failed to execute rebuild.
     */
    private void execute0(
        int cacheId,
        String idxName,
        IndexBuildStatusStorage storage,
        CheckpointManager cpMgr
    ) throws Exception {
        GridCacheContext<?, ?> cctx = ctx.cache().context().cacheContext(cacheId);

        String cacheName = cctx.name();

        SchemaManager schemaMgr = ctx.query().schemaManager();

        String schemaName = schemaMgr.schemaName(cacheName);

        if (F.isEmpty(schemaName))
            return;

        IndexDescriptor idxDesc = schemaMgr.index(schemaName, idxName);

        if (idxDesc == null)
            return;

        schemaMgr.dropIndex(schemaName, idxName, true);

        recreateIndex(idxDesc, cctx, cacheName, storage, schemaMgr);

        cpMgr.forceCheckpoint("afterIndexRebuild", null).futureFor(FINISHED).get();
    }

    /**
     * Creates new index from the old one and builds it.
     *
     * @param oldIdxDesc Old index.
     * @param cctx Cache context.
     * @param cacheName Cache name.
     * @param storage Index build status storage.
     * @param schemaMgr Schema manager.
     */
    private void recreateIndex(
        IndexDescriptor oldIdxDesc,
        GridCacheContext<?, ?> cctx,
        String cacheName,
        IndexBuildStatusStorage storage,
        SchemaManager schemaMgr
    ) {
        GridFutureAdapter<Void> createIdxFut = new GridFutureAdapter<>();

        IndexRebuildCancelToken token = new IndexRebuildCancelToken();

        SchemaIndexCacheVisitorImpl visitor = new SchemaIndexCacheVisitorImpl(cctx, token, createIdxFut) {
            /** {@inheritDoc} */
            @Override public void visit(SchemaIndexCacheVisitorClosure clo) {
                // Rebuild index after it is created.
                storage.onStartRebuildIndexes(cctx);

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

        IndexProcessor idxProc = ctx.indexProcessor();

        assert oldIdxDesc.type() == QueryIndexType.SORTED : oldIdxDesc.type();

        TableDescriptor tblDesc = oldIdxDesc.table();
        IndexDefinition definition = oldIdxDesc.index().unwrap(InlineIndexImpl.class).indexDefinition();

        Index newIdx = idxProc.createIndexDynamically(tblDesc.cacheInfo().cacheContext(),
            InlineIndexFactory.INSTANCE, definition, visitor);

        IndexDescriptor newIdxDesc = new IndexDescriptor(tblDesc, oldIdxDesc.name(), oldIdxDesc.type(),
            oldIdxDesc.keyDefinitions(), oldIdxDesc.isPk(), oldIdxDesc.isAffinity(), oldIdxDesc.inlineSize(), newIdx);

        try {
            schemaMgr.addIndex(tblDesc, newIdxDesc);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to register index in schema manager", e);
        }

        // This future must be already finished by the schema index cache visitor above
        assert createIdxFut.isDone();
    }

    /**
     * Prepares system for the rebuild.
     *
     * @param db Database manager.
     * @param cpMgr Checkpoint manager.
     * @param storage Index build status storage.
     * @throws IgniteCheckedException If failed.
     */
    private void prepareForRebuild(
        GridCacheDatabaseSharedManager db,
        CheckpointManager cpMgr,
        IndexBuildStatusStorage storage
    ) throws IgniteCheckedException {
        // Enable WAL
        db.resumeWalLogging();

        // Enable checkpointer
        db.onStateRestored(null);

        // IndexBuildStatusStorage listens for checkpoint to update the status of the rebuild in the metastorage.
        // We need to set up the listener manually here, because it's maintenance mode.
        cpMgr.addCheckpointListener(storage, null);
    }

    /**
     * Cleans up after the rebuild.
     *
     * @param cpMgr Checkpoint manager.
     * @param storage Index build status storage.
     */
    private void cleanUpAfterRebuild(CheckpointManager cpMgr, IndexBuildStatusStorage storage) {
        // Remove the checkpoint listener
        cpMgr.removeCheckpointListener(storage);
    }

    /**
     * Removes maintenance task.
     */
    private void unregisterMaintenanceTask() {
        ctx.maintenanceRegistry().unregisterMaintenanceTask(INDEX_REBUILD_MNTC_TASK_NAME);
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

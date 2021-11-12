/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.cache.query.index.sorted;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTaskV2.NoopRowHandlerFactory;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.metric.IoStatisticsHolderIndex;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTaskResult;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIoResolver;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.singleton;
import static org.apache.ignite.internal.metric.IoStatisticsType.SORTED_INDEX;

/**
 * Tasks that cleans up index tree.
 *
 * @deprecated Use {@link DurableBackgroundCleanupIndexTreeTaskV2}.
 */
@Deprecated
public class DurableBackgroundCleanupIndexTreeTask implements DurableBackgroundTask {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private List<Long> rootPages;

    /** */
    private transient volatile List<InlineIndexTree> trees;

    /** */
    private String cacheGrpName;

    /** */
    private final String cacheName;

    /** */
    private String schemaName;

    /** */
    private final String treeName;

    /** */
    private final String idxName;

    /** */
    private final String id;

    /** Logger. */
    @Nullable private transient volatile IgniteLogger log;

    /** Worker tasks. */
    @Nullable private transient volatile GridWorker worker;

    /** */
    public DurableBackgroundCleanupIndexTreeTask(
        List<Long> rootPages,
        List<InlineIndexTree> trees,
        String cacheGrpName,
        String cacheName,
        IndexName idxName,
        String treeName
    ) {
        this.rootPages = rootPages;
        this.trees = trees;
        this.cacheGrpName = cacheGrpName;
        this.cacheName = cacheName;
        this.id = UUID.randomUUID().toString();
        this.idxName = idxName.idxName();
        this.schemaName = idxName.schemaName();
        this.treeName = treeName;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "DROP_SQL_INDEX-" + schemaName + "." + idxName + "-" + id;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<DurableBackgroundTaskResult> executeAsync(GridKernalContext ctx) {
        log = ctx.log(this.getClass());

        assert worker == null;

        GridFutureAdapter<DurableBackgroundTaskResult> fut = new GridFutureAdapter<>();

        worker = new GridWorker(
            ctx.igniteInstanceName(),
            "async-durable-background-task-executor-" + name(),
            log
        ) {
            /** {@inheritDoc} */
            @Override protected void body() {
                try {
                    execute(ctx);

                    worker = null;

                    fut.onDone(DurableBackgroundTaskResult.complete(null));
                }
                catch (Throwable t) {
                    worker = null;

                    fut.onDone(DurableBackgroundTaskResult.restart(t));
                }
            }
        };

        new IgniteThread(worker).start();

        return fut;
    }

    /**
     * Task execution.
     *
     * @param ctx Kernal context.
     */
    private void execute(GridKernalContext ctx) {
        List<InlineIndexTree> trees0 = trees;

        if (trees0 == null) {
            trees0 = new ArrayList<>(rootPages.size());

            GridCacheContext cctx = ctx.cache().context().cacheContext(CU.cacheId(cacheName));

            int grpId = CU.cacheGroupId(cacheName, cacheGrpName);

            CacheGroupContext grpCtx = ctx.cache().cacheGroup(grpId);

            // If group context is null, it means that group doesn't exist and we don't need this task anymore.
            if (grpCtx == null)
                return;

            IgniteCacheOffheapManager offheap = grpCtx.offheap();

            if (treeName != null) {
                ctx.cache().context().database().checkpointReadLock();

                try {
                    int cacheId = CU.cacheId(cacheName);

                    for (int segment = 0; segment < rootPages.size(); segment++) {
                        try {
                            RootPage rootPage = offheap.findRootPageForIndex(cacheId, treeName, segment);

                            if (rootPage != null && rootPages.get(segment) == rootPage.pageId().pageId())
                                offheap.dropRootPageForIndex(cacheId, treeName, segment);
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException(e);
                        }
                    }
                }
                finally {
                    ctx.cache().context().database().checkpointReadUnlock();
                }
            }

            IoStatisticsHolderIndex stats = new IoStatisticsHolderIndex(
                SORTED_INDEX,
                cctx.name(),
                idxName,
                cctx.kernalContext().metric()
            );

            PageMemory pageMem = grpCtx.dataRegion().pageMemory();

            for (int i = 0; i < rootPages.size(); i++) {
                Long rootPage = rootPages.get(i);

                assert rootPage != null;

                if (skipDeletedRoot(grpId, pageMem, rootPage)) {
                    ctx.log(getClass()).warning(S.toString("Skipping deletion of the index tree",
                        "cacheGrpName", cacheGrpName, false,
                        "cacheName", cacheName, false,
                        "idxName", idxName, false,
                        "segment", i, false,
                        "rootPageId", PageIdUtils.toDetailString(rootPage), false
                    ));

                    continue;
                }

                // Below we create a fake index tree using it's root page, stubbing some parameters,
                // because we just going to free memory pages that are occupied by tree structure.
                try {
                    String treeName = "deletedTree_" + i + "_" + name();

                    InlineIndexTree tree = new InlineIndexTree(
                        null, grpCtx, treeName, cctx.offheap(), cctx.offheap().reuseListForIndex(treeName),
                        cctx.dataRegion().pageMemory(), PageIoResolver.DEFAULT_PAGE_IO_RESOLVER,
                        rootPage, false, 0, 0, new IndexKeyTypeSettings(), null,
                        stats, new NoopRowHandlerFactory(), null);

                    trees0.add(tree);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }

        ctx.cache().context().database().checkpointReadLock();

        try {
            for (int i = 0; i < trees0.size(); i++) {
                BPlusTree tree = trees0.get(i);

                try {
                    tree.destroy(null, true);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }
        finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }
    }

    /**
     * Checks that pageId is still relevant and has not been deleted / reused.
     * @param grpId Cache group id.
     * @param pageMem Page memory instance.
     * @param rootPageId Root page identifier.
     * @return {@code true} if root page was deleted/reused, {@code false} otherwise.
     */
    private boolean skipDeletedRoot(int grpId, PageMemory pageMem, long rootPageId) {
        try {
            long page = pageMem.acquirePage(grpId, rootPageId);

            try {
                long pageAddr = pageMem.readLock(grpId, rootPageId, page);

                try {
                    return pageAddr == 0;
                }
                finally {
                    if (pageAddr != 0)
                        pageMem.readUnlock(grpId, rootPageId, page);
                }
            }
            finally {
                pageMem.releasePage(grpId, rootPageId, page);
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Cannot acquire tree root page.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        trees = null;

        GridWorker w = worker;

        if (w != null) {
            worker = null;

            U.awaitForWorkersStop(singleton(w), true, log);
        }
    }

    /** {@inheritDoc} */
    @Override public DurableBackgroundTask<?> convertAfterRestoreIfNeeded() {
        return new DurableBackgroundCleanupIndexTreeTaskV2(
            cacheGrpName,
            cacheName,
            idxName,
            treeName,
            UUID.randomUUID().toString(),
            rootPages.size(),
            null
        );
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DurableBackgroundCleanupIndexTreeTask.class, this);
    }
}

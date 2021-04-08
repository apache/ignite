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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.metric.IoStatisticsHolderIndex;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIoResolver;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.metric.IoStatisticsType.SORTED_INDEX;

/**
 * Tasks that cleans up index tree.
 */
public class DurableBackgroundCleanupIndexTreeTask implements DurableBackgroundTask {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private List<Long> rootPages;

    /** */
    private transient List<InlineIndexTree> trees;

    /** */
    private transient volatile boolean completed;

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
        this.completed = false;
        this.cacheGrpName = cacheGrpName;
        this.cacheName = cacheName;
        this.id = UUID.randomUUID().toString();
        this.idxName = idxName.idxName();
        this.schemaName = idxName.schemaName();
        this.treeName = treeName;
    }

    /** {@inheritDoc} */
    @Override public String shortName() {
        return "DROP_SQL_INDEX-" + schemaName + "." + idxName + "-" + id;
    }

    /** {@inheritDoc} */
    @Override public void execute(GridKernalContext ctx) {
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
                    String treeName = "deletedTree_" + i + "_" + shortName();

                    InlineIndexTree tree = new InlineIndexTree(
                        null, cctx, treeName, cctx.offheap(), cctx.offheap().reuseListForIndex(treeName),
                        cctx.dataRegion().pageMemory(), PageIoResolver.DEFAULT_PAGE_IO_RESOLVER,
                        rootPage, false, 0, new IndexKeyTypeSettings(), null,
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
    @Override public void complete() {
        completed = true;
    }

    /** {@inheritDoc} */
    @Override public boolean isCompleted() {
        return completed;
    }

    /** {@inheritDoc} */
    @Override public void onCancel() {
        trees = null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DurableBackgroundCleanupIndexTreeTask.class, this);
    }

    /** */
    private static class NoopRowHandlerFactory implements InlineIndexRowHandlerFactory {
        /** {@inheritDoc} */
        @Override public InlineIndexRowHandler create(SortedIndexDefinition sdef, IndexKeyTypeSettings keyTypeSettings) {
            return new InlineIndexRowHandler() {
                /** {@inheritDoc} */
                @Override public IndexKey indexKey(int idx, CacheDataRow row) {
                    return null;
                }

                /** {@inheritDoc} */
                @Override public List<InlineIndexKeyType> inlineIndexKeyTypes() {
                    return Collections.emptyList();
                }

                /** {@inheritDoc} */
                @Override public List<IndexKeyDefinition> indexKeyDefinitions() {
                    return Collections.emptyList();
                }

                @Override public IndexKeyTypeSettings indexKeyTypeSettings() {
                    return null;
                }

                /** {@inheritDoc} */
                @Override public int partition(CacheDataRow row) {
                    return 0;
                }

                /** {@inheritDoc} */
                @Override public Object cacheKey(CacheDataRow row) {
                    return null;
                }

                /** {@inheritDoc} */
                @Override public Object cacheValue(CacheDataRow row) {
                    return null;
                }
            };
        }
    }
}

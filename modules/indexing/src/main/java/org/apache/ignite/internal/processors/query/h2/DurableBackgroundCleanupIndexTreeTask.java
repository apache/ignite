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
package org.apache.ignite.internal.processors.query.h2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.metric.IoStatisticsHolderIndex;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
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
    private transient List<H2Tree> trees;

    /** */
    private transient volatile boolean completed;

    /** */
    private String cacheGrpName;

    /** */
    private String cacheName;

    /** */
    private String schemaName;

    /** */
    private String idxName;

    /** */
    private String id;

    /** */
    public DurableBackgroundCleanupIndexTreeTask(
        List<Long> rootPages,
        List<H2Tree> trees,
        String cacheGrpName,
        String cacheName,
        String schemaName,
        String idxName
    ) {
        this.rootPages = rootPages;
        this.trees = trees;
        this.completed = false;
        this.cacheGrpName = cacheGrpName;
        this.cacheName = cacheName;
        this.schemaName = schemaName;
        this.idxName = idxName;
        this.id = UUID.randomUUID().toString();
    }

    /** {@inheritDoc} */
    @Override public String shortName() {
        return "DROP_SQL_INDEX-" + schemaName + "." + idxName + "-" + id;
    }

    /** {@inheritDoc} */
    @Override public void execute(GridKernalContext ctx) {
        List<H2Tree> trees0 = trees;

        if (trees0 == null) {
            trees0 = new ArrayList<>(rootPages.size());

            GridCacheContext cctx = ctx.cache().context().cacheContext(CU.cacheId(cacheName));

            IoStatisticsHolderIndex stats = new IoStatisticsHolderIndex(
                SORTED_INDEX,
                cctx.name(),
                idxName,
                cctx.kernalContext().metric()
            );

            for (int i = 0; i < rootPages.size(); i++) {
                Long rootPage = rootPages.get(i);

                assert rootPage != null;

                // Below we create a fake index tree using it's root page, stubbing some parameters,
                // because we just going to free memory pages that are occupied by tree structure.
                try {
                    String treeName = "deletedTree_" + i + "_" + shortName();

                    H2Tree tree = new H2Tree(
                        cctx,
                        null,
                        treeName,
                        idxName,
                        cacheName,
                        null,
                        cctx.offheap().reuseListForIndex(treeName),
                        CU.cacheGroupId(cacheName, cacheGrpName),
                        cacheGrpName,
                        cctx.dataRegion().pageMemory(),
                        ctx.cache().context().wal(),
                        cctx.offheap().globalRemoveId(),
                        rootPage,
                        false,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new AtomicInteger(0),
                        false,
                        false,
                        false,
                        null,
                        ctx.failure(),
                        null,
                        stats,
                        null,
                        0
                    );

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
}

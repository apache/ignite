/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.verify;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class for idle verify command.
 */
public class IdleVerifyUtility {
    /** Cluster not idle message. */
    public static final String CLUSTER_NOT_IDLE_MSG = "Checkpoint with dirty pages started! Cluster not idle!";

    /**
     * See {@link IdleVerifyUtility#checkPartitionsPageCrcSum(FilePageStore, CacheGroupContext, int, byte,
     * AtomicBoolean)}.
     */
    public static void checkPartitionsPageCrcSum(
        @Nullable FilePageStoreManager pageStoreMgr,
        CacheGroupContext grpCtx,
        int partId,
        byte pageType,
        AtomicBoolean cpFlag
    ) throws IgniteCheckedException, GridNotIdleException {
        if (!grpCtx.persistenceEnabled() || pageStoreMgr == null)
            return;

        FilePageStore pageStore = (FilePageStore)pageStoreMgr.getStore(grpCtx.groupId(), partId);

        checkPartitionsPageCrcSum(pageStore, grpCtx, partId, pageType, cpFlag);
    }

    /**
     * Checks CRC sum of pages with {@code pageType} page type stored in partiion with {@code partId} id and assosiated
     * with cache group. <br/> Method could be invoked only on idle cluster!
     *
     * @param pageStore Page store.
     * @param grpCtx Passed cache group context.
     * @param partId Partition id.
     * @param pageType Page type. Possible types {@link PageIdAllocator#FLAG_DATA}, {@link PageIdAllocator#FLAG_IDX}.
     * @param cpFlag Checkpoint flag for detecting start checkpoint with dirty pages.
     * @throws IgniteCheckedException If reading page failed.
     * @throws GridNotIdleException If cluster not idle.
     */
    public static void checkPartitionsPageCrcSum(
        FilePageStore pageStore,
        CacheGroupContext grpCtx,
        int partId,
        byte pageType,
        AtomicBoolean cpFlag
    ) throws IgniteCheckedException, GridNotIdleException {
        assert pageType == PageIdAllocator.FLAG_DATA || pageType == PageIdAllocator.FLAG_IDX : pageType;

        long pageId = PageIdUtils.pageId(partId, pageType, 0);

        ByteBuffer buf = ByteBuffer.allocateDirect(grpCtx.dataRegion().pageMemory().pageSize());

        buf.order(ByteOrder.nativeOrder());

        for (int pageNo = 0; pageNo < pageStore.pages(); pageId++, pageNo++) {
            buf.clear();

            if (cpFlag.get())
                throw new GridNotIdleException(CLUSTER_NOT_IDLE_MSG);

            pageStore.read(pageId, buf, true);
        }

        if (cpFlag.get())
            throw new GridNotIdleException(CLUSTER_NOT_IDLE_MSG);
    }

    /**
     * @param db Shared DB manager.
     * @return {@code True} if checkpoint is now, {@code False} otherwise.
     */
    public static boolean isCheckpointNow(@Nullable IgniteCacheDatabaseSharedManager db) {
        if (!(db instanceof GridCacheDatabaseSharedManager))
            return false;

        GridCacheDatabaseSharedManager.CheckpointProgress progress =
            ((GridCacheDatabaseSharedManager)db).getCheckpointer().currentProgress();

        if (progress == null)
            return false;

        return progress.started() && !progress.finished();
    }

    /** */
    private IdleVerifyUtility() {
        /* No-op. */
    }
}

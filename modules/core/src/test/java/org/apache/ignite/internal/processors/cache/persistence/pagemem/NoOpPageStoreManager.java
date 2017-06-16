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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteFuture;

/**
 *
 */
public class NoOpPageStoreManager implements IgnitePageStoreManager {
    /** */
    private ConcurrentMap<FullPageId, AtomicInteger> allocators = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public void beginRecover() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void finishRecover() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void initializeForCache(CacheGroupDescriptor grpDesc,
        StoredCacheData cacheData) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void shutdownForCacheGroup(CacheGroupContext grp, boolean destroy) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onPartitionCreated(int grpId, int partId) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onPartitionDestroyed(int cacheId, int partId, int tag) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void read(int cacheId, long pageId, ByteBuffer pageBuf) throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public boolean exists(int cacheId, int partId) throws IgniteCheckedException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void readHeader(int cacheId, int partId, ByteBuffer buf) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void write(int cacheId, long pageId, ByteBuffer pageBuf, int tag) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void sync(int cacheId, int partId) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void ensure(int cacheId, int partId) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public long pageOffset(int cacheId, long pageId) throws IgniteCheckedException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long allocatePage(int cacheId, int partId, byte flags) throws IgniteCheckedException {
        long root = PageIdUtils.pageId(partId, flags, 0);

        FullPageId fullId = new FullPageId(root, cacheId);

        AtomicInteger allocator = allocators.get(fullId);

        if (allocator == null)
            allocator = F.addIfAbsent(allocators, fullId, new AtomicInteger(1));

        return PageIdUtils.pageId(partId, flags, allocator.getAndIncrement());
    }

    /** {@inheritDoc} */
    @Override public int pages(int cacheId, int partId) throws IgniteCheckedException {
        long root = PageIdUtils.pageId(partId, (byte)0, 0);

        FullPageId fullId = new FullPageId(root, cacheId);

        AtomicInteger allocator = allocators.get(fullId);

        if (allocator == null)
            allocator = F.addIfAbsent(allocators, fullId, new AtomicInteger(2));

        return allocator.get();
    }

    /** {@inheritDoc} */
    @Override public long metaPageId(int cacheId) {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public void start(GridCacheSharedContext cctx) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean reconnect) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture reconnectFut) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Map<String, StoredCacheData> readCacheConfigurations() throws IgniteCheckedException {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public void storeCacheData(CacheGroupDescriptor grpDesc,
        StoredCacheData cacheData) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean hasIndexStore(int grpId) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) {

    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {

    }
}

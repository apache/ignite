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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.file.PageStoreCollection;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.lang.IgniteFuture;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.MAX_PARTITION_ID;

/** */
//TODO Checkpointing stuff is not implemented. It is yet to be defined.
//TODO create common "superclass" with FilePageStoreManager? It could be tricky.
public class DefragmentationPageStoreManager implements IgnitePageStoreManager {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final PageStoreCollection pageStores;

    /** */
    private final long metaPageId = PageIdUtils.pageId(INDEX_PARTITION, PageMemory.FLAG_IDX, 0);

    /**
     * @param ctx Kernal context.
     * @param pageStores Page stores.
     */
    public DefragmentationPageStoreManager(
        GridKernalContext ctx,
        PageStoreCollection pageStores
    ) {
        this.ctx = ctx;
        this.pageStores = pageStores;
    }

    /** {@inheritDoc} */
    @Override public long metaPageId(int grpId) {
        return metaPageId;
    }

    /** {@inheritDoc} */
    @Override public void read(int grpId, long pageId, ByteBuffer pageBuf) throws IgniteCheckedException {
        PageStore store = pageStores.getStore(grpId, PageIdUtils.partId(pageId));

        try {
            store.read(pageId, pageBuf, false);

            ctx.compress().decompressPage(pageBuf, store.getPageSize());
        }
        catch (StorageException e) {
            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    // Required for replacer...
    /** {@inheritDoc} */
    @Override public void write(int grpId, long pageId, ByteBuffer pageBuf, int tag) throws IgniteCheckedException {
        writeInternal(grpId, pageId, pageBuf, tag, true);
    }

    /**
     * @param cacheId Cache ID to write.
     * @param pageId Page ID.
     * @param pageBuf Page buffer.
     * @param tag Partition tag (growing 1-based partition file version). Used to validate page is not outdated
     * @param calculateCrc if {@code False} crc calculation will be forcibly skipped.
     * @return PageStore to which the page has been written.
     * @throws IgniteCheckedException If IO error occurred.
     */
    //TODO this is a direct copy of org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager#writeInternal
    public PageStore writeInternal(int cacheId, long pageId, ByteBuffer pageBuf, int tag, boolean calculateCrc)
        throws IgniteCheckedException {
        int partId = PageIdUtils.partId(pageId);

        PageStore store = pageStores.getStore(cacheId, partId);

        try {
            int pageSize = store.getPageSize();
            int compressedPageSize = pageSize;

            GridCacheContext<?, ?> cctx0 = ctx.cache().context().cacheContext(cacheId);

            if (cctx0 != null) {
                assert pageBuf.position() == 0 && pageBuf.limit() == pageSize : pageBuf;

                ByteBuffer compressedPageBuf = cctx0.compress().compressPage(pageBuf, store);

                if (compressedPageBuf != pageBuf) {
                    compressedPageSize = PageIO.getCompressedSize(compressedPageBuf);

                    if (!calculateCrc) {
                        calculateCrc = true;
                        PageIO.setCrc(compressedPageBuf, 0); // It will be recalculated over compressed data further.
                    }

                    PageIO.setCrc(pageBuf, 0); // It is expected to be reset to 0 after each write.
                    pageBuf = compressedPageBuf;
                }
            }

            store.write(pageId, pageBuf, tag, calculateCrc);

            if (pageSize > compressedPageSize)
                store.punchHole(pageId, compressedPageSize); // TODO maybe add async punch mode?
        }
        catch (StorageException e) {
            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }

        return store;
    }

    /** {@inheritDoc} */
    @Override public long allocatePage(int grpId, int partId, byte flags) throws IgniteCheckedException {
        assert partId <= MAX_PARTITION_ID || partId == INDEX_PARTITION;

        PageStore store = pageStores.getStore(grpId, partId);

        try {
            long pageIdx = store.allocatePage();

            return PageIdUtils.pageId(partId, flags, (int)pageIdx);
        }
        catch (StorageException e) {
            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void beginRecover() {
        throw new UnsupportedOperationException("beginRecover");
    }

    /** {@inheritDoc} */
    @Override public void finishRecover() {
        throw new UnsupportedOperationException("finishRecover");
    }

    /** {@inheritDoc} */
    @Override public void initialize(int cacheId, int partitions, String workingDir, LongConsumer tracker) {
        throw new UnsupportedOperationException("initialize");
    }

    /** {@inheritDoc} */
    @Override public void initializeForCache(CacheGroupDescriptor grpDesc, StoredCacheData cacheData) {
        throw new UnsupportedOperationException("initializeForCache");
    }

    /** {@inheritDoc} */
    @Override public void initializeForMetastorage() {
        throw new UnsupportedOperationException("initializeForMetastorage");
    }

    /** {@inheritDoc} */
    @Override public void shutdownForCacheGroup(CacheGroupContext grp, boolean destroy) {
        throw new UnsupportedOperationException("shutdownForCacheGroup");
    }

    /** {@inheritDoc} */
    @Override public void onPartitionCreated(int grpId, int partId) {
        throw new UnsupportedOperationException("onPartitionCreated");
    }

    /** {@inheritDoc} */
    @Override public void onPartitionDestroyed(int grpId, int partId, int tag) {
        throw new UnsupportedOperationException("onPartitionDestroyed");
    }

    /** {@inheritDoc} */
    @Override public boolean exists(int grpId, int partId) {
        throw new UnsupportedOperationException("exists");
    }

    /** {@inheritDoc} */
    @Override public void readHeader(int grpId, int partId, ByteBuffer buf) {
        throw new UnsupportedOperationException("readHeader");
    }

    /** {@inheritDoc} */
    @Override public long pageOffset(int grpId, long pageId) {
        throw new UnsupportedOperationException("pageOffset");
    }

    /** {@inheritDoc} */
    @Override public void sync(int grpId, int partId) {
        throw new UnsupportedOperationException("sync");
    }

    /** {@inheritDoc} */
    @Override public void ensure(int grpId, int partId) {
        throw new UnsupportedOperationException("ensure");
    }

    /** {@inheritDoc} */
    @Override public int pages(int grpId, int partId) {
        throw new UnsupportedOperationException("pages");
    }

    /** {@inheritDoc} */
    @Override public Map<String, StoredCacheData> readCacheConfigurations() {
        throw new UnsupportedOperationException("readCacheConfigurations");
    }

    /** {@inheritDoc} */
    @Override public void storeCacheData(StoredCacheData cacheData, boolean overwrite) {
        throw new UnsupportedOperationException("storeCacheData");
    }

    /** {@inheritDoc} */
    @Override public void removeCacheData(StoredCacheData cacheData) {
        throw new UnsupportedOperationException("removeCacheData");
    }

    /** {@inheritDoc} */
    @Override public boolean hasIndexStore(int grpId) {
        throw new UnsupportedOperationException("hasIndexStore");
    }

    /** {@inheritDoc} */
    @Override public void beforeCacheGroupStart(CacheGroupDescriptor grpDesc) {
        throw new UnsupportedOperationException("beforeCacheGroupStart");
    }

    /** {@inheritDoc} */
    @Override public long pagesAllocated(int grpId) {
        throw new UnsupportedOperationException("pagesAllocated");
    }

    /** {@inheritDoc} */
    @Override public void cleanupPersistentSpace(CacheConfiguration cacheConfiguration) {
        throw new UnsupportedOperationException("cleanupPersistentSpace");
    }

    /** {@inheritDoc} */
    @Override public void cleanupPersistentSpace() {
        throw new UnsupportedOperationException("cleanupPersistentSpace");
    }

    /** {@inheritDoc} */
    @Override public void cleanupPageStoreIfMatch(Predicate<Integer> cacheGrpPred, boolean cleanFiles) {
        throw new UnsupportedOperationException("cleanupPageStoreIfMatch");
    }

    /** {@inheritDoc} */
    @Override public boolean checkAndInitCacheWorkDir(CacheConfiguration cacheCfg) {
        throw new UnsupportedOperationException("checkAndInitCacheWorkDir");
    }

    /** {@inheritDoc} */
    @Override public void start(GridCacheSharedContext cctx) {
        throw new UnsupportedOperationException("start");
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) {
        throw new UnsupportedOperationException("onKernalStart");
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        throw new UnsupportedOperationException("stop");
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        throw new UnsupportedOperationException("onKernalStop");
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture reconnectFut) {
        throw new UnsupportedOperationException("onDisconnected");
    }

    /** {@inheritDoc} */
    @Override public void onReconnected(boolean active) {
        throw new UnsupportedOperationException("onReconnected");
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        throw new UnsupportedOperationException("printMemoryStats");
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) {
        throw new UnsupportedOperationException("onActivate");
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        throw new UnsupportedOperationException("onDeActivate");
    }
}

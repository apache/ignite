package org.apache.ignite.cache.database.pagemem;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
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
    @Override public void initializeForCache(CacheConfiguration ccfg) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void shutdownForCache(GridCacheContext cacheCtx, boolean destroy) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onPartitionCreated(int cacheId, int partId) throws IgniteCheckedException {
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
    @Override public Set<String> savedCacheNames() {
        return Collections.emptySet();
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration readConfiguration(String cacheName) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasIndexStore(int cacheId) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) {

    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {

    }
}

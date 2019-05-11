package org.apache.ignite.internal.benchmarks.jmh.diagnostic.pagelocktracker.stack;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;

public class LockTrackerNoBarrier implements PageLockListener {

    private final PageLockTracker delegate;

    public LockTrackerNoBarrier(
        PageLockTracker delegate
    ) {
        this.delegate = delegate;
    }

    @Override public void onBeforeWriteLock(int cacheId, long pageId, long page) {
        delegate.onBeforeWriteLock0(cacheId, pageId, page);
    }

    @Override public void onWriteLock(int cacheId, long pageId, long page, long pageAddr) {
        delegate.onWriteLock0(cacheId, pageId, page, pageAddr);
    }

    @Override public void onWriteUnlock(int cacheId, long pageId, long page, long pageAddr) {
        delegate.onWriteUnlock0(cacheId, pageId, page, pageAddr);
    }

    @Override public void onBeforeReadLock(int cacheId, long pageId, long page) {
        delegate.onBeforeReadLock0(cacheId, pageId, page);
    }

    @Override public void onReadLock(int cacheId, long pageId, long page, long pageAddr) {
        delegate.onReadLock0(cacheId, pageId, page, pageAddr);
    }

    @Override public void onReadUnlock(int cacheId, long pageId, long page, long pageAddr) {
        delegate.onReadUnlock(cacheId, pageId, page, pageAddr);
    }
}
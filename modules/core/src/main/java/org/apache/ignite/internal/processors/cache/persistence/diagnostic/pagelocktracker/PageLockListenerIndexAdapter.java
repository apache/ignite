package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;

public class PageLockListenerIndexAdapter implements PageLockListener {
    private final int idx;

    private final PageLockListener delegate;

    public PageLockListenerIndexAdapter(int idx, PageLockListener delegate) {
        this.idx = idx;
        this.delegate = delegate;
    }

    @Override public void onBeforeWriteLock(int cacheId, long pageId, long page) {
        delegate.onBeforeWriteLock(idx, pageId, page);
    }

    @Override public void onWriteLock(int cacheId, long pageId, long page, long pageAddr) {
        delegate.onWriteLock(idx, pageId, page, pageAddr);
    }

    @Override public void onWriteUnlock(int cacheId, long pageId, long page, long pageAddr) {
        delegate.onWriteUnlock(idx, pageId, page, pageAddr);
    }

    @Override public void onBeforeReadLock(int cacheId, long pageId, long page) {
        delegate.onBeforeReadLock(idx, pageId, page);
    }

    @Override public void onReadLock(int cacheId, long pageId, long page, long pageAddr) {
        delegate.onReadLock(idx, pageId, page, pageAddr);
    }

    @Override public void onReadUnlock(int cacheId, long pageId, long page, long pageAddr) {
        delegate.onReadUnlock(idx, pageId, page, pageAddr);
    }
}

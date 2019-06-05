/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;

/**
 * Page lock listener adapter with Id.
 */
public class PageLockListenerIndexAdapter implements PageLockListener {
    /** Adapter id. */
    private final int id;

    /** Real listener. */
    private final PageLockListener delegate;

    /**
     * @param id Adapter id.
     * @param delegate Real listener.
     */
    public PageLockListenerIndexAdapter(int id, PageLockListener delegate) {
        this.id = id;
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public void onBeforeWriteLock(int cacheId, long pageId, long page) {
        delegate.onBeforeWriteLock(id, pageId, page);
    }

    /** {@inheritDoc} */
    @Override public void onWriteLock(int cacheId, long pageId, long page, long pageAddr) {
        delegate.onWriteLock(id, pageId, page, pageAddr);
    }

    /** {@inheritDoc} */
    @Override public void onWriteUnlock(int cacheId, long pageId, long page, long pageAddr) {
        delegate.onWriteUnlock(id, pageId, page, pageAddr);
    }

    /** {@inheritDoc} */
    @Override public void onBeforeReadLock(int cacheId, long pageId, long page) {
        delegate.onBeforeReadLock(id, pageId, page);
    }

    /** {@inheritDoc} */
    @Override public void onReadLock(int cacheId, long pageId, long page, long pageAddr) {
        delegate.onReadLock(id, pageId, page, pageAddr);
    }

    /** {@inheritDoc} */
    @Override public void onReadUnlock(int cacheId, long pageId, long page, long pageAddr) {
        delegate.onReadUnlock(id, pageId, page, pageAddr);
    }
}

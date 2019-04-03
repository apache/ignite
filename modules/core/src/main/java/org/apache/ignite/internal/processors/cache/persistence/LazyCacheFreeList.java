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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.stat.IoStatisticsHolder;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 *
 */
public abstract class LazyCacheFreeList implements CacheFreeList<CacheDataRow> {
    /** */
    private static final AtomicReferenceFieldUpdater<LazyCacheFreeList, CountDownLatch> initLatchUpdater =
        AtomicReferenceFieldUpdater.newUpdater(LazyCacheFreeList.class, CountDownLatch.class, "initLatch");

    /** */
    private volatile CacheFreeList<CacheDataRow> delegate;

    /** */
    private IgniteCheckedException initErr;

    /** */
    private volatile CountDownLatch initLatch;

    /** {@inheritDoc} */
    @Override public void saveMetadata() throws IgniteCheckedException {
        CacheFreeList delegate = this.delegate;

        if (delegate != null)
            delegate.saveMetadata();
    }

    /** {@inheritDoc} */
    @Override public void insertDataRow(CacheDataRow row, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        initDelegateIfNeeded(true).insertDataRow(row, statHolder);
    }

    /** {@inheritDoc} */
    @Override public boolean updateDataRow(long link, CacheDataRow row, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        return initDelegateIfNeeded(true).updateDataRow(link, row, statHolder);
    }

    /** {@inheritDoc} */
    @Override public <S, R> R updateDataRow(long link, PageHandler<S, R> pageHnd, S arg, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        return (R)initDelegateIfNeeded(true).updateDataRow(link, pageHnd, arg, statHolder);
    }

    /** {@inheritDoc} */
    @Override public void removeDataRowByLink(long link, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        initDelegateIfNeeded(true).removeDataRowByLink(link, statHolder);
    }

    /** {@inheritDoc} */
    @Override public int emptyDataPages() {
        try {
            CacheFreeList<CacheDataRow> freeList = initDelegateIfNeeded(false);

            return freeList != null ? freeList.emptyDataPages() : 0;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to initialize FreeList", e);
        }
    }

    /** {@inheritDoc} */
    @Override public long freeSpace() {
        try {
            CacheFreeList<CacheDataRow> freeList = initDelegateIfNeeded(false);

            return freeList != null ? freeList.freeSpace() : null;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to initialize FreeList", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void dumpStatistics(IgniteLogger log) {
        CacheFreeList delegate = this.delegate;

        if (delegate != null)
            delegate.dumpStatistics(log);
    }

    /** {@inheritDoc} */
    @Override public void addForRecycle(ReuseBag bag) throws IgniteCheckedException {
        initDelegateIfNeeded(true).addForRecycle(bag);
    }

    /** {@inheritDoc} */
    @Override public long takeRecycledPage() throws IgniteCheckedException {
        return initDelegateIfNeeded(true).takeRecycledPage();
    }

    /** {@inheritDoc} */
    @Override public long recycledPagesCount() throws IgniteCheckedException {
        return  initDelegateIfNeeded(true).recycledPagesCount();
    }

    /**
     * @return Cache free list.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract CacheFreeList<CacheDataRow> createDelegate(boolean create) throws IgniteCheckedException;

    /**
     * @return Cache free list.
     * @throws IgniteCheckedException If failed to initialize free list.
     */
    private CacheFreeList<CacheDataRow> initDelegateIfNeeded(boolean create) throws IgniteCheckedException {
        CacheFreeList<CacheDataRow> delegate = this.delegate;

        if (delegate != null)
            return delegate;

        CountDownLatch initLatch = this.initLatch;

        if (initLatch != null)
            U.await(initLatch);
        else {
            initLatch = new CountDownLatch(1);

            if (initLatchUpdater.compareAndSet(this, null, initLatch)) {
                try {
                    this.delegate = createDelegate(create);
                }
                catch (IgniteCheckedException e) {
                    initErr = e;
                }
                catch (Throwable e) {
                    initErr = new IgniteCheckedException(e);

                    throw e;
                }
                finally {
                    initLatch.countDown();
                }
            }
            else {
                initLatch = this.initLatch;

                assert initLatch != null;

                U.await(initLatch);
            }
        }

        IgniteCheckedException initErr = this.initErr;

        if (initErr != null)
            throw initErr;

        return this.delegate;
    }
}

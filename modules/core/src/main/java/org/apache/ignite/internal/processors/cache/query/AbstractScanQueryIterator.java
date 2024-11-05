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

package org.apache.ignite.internal.processors.cache.query;

import java.util.NoSuchElementException;
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectUtils;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheEntryFilter;
import org.apache.ignite.internal.processors.security.SecurityUtils;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager.injectResources;
import static org.apache.ignite.internal.processors.security.SecurityUtils.securitySubjectId;

/**
 * Abstract scan query iterator.
 */
public abstract class AbstractScanQueryIterator<K, V, R> extends GridCloseableIteratorAdapter<R> {
    /** */
    protected final IgniteBiPredicate<K, V> filter;

    /** */
    protected final boolean statsEnabled;

    /** */
    protected final boolean keepBinary;

    /** */
    protected final boolean readEvt;

    /** */
    protected final UUID subjId;

    /** */
    protected final String taskName;

    /** */
    protected final IgniteClosure<Cache.Entry<K, V>, R> transform;

    /** */
    protected final GridCacheContext<K, V> cctx;

    /** */
    protected final boolean locNode;

    /** */
    protected R next;

    /** */
    private boolean needAdvance;

    /**
     * @param cctx Grid cache context.
     * @param qry Query adapter.
     * @param transform Optional transformer.
     * @param locNode Flag for local node iterator.
     * @throws IgniteCheckedException If failed.
     */
    protected AbstractScanQueryIterator(
        GridCacheContext<K, V> cctx,
        CacheQuery<R> qry,
        IgniteClosure<Cache.Entry<K, V>, R> transform,
        boolean locNode
    ) throws IgniteCheckedException {
        this.cctx = cctx;
        this.filter = prepareFilter(qry.scanFilter());
        this.transform = SecurityUtils.sandboxedProxy(cctx.kernalContext(), IgniteClosure.class, injectResources(transform, cctx));
        this.locNode = locNode;

        statsEnabled = cctx.statisticsEnabled();

        readEvt = cctx.events().isRecordable(EVT_CACHE_QUERY_OBJECT_READ) &&
            cctx.gridEvents().hasListener(EVT_CACHE_QUERY_OBJECT_READ);

        taskName = readEvt ? cctx.kernalContext().task().resolveTaskName(qry.taskHash()) : null;

        subjId = securitySubjectId(cctx);

        // keep binary for remote scans if possible
        keepBinary = (!locNode && filter == null && transform == null && !readEvt) || qry.keepBinary();

        needAdvance = true;
    }

    /** {@inheritDoc} */
    @Override protected R onNext() {
        if (needAdvance)
            next = advance();
        else
            needAdvance = true;

        if (next == null)
            throw new NoSuchElementException();

        return next;
    }

    /** {@inheritDoc} */
    @Override protected boolean onHasNext() {
        if (needAdvance) {
            next = advance();

            needAdvance = false;
        }

        return next != null;
    }

    /** {@inheritDoc} */
    @Override protected void onClose() throws IgniteCheckedException {
        close(filter);
    }

    /** Moves the iterator to the next cache entry. */
    protected abstract R advance();

    /**
     * Perform filtering and transformation of key-value pair.
     *
     * @return Object to return to the user, or {@code null} if filtered.
     */
    public R filterAndTransform(
        final KeyCacheObject key,
        final CacheObject val,
        final long start
    ) {
        if (statsEnabled) {
            CacheMetricsImpl metrics = cctx.cache().metrics0();

            metrics.onRead(true);

            metrics.addGetTimeNanos(System.nanoTime() - start);
        }

        K key0 = (K)CacheObjectUtils.unwrapBinaryIfNeeded(cctx.cacheObjectContext(), key, keepBinary, false);
        V val0 = (V)CacheObjectUtils.unwrapBinaryIfNeeded(cctx.cacheObjectContext(), val, keepBinary, false);

        if (filter != null) {
            try {
                if (!filter.apply(key0, val0))
                    return null;
            }
            catch (Throwable e) {
                throw new IgniteException(e);
            }
        }

        if (readEvt) {
            cctx.gridEvents().record(new CacheQueryReadEvent<>(
                cctx.localNode(),
                "Scan query entry read.",
                EVT_CACHE_QUERY_OBJECT_READ,
                CacheQueryType.SCAN.name(),
                cctx.name(),
                null,
                null,
                filter,
                null,
                null,
                subjId,
                taskName,
                key0,
                val0,
                null,
                null));
        }

        try {
            Cache.Entry<K, V> res = new CacheQueryEntry<>(key0, val0);

            return transform == null ? (R)res : transform.apply(res);
        }
        catch (Throwable e) {
            throw new IgniteException(e);
        }
    }

    /** */
    @Nullable
    public IgniteBiPredicate<K, V> filter() {
        return filter;
    }

    /** */
    private @Nullable IgniteBiPredicate<K, V> prepareFilter(IgniteBiPredicate<K, V> keyValFilter) throws IgniteCheckedException {
        if (keyValFilter == null)
            return null;

        try {
            if (keyValFilter instanceof PlatformCacheEntryFilter)
                ((PlatformCacheEntryFilter)keyValFilter).cacheContext(cctx);
            else
                injectResources(keyValFilter, cctx);

            keyValFilter = SecurityUtils.sandboxedProxy(cctx.kernalContext(), IgniteBiPredicate.class, keyValFilter);

            return keyValFilter;
        }
        catch (IgniteCheckedException | RuntimeException e) {
            close(keyValFilter);

            throw e;
        }
    }

    /** */
    private void close(IgniteBiPredicate<?, ?> scanFilter) {
        if (scanFilter instanceof PlatformCacheEntryFilter)
            ((PlatformCacheEntryFilter)scanFilter).onClose();
    }
}

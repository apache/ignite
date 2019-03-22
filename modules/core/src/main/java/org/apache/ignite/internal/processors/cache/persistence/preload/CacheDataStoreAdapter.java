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

package org.apache.ignite.internal.processors.cache.persistence.preload;

import java.util.List;
import java.util.Map;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMvccEntryInfo;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.RowStore;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccUpdateResult;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class CacheDataStoreAdapter implements IgniteCacheOffheapManager.CacheDataStore {
    /** */
    private static final String UNSUPPORTED_MSG = "The store doesn't support this operation";

    /** {@inheritDoc} */
    @Override public int partId() {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public void init(long size, long updCntr, @Nullable Map<Integer, Long> cacheSizes) {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public long cacheSize(int cacheId) {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Long> cacheSizes() {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public long fullSize() {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public void updateSize(int cacheId, long delta) {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public long updateCounter() {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public void updateCounter(long val) {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public void updateCounter(long start, long delta) {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public long nextUpdateCounter() {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public long getAndIncrementUpdateCounter(long delta) {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public long initialUpdateCounter() {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow createRow(GridCacheContext cctx, KeyCacheObject key, CacheObject val,
        GridCacheVersion ver, long expireTime, @Nullable CacheDataRow oldRow) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public int cleanup(GridCacheContext cctx,
        @Nullable List<MvccLinkAwareSearchRow> cleanupRows) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public void updateTxState(GridCacheContext cctx, CacheSearchRow row) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public void update(GridCacheContext cctx, KeyCacheObject key, CacheObject val, GridCacheVersion ver,
        long expireTime, @Nullable CacheDataRow oldRow) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public boolean mvccInitialValue(GridCacheContext cctx, KeyCacheObject key, @Nullable CacheObject val,
        GridCacheVersion ver, long expireTime, MvccVersion mvccVer,
        MvccVersion newMvccVer) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public boolean mvccApplyHistoryIfAbsent(GridCacheContext cctx, KeyCacheObject key,
        List<GridCacheMvccEntryInfo> hist) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public boolean mvccUpdateRowWithPreloadInfo(GridCacheContext cctx, KeyCacheObject key,
        @Nullable CacheObject val, GridCacheVersion ver, long expireTime, MvccVersion mvccVer, MvccVersion newMvccVer,
        byte mvccTxState, byte newMvccTxState) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public MvccUpdateResult mvccUpdate(GridCacheContext cctx, KeyCacheObject key, CacheObject val,
        GridCacheVersion ver, long expireTime, MvccSnapshot mvccSnapshot, @Nullable CacheEntryPredicate filter,
        EntryProcessor entryProc, Object[] invokeArgs, boolean primary, boolean needHist, boolean noCreate,
        boolean needOldVal, boolean retVal, boolean keepBinary) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public MvccUpdateResult mvccRemove(GridCacheContext cctx, KeyCacheObject key, MvccSnapshot mvccSnapshot,
        @Nullable CacheEntryPredicate filter, boolean primary, boolean needHistory, boolean needOldVal,
        boolean retVal) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public MvccUpdateResult mvccLock(GridCacheContext cctx, KeyCacheObject key,
        MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public void mvccRemoveAll(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public void invoke(GridCacheContext cctx, KeyCacheObject key,
        IgniteCacheOffheapManager.OffheapInvokeClosure c) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public void mvccApplyUpdate(GridCacheContext cctx, KeyCacheObject key, CacheObject val,
        GridCacheVersion ver, long expireTime, MvccVersion mvccVer) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public void remove(GridCacheContext cctx, KeyCacheObject key, int partId) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow find(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<CacheDataRow> mvccAllVersionsCursor(GridCacheContext cctx, KeyCacheObject key,
        Object x) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow mvccFind(GridCacheContext cctx, KeyCacheObject key,
        MvccSnapshot snapshot) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public List<IgniteBiTuple<Object, MvccVersion>> mvccFindAllVersions(GridCacheContext cctx,
        KeyCacheObject key) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor() throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(Object x) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(
        MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId,
        MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower,
        KeyCacheObject upper) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower, KeyCacheObject upper,
        Object x) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId, KeyCacheObject lower, KeyCacheObject upper,
        Object x, MvccSnapshot snapshot) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public void clear(int cacheId) throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public RowStore rowStore() {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public void updateInitialCounter(long cntr) {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public void setRowCacheCleaner(GridQueryRowCacheCleaner rowCacheCleaner) {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public PendingEntriesTree pendingTree() {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public GridLongList finalizeUpdateCounters() {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    /** {@inheritDoc} */
    @Override public void preload() throws IgniteCheckedException {
        throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
}
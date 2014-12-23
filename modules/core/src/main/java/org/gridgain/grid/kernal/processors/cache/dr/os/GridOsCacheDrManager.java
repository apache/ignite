/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.dr.os;

import org.apache.ignite.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.dr.*;
import org.gridgain.grid.kernal.processors.cache.transactions.*;
import org.gridgain.grid.kernal.processors.dr.*;
import org.jetbrains.annotations.*;

/**
 * No-op implementation for {@link GridCacheDrManager}.
 */
public class GridOsCacheDrManager<K, V> implements GridCacheDrManager<K, V> {
    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void start(GridCacheContext<K, V> cctx) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public byte dataCenterId() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void replicate(K key,
        @Nullable byte[] keyBytes,
        @Nullable V val,
        @Nullable byte[] valBytes,
        long ttl,
        long expireTime,
        GridCacheVersion ver,
        GridDrType drType) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public GridDrResolveResult<V> resolveAtomic(GridCacheEntryEx<K, V> e,
        GridCacheOperation op,
        @Nullable Object writeObj,
        @Nullable byte[] valBytes,
        long ttl,
        long drTtl,
        long drExpireTime,
        @Nullable GridCacheVersion drVer) throws IgniteCheckedException, GridCacheEntryRemovedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridDrResolveResult<V> resolveTx(GridCacheEntryEx<K, V> e,
        IgniteTxEntry<K, V> txEntry,
        GridCacheVersion newVer,
        GridCacheOperation op,
        V newVal,
        byte[] newValBytes,
        long newTtl,
        long newDrExpireTime) throws IgniteCheckedException, GridCacheEntryRemovedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void beforeExchange(long topVer, boolean left) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void partitionEvicted(int part) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onReceiveCacheEntriesReceived(int entriesCnt) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean receiveEnabled() {
        return false;
    }
}

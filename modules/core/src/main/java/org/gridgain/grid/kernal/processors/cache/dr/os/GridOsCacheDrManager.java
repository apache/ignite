/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.dr.os;

import org.gridgain.grid.*;
import org.gridgain.grid.dr.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.dr.*;
import org.gridgain.grid.kernal.processors.dr.*;

/**
 * No-op implementation for {@link GridCacheDrManager}.
 */
public class GridOsCacheDrManager<K, V> implements GridCacheDrManager<K, V> {
    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void start(GridCacheContext<K, V> cctx) throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
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
    @Override public void replicate(GridDrRawEntry<K, V> entry, GridDrType drType) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean needResolve(GridCacheVersion oldVer, GridCacheVersion newVer) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public GridDrReceiverConflictContextImpl<K, V> resolveConflict(K key,
        GridDrEntry<K, V> oldEntry,
        GridDrEntry<K, V> newEntry) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void beforeExchange(long topVer, boolean left) throws GridException {
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

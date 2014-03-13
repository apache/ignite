/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.dr.os;

import org.gridgain.grid.*;
import org.gridgain.grid.dr.cache.sender.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.dr.*;
import org.gridgain.grid.kernal.processors.dr.*;

import java.util.*;

/**
 * No-op implementation for {@link GridCacheDrManager}.
 */
public class GridOsCacheDrManager<K, V> implements GridCacheDrManager<K, V> {
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
    @Override public void replicate(GridDrRawEntry<K, V> entry, GridDrType drType) {
        // No-op.
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
    @Override public GridFuture<?> stateTransfer(Collection<Byte> dataCenterIds) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridDrStateTransfer> listStateTransfers() throws GridException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void pause() throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void resume() throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isPaused() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public int queuedKeysCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int backupQueueSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int batchWaitingSendCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int batchWaitingAcknowledgeCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int senderHubsCount() {
        return 0;
    }
}

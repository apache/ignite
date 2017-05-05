package org.apache.ignite.cache.database.pagemem;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.StorageException;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.lang.IgniteFuture;

/**
 *
 */
public class NoOpWALManager implements IgniteWriteAheadLogManager {
    /** {@inheritDoc} */
    @Override public boolean isAlwaysWriteFullPages() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isFullSync() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void resumeLogging(WALPointer ptr) throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public WALPointer log(WALRecord entry) throws IgniteCheckedException, StorageException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void fsync(WALPointer ptr) throws IgniteCheckedException, StorageException {

    }

    /** {@inheritDoc} */
    @Override public WALIterator replay(WALPointer start) throws IgniteCheckedException, StorageException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean reserve(WALPointer start) throws IgniteCheckedException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void release(WALPointer start) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int truncate(WALPointer ptr) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean reserved(WALPointer ptr) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void start(GridCacheSharedContext cctx) throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {

    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean reconnect) throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {

    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture reconnectFut) {

    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {

    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) throws IgniteCheckedException {

    }
}

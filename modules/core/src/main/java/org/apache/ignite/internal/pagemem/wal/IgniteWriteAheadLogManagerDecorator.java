package org.apache.ignite.internal.pagemem.wal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.lang.IgniteFuture;

/**
 *
 */
public class IgniteWriteAheadLogManagerDecorator implements IgniteWriteAheadLogManager {

    protected final IgniteWriteAheadLogManager delegate;

    public IgniteWriteAheadLogManagerDecorator(IgniteWriteAheadLogManager delegate) {
        this.delegate = delegate;
    }

    @Override
    public void start(GridCacheSharedContext cctx) throws IgniteCheckedException {
        delegate.start(cctx);
    }

    @Override
    public void stop(boolean cancel) {
        delegate.stop(cancel);
    }

    @Override
    public void onKernalStop(boolean cancel) {
        delegate.onKernalStop(cancel);
    }

    @Override
    public void printMemoryStats() {
        delegate.printMemoryStats();
    }

    @Override
    public void onDisconnected(IgniteFuture reconnectFut) {
        delegate.onDisconnected(reconnectFut);
    }

    @Override
    public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        delegate.onActivate(kctx);
    }

    @Override
    public void onDeActivate(GridKernalContext kctx) {
        delegate.onDeActivate(kctx);
    }

    @Override
    public boolean isAlwaysWriteFullPages() {
        return delegate.isAlwaysWriteFullPages();
    }

    @Override
    public boolean isFullSync() {
        return delegate.isFullSync();
    }

    @Override
    public void resumeLogging(WALPointer lastWrittenPtr) throws IgniteCheckedException {
        delegate.resumeLogging(lastWrittenPtr);
    }

    @Override
    public WALPointer log(WALRecord entry) throws IgniteCheckedException, StorageException {
        return delegate.log(entry);
    }

    @Override
    public void fsync(WALPointer ptr) throws IgniteCheckedException, StorageException {
        delegate.fsync(ptr);
    }

    @Override
    public WALIterator replay(WALPointer start) throws IgniteCheckedException, StorageException {
        return delegate.replay(start);
    }

    @Override
    public boolean reserve(WALPointer start) throws IgniteCheckedException {
        return delegate.reserve(start);
    }

    @Override
    public void release(WALPointer start) throws IgniteCheckedException {
        delegate.release(start);
    }

    @Override
    public int truncate(WALPointer ptr) {
        return delegate.truncate(ptr);
    }

    @Override
    public int walArchiveSegments() {
        return delegate.walArchiveSegments();
    }

    @Override
    public boolean reserved(WALPointer ptr) {
        return delegate.reserved(ptr);
    }
}

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper around GridDhtLocalPartition to be extended in test cases
 */
public abstract class MockGridDhtLocalPartition extends GridDhtLocalPartition{
    /**
     * fake id generator in order to bypass construction stage failure (if invoked with real id)
     */
    private static AtomicInteger cntr = new AtomicInteger(Integer.MAX_VALUE);

    /**
     * Real object
     */
    private GridDhtLocalPartition internal;

    /**
     * @param ctx Context.
     * @param grp Cache group.
     * @param id Partition ID.
     */
    private MockGridDhtLocalPartition(GridCacheSharedContext ctx,
        CacheGroupContext grp, int id) {
        super(ctx, grp, id);
    }

    /** */
    protected MockGridDhtLocalPartition(GridCacheSharedContext ctx,
        CacheGroupContext grp, GridDhtLocalPartition internal){
        this(ctx, grp, cntr.getAndDecrement());
        this.internal = internal;
    }

    /** */
    protected GridDhtLocalPartition getInternal(){
        return internal;
    }

    /** {@inheritDoc} */
    @Override public int internalSize() {
        return internal.internalSize();
    }

    /** {@inheritDoc} */
    @Override protected CacheMapHolder entriesMap(GridCacheContext cctx) {
        return internal.entriesMap(cctx);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected CacheMapHolder entriesMapIfExists(Integer cacheId) {
        return internal.entriesMapIfExists(cacheId);
    }

    /** {@inheritDoc} */
    @Override public IgniteCacheOffheapManager.CacheDataStore dataStore() {
        return internal.dataStore();
    }

    /** {@inheritDoc} */
    @Override public boolean addReservation(GridDhtPartitionsReservation r) {
        return internal.addReservation(r);
    }

    /** {@inheritDoc} */
    @Override public void removeReservation(GridDhtPartitionsReservation r) {
        internal.removeReservation(r);
    }

    /** {@inheritDoc} */
    @Override public int id() {
        return internal.id();
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionState state() {
        return internal.state();
    }

    /** {@inheritDoc} */
    @Override public int reservations() {
        return internal.reservations();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return internal.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean valid() {
        return internal.valid();
    }

    /** {@inheritDoc} */
    @Override public void cleanupRemoveQueue() {
        internal.cleanupRemoveQueue();
    }

    /** {@inheritDoc} */
    @Override public void onDeferredDelete(int cacheId, KeyCacheObject key, GridCacheVersion ver) {
        internal.onDeferredDelete(cacheId,key,ver);
    }

    /** {@inheritDoc} */
    @Override public void lock() {
        internal.lock();
    }

    /** {@inheritDoc} */
    @Override public void unlock() {
        internal.unlock();
    }

    /** {@inheritDoc} */
    @Override public boolean reserve() {
        return internal.reserve();
    }

    /** {@inheritDoc} */
    @Override public void release() {
        internal.release();
    }

    /** {@inheritDoc} */
    @Override protected void release(int sizeChange, CacheMapHolder hld, GridCacheEntryEx e) {
        internal.release();
    }

    /** {@inheritDoc} */
    @Override public void restoreState(GridDhtPartitionState stateToRestore) {
        internal.restoreState(stateToRestore);
    }

    /** {@inheritDoc} */
    @Override public void moving() {

        internal.moving();
    }


    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> rent(boolean updateSeq) {
        return  internal.rent(updateSeq);
    }

    /** {@inheritDoc} */
    @Override public void clearAsync() {
        internal.clearAsync();
    }

    /** {@inheritDoc} */
    @Override public boolean markForDestroy() {
        return internal.markForDestroy();
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        internal.destroy();
    }


    /** {@inheritDoc} */
    @Override public void awaitDestroy() {
        internal.awaitDestroy();
    }


    /** {@inheritDoc} */
    @Override public void onClearFinished(IgniteInClosure<? super IgniteInternalFuture<?>> lsnr) {
        internal.onClearFinished(lsnr);
    }


    /** {@inheritDoc} */
    @Override public boolean isClearing() {
        return internal.isClearing();
    }


    /** {@inheritDoc} */
    @Override public boolean tryClear(EvictionContext evictionCtx) throws NodeStoppingException {
        return internal.tryClear(evictionCtx);
    }

    /** {@inheritDoc} */
    @Override public boolean primary(AffinityTopologyVersion topVer) {
        return internal.primary(topVer);
    }


    /** {@inheritDoc} */
    @Override public boolean backup(AffinityTopologyVersion topVer) {
        return internal.backup(topVer);
    }

    /** {@inheritDoc} */
    @Override public long initialUpdateCounter() {
        return internal.initialUpdateCounter();
    }


    /** {@inheritDoc} */
    @Override public void updateCounter(long val) {

        internal.updateCounter(val);
    }


    /** {@inheritDoc} */
    @Override public void initialUpdateCounter(long val) {

        internal.initialUpdateCounter(val);
    }


    /** {@inheritDoc} */
    @Override public long fullSize() {

        return internal.fullSize();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {

        return internal.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {

        return internal.equals(obj);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull GridDhtLocalPartition part) {
        return internal.compareTo(part);
    }

    /** {@inheritDoc} */
    @Override public String toString() {

        return internal.toString();
    }

    /** {@inheritDoc} */
    @Override public int publicSize(int cacheId) {

        return internal.publicSize(cacheId);
    }

    /** {@inheritDoc} */
    @Override public void incrementPublicSize(@Nullable CacheMapHolder hld, GridCacheEntryEx e) {
        internal.incrementPublicSize(hld,e);
    }

    /** {@inheritDoc} */
    @Override public void decrementPublicSize(@Nullable CacheMapHolder hld, GridCacheEntryEx e) {
        internal.decrementPublicSize(hld,e);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheMapEntry getEntry(GridCacheContext ctx, KeyCacheObject key) {
        return internal.getEntry(ctx,key);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheMapEntry putEntryIfObsoleteOrAbsent(
        GridCacheContext ctx,
        final AffinityTopologyVersion topVer,
        KeyCacheObject key,
        final boolean create,
        final boolean touch) {
        return internal.putEntryIfObsoleteOrAbsent(ctx, topVer, key, create, touch);
    }

    /** {@inheritDoc} */
    @Override public boolean removeEntry(final GridCacheEntryEx entry) {

        return  internal.removeEntry(entry);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheMapEntry> entries(int cacheId, final CacheEntryPredicate... filter) {
        return internal.entries(cacheId, filter);
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheMapEntry> entrySet(int cacheId, final CacheEntryPredicate... filter) {
        return internal.entrySet(cacheId, filter);
    }
}

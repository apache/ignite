/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.transactions.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.events.IgniteEventType.*;

/**
 * Near cache entry.
 */
@SuppressWarnings({"NonPrivateFieldAccessedInSynchronizedContext", "TooBroadScope"})
public class GridNearCacheEntry<K, V> extends GridDistributedCacheEntry<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int NEAR_SIZE_OVERHEAD = 36;

    /** ID of primary node from which this entry was last read. */
    private volatile UUID primaryNodeId;

    /** DHT version which caused the last update. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private GridCacheVersion dhtVer;

    /** Partition. */
    private int part;

    /**
     * @param ctx Cache context.
     * @param key Cache key.
     * @param hash Key hash value.
     * @param val Entry value.
     * @param next Next entry in the linked list.
     * @param ttl Time to live.
     * @param hdrId Header id.
     */
    public GridNearCacheEntry(GridCacheContext<K, V> ctx, K key, int hash, V val, GridCacheMapEntry<K, V> next,
        long ttl, int hdrId) {
        super(ctx, key, hash, val, next, ttl, hdrId);

        part = ctx.affinity().partition(key);
    }

    /** {@inheritDoc} */
    @Override public int memorySize() throws IgniteCheckedException {
        return super.memorySize() + NEAR_SIZE_OVERHEAD;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return part;
    }

    /** {@inheritDoc} */
    @Override public boolean isNear() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean valid(long topVer) {
        assert topVer > 0 : "Topology version is invalid: " + topVer;

        UUID primaryNodeId = this.primaryNodeId;

        if (primaryNodeId == null)
            return false;

        if (cctx.discovery().node(primaryNodeId) == null) {
            this.primaryNodeId = null;

            return false;
        }

        // Make sure that primary node is alive before returning this value.
        ClusterNode primary = cctx.affinity().primary(key(), topVer);

        if (primary != null && primary.id().equals(primaryNodeId))
            return true;

        // Primary node changed.
        this.primaryNodeId = null;

        return false;
    }

    /**
     * @param topVer Topology version.
     * @return {@code True} if this entry was initialized by this call.
     * @throws GridCacheEntryRemovedException If this entry is obsolete.
     */
    public boolean initializeFromDht(long topVer) throws GridCacheEntryRemovedException {
        while (true) {
            GridDhtCacheEntry<K, V> entry = cctx.near().dht().peekExx(key);

            if (entry != null) {
                GridCacheEntryInfo<K, V> e = entry.info();

                if (e != null) {
                    GridCacheVersion enqueueVer = null;

                    try {
                        synchronized (this) {
                            checkObsolete();

                            if (isNew() || !valid(topVer)) {
                                // Version does not change for load ops.
                                update(e.value(), e.valueBytes(), e.expireTime(), e.ttl(), e.isNew() ? ver : e.version());

                                if (cctx.deferredDelete()) {
                                    boolean deleted = val == null && valBytes == null;

                                    if (deleted != deletedUnlocked()) {
                                        deletedUnlocked(deleted);

                                        if (deleted)
                                            enqueueVer = e.version();
                                    }
                                }

                                recordNodeId(cctx.affinity().primary(key, topVer).id());

                                dhtVer = e.isNew() || e.isDeleted() ? null : e.version();

                                return true;
                            }

                            return false;
                        }
                    }
                    finally {
                        if (enqueueVer != null)
                            cctx.onDeferredDelete(this, enqueueVer);
                    }
                }
            }
            else
                return false;
        }
    }

    /**
     * This method should be called only when lock is owned on this entry.
     *
     * @param val Value.
     * @param valBytes Value bytes.
     * @param ver Version.
     * @param dhtVer DHT version.
     * @param primaryNodeId Primary node ID.
     * @return {@code True} if reset was done.
     * @throws GridCacheEntryRemovedException If obsolete.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings( {"RedundantTypeArguments"})
    public boolean resetFromPrimary(V val, byte[] valBytes, GridCacheVersion ver, GridCacheVersion dhtVer,
        UUID primaryNodeId) throws GridCacheEntryRemovedException, IgniteCheckedException {
        assert dhtVer != null;

        cctx.versions().onReceived(primaryNodeId, dhtVer);

        if (valBytes != null && val == null && !cctx.config().isStoreValueBytes()) {
            GridCacheVersion curDhtVer = dhtVersion();

            if (!F.eq(dhtVer, curDhtVer))
                val = cctx.marshaller().<V>unmarshal(valBytes, cctx.deploy().globalLoader());
        }

        synchronized (this) {
            checkObsolete();

            this.primaryNodeId = primaryNodeId;

            if (!F.eq(this.dhtVer, dhtVer)) {
                value(val, valBytes);

                this.ver = ver;
                this.dhtVer = dhtVer;

                return true;
            }
        }

        return false;
    }

    /**
     * This method should be called only when lock is owned on this entry.
     *
     * @param dhtVer DHT version.
     * @param val Value associated with version.
     * @param valBytes Value bytes.
     * @param expireTime Expire time.
     * @param ttl Time to live.
     * @param primaryNodeId Primary node ID.
     */
    public void updateOrEvict(GridCacheVersion dhtVer, @Nullable V val, @Nullable byte[] valBytes, long expireTime,
        long ttl, UUID primaryNodeId) {
        assert dhtVer != null;

        cctx.versions().onReceived(primaryNodeId, dhtVer);

        synchronized (this) {
            if (!obsolete()) {
                // Don't set DHT version to null until we get a match from DHT remote transaction.
                if (F.eq(this.dhtVer, dhtVer))
                    this.dhtVer = null;

                // If we are here, then we already tried to evict this entry.
                // If cannot evict, then update.
                if (this.dhtVer == null) {
                    if (!markObsolete(dhtVer)) {
                        value(val, valBytes);

                        ttlAndExpireTimeExtras((int) ttl, expireTime);

                        this.primaryNodeId = primaryNodeId;
                    }
                }
            }
        }
    }

    /**
     * @return DHT version for this entry.
     * @throws GridCacheEntryRemovedException If obsolete.
     */
    @Nullable public synchronized GridCacheVersion dhtVersion() throws GridCacheEntryRemovedException {
        checkObsolete();

        return dhtVer;
    }

    /**
     * @return Tuple with version and value of this entry.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     */
    @Nullable public synchronized GridTuple3<GridCacheVersion, V, byte[]> versionedValue()
        throws GridCacheEntryRemovedException {
        checkObsolete();

        if (dhtVer == null)
            return null;
        else {
            V val0 = null;
            byte[] valBytes0 = null;

            GridCacheValueBytes valBytesTuple = valueBytes();

            if (!valBytesTuple.isNull()) {
                if (valBytesTuple.isPlain())
                    val0 = (V)valBytesTuple.get();
                else
                    valBytes0 = valBytesTuple.get();
            }
            else
                val0 = val;

            return F.t(dhtVer, val0, valBytes0);
        }
    }

    /**
     * @return ID of primary node from which this value was loaded.
     */
    UUID nodeId() {
        return primaryNodeId;
    }

    /** {@inheritDoc} */
    @Override protected void recordNodeId(UUID primaryNodeId) {
        assert Thread.holdsLock(this);

        this.primaryNodeId = primaryNodeId;
    }

    /**
     * This method should be called only when committing optimistic transactions.
     *
     * @param dhtVer DHT version to record.
     */
    public synchronized void recordDhtVersion(GridCacheVersion dhtVer) {
        // Version manager must be updated separately, when adding DHT version
        // to transaction entries.
        this.dhtVer = dhtVer;
    }

    /** {@inheritDoc} */
    @Override protected void refreshAhead(K key, GridCacheVersion matchVer) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected V readThrough(IgniteTxEx<K, V> tx, K key, boolean reload,
        IgnitePredicate<GridCacheEntry<K, V>>[] filter, UUID subjId, String taskName) throws IgniteCheckedException {
        return cctx.near().loadAsync(tx, F.asList(key), reload, /*force primary*/false, filter, subjId, taskName, true).
            get().get(key);
    }

    /**
     * @param tx Transaction.
     * @param primaryNodeId Primary node ID.
     * @param val New value.
     * @param valBytes Value bytes.
     * @param ver Version to use.
     * @param dhtVer DHT version received from remote node.
     * @param expVer Optional version to match.
     * @param ttl Time to live.
     * @param expireTime Expiration time.
     * @param evt Event flag.
     * @param topVer Topology version.
     * @return {@code True} if initial value was set.
     * @throws IgniteCheckedException In case of error.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    public boolean loadedValue(@Nullable IgniteTxEx tx, UUID primaryNodeId, V val, byte[] valBytes,
        GridCacheVersion ver, GridCacheVersion dhtVer, @Nullable GridCacheVersion expVer, long ttl, long expireTime,
        boolean evt, long topVer, UUID subjId)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        boolean valid = valid(tx != null ? tx.topologyVersion() : cctx.affinity().affinityTopologyVersion());

        if (valBytes != null && val == null && (isNewLocked() || !valid))
            val = cctx.marshaller().<V>unmarshal(valBytes, cctx.deploy().globalLoader());

        GridCacheVersion enqueueVer = null;

        try {
            synchronized (this) {
                checkObsolete();

                cctx.cache().metrics0().onRead(false);

                boolean ret = false;

                V old = this.val;
                boolean hasVal = hasValueUnlocked();

                if (isNew() || !valid || expVer == null || expVer.equals(this.dhtVer)) {
                    this.primaryNodeId = primaryNodeId;

                    refreshingLocked(false);

                    // Change entry only if dht version has changed.
                    if (!dhtVer.equals(dhtVersion())) {
                        update(val, valBytes, expireTime, ttl, ver);

                        if (cctx.deferredDelete()) {
                            boolean deleted = val == null && valBytes == null;

                            if (deleted != deletedUnlocked()) {
                                deletedUnlocked(deleted);

                                if (deleted)
                                    enqueueVer = ver;
                            }
                        }

                        recordDhtVersion(dhtVer);

                        ret = true;
                    }
                }

                if (evt && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ))
                    cctx.events().addEvent(partition(), key, tx, null, EVT_CACHE_OBJECT_READ,
                        val, val != null || valBytes != null, old, hasVal, subjId, null, null);

                return ret;
            }
        }
        finally {
            if (enqueueVer != null)
                cctx.onDeferredDelete(this, enqueueVer);
        }
    }

    /** {@inheritDoc} */
    @Override protected void updateIndex(V val, byte[] valBytes, long expireTime,
        GridCacheVersion ver, V old) throws IgniteCheckedException {
        // No-op: queries are disabled for near cache.
    }

    /** {@inheritDoc} */
    @Override protected void clearIndex(V val) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvccCandidate<K> addLocal(
        long threadId,
        GridCacheVersion ver,
        long timeout,
        boolean reenter,
        boolean tx,
        boolean implicitSingle) throws GridCacheEntryRemovedException {
        return addNearLocal(
            null,
            threadId,
            ver,
            timeout,
            reenter,
            tx,
            implicitSingle
        );
    }

    /**
     * Add near local candidate.
     *
     * @param dhtNodeId DHT node ID.
     * @param threadId Owning thread ID.
     * @param ver Lock version.
     * @param timeout Timeout to acquire lock.
     * @param reenter Reentry flag.
     * @param tx Transaction flag.
     * @param implicitSingle Implicit flag.
     * @return New candidate.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     */
    @Nullable public GridCacheMvccCandidate<K> addNearLocal(
        @Nullable UUID dhtNodeId,
        long threadId,
        GridCacheVersion ver,
        long timeout,
        boolean reenter,
        boolean tx,
        boolean implicitSingle)
        throws GridCacheEntryRemovedException {
        GridCacheMvccCandidate<K> prev;
        GridCacheMvccCandidate<K> owner;
        GridCacheMvccCandidate<K> cand;

        V val;

        UUID locId = cctx.nodeId();

        synchronized (this) {
            checkObsolete();

            GridCacheMvcc<K> mvcc = mvccExtras();

            if (mvcc == null) {
                mvcc = new GridCacheMvcc<>(cctx);

                mvccExtras(mvcc);
            }

            GridCacheMvccCandidate<K> c = mvcc.localCandidate(locId, threadId);

            if (c != null)
                return reenter ? c.reenter() : null;

            prev = mvcc.anyOwner();

            boolean emptyBefore = mvcc.isEmpty();

            // Lock could not be acquired.
            if (timeout < 0 && !emptyBefore)
                return null;

            // Local lock for near cache is a local lock.
            cand = mvcc.addNearLocal(this, locId, dhtNodeId, threadId, ver, timeout, tx, implicitSingle);

            owner = mvcc.anyOwner();

            boolean emptyAfter = mvcc.isEmpty();

            checkCallbacks(emptyBefore, emptyAfter);

            val = this.val;

            if (emptyAfter)
                mvccExtras(null);
        }

        // This call must be outside of synchronization.
        checkOwnerChanged(prev, owner, val);

        return cand;
    }

    /**
     * @param ver Version to set DHT node ID for.
     * @param dhtNodeId DHT node ID.
     * @return {@code true} if candidate was found.
     * @throws GridCacheEntryRemovedException If entry is removed.
     */
    @Nullable public synchronized GridCacheMvccCandidate<K> dhtNodeId(GridCacheVersion ver, UUID dhtNodeId)
        throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc<K> mvcc = mvccExtras();

        GridCacheMvccCandidate<K> cand = mvcc == null ? null : mvcc.candidate(ver);

        if (cand == null)
            return null;

        cand.otherNodeId(dhtNodeId);

        return cand;
    }

    /**
     * Unlocks local lock.
     *
     * @return Removed candidate, or <tt>null</tt> if thread still holds the lock.
     */
    @Nullable @Override public GridCacheMvccCandidate<K> removeLock() {
        GridCacheMvccCandidate<K> prev = null;
        GridCacheMvccCandidate<K> owner = null;

        V val;

        UUID locId = cctx.nodeId();

        GridCacheMvccCandidate<K> cand = null;

        synchronized (this) {
            GridCacheMvcc<K> mvcc = mvccExtras();

            if (mvcc != null) {
                prev = mvcc.anyOwner();

                boolean emptyBefore = mvcc.isEmpty();

                cand = mvcc.localCandidate(locId, Thread.currentThread().getId());

                assert cand == null || cand.nearLocal();

                if (cand != null && cand.owner()) {
                    // If a reentry, then release reentry. Otherwise, remove lock.
                    GridCacheMvccCandidate<K> reentry = cand.unenter();

                    if (reentry != null) {
                        assert reentry.reentry();

                        return reentry;
                    }

                    mvcc.remove(cand.version());

                    owner = mvcc.anyOwner();
                }
                else
                    return null;

                boolean emptyAfter = mvcc.isEmpty();

                checkCallbacks(emptyBefore, emptyAfter);

                if (emptyAfter)
                    mvccExtras(null);
            }

            val = this.val;
        }

        assert cand != null;
        assert owner != prev;

        if (log.isDebugEnabled())
            log.debug("Released local candidate from entry [owner=" + owner + ", prev=" + prev +
                ", entry=" + this + ']');

        cctx.mvcc().removeExplicitLock(cand);

        if (prev != null && owner != prev)
            checkThreadChain(prev);

        // This call must be outside of synchronization.
        checkOwnerChanged(prev, owner, val);

        return owner != prev ? prev : null;
    }

    /** {@inheritDoc} */
    @Override protected void onInvalidate() {
        dhtVer = null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntry<K, V> wrap(boolean prjAware) {
        GridCacheProjectionImpl<K, V> prjPerCall = null;

        if (prjAware)
            prjPerCall = cctx.projectionPerCall();

        return new GridPartitionedCacheEntryImpl<>(prjPerCall, cctx, key, this);
    }

    /** {@inheritDoc} */
    @Override public synchronized String toString() {
        return S.toString(GridNearCacheEntry.class, this, "super", super.toString());
    }
}

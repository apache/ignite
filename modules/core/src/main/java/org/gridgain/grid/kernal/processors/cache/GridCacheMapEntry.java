/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.dr.*;
import org.gridgain.grid.dr.cache.sender.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.extras.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.kernal.processors.dr.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheFlag.*;
import static org.gridgain.grid.cache.GridCacheTxState.*;
import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheOperation.*;
import static org.gridgain.grid.kernal.processors.dr.GridDrType.*;

/**
 * Adapter for cache entry.
 */
@SuppressWarnings({
    "NonPrivateFieldAccessedInSynchronizedContext", "TooBroadScope", "FieldAccessedSynchronizedAndUnsynchronized"})
public abstract class GridCacheMapEntry<K, V> implements GridCacheEntryEx<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final byte IS_REFRESHING_MASK = 0x01;

    /** */
    private static final byte IS_DELETED_MASK = 0x02;

    /** */
    private static final byte IS_UNSWAPPED_MASK = 0x04;

    /** */
    private static final Comparator<GridCacheVersion> ATOMIC_VER_COMPARATOR = new GridCacheAtomicVersionComparator();

    /**
     * NOTE
     * ====
     * Make sure to recalculate this value any time when adding or removing fields from entry.
     * The size should be count as follows:
     * <ul>
     * <li>Primitives: byte/boolean = 1, short = 2, int/float = 4, long/double = 8</li>
     * <li>References: 8 each</li>
     * <li>Each nested object should be analyzed in the same way as above.</li>
     * </ul>
     */
    private static final int SIZE_OVERHEAD = 87 /*entry*/ + 32 /* version */;

    /** Static logger to avoid re-creation. Made static for test purpose. */
    protected static final AtomicReference<GridLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static volatile GridLogger log;

    /** Cache registry. */
    @GridToStringExclude
    protected final GridCacheContext<K, V> cctx;

    /** Key. */
    @GridToStringInclude
    protected final K key;

    /** Value. */
    @GridToStringInclude
    protected V val;

    /** Start version. */
    @GridToStringInclude
    protected final long startVer;

    /** Version. */
    @GridToStringInclude
    protected GridCacheVersion ver;

    /** Next entry in the linked list. */
    @GridToStringExclude
    private volatile GridCacheMapEntry<K, V> next0;

    /** Next entry in the linked list. */
    @GridToStringExclude
    private volatile GridCacheMapEntry<K, V> next1;

    /** Key hash code. */
    @GridToStringInclude
    private final int hash;

    /** Key bytes. */
    @GridToStringExclude
    private volatile byte[] keyBytes;

    /** Value bytes. */
    @GridToStringExclude
    protected byte[] valBytes;

    /** Off-heap value pointer. */
    private long valPtr;

    /** Extras */
    @GridToStringInclude
    private GridCacheEntryExtras<K> extras;

    /**
     * Flags:
     * <ul>
     *     <li>Refreshing flag - mask {@link #IS_REFRESHING_MASK}</li>
     *     <li>Deleted flag - mask {@link #IS_DELETED_MASK}</li>
     * </ul>
     */
    @GridToStringInclude
    protected byte flags;

    /**
     * @param cctx Cache context.
     * @param key Cache key.
     * @param hash Key hash value.
     * @param val Entry value.
     * @param next Next entry in the linked list.
     * @param ttl Time to live.
     * @param hdrId Header id.
     */
    protected GridCacheMapEntry(GridCacheContext<K, V> cctx, K key, int hash, V val,
        GridCacheMapEntry<K, V> next, long ttl, int hdrId) {
        log = U.logger(cctx.kernalContext(), logRef, this);

        if (cctx.portableEnabled())
            key = (K)cctx.kernalContext().portable().detachPortable(key);

        this.key = key;
        this.hash = hash;
        this.cctx = cctx;

        ttlAndExpireTimeExtras(ttl, toExpireTime(ttl));

        if (cctx.portableEnabled())
            val = (V)cctx.kernalContext().portable().detachPortable(val);

        synchronized (this) {
            value(val, null);
        }

        next(hdrId, next);

        ver = cctx.versions().next();

        startVer = ver.order();
    }

    /** {@inheritDoc} */
    @Override public long startVersion() {
        return startVer;
    }

    /**
     * Sets entry value. If off-heap value storage is enabled, will serialize value to off-heap.
     *
     * @param val Value to store.
     * @param valBytes Value bytes to store.
     */
    protected void value(@Nullable V val, @Nullable byte[] valBytes) {
        assert Thread.holdsLock(this);

        // In case we deal with GGFS cache, count updated data
        if (cctx.cache().isGgfsDataCache() && cctx.kernalContext().ggfsHelper().isGgfsBlockKey(key())) {
            int newSize = valueLength((byte[])val, valBytes != null ? GridCacheValueBytes.marshaled(valBytes) :
                GridCacheValueBytes.nil());
            int oldSize = valueLength((byte[])this.val, this.val == null ? valueBytesUnlocked() :
                GridCacheValueBytes.nil());

            int delta = newSize - oldSize;

            if (delta != 0 && !cctx.isNear())
                cctx.cache().onGgfsDataSizeChanged(delta);
        }

        if (!isOffHeapValuesOnly()) {
            this.val = val;
            this.valBytes = isStoreValueBytes() ? valBytes : null;
        }
        else {
            try {
                if (cctx.kernalContext().config().isPeerClassLoadingEnabled()) {
                    if (val != null || valBytes != null) {
                        if (val == null)
                            val = cctx.marshaller().unmarshal(valBytes, cctx.deploy().globalLoader());

                        if (val != null)
                            cctx.gridDeploy().deploy(val.getClass(), val.getClass().getClassLoader());
                    }

                    if (U.p2pLoader(val)) {
                        cctx.deploy().addDeploymentContext(
                            new GridDeploymentInfoBean((GridDeploymentInfo)val.getClass().getClassLoader()));
                    }
                }

                GridUnsafeMemory mem = cctx.unsafeMemory();

                assert mem != null;

                if (val != null || valBytes != null) {
                    boolean valIsByteArr = val != null && val instanceof byte[];

                    if (valBytes == null && !valIsByteArr)
                        valBytes = CU.marshal(cctx, val);

                    valPtr = mem.putOffHeap(valPtr, valIsByteArr ? (byte[]) val : valBytes, valIsByteArr);
                }
                else {
                    mem.removeOffHeap(valPtr);

                    valPtr = 0;
                }
            }
            catch (GridException e) {
                U.error(log, "Failed to deserialize value [entry=" + this + ", val=" + val + ']');

                throw new GridRuntimeException(e);
            }
        }
    }

    /**
     * Isolated method to get length of GGFS block.
     *
     * @param val Value.
     * @param valBytes Value bytes.
     * @return Length of value.
     */
    private int valueLength(@Nullable byte[] val, GridCacheValueBytes valBytes) {
        assert valBytes != null;

        return val != null ? val.length : valBytes.isNull() ? 0 : valBytes.get().length - (valBytes.isPlain() ? 0 : 6);
    }

    /**
     * @return Value bytes.
     */
    protected GridCacheValueBytes valueBytesUnlocked() {
        assert Thread.holdsLock(this);

        if (!isOffHeapValuesOnly()) {
            if (valBytes != null)
                return GridCacheValueBytes.marshaled(valBytes);
        }
        else {
            if (valPtr != 0) {
                GridUnsafeMemory mem = cctx.unsafeMemory();

                assert mem != null;

                return mem.getOffHeap(valPtr);
            }
        }

        return GridCacheValueBytes.nil();
    }

    /** {@inheritDoc} */
    @Override public int memorySize() throws GridException {
        byte[] kb;
        GridCacheValueBytes vb;

        V v;

        int extrasSize;

        synchronized (this) {
            kb = keyBytes;
            vb = valueBytesUnlocked();

            v = val;

            extrasSize = extrasSize();
        }

        if (kb == null || (vb.isNull() && v != null)) {
            if (kb == null)
                kb = CU.marshal(cctx, key);

            if (vb.isNull())
                vb = (v != null && v instanceof byte[]) ? GridCacheValueBytes.plain(v) :
                    GridCacheValueBytes.marshaled(CU.marshal(cctx, v));

            synchronized (this) {
                if (keyBytes == null)
                    keyBytes = kb;

                // If value didn't change.
                if (!isOffHeapValuesOnly() && valBytes == null && val == v)
                    valBytes = vb.isPlain() ? null : vb.get();
            }
        }

        return SIZE_OVERHEAD + extrasSize + kb.length + (vb.isNull() ? 0 : vb.get().length);
    }

    /** {@inheritDoc} */
    @Override public boolean isInternal() {
        return key instanceof GridCacheInternal;
    }

    /** {@inheritDoc} */
    @Override public boolean isDht() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isNear() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isReplicated() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean detached() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public GridCacheContext<K, V> context() {
        return cctx;
    }

    /** {@inheritDoc} */
    @Override public boolean isNew() throws GridCacheEntryRemovedException {
        assert Thread.holdsLock(this);

        checkObsolete();

        return isStartVersion();
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean isNewLocked() throws GridCacheEntryRemovedException {
        checkObsolete();

        return isStartVersion();
    }

    /**
     * @return {@code True} if start version.
     */
    protected boolean isStartVersion() {
        return ver.nodeOrder() == cctx.localNode().order() && ver.order() == startVer;
    }

    /** {@inheritDoc} */
    @Override public boolean valid(long topVer) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean partitionValid() {
        return true;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheEntryInfo<K, V> info() {
        GridCacheEntryInfo<K, V> info = null;

        long time = U.currentTimeMillis();

        try {
            synchronized (this) {
                if (!obsolete()) {
                    info = new GridCacheEntryInfo<>();

                    info.key(key);

                    long expireTime = expireTimeExtras();

                    boolean expired = expireTime != 0 && expireTime <= time;

                    info.keyBytes(keyBytes);
                    info.ttl(ttlExtras());
                    info.expireTime(expireTime);
                    info.version(ver);
                    info.setNew(isStartVersion());
                    info.setDeleted(deletedUnlocked());

                    if (!expired) {
                        info.value(cctx.kernalContext().config().isPeerClassLoadingEnabled() ?
                            rawGetOrUnmarshalUnlocked() : val);

                        GridCacheValueBytes valBytes = valueBytesUnlocked();

                        if (!valBytes.isNull()) {
                            if (valBytes.isPlain())
                                info.value((V)valBytes.get());
                            else
                                info.valueBytes(valBytes.get());
                        }
                    }
                }
            }
        }
        catch (GridException e) {
            throw new GridRuntimeException("Failed to unmarshal object while creating entry info: " + this, e);
        }

        return info;
    }

    /**
     * Unswaps an entry.
     *
     * @throws GridException If failed.
     */
    @Override public V unswap() throws GridException {
        return unswap(false);
    }

    /**
     * Unswaps an entry.
     *
     * @param ignoreFlags Whether to ignore swap flags.
     * @throws GridException If failed.
     */
    @Override public V unswap(boolean ignoreFlags) throws GridException {
        boolean swapEnabled = cctx.swap().swapEnabled() && (ignoreFlags || !cctx.hasFlag(SKIP_SWAP));

        if (!swapEnabled && !cctx.isOffHeapEnabled())
            return null;

        synchronized (this) {
            if (isStartVersion() && ((flags & IS_UNSWAPPED_MASK) == 0)) {
                GridCacheSwapEntry<V> e = detached() ? cctx.swap().read(this) : cctx.swap().readAndRemove(this);

                if (log.isDebugEnabled())
                    log.debug("Read swap entry [swapEntry=" + e + ", cacheEntry=" + this + ']');

                flags |= IS_UNSWAPPED_MASK;

                // If there is a value.
                if (e != null) {
                    long delta = e.expireTime() == 0 ? 0 : e.expireTime() - U.currentTimeMillis();

                    if (delta >= 0) {
                        V val = e.value();

                        if (cctx.portableEnabled())
                            val = (V)cctx.kernalContext().portable().detachPortable(val);

                        // Set unswapped value.
                        update(val, e.valueBytes(), e.expireTime(), e.ttl(), e.version());

                        return val;
                    }
                    else
                        clearIndex(e.value());
                }
            }
        }

        return null;
    }

    /**
     * @throws GridException If failed.
     */
    private void swap() throws GridException {
        if (cctx.isSwapOrOffheapEnabled() && !deletedUnlocked()) {
            assert Thread.holdsLock(this);

            long expireTime = expireTimeExtras();

            if (expireTime > 0 && U.currentTimeMillis() >= expireTime)
                // Don't swap entry if it's expired.
                return;

            V val = rawGetOrUnmarshalUnlocked();
            GridCacheValueBytes valBytes = valueBytesUnlocked();

            if (valBytes.isNull())
                valBytes = createValueBytes(val);

            GridUuid valClsLdrId = null;

            if (val != null)
                valClsLdrId = cctx.deploy().getClassLoaderId(val.getClass().getClassLoader());

            cctx.swap().write(key(), getOrMarshalKeyBytes(), hash, valBytes.get(), valBytes.isPlain(), ver,
                ttlExtras(), expireTime, cctx.deploy().getClassLoaderId(U.detectObjectClassLoader(key)), valClsLdrId);

            if (log.isDebugEnabled())
                log.debug("Wrote swap entry: " + this);
        }
    }

    /**
     * @throws GridException If failed.
     */
    protected final void releaseSwap() throws GridException {
        if (cctx.isSwapOrOffheapEnabled()) {
            synchronized (this){
                cctx.swap().remove(key(), getOrMarshalKeyBytes());
            }

            if (log.isDebugEnabled())
                log.debug("Removed swap entry [entry=" + this + ']');
        }
    }

    /**
     * @param key Key.
     * @param matchVer Version to match.
     */
    protected void refreshAhead(final K key, final GridCacheVersion matchVer) {
        if (log.isDebugEnabled())
            log.debug("Scheduling asynchronous refresh for entry: " + this);

        // Asynchronous execution (we don't check filter here).
        cctx.closures().runLocalSafe(new GPR() {
            @SuppressWarnings({"unchecked"})
            @Override public void run() {
                if (log.isDebugEnabled())
                    log.debug("Refreshing-ahead entry: " + GridCacheMapEntry.this);

                synchronized (GridCacheMapEntry.this){
                    // If there is a point to refresh.
                    if (!matchVer.equals(ver)) {
                        refreshingLocked(false);

                        if (log.isDebugEnabled())
                            log.debug("Will not refresh value as entry has been recently updated: " +
                                GridCacheMapEntry.this);

                        return;
                    }
                }

                V val = null;

                try {
                    val = cctx.store().loadFromStore(null, key);
                }
                catch (GridException e) {
                    U.error(log, "Failed to refresh-ahead entry: " + GridCacheMapEntry.this, e);
                }
                finally {
                    synchronized (GridCacheMapEntry.this) {
                        refreshingLocked(false);

                        // If version matched, set value. Note that we don't update
                        // swap here, as asynchronous refresh happens only if
                        // value is already in memory.
                        if (val != null && matchVer.equals(ver)) {
                            try {
                                V prev = rawGetOrUnmarshalUnlocked();

                                long ttl = ttlExtras();

                                long expTime = toExpireTime(ttl);

                                // Detach value before index update.
                                if (cctx.portableEnabled())
                                    val = (V)cctx.kernalContext().portable().detachPortable(val);

                                updateIndex(val, null, expTime, ver, prev);

                                // Don't change version for read-through.
                                update(val, null, expTime, ttl, ver);
                            }
                            catch (GridException e) {
                                U.error(log, "Failed to update cache index: " + GridCacheMapEntry.this, e);
                            }
                        }
                    }
                }
            }
        }, true);
    }

    /**
     * @param tx Transaction.
     * @param key Key.
     * @param reload flag.
     * @param filter Filter.
     * @return Read value.
     * @throws GridException If failed.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Nullable protected V readThrough(@Nullable GridCacheTxEx<K, V> tx, K key, boolean reload,
        GridPredicate<GridCacheEntry<K, V>>[] filter, UUID subjId, String taskName) throws GridException {
        return cctx.store().loadFromStore(tx, key);
    }

    /** {@inheritDoc} */
    @Nullable @Override public final V innerGet(@Nullable GridCacheTxEx<K, V> tx, boolean readSwap,
        boolean readThrough, boolean failFast, boolean unmarshal, boolean updateMetrics, boolean evt,
        UUID subjId, Object transformClo, String taskName, GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException,
        GridCacheEntryRemovedException, GridCacheFilterFailedException {
        cctx.denyOnFlag(LOCAL);

        return innerGet0(tx, readSwap, readThrough, evt, failFast, unmarshal, updateMetrics, subjId, transformClo,
            taskName, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "RedundantTypeArguments", "TooBroadScope"})
    private V innerGet0(GridCacheTxEx<K, V> tx, boolean readSwap, boolean readThrough, boolean evt, boolean failFast,
        boolean unmarshal, boolean updateMetrics, UUID subjId, Object transformClo, String taskName,
        GridPredicate<GridCacheEntry<K, V>>[] filter)
        throws GridException, GridCacheEntryRemovedException, GridCacheFilterFailedException {
        // Disable read-through if there is no store.
        if (readThrough && !cctx.isStoreEnabled())
            readThrough = false;

        GridCacheMvccCandidate<K> owner;

        V old;
        V ret = null;

        if (!F.isEmptyOrNulls(filter) && !cctx.isAll(
            (new GridCacheFilterEvaluationEntry<>(key, rawGetOrUnmarshal(), this, true)), filter))
            return CU.<V>failed(failFast);

        boolean asyncRefresh = false;

        GridCacheVersion startVer;

        boolean expired = false;

        V expiredVal = null;

        boolean hasOldBytes;

        synchronized (this) {
            checkObsolete();

            // Cache version for optimistic check.
            startVer = ver;

            GridCacheMvcc<K> mvcc = mvccExtras();

            owner = mvcc == null ? null : mvcc.anyOwner();

            double delta = Double.MAX_VALUE;

            long expireTime = expireTimeExtras();

            if (expireTime > 0) {
                delta = expireTime - U.currentTimeMillis();

                if (log.isDebugEnabled())
                    log.debug("Checked expiration time for entry [timeLeft=" + delta + ", entry=" + this + ']');

                if (delta <= 0)
                    expired = true;
            }

            V val = this.val;

            hasOldBytes = valBytes != null || valPtr != 0;

            if ((unmarshal || isOffHeapValuesOnly()) && !expired && val == null && hasOldBytes)
                val = rawGetOrUnmarshalUnlocked();

            boolean valid = valid(tx != null ? tx.topologyVersion() : cctx.affinity().affinityTopologyVersion());

            // Attempt to load from swap.
            if (val == null && !hasOldBytes && readSwap) {
                // Only promote when loading initial state.
                if (isNew() || !valid) {
                    // If this entry is already expired (expiration time was too low),
                    // we simply remove from swap and clear index.
                    if (expired) {
                        releaseSwap();

                        // Previous value is guaranteed to be null
                        clearIndex(null);
                    }
                    else {
                        // Read and remove swap entry.
                        val = unswap();

                        // Recalculate expiration after swap read.
                        if (expireTime > 0) {
                            delta = expireTime - U.currentTimeMillis();

                            if (log.isDebugEnabled())
                                log.debug("Checked expiration time for entry [timeLeft=" + delta +
                                    ", entry=" + this + ']');

                            if (delta <= 0)
                                expired = true;
                        }
                    }
                }
            }

            // Only calculate asynchronous refresh-ahead, if there is no other
            // one in progress and if not expired.
            if (delta > 0 && expireTime > 0 && !refreshingUnlocked()) {
                double refreshRatio = cctx.config().getRefreshAheadRatio();

                if (1 - delta / ttlExtras() >= refreshRatio)
                    asyncRefresh = true;
            }

            old = expired || !valid ? null : val;

            if (expired) {
                expiredVal = val;

                value(null, null);
            }

            if (old == null && !hasOldBytes) {
                asyncRefresh = false;

                if (updateMetrics)
                    cctx.cache().metrics0().onRead(false);
            }
            else {
                if (updateMetrics)
                    cctx.cache().metrics0().onRead(true);

                // Set retVal here for event notification.
                ret = old;

                // Mark this entry as refreshing, so other threads won't schedule
                // asynchronous refresh while this one is in progress.
                if (asyncRefresh || readThrough)
                    refreshingLocked(true);
            }

            if (evt && expired && cctx.events().isRecordable(EVT_CACHE_OBJECT_EXPIRED)) {
                cctx.events().addEvent(partition(), key, tx, owner, EVT_CACHE_OBJECT_EXPIRED, null, false, expiredVal,
                    expiredVal != null || hasOldBytes, subjId, null, taskName);

                // No more notifications.
                evt = false;
            }

            if (evt && !expired && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ)) {
                cctx.events().addEvent(partition(), key, tx, owner, EVT_CACHE_OBJECT_READ, ret, ret != null, old,
                    hasOldBytes || old != null, subjId,
                    transformClo != null ? transformClo.getClass().getName() : null, taskName);

                // No more notifications.
                evt = false;
            }
        }

        if (asyncRefresh && !readThrough && cctx.isStoreEnabled()) {
            assert ret != null;

            refreshAhead(key, startVer);
        }

        // Check before load.
        if (!cctx.isAll(this, filter))
            return CU.<V>failed(failFast, ret);

        if (ret != null) {
            // If return value is consistent, then done.
            if (F.isEmptyOrNulls(filter) || version().equals(startVer))
                return ret;

            // Try again (recursion).
            return innerGet0(tx, readSwap, readThrough, false, failFast, unmarshal, updateMetrics, subjId,
                transformClo, taskName, filter);
        }

        boolean loadedFromStore = false;

        if (ret == null && readThrough) {
            GridCacheTxEx tx0 = null;

            if (tx != null && tx.local()) {
                if (cctx.isReplicated() || cctx.isColocated() || tx.near())
                    tx0 = tx;
                else if (tx.dht()) {
                    GridCacheVersion ver = ((GridDhtTxLocalAdapter)tx).nearXidVersion();

                    tx0 = cctx.dht().near().context().tm().tx(ver);
                }
            }

            ret = readThrough(tx0, key, false, filter, subjId, taskName);

            loadedFromStore = true;
        }

        boolean match = false;

        synchronized (this) {
            long ttl = ttlExtras();

            // If version matched, set value.
            if (startVer.equals(ver)) {
                match = true;

                if (ret != null) {
                    // Detach value before index update.
                    if (cctx.portableEnabled())
                        ret = (V)cctx.kernalContext().portable().detachPortable(ret);

                    GridCacheVersion nextVer = nextVersion();

                    V prevVal = rawGetOrUnmarshalUnlocked();

                    long expTime = toExpireTime(ttl);

                    if (loadedFromStore)
                        // Update indexes before actual write to entry.
                        updateIndex(ret, null, expTime, nextVer, prevVal);

                    // Don't change version for read-through.
                    update(ret, null, expTime, ttl, nextVer);

                    if (cctx.deferredDelete() && deletedUnlocked() && !isInternal() && !detached())
                        deletedUnlocked(false);
                }

                if (evt && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ))
                    cctx.events().addEvent(partition(), key, tx, owner, EVT_CACHE_OBJECT_READ, ret, ret != null,
                        old, hasOldBytes, subjId, transformClo != null ? transformClo.getClass().getName() : null,
                        taskName);
            }
        }

        if (F.isEmptyOrNulls(filter) || match)
            return ret;

        // Try again (recursion).
        return innerGet0(tx, readSwap, readThrough, false, failFast, unmarshal, updateMetrics, subjId,
            transformClo, taskName, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "TooBroadScope"})
    @Nullable @Override public final V innerReload(GridPredicate<GridCacheEntry<K, V>>[] filter)
        throws GridException, GridCacheEntryRemovedException {
        cctx.denyOnFlag(GridCacheFlag.READ);

        CU.checkStore(cctx);

        GridCacheVersion startVer;

        boolean wasNew;

        synchronized (this) {
            checkObsolete();

            // Cache version for optimistic check.
            startVer = ver;

            wasNew = isNew();
        }

        String taskName = cctx.kernalContext().job().currentTaskName();

        // Check before load.
        if (cctx.isAll(this, filter)) {
            V ret = readThrough(null, key, true, filter, cctx.localNodeId(), taskName);

            boolean touch = false;

            try {
                synchronized (this) {
                    long ttl = ttlExtras();

                    // Generate new version.
                    GridCacheVersion nextVer = cctx.versions().nextForLoad(ver);

                    // If entry was loaded during read step.
                    if (wasNew && !isNew())
                        // Map size was updated on entry creation.
                        return ret;

                    // If version matched, set value.
                    if (startVer.equals(ver)) {
                        releaseSwap();

                        V old = rawGetOrUnmarshalUnlocked();

                        long expTime = toExpireTime(ttl);

                        // Detach value before index update.
                        if (cctx.portableEnabled())
                            ret = (V)cctx.kernalContext().portable().detachPortable(ret);

                        // Update indexes.
                        if (ret != null) {
                            updateIndex(ret, null, expTime, nextVer, old);

                            if (cctx.deferredDelete() && !isInternal() && !detached() && deletedUnlocked())
                                deletedUnlocked(false);
                        }
                        else {
                            clearIndex(old);

                            if (cctx.deferredDelete() && !isInternal() && !detached() && !deletedUnlocked())
                                deletedUnlocked(true);
                        }

                        update(ret, null, expTime, ttl, nextVer);

                        touch = true;

                        // If value was set - return, otherwise try again.
                        return ret;
                    }
                }

                if (F.isEmptyOrNulls(filter)) {
                    touch = true;

                    return ret;
                }
            }
            finally {
                if (touch)
                    cctx.evicts().touch(this, cctx.affinity().affinityTopologyVersion());
            }

            // Recursion.
            return innerReload(filter);
        }

        // If filter didn't pass.
        return null;
    }

    /**
     * @param nodeId Node ID.
     */
    protected void recordNodeId(UUID nodeId) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public final GridCacheUpdateTxResult<V> innerSet(
        @Nullable GridCacheTxEx<K, V> tx,
        UUID evtNodeId,
        UUID affNodeId,
        V val,
        @Nullable byte[] valBytes,
        boolean writeThrough,
        boolean retval,
        long ttl,
        boolean evt,
        boolean metrics,
        long topVer,
        GridPredicate<GridCacheEntry<K, V>>[] filter,
        GridDrType drType,
        long drExpireTime,
        @Nullable GridCacheVersion explicitVer,
        @Nullable UUID subjId,
        String taskName
    ) throws GridException, GridCacheEntryRemovedException {
        V old;

        boolean valid = valid(tx != null ? tx.topologyVersion() : topVer);

        // Lock should be held by now.
        if (!cctx.isAll(this, filter))
            return new GridCacheUpdateTxResult<>(false, null);

        final GridCacheVersion newVer;

        boolean intercept = cctx.config().getInterceptor() != null;

        synchronized (this) {
            checkObsolete();

            if (cctx.kernalContext().config().isCacheSanityCheckEnabled()) {
                if (tx != null && tx.groupLock())
                    groupLockSanityCheck(tx);
                else
                    assert tx == null || (!tx.local() && tx.onePhaseCommit()) || tx.ownsLock(this) :
                        "Transaction does not own lock for update [entry=" + this + ", tx=" + tx + ']';
            }

            // Load and remove from swap if it is new.
            boolean startVer = isStartVersion();

            if (startVer)
                unswap(true);

            newVer = explicitVer != null ? explicitVer : tx == null ?
                nextVersion() : tx.writeVersion();

            assert newVer != null : "Failed to get write version for tx: " + tx;

            if (tx != null && !tx.local() && tx.onePhaseCommit() && explicitVer == null) {
                if (!(isNew() || !valid) && ver.compareTo(newVer) > 0) {
                    if (log.isDebugEnabled())
                        log.debug("Skipping entry update for one-phase commit since current entry version is " +
                            "greater than write version [entry=" + this + ", newVer=" + newVer + ']');

                    return new GridCacheUpdateTxResult<>(false, null);
                }
            }

            old = (retval || intercept) ? rawGetOrUnmarshalUnlocked() : this.val;

            GridCacheValueBytes oldBytes = valueBytesUnlocked();

            if (intercept) {
                V interceptorVal = (V)cctx.config().getInterceptor().onBeforePut(key, old, val);

                if (interceptorVal == null)
                    return new GridCacheUpdateTxResult<>(false, old);
                else if (interceptorVal != val) {
                    val = interceptorVal;

                    valBytes = null;
                }
            }

            // Determine new ttl and expire time.
            if (ttl < 0)
                ttl = ttlExtras();

            assert ttl >= 0;

            long expireTime = drExpireTime < 0L ? toExpireTime(ttl) : drExpireTime;

            // Detach value before index update.
            if (cctx.portableEnabled())
                val = (V)cctx.kernalContext().portable().detachPortable(val);

            // Update index inside synchronization since it can be updated
            // in load methods without actually holding entry lock.
            if (val != null || valBytes != null) {
                updateIndex(val, valBytes, expireTime, newVer, old);

                if (cctx.deferredDelete() && deletedUnlocked() && !isInternal() && !detached())
                    deletedUnlocked(false);
            }

            update(val, valBytes, expireTime, ttl, newVer);

            drReplicate(drType, val, valBytes, newVer);

            recordNodeId(affNodeId);

            if (metrics)
                cctx.cache().metrics0().onWrite();

            if (evt && newVer != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_PUT))
                cctx.events().addEvent(partition(), key, evtNodeId, tx == null ? null : tx.xid(),
                    newVer, EVT_CACHE_OBJECT_PUT, val, val != null, old, old != null || hasValueUnlocked(),
                    subjId, null, taskName);

            GridCacheMode mode = cctx.config().getCacheMode();

            if (mode == GridCacheMode.LOCAL || mode == GridCacheMode.REPLICATED ||
                (tx != null && (tx.dht() || tx.colocated()) && tx.local()))
                cctx.continuousQueries().onEntryUpdate(this, key, val, valueBytesUnlocked(), old, oldBytes);

            cctx.dataStructures().onEntryUpdated(key, false);
        }

        if (log.isDebugEnabled())
            log.debug("Updated cache entry [val=" + val + ", old=" + old + ", entry=" + this + ']');

        // Persist outside of synchronization. The correctness of the
        // value will be handled by current transaction.
        if (writeThrough)
            cctx.store().putToStore(tx, key, val, newVer);

        if (intercept)
            cctx.config().getInterceptor().onAfterPut(key, val);

        return valid ? new GridCacheUpdateTxResult<>(true, old) : new GridCacheUpdateTxResult<V>(false, null);
    }

    /** {@inheritDoc} */
    @Override public final GridCacheUpdateTxResult<V> innerRemove(
        @Nullable GridCacheTxEx<K, V> tx,
        UUID evtNodeId,
        UUID affNodeId,
        boolean writeThrough,
        boolean retval,
        boolean evt,
        boolean metrics,
        long topVer,
        GridPredicate<GridCacheEntry<K, V>>[] filter,
        GridDrType drType,
        @Nullable GridCacheVersion explicitVer,
        @Nullable UUID subjId,
        String taskName
    ) throws GridException, GridCacheEntryRemovedException {
        assert cctx.transactional();

        V old;

        GridCacheVersion newVer;

        boolean valid = valid(tx != null ? tx.topologyVersion() : topVer);

        // Lock should be held by now.
        if (!cctx.isAll(this, filter))
            return new GridCacheUpdateTxResult<>(false, null);

        GridCacheVersion obsoleteVer = null;

        GridCacheVersion enqueueVer = null;

        boolean intercept = cctx.config().getInterceptor() != null;

        GridBiTuple<Boolean, V> interceptRes = null;

        try {
            synchronized (this) {
                checkObsolete();

                if (tx != null && tx.groupLock() && cctx.kernalContext().config().isCacheSanityCheckEnabled())
                    groupLockSanityCheck(tx);
                else
                    assert tx == null || (!tx.local() && tx.onePhaseCommit()) || tx.ownsLock(this) :
                        "Transaction does not own lock for remove[entry=" + this + ", tx=" + tx + ']';

                boolean startVer = isStartVersion();

                if (startVer) {
                    if (tx != null && !tx.local() && tx.onePhaseCommit())
                        // Must promote to check version for one-phase commit tx.
                        unswap(true);
                    else
                        // Release swap.
                        releaseSwap();
                }

                newVer = explicitVer != null ? explicitVer : tx == null ? nextVersion() : tx.writeVersion();

                if (tx != null && !tx.local() && tx.onePhaseCommit() && explicitVer == null) {
                    if (!startVer && ver.compareTo(newVer) > 0) {
                        if (log.isDebugEnabled())
                            log.debug("Skipping entry removal for one-phase commit since current entry version is " +
                                "greater than write version [entry=" + this + ", newVer=" + newVer + ']');

                        return new GridCacheUpdateTxResult<>(false, null);
                    }

                    if (!detached())
                        enqueueVer = newVer;
                }

                old = (retval || intercept) ? rawGetOrUnmarshalUnlocked() : val;

                if (intercept) {
                    interceptRes = cctx.config().<K, V>getInterceptor().onBeforeRemove(key, old);

                    if (cctx.cancelRemove(interceptRes))
                        return new GridCacheUpdateTxResult<>(false, interceptRes.get2());
                }

                GridCacheValueBytes oldBytes = valueBytesUnlocked();

                if (old == null)
                    old = saveValueForIndexUnlocked();

                // Clear indexes inside of synchronization since indexes
                // can be updated without actually holding entry lock.
                clearIndex(old);

                update(null, null, 0, 0, newVer);

                if (cctx.deferredDelete() && !detached() && !isInternal()) {
                    if (!deletedUnlocked())
                        deletedUnlocked(true);

                    enqueueVer = newVer;
                }

                drReplicate(drType, null, null, newVer);

                if (metrics)
                    cctx.cache().metrics0().onWrite();

                if (tx == null)
                    obsoleteVer = newVer;
                else {
                    // Only delete entry if the lock is not explicit.
                    if (tx.groupLock() || lockedBy(tx.xidVersion()))
                        obsoleteVer = tx.xidVersion();
                    else if (log.isDebugEnabled())
                        log.debug("Obsolete version was not set because lock was explicit: " + this);
                }

                if (evt && newVer != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_REMOVED))
                    cctx.events().addEvent(partition(), key, evtNodeId, tx == null ? null : tx.xid(), newVer,
                        EVT_CACHE_OBJECT_REMOVED, null, false, old, old != null || hasValueUnlocked(), subjId, null,
                        taskName);

                GridCacheMode mode = cctx.config().getCacheMode();

                if (mode == GridCacheMode.LOCAL || mode == GridCacheMode.REPLICATED ||
                    (tx != null && (tx.dht() || tx.colocated()) && tx.local()))
                    cctx.continuousQueries().onEntryUpdate(this, key, null, null, old, oldBytes);

                cctx.dataStructures().onEntryUpdated(key, true);
            }
        }
        finally {
            if (enqueueVer != null) {
                assert cctx.deferredDelete();

                cctx.onDeferredDelete(this, enqueueVer);
            }
        }

        // Persist outside of synchronization. The correctness of the
        // value will be handled by current transaction.
        if (writeThrough)
            cctx.store().removeFromStore(tx, key);

        if (!cctx.deferredDelete()) {
            boolean marked = false;

            synchronized (this) {
                // If entry is still removed.
                if (newVer == ver) {
                    if (obsoleteVer == null || !(marked = markObsolete0(obsoleteVer, true))) {
                        if (log.isDebugEnabled())
                            log.debug("Entry could not be marked obsolete (it is still used): " + this);
                    }
                    else {
                        recordNodeId(affNodeId);

                        // If entry was not marked obsolete, then removed lock
                        // will be registered whenever removeLock is called.
                        cctx.mvcc().addRemoved(obsoleteVer);

                        if (log.isDebugEnabled())
                            log.debug("Entry was marked obsolete: " + this);
                    }
                }
            }

            if (marked)
                onMarkedObsolete();
        }

        if (intercept)
            cctx.config().getInterceptor().onAfterRemove(key, old);

        return valid ? new GridCacheUpdateTxResult<>(true, interceptRes != null ? interceptRes.get2() : old) :
            new GridCacheUpdateTxResult<V>(false, null);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridBiTuple<Boolean, V> innerUpdateLocal(
        GridCacheVersion ver,
        GridCacheOperation op,
        @Nullable Object writeObj,
        boolean writeThrough,
        boolean retval,
        long ttl,
        boolean evt,
        boolean metrics,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter,
        boolean intercept,
        @Nullable UUID subjId,
        String taskName
    ) throws GridException, GridCacheEntryRemovedException {
        assert cctx.isLocal() && cctx.atomic();

        V old;
        boolean res = true;

        GridBiTuple<Boolean, ?> interceptorRes = null;

        synchronized (this) {
            boolean needVal = retval || intercept || op == GridCacheOperation.TRANSFORM || !F.isEmpty(filter);

            checkObsolete();

            // Load and remove from swap if it is new.
            if (isNew())
                unswap(true);

            long newTtl = ttl;

            if (newTtl < 0)
                newTtl = ttlExtras();

            long newExpireTime = toExpireTime(newTtl);

            // Possibly get old value form store.
            old = needVal ? rawGetOrUnmarshalUnlocked() : val;

            GridCacheValueBytes oldBytes = valueBytesUnlocked();

            if (needVal && old == null) {
                old = readThrough(null, key, false, CU.<K, V>empty(), subjId, taskName);

                // Detach value before index update.
                if (cctx.portableEnabled())
                    old = (V)cctx.kernalContext().portable().detachPortable(old);

                if (old != null)
                    updateIndex(old, null, expireTime(), ver, null);
                else
                    clearIndex(null);

                update(old, null, 0, 0, ver);
            }

            // Check filter inside of synchronization.
            if (!F.isEmpty(filter)) {
                boolean pass = cctx.isAll(wrapFilterLocked(), filter);

                if (!pass)
                    return new GridBiTuple<>(false, old);
            }

            // Apply metrics.
            if (metrics && needVal)
                cctx.cache().metrics0().onRead(old != null);

            String transformCloClsName = null;

            V updated;

            // Calculate new value.
            if (op == GridCacheOperation.TRANSFORM) {
                transformCloClsName = writeObj.getClass().getName();

                GridClosure<V, V> transform = (GridClosure<V, V>)writeObj;

                assert transform != null;

                updated = transform.apply(old);
            }
            else
                updated = (V)writeObj;

            op = updated == null ? GridCacheOperation.DELETE : GridCacheOperation.UPDATE;

            if (intercept) {
                if (op == GridCacheOperation.UPDATE) {
                    updated = (V)cctx.config().getInterceptor().onBeforePut(key, old, updated);

                    if (updated == null)
                        return new GridBiTuple<>(false, old);
                }
                else {
                    interceptorRes = cctx.config().getInterceptor().onBeforeRemove(key, old);

                    if (cctx.cancelRemove(interceptorRes))
                        return new GridBiTuple<>(false, (V)interceptorRes.get2());
                }
            }

            boolean hadVal = hasValueUnlocked();

            // Try write-through.
            if (op == GridCacheOperation.UPDATE) {
                // Detach value before index update.
                if (cctx.portableEnabled())
                    updated = (V)cctx.kernalContext().portable().detachPortable(updated);

                if (writeThrough)
                    // Must persist inside synchronization in non-tx mode.
                    cctx.store().putToStore(null, key, updated, ver);

                // Update index inside synchronization since it can be updated
                // in load methods without actually holding entry lock.
                updateIndex(updated, null, newExpireTime, ver, old);

                update(updated, null, newExpireTime, newTtl, ver);

                if (evt) {
                    cctx.events().addEvent(partition(), key, cctx.localNodeId(), null,
                        (GridCacheVersion)null, EVT_CACHE_OBJECT_READ, old, old != null || hadVal, old,
                        old != null || hadVal, subjId, transformCloClsName, taskName);

                    if (cctx.events().isRecordable(EVT_CACHE_OBJECT_PUT))
                        cctx.events().addEvent(partition(), key, cctx.localNodeId(), null,
                            (GridCacheVersion)null, EVT_CACHE_OBJECT_PUT, updated, updated != null, old,
                            old != null || hadVal, subjId, null, taskName);
                }
            }
            else {
                if (writeThrough)
                    // Must persist inside synchronization in non-tx mode.
                    cctx.store().removeFromStore(null, key);

                // Update index inside synchronization since it can be updated
                // in load methods without actually holding entry lock.
                clearIndex(old);

                update(null, null, 0, 0, ver);

                if (evt) {
                    if (transformCloClsName != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ))
                        cctx.events().addEvent(partition(), key, cctx.localNodeId(), null,
                            (GridCacheVersion)null, EVT_CACHE_OBJECT_READ, old, old != null || hadVal, old,
                            old != null || hadVal, subjId, transformCloClsName, taskName);

                    if (cctx.events().isRecordable(EVT_CACHE_OBJECT_REMOVED))
                        cctx.events().addEvent(partition(), key, cctx.localNodeId(), null, (GridCacheVersion)null,
                            EVT_CACHE_OBJECT_REMOVED, null, false, old, old != null || hadVal, subjId, null, taskName);
                }

                res = hadVal;
            }

            if (metrics)
                cctx.cache().metrics0().onWrite();

            cctx.continuousQueries().onEntryUpdate(this, key, val, valueBytesUnlocked(), old, oldBytes);

            cctx.dataStructures().onEntryUpdated(key, op == DELETE);

            if (intercept) {
                if (op == UPDATE)
                    cctx.config().getInterceptor().onAfterPut(key, val);
                else
                    cctx.config().getInterceptor().onAfterRemove(key, old);
            }
        }

        return new GridBiTuple<>(res, interceptorRes != null ? (V)interceptorRes.get2() : old);
    }

    /** {@inheritDoc} */
    @Override public GridCacheUpdateAtomicResult<K, V> innerUpdate(
        GridCacheVersion newVer,
        UUID evtNodeId,
        UUID affNodeId,
        GridCacheOperation op,
        @Nullable Object writeObj,
        @Nullable byte[] valBytes,
        boolean writeThrough,
        boolean retval,
        long ttl,
        boolean evt,
        boolean metrics,
        boolean primary,
        boolean verCheck,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter,
        GridDrType drType,
        long drTtl,
        long drExpireTime,
        @Nullable GridCacheVersion drVer,
        boolean drResolve,
        boolean intercept,
        @Nullable UUID subjId,
        String taskName
    ) throws GridException, GridCacheEntryRemovedException, GridClosureException {
        assert cctx.atomic();

        V old;

        boolean res = true;

        V updated;

        GridCacheVersion enqueueVer = null;

        GridDrReceiverConflictContextImpl<K, V> drRes = null;

        long newTtl = 0L;
        long newExpireTime = 0L;
        long newDrExpireTime = -1L; // Explicit DR expire time which possibly will be sent to DHT node.

        synchronized (this) {
            boolean needVal = intercept || retval || op == GridCacheOperation.TRANSFORM || !F.isEmptyOrNulls(filter);

            checkObsolete();

            // Load and remove from swap if it is new.
            if (isNew())
                unswap(true);

            boolean newTtlResolved = false;

            boolean drNeedResolve = false;

            Object transformClo = null;

            if (drResolve) {
                GridCacheVersion oldDrVer = version().drVersion();

                drNeedResolve = cctx.drNeedResolve(oldDrVer, drVer);

                if (drNeedResolve) {
                    // Get old value.
                    V oldVal = rawGetOrUnmarshalUnlocked();

                    if (writeObj == null && valBytes != null)
                        writeObj = cctx.marshaller().unmarshal(valBytes, cctx.deploy().globalLoader());

                    if (op == TRANSFORM) {
                        transformClo = writeObj;

                        writeObj = ((GridClosure<V, V>) writeObj).apply(oldVal);
                    }

                    K k = key();

                    if (drTtl >= 0L) {
                        // DR TTL is set explicitly
                        assert drExpireTime >= 0L;

                        newTtl = drTtl;
                        newExpireTime = drExpireTime;
                    }
                    else {
                        newTtl = ttl < 0 ? ttlExtras() : ttl;
                        newExpireTime = CU.toExpireTime(newTtl);
                    }

                    newTtlResolved = true;

                    GridDrEntry<K, V> oldEntry = drEntry();
                    GridDrEntry<K, V> newEntry = new GridDrPlainEntry<>(k, (V)writeObj, newTtl, newExpireTime, drVer);

                    drRes = cctx.drResolveConflict(k, oldEntry, newEntry);

                    assert drRes != null;

                    if (drRes.isUseOld()) {
                        old = retval ? rawGetOrUnmarshalUnlocked() : val;

                        return new GridCacheUpdateAtomicResult<>(false, old, null, 0L, -1L, null, null, false);
                    }
                    else if (drRes.isUseNew())
                        op = writeObj != null ? UPDATE : DELETE;
                    else {
                        assert drRes.isMerge();

                        writeObj = drRes.mergeValue();
                        valBytes = null;

                        drVer = null; // Update will be considered as local.

                        op = writeObj != null ? UPDATE : DELETE;
                    }

                    newTtl = drRes.ttl();
                    newExpireTime = drRes.expireTime();

                    // Explicit DR expire time will be passed to remote node only in that case.
                    if (!drRes.explicitTtl() && !drRes.isMerge()) {
                        if (drRes.isUseNew() && newEntry.dataCenterId() != cctx.gridConfig().getDataCenterId() ||
                            drRes.isUseOld() && oldEntry.dataCenterId() != cctx.gridConfig().getDataCenterId())
                            newDrExpireTime = drRes.expireTime();
                    }
                }
                else
                    // Nullify DR version on this update, so that we will use regular version during next updates.
                    drVer = null;
            }

            if (!drNeedResolve) { // Perform version check only in case there will be no explicit conflict resolution.
                if (verCheck) {
                    if (!isNew() && ATOMIC_VER_COMPARATOR.compare(ver, newVer) >= 0) {
                        if (ATOMIC_VER_COMPARATOR.compare(ver, newVer) == 0 && cctx.isStoreEnabled() && primary) {
                            if (log.isDebugEnabled())
                                log.debug("Received entry update with same version as current (will update store) " +
                                    "[entry=" + this + ", newVer=" + newVer + ']');

                            V val = rawGetOrUnmarshalUnlocked();

                            if (val == null) {
                                assert deletedUnlocked();

                                cctx.store().removeFromStore(null, key());
                            }
                            else
                                cctx.store().putToStore(null, key(), val, ver);
                        }
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Received entry update with smaller version than current (will ignore) " +
                                    "[entry=" + this + ", newVer=" + newVer + ']');
                        }

                        old = retval ? rawGetOrUnmarshalUnlocked() : val;

                        return new GridCacheUpdateAtomicResult<>(false, old, null, 0L, -1L, null, null, false);
                    }
                }
                else
                    assert isNew() || ATOMIC_VER_COMPARATOR.compare(ver, newVer) <= 0 :
                        "Invalid version for inner update [entry=" + this + ", newVer=" + newVer + ']';
            }

            if (!newTtlResolved) {
                // Calculate TTL and expire time for local update.
                if (drTtl >= 0L) {
                    assert drExpireTime >= 0L;

                    newTtl = drTtl;
                    newExpireTime = drExpireTime;
                }
                else {
                    assert drExpireTime == -1L;

                    newTtl = ttl;

                    if (newTtl < 0)
                        newTtl = ttlExtras();

                    newExpireTime = toExpireTime(newTtl);
                }
            }

            // Possibly get old value form store.
            old = needVal ? rawGetOrUnmarshalUnlocked() : val;

            GridCacheValueBytes oldBytes = valueBytesUnlocked();

            if (needVal && old == null) {
                old = readThrough(null, key, false, CU.<K, V>empty(), subjId, taskName);

                // Detach value before index update.
                if (cctx.portableEnabled())
                    old = (V)cctx.kernalContext().portable().detachPortable(old);

                if (old != null)
                    updateIndex(old, null, expireTime(), ver, null);
                else
                    clearIndex(null);

                update(old, null, 0, 0, ver);

                if (deletedUnlocked() && old != null && !isInternal())
                    deletedUnlocked(false);
            }

            // Check filter inside of synchronization.
            if (!F.isEmptyOrNulls(filter)) {
                boolean pass = cctx.isAll(wrapFilterLocked(), filter);

                if (!pass)
                    return new GridCacheUpdateAtomicResult<>(false, old, null, 0L, -1L, null, null, false);
            }

            // Apply metrics.
            if (metrics && needVal)
                cctx.cache().metrics0().onRead(old != null);

            // Calculate new value.
            if (op == GridCacheOperation.TRANSFORM) {
                transformClo = writeObj;

                GridClosure<V, V> transform = (GridClosure<V, V>) writeObj;

                updated = transform.apply(old);

                valBytes = null;
            }
            else
                updated = (V)writeObj;

            op = updated == null ? GridCacheOperation.DELETE : GridCacheOperation.UPDATE;

            assert op == GridCacheOperation.UPDATE || (op == GridCacheOperation.DELETE && updated == null);

            boolean hadVal = hasValueUnlocked();

            // Incorporate DR version into new version if needed.
            if (drVer != null && drVer != newVer)
                newVer = new GridCacheVersionEx(newVer.topologyVersion(), newVer.globalTime(), newVer.order(),
                    newVer.nodeOrder(), newVer.dataCenterId(), drVer);

            GridBiTuple<Boolean, V> interceptRes = null;

            if (op == GridCacheOperation.UPDATE) {
                if (intercept) {
                    V interceptorVal = (V)cctx.config().getInterceptor().onBeforePut(key, old, updated);

                    if (interceptorVal == null)
                        return new GridCacheUpdateAtomicResult<>(false, old, null, 0L, -1L, null, null, false);
                    else if (interceptorVal != updated) {
                        updated = interceptorVal;
                        valBytes = null;
                    }
                }

                // Try write-through.
                if (writeThrough)
                    // Must persist inside synchronization in non-tx mode.
                    cctx.store().putToStore(null, key, updated, newVer);

                if (!hadVal) {
                    boolean new0 = isNew();

                    assert deletedUnlocked() || new0 || isInternal(): "Invalid entry [entry=" + this + ", locNodeId=" +
                        cctx.localNodeId() + ']';

                    if (!new0 && !isInternal())
                        deletedUnlocked(false);
                }
                else {
                    assert !deletedUnlocked() : "Invalid entry [entry=" + this +
                        ", locNodeId=" + cctx.localNodeId() + ']';

                    // Do not change size;
                }

                if (cctx.portableEnabled())
                    updated = (V)cctx.kernalContext().portable().detachPortable(updated);

                // Update index inside synchronization since it can be updated
                // in load methods without actually holding entry lock.
                updateIndex(updated, valBytes, newExpireTime, newVer, old);

                update(updated, valBytes, newExpireTime, newTtl, newVer);

                drReplicate(drType, updated, valBytes, newVer);

                recordNodeId(affNodeId);

                if (evt) {
                    if (transformClo != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ))
                        cctx.events().addEvent(partition(), key, evtNodeId, null,
                            newVer, EVT_CACHE_OBJECT_READ, old, old != null || hadVal, old,
                            old != null || hadVal, subjId, transformClo.getClass().getName(), taskName);

                    if (newVer != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_PUT))
                        cctx.events().addEvent(partition(), key, evtNodeId, null,
                            newVer, EVT_CACHE_OBJECT_PUT, updated, updated != null, old,
                            old != null || hadVal, subjId, null, taskName);
                }
            }
            else {
                if (intercept) {
                    interceptRes = cctx.config().<K, V>getInterceptor().onBeforeRemove(key, old);

                    if (cctx.cancelRemove(interceptRes))
                        return new GridCacheUpdateAtomicResult<>(false, interceptRes.get2(), null, 0L, -1L, null,
                            null, false);
                }

                if (writeThrough)
                    // Must persist inside synchronization in non-tx mode.
                    cctx.store().removeFromStore(null, key);

                // Update index inside synchronization since it can be updated
                // in load methods without actually holding entry lock.
                clearIndex(old);

                if (hadVal) {
                    assert !deletedUnlocked();

                    if (!isInternal())
                        deletedUnlocked(true);
                }
                else {
                    boolean new0 = isNew();

                    assert deletedUnlocked() || new0 || isInternal() : "Invalid entry [entry=" + this + ", locNodeId=" +
                        cctx.localNodeId() + ']';

                    if (new0) {
                        if (!isInternal())
                            deletedUnlocked(true);
                    }
                }

                enqueueVer = newVer;

                // Clear value on backup. Entry will be removed from cache when it got evicted from queue.
                update(null, null, 0, 0, newVer);

                clearReaders();

                recordNodeId(affNodeId);

                drReplicate(drType, null, null, newVer);

                if (evt) {
                    if (transformClo != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ))
                        cctx.events().addEvent(partition(), key, evtNodeId, null,
                            newVer, EVT_CACHE_OBJECT_READ, old, old != null || hadVal, old,
                            old != null || hadVal, subjId, transformClo.getClass().getName(), taskName);

                    if (newVer != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_REMOVED))
                        cctx.events().addEvent(partition(), key, evtNodeId, null, newVer,
                            EVT_CACHE_OBJECT_REMOVED, null, false, old, old != null || hadVal, subjId, null, taskName);
                }

                res = hadVal;

                // Do not propagate zeroed TTL and expire time.
                newTtl = 0L;
                newDrExpireTime = -1L;
            }

            if (metrics)
                cctx.cache().metrics0().onWrite();

            if (primary || cctx.isReplicated())
                cctx.continuousQueries().onEntryUpdate(this, key, val, valueBytesUnlocked(), old, oldBytes);

            cctx.dataStructures().onEntryUpdated(key, op == DELETE);

            if (intercept) {
                if (op == UPDATE)
                    cctx.config().getInterceptor().onAfterPut(key, val);
                else
                    cctx.config().getInterceptor().onAfterRemove(key, old);

                if (interceptRes != null)
                    old = interceptRes.get2();
            }
        }

        if (log.isDebugEnabled())
            log.debug("Updated cache entry [val=" + val + ", old=" + old + ", entry=" + this + ']');

        return new GridCacheUpdateAtomicResult<>(res, old, updated, newTtl, newDrExpireTime, enqueueVer, drRes, true);
    }

    /**
     * Perform DR if needed.
     *
     * @param drType DR type.
     * @param val Value.
     * @param valBytes Value bytes.
     * @param ver Version.
     * @throws GridException In case of exception.
     */
    private void drReplicate(GridDrType drType, @Nullable V val, @Nullable byte[] valBytes, GridCacheVersion ver)
        throws GridException{
        if (cctx.isDrEnabled() && drType != DR_NONE && !isInternal()) {
            GridDrSenderCacheConfiguration drSndCfg = cctx.config().getDrSenderConfiguration();

            GridDrSenderCacheEntryFilter<K, V> drFilter = drSndCfg != null ?
                (GridDrSenderCacheEntryFilter<K, V>)drSndCfg.getEntryFilter() : null;

            GridDrRawEntry<K, V> entry = new GridDrRawEntry<>(key, keyBytes, val, valBytes, rawTtl(), rawExpireTime(),
                ver.drVersion());

            boolean apply = drFilter == null;

            if (!apply) {
                // Unmarshall entry and pass it to filter.
                entry.unmarshal(cctx.marshaller());

                apply = drFilter.accept(entry);
            }

            if (apply)
                cctx.dr().replicate(entry, drType);
            else
                cctx.cache().metrics0().onSenderCacheEntryFiltered();
        }
    }

    /**
     * @return {@code true} if entry has readers. It makes sense only for dht entry.
     * @throws GridCacheEntryRemovedException If removed.
     */
    protected boolean hasReaders() throws GridCacheEntryRemovedException {
        return false;
    }

    /**
     *
     */
    protected void clearReaders() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean clear(GridCacheVersion ver, boolean readers,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        cctx.denyOnFlag(GridCacheFlag.READ);

        boolean ret;
        boolean rmv;
        boolean marked;

        while (true) {
            ret = false;
            rmv = false;
            marked = false;

            // For optimistic check.
            GridCacheVersion startVer = null;

            if (!F.isEmptyOrNulls(filter)) {
                synchronized (this) {
                    startVer = this.ver;
                }

                if (!cctx.isAll(this, filter))
                    return false;
            }

            synchronized (this) {
                if (startVer != null && !startVer.equals(this.ver))
                    // Version has changed since filter checking.
                    continue;

                V val = saveValueForIndexUnlocked();

                try {
                    if ((!hasReaders() || readers)) {
                        // markObsolete will clear the value.
                        if (!(marked = markObsolete0(ver, true))) {
                            if (log.isDebugEnabled())
                                log.debug("Entry could not be marked obsolete (it is still used): " + this);

                            break;
                        }

                        clearReaders();
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Entry could not be marked obsolete (it still has readers): " + this);

                        break;
                    }
                }
                catch (GridCacheEntryRemovedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry when clearing (will simply return): " + this);

                    ret = true;

                    break;
                }

                if (log.isDebugEnabled())
                    log.debug("Entry has been marked obsolete: " + this);

                clearIndex(val);

                releaseSwap();

                ret = true;
                rmv = true;

                break;
            }
        }

        if (marked)
            onMarkedObsolete();

        if (rmv)
            cctx.cache().removeEntry(this); // Clear cache.

        return ret;
    }

    /** {@inheritDoc} */
    @Override public synchronized GridCacheVersion obsoleteVersion() {
        return obsoleteVersionExtras();
    }

    /** {@inheritDoc} */
    @Override public boolean markObsolete(GridCacheVersion ver) {
        boolean obsolete;

        synchronized (this) {
            obsolete = markObsolete0(ver, true);
        }

        if (obsolete)
            onMarkedObsolete();

        return obsolete;
    }

    /** {@inheritDoc} */
    @Override public boolean markObsoleteIfEmpty(@Nullable GridCacheVersion ver) throws GridException {
        boolean obsolete = false;
        boolean deferred = false;

        try {
            synchronized (this) {
                if (obsoleteVersionExtras() != null)
                    return false;

                if (!hasValueUnlocked() || checkExpired()) {
                    if (ver == null)
                        ver = nextVersion();

                    if (cctx.deferredDelete() && !isStartVersion() && !detached() && !isInternal()) {
                        if (!deletedUnlocked()) {
                            update(null, null, 0L, 0L, ver);

                            deletedUnlocked(true);

                            deferred = true;
                        }
                    }
                    else
                        obsolete = markObsolete0(ver, true);
                }
            }
        }
        finally {
            if (obsolete)
                onMarkedObsolete();

            if (deferred)
                cctx.onDeferredDelete(this, ver);
        }

        return obsolete;
    }

    /** {@inheritDoc} */
    @Override public boolean markObsoleteVersion(GridCacheVersion ver) {
        assert cctx.deferredDelete();

        boolean marked;

        synchronized (this) {
            if (obsoleteVersionExtras() != null)
                return true;

            if (!this.ver.equals(ver))
                return false;

            marked = markObsolete0(ver, true);
        }

        if (marked)
            onMarkedObsolete();

        return marked;
    }

    /**
     * <p>
     * Note that {@link #onMarkedObsolete()} should always be called after this method
     * returns {@code true}.
     *
     * @param ver Version.
     * @param clear {@code True} to clear.
     * @return {@code True} if entry is obsolete, {@code false} if entry is still used by other threads or nodes.
     */
    protected final boolean markObsolete0(GridCacheVersion ver, boolean clear) {
        assert Thread.holdsLock(this);

        GridCacheVersion obsoleteVer = obsoleteVersionExtras();

        if (ver != null) {
            // If already obsolete, then do nothing.
            if (obsoleteVer != null)
                return true;

            GridCacheMvcc<K> mvcc = mvccExtras();

            if (mvcc == null || mvcc.isEmpty(ver)) {
                obsoleteVer = ver;

                obsoleteVersionExtras(obsoleteVer);

                if (clear)
                    value(null, null);
            }

            return obsoleteVer != null;
        }
        else
            return obsoleteVer != null;
    }

    /** {@inheritDoc} */
    @Override public void onMarkedObsolete() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public final synchronized boolean obsolete() {
        return obsoleteVersionExtras() != null;
    }

    /** {@inheritDoc} */
    @Override public final synchronized boolean obsolete(GridCacheVersion exclude) {
        GridCacheVersion obsoleteVer = obsoleteVersionExtras();

        return obsoleteVer != null && !obsoleteVer.equals(exclude);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean invalidate(@Nullable GridCacheVersion curVer, GridCacheVersion newVer)
        throws GridException {
        assert newVer != null;

        if (curVer == null || ver.equals(curVer)) {
            V val = saveValueForIndexUnlocked();

            value(null, null);

            ver = newVer;

            releaseSwap();

            clearIndex(val);

            onInvalidate();
        }

        return obsoleteVersionExtras() != null;
    }

    /**
     * Called when entry invalidated.
     */
    protected void onInvalidate() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean invalidate(@Nullable GridPredicate<GridCacheEntry<K, V>>[] filter)
        throws GridCacheEntryRemovedException, GridException {
        if (F.isEmptyOrNulls(filter)) {
            synchronized (this) {
                checkObsolete();

                invalidate(null, nextVersion());

                return true;
            }
        }
        else {
            // For optimistic checking.
            GridCacheVersion startVer;

            synchronized (this){
                checkObsolete();

                startVer = ver;
            }

            if (!cctx.isAll(this, filter))
                return false;

            synchronized (this) {
                checkObsolete();

                if (startVer.equals(ver)) {
                    invalidate(null, nextVersion());

                    return true;
                }
            }

            // If version has changed then repeat the process.
            return invalidate(filter);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean compact(@Nullable GridPredicate<GridCacheEntry<K, V>>[] filter)
        throws GridCacheEntryRemovedException, GridException {
        // For optimistic checking.
        GridCacheVersion startVer;

        synchronized (this) {
            checkObsolete();

            startVer = ver;
        }

        if (!cctx.isAll(this, filter))
            return false;

        synchronized (this) {
            checkObsolete();

            if (deletedUnlocked())
                return false; // Cannot compact soft-deleted entries.

            if (startVer.equals(ver)) {
                if (hasValueUnlocked() && !checkExpired()) {
                    if (!isOffHeapValuesOnly()) {
                        if (val != null)
                            valBytes = null;
                    }

                    return false;
                }
                else
                    return clear(nextVersion(), false, filter);
            }
        }

        // If version has changed do it again.
        return compact(filter);
    }

    /**
     *
     * @param val New value.
     * @param valBytes New value bytes.
     * @param expireTime Expiration time.
     * @param ttl Time to live.
     * @param ver Update version.
     */
    protected final void update(@Nullable V val, @Nullable byte[] valBytes, long expireTime, long ttl,
        GridCacheVersion ver) {
        assert ver != null;
        assert Thread.holdsLock(this);

        long oldExpireTime = expireTimeExtras();

        if (oldExpireTime != 0 && expireTime != oldExpireTime && cctx.config().isEagerTtl())
            cctx.ttl().removeTrackedEntry(this);

        value(val, valBytes);

        ttlAndExpireTimeExtras(ttl, expireTime);

        if (expireTime != 0 && expireTime != oldExpireTime && cctx.config().isEagerTtl())
            cctx.ttl().addTrackedEntry(this);

        this.ver = ver;
    }

    /**
     * @return {@code true} If value bytes should be stored.
     */
    protected boolean isStoreValueBytes() {
        return cctx.config().isStoreValueBytes();
    }

    /**
     * @return {@code True} if values should be stored off-heap.
     */
    protected boolean isOffHeapValuesOnly() {
        return cctx.config().getMemoryMode() == GridCacheMemoryMode.OFFHEAP_VALUES;
    }

    /**
     * @param ttl Time to live.
     * @return Expiration time.
     */
    protected long toExpireTime(long ttl) {
        long expireTime = ttl == 0 ? 0 : U.currentTimeMillis() + ttl;

        // Account for overflow.
        if (expireTime < 0)
            expireTime = 0;

        return expireTime;
    }

    /**
     * @throws GridCacheEntryRemovedException If entry is obsolete.
     */
    protected void checkObsolete() throws GridCacheEntryRemovedException {
        assert Thread.holdsLock(this);

        if (obsoleteVersionExtras() != null)
            throw new GridCacheEntryRemovedException();
    }

    /** {@inheritDoc} */
    @Override public K key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public synchronized GridCacheVersion version() throws GridCacheEntryRemovedException {
        checkObsolete();

        return ver;
    }

    /**
     * Gets hash value for the entry key.
     *
     * @return Hash value.
     */
    int hash() {
        return hash;
    }

    /**
     * Gets next entry in bucket linked list within a hash map segment.
     *
     * @param segId Segment ID.
     * @return Next entry.
     */
    GridCacheMapEntry<K, V> next(int segId) {
        return segId % 2 == 0 ? next0 : next1;
    }

    /**
     * Sets next entry in bucket linked list within a hash map segment.
     *
     * @param segId Segment ID.
     * @param next Next entry.
     */
    void next(int segId, @Nullable GridCacheMapEntry<K, V> next) {
        if (segId % 2 == 0)
            next0 = next;
        else
            next1 = next;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V peek(GridCachePeekMode mode, GridPredicate<GridCacheEntry<K, V>>[] filter)
        throws GridCacheEntryRemovedException {
        try {
            GridTuple<V> peek = peek0(false, mode, filter, cctx.tm().localTxx());

            return peek != null ? peek.get() : null;
        }
        catch (GridCacheFilterFailedException ignore) {
            assert false;

            return null;
        }
        catch (GridException e) {
            throw new GridRuntimeException("Unable to perform entry peek() operation.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public V peek(Collection<GridCachePeekMode> modes, GridPredicate<GridCacheEntry<K, V>>[] filter)
        throws GridCacheEntryRemovedException {
        assert modes != null;

        for (GridCachePeekMode mode : modes) {
            try {
                GridTuple<V> val = peek0(false, mode, filter, cctx.tm().localTxx());

                if (val != null)
                    return val.get();
            }
            catch (GridCacheFilterFailedException ignored) {
                assert false;

                return null;
            }
            catch (GridException e) {
                throw new GridRuntimeException("Unable to perform entry peek() operation.", e);
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V peekFailFast(GridCachePeekMode mode,
        GridPredicate<GridCacheEntry<K, V>>[] filter)
        throws GridCacheEntryRemovedException, GridCacheFilterFailedException {
        try {
            GridTuple<V> peek = peek0(true, mode, filter, cctx.tm().localTxx());

            return peek != null ? peek.get() : null;
        }
        catch (GridException e) {
            throw new GridRuntimeException("Unable to perform entry peek() operation.", e);
        }
    }

    /**
     * @param failFast Fail-fast flag.
     * @param mode Peek mode.
     * @param filter Filter.
     * @param tx Transaction to peek value at (if mode is TX value).
     * @return Peeked value.
     * @throws GridException In case of error.
     * @throws GridCacheEntryRemovedException If removed.
     * @throws GridCacheFilterFailedException If filter failed.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Nullable @Override public GridTuple<V> peek0(boolean failFast, GridCachePeekMode mode,
        GridPredicate<GridCacheEntry<K, V>>[] filter, @Nullable GridCacheTxEx<K, V> tx)
        throws GridCacheEntryRemovedException, GridCacheFilterFailedException, GridException {
        assert tx == null || tx.local();

        long topVer = tx != null ? tx.topologyVersion() : cctx.affinity().affinityTopologyVersion();

        switch (mode) {
            case TX:
                return peekTx(failFast, filter, tx);

            case GLOBAL:
                return peekGlobal(failFast, topVer, filter);

            case NEAR_ONLY:
                return peekGlobal(failFast, topVer, filter);

            case PARTITIONED_ONLY:
                return peekGlobal(failFast, topVer, filter);

            case SMART:
                /*
                 * If there is no ongoing transaction, or transaction is NOT in ACTIVE state,
                 * which means that it is either rolling back, preparing to commit, or committing,
                 * then we only check the global cache storage because value has already been
                 * validated against filter and enlisted into transaction and, therefore, second
                 * validation against the same enlisted value will be invalid (it will always be false).
                 *
                 * However, in ACTIVE state, we must also validate against other values that
                 * may have enlisted into the same transaction and that's why we pass 'true'
                 * to 'e.peek(true)' method in this case.
                 */
                return tx == null || tx.state() != ACTIVE ? peekGlobal(failFast, topVer, filter) :
                    peekTxThenGlobal(failFast, filter, tx);

            case SWAP:
                return peekSwap(failFast, filter);

            case DB:
                return F.t(peekDb(failFast, filter));

            default: // Should never be reached.
                assert false;

                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public V poke(V val) throws GridCacheEntryRemovedException, GridException {
        assert val != null;

        V old;

        synchronized (this) {
            checkObsolete();

            if (isNew() || !valid(-1))
                unswap(true);

            if (deletedUnlocked())
                return null;

            old = rawGetOrUnmarshalUnlocked();

            GridCacheVersion nextVer = nextVersion();

            // Update index inside synchronization since it can be updated
            // in load methods without actually holding entry lock.
            long expireTime = expireTimeExtras();

            if (cctx.portableEnabled())
                val = (V)cctx.kernalContext().portable().detachPortable(val);

            updateIndex(val, null, expireTime, nextVer, old);

            update(val, null, expireTime, ttlExtras(), nextVer);
        }

        if (log.isDebugEnabled())
            log.debug("Poked cache entry [newVal=" + val + ", oldVal=" + old + ", entry=" + this + ']');

        return old;
    }

    /**
     * Checks that entries in group locks transactions are not locked during commit.
     *
     * @param tx Transaction to check.
     * @throws GridCacheEntryRemovedException If entry is obsolete.
     * @throws GridException If entry was externally locked.
     */
    private void groupLockSanityCheck(GridCacheTxEx<K, V> tx) throws GridCacheEntryRemovedException, GridException {
        assert tx.groupLock();

        GridCacheTxEntry<K, V> txEntry = tx.entry(key);

        if (txEntry.groupLockEntry()) {
            if (lockedByAny())
                throw new GridException("Failed to update cache entry (entry was externally locked while " +
                    "accessing entry within group lock transaction) [entry=" + this + ", tx=" + tx + ']');
        }
    }

    /**
     * @param failFast Fail fast flag.
     * @param filter Filter.
     * @param tx Transaction to peek value at (if mode is TX value).
     * @return Peeked value.
     * @throws GridCacheFilterFailedException If filter failed.
     * @throws GridCacheEntryRemovedException If entry got removed.
     * @throws GridException If unexpected cache failure occurred.
     */
    @Nullable private GridTuple<V> peekTxThenGlobal(boolean failFast, GridPredicate<GridCacheEntry<K, V>>[] filter,
        GridCacheTxEx<K, V> tx) throws GridCacheFilterFailedException, GridCacheEntryRemovedException, GridException {
        GridTuple<V> peek = peekTx(failFast, filter, tx);

        // If transaction has value (possibly null, which means value is to be deleted).
        if (peek != null)
            return peek;

        long topVer = tx == null ? cctx.affinity().affinityTopologyVersion() : tx.topologyVersion();

        return peekGlobal(failFast, topVer, filter);
    }

    /**
     * @param failFast Fail fast flag.
     * @param filter Filter.
     * @param tx Transaction to peek value at (if mode is TX value).
     * @return Peeked value.
     * @throws GridCacheFilterFailedException If filter failed.
     */
    @Nullable private GridTuple<V> peekTx(boolean failFast,
        GridPredicate<GridCacheEntry<K, V>>[] filter,
        @Nullable GridCacheTxEx<K, V> tx) throws GridCacheFilterFailedException {
        return tx == null ? null : tx.peek(failFast, key, filter);
    }

    /**
     * @param failFast Fail fast flag.
     * @param filter Filter.
     * @return Peeked value.
     * @throws GridCacheFilterFailedException If filter failed.
     * @throws GridCacheEntryRemovedException If entry got removed.
     * @throws GridException If unexpected cache failure occurred.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Nullable private GridTuple<V> peekGlobal(boolean failFast, long topVer,
        GridPredicate<GridCacheEntry<K, V>>[] filter)
        throws GridCacheEntryRemovedException, GridCacheFilterFailedException, GridException {
        if (!valid(topVer))
            return null;

        boolean rmv = false;

        try {
            while (true) {
                GridCacheVersion ver;
                V val;

                synchronized (this) {
                    if (checkExpired()) {
                        rmv = markObsolete0(cctx.versions().next(this.ver), true);

                        return null;
                    }

                    checkObsolete();

                    ver = this.ver;
                    val = rawGetOrUnmarshalUnlocked();
                }

                if (!cctx.isAll(wrap(false), filter))
                    return F.t(CU.<V>failed(failFast));

                if (F.isEmptyOrNulls(filter) || ver.equals(version()))
                    return F.t(val);
            }
        }
        finally {
            if (rmv) {
                onMarkedObsolete();

                cctx.cache().map().removeEntry(this);
            }
        }
    }

    /**
     * @param failFast Fail fast flag.
     * @param filter Filter.
     * @return Value from swap storage.
     * @throws GridException In case of any errors.
     * @throws GridCacheFilterFailedException If filter failed.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable private GridTuple<V> peekSwap(boolean failFast, GridPredicate<GridCacheEntry<K, V>>[] filter)
        throws GridException, GridCacheFilterFailedException {
        if (!cctx.isAll(wrap(false), filter))
            return F.t((V)CU.failed(failFast));

        synchronized (this) {
            if (checkExpired())
                return null;
        }

        GridCacheSwapEntry<V> e = cctx.swap().read(this);

        return e != null ? F.t(e.value()) : null;
    }

    /**
     * @param failFast Fail fast flag.
     * @param filter Filter.
     * @return Value from persistent store.
     * @throws GridException In case of any errors.
     * @throws GridCacheFilterFailedException If filter failed.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable private V peekDb(boolean failFast, GridPredicate<GridCacheEntry<K, V>>[] filter)
        throws GridException, GridCacheFilterFailedException {
        if (!cctx.isAll(wrap(false), filter))
            return CU.failed(failFast);

        synchronized (this) {
            if (checkExpired())
                return null;
        }

        return cctx.store().loadFromStore(cctx.tm().localTxx(), key);
    }

    /**
     * TODO: GG-4009: do we need to generate event and invalidate value?
     *
     * @return {@code true} if expired.
     * @throws GridException In case of failure.
     */
    private boolean checkExpired() throws GridException {
        assert Thread.holdsLock(this);

        long expireTime = expireTimeExtras();

        if (expireTime > 0) {
            long delta = expireTime - U.currentTimeMillis();

            if (log.isDebugEnabled())
                log.debug("Checked expiration time for entry [timeLeft=" + delta + ", entry=" + this + ']');

            if (delta <= 0) {
                releaseSwap();

                clearIndex(saveValueForIndexUnlocked());

                return true;
            }
        }

        return false;
    }

    /**
     * @return Value.
     */
    @Override public synchronized V rawGet() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public synchronized V rawGetOrUnmarshal() throws GridException {
        return rawGetOrUnmarshalUnlocked();
    }

    /**
     * @return Value (unmarshalled if needed).
     * @throws GridException If failed.
     */
    protected V rawGetOrUnmarshalUnlocked() throws GridException {
        assert Thread.holdsLock(this);

        V val = this.val;

        if (val != null)
            return val;

        GridCacheValueBytes valBytes = valueBytesUnlocked();

        if (!valBytes.isNull())
            val = valBytes.isPlain() ? (V)valBytes.get() : cctx.marshaller().<V>unmarshal(valBytes.get(),
                cctx.deploy().globalLoader());

        return val;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean hasValue() {
        return hasValueUnlocked();
    }

    /**
     * @return {@code True} if this entry has value.
     */
    protected boolean hasValueUnlocked() {
        assert Thread.holdsLock(this);

        return val != null || valBytes != null || valPtr != 0;
    }

    /** {@inheritDoc} */
    @Override public synchronized GridDrEntry<K, V> drEntry() throws GridException {
        return new GridDrPlainEntry<>(key, isStartVersion() ? unswap(true) : rawGetOrUnmarshalUnlocked(),
            ttlExtras(), expireTimeExtras(), ver.drVersion());
    }

    /** {@inheritDoc} */
    @Override public synchronized V rawPut(V val, long ttl) {
        V old = this.val;

        update(val, null, toExpireTime(ttl), ttl, nextVersion());

        return old;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Override public boolean initialValue(V val, byte[] valBytes, GridCacheVersion ver, long ttl, long expireTime,
        boolean preload, long topVer, GridDrType drType) throws GridException, GridCacheEntryRemovedException {
        if (cctx.isUnmarshalValues() && valBytes != null && val == null && isNewLocked())
            val = cctx.marshaller().<V>unmarshal(valBytes, cctx.deploy().globalLoader());

        synchronized (this) {
            checkObsolete();

            if (isNew() || (!preload && deletedUnlocked())) {
                long expTime = expireTime < 0 ? toExpireTime(ttl) : expireTime;

                if (cctx.portableEnabled())
                    val = (V)cctx.kernalContext().portable().detachPortable(val);

                if (val != null || valBytes != null)
                    updateIndex(val, valBytes, expTime, ver, null);

                // Version does not change for load ops.
                update(val, valBytes, expTime, ttl, ver);

                boolean skipQryNtf = false;

                if (val == null && valBytes == null) {
                    skipQryNtf = true;

                    if (cctx.deferredDelete() && !isInternal()) {
                        assert !deletedUnlocked();

                        deletedUnlocked(true);
                    }
                }
                else if (deletedUnlocked())
                    deletedUnlocked(false);

                drReplicate(drType, val, valBytes, ver);

                if (!skipQryNtf) {
                    if (cctx.affinity().primary(cctx.localNode(), key, topVer) || cctx.isReplicated())
                        cctx.continuousQueries().onEntryUpdate(this, key, val, valueBytesUnlocked(), null, null);

                    cctx.dataStructures().onEntryUpdated(key, false);
                }

                return true;
            }

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean initialValue(K key, GridCacheSwapEntry <V> unswapped) throws
        GridException,
        GridCacheEntryRemovedException {
        checkObsolete();

        if (isNew()) {
            if (cctx.portableEnabled())
                val = (V)cctx.kernalContext().portable().detachPortable(val);

            // Version does not change for load ops.
            update(unswapped.value(),
                unswapped.valueBytes(),
                unswapped.expireTime(),
                unswapped.ttl(),
                unswapped.version()
            );

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean versionedValue(V val, GridCacheVersion curVer, GridCacheVersion newVer)
        throws GridException, GridCacheEntryRemovedException {
        checkObsolete();

        if (curVer == null || curVer.equals(ver)) {
            if (val != this.val) {
                if (newVer == null)
                    newVer = nextVersion();

                V old = rawGetOrUnmarshalUnlocked();

                long ttl = ttlExtras();

                long expTime = toExpireTime(ttl);

                // Detach value before index update.
                if (cctx.portableEnabled())
                    val = (V)cctx.kernalContext().portable().detachPortable(val);

                if (val != null) {
                    updateIndex(val, null, expTime, newVer, old);

                    if (deletedUnlocked())
                        deletedUnlocked(false);
                }

                // Version does not change for load ops.
                update(val, null, expTime, ttl, newVer);
            }

            return true;
        }

        return false;
    }

    /**
     * Gets next version for this cache entry.
     *
     * @return Next version.
     */
    private GridCacheVersion nextVersion() {
        // Do not change topology version when generating next version.
        return cctx.versions().next(ver);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean hasLockCandidate(GridCacheVersion ver) throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc<K> mvcc = mvccExtras();

        return mvcc != null && mvcc.hasCandidate(ver);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean hasLockCandidate(long threadId) throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc<K> mvcc = mvccExtras();

        return mvcc != null && mvcc.localCandidate(threadId) != null;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean lockedByAny(GridCacheVersion... exclude)
        throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc<K> mvcc = mvccExtras();

        return mvcc != null && !mvcc.isEmpty(exclude);
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByThread() throws GridCacheEntryRemovedException {
        return lockedByThread(Thread.currentThread().getId());
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean lockedLocally(GridCacheVersion lockVer) throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc<K> mvcc = mvccExtras();

        return mvcc != null && mvcc.isLocallyOwned(lockVer);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean lockedByThread(long threadId, GridCacheVersion exclude)
        throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc<K> mvcc = mvccExtras();

        return mvcc != null && mvcc.isLocallyOwnedByThread(threadId, false, exclude);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean lockedLocallyByIdOrThread(GridCacheVersion lockVer, long threadId)
        throws GridCacheEntryRemovedException {
        GridCacheMvcc<K> mvcc = mvccExtras();

        return mvcc != null && mvcc.isLocallyOwnedByIdOrThread(lockVer, threadId);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean lockedByThread(long threadId) throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc<K> mvcc = mvccExtras();

        return mvcc != null && mvcc.isLocallyOwnedByThread(threadId, true);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean lockedBy(GridCacheVersion ver) throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc<K> mvcc = mvccExtras();

        return mvcc != null && mvcc.isOwnedBy(ver);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean lockedByThreadUnsafe(long threadId) {
        GridCacheMvcc<K> mvcc = mvccExtras();

        return mvcc != null && mvcc.isLocallyOwnedByThread(threadId, true);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean lockedByUnsafe(GridCacheVersion ver) {
        GridCacheMvcc<K> mvcc = mvccExtras();

        return mvcc != null && mvcc.isOwnedBy(ver);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean lockedLocallyUnsafe(GridCacheVersion lockVer) {
        GridCacheMvcc<K> mvcc = mvccExtras();

        return mvcc != null && mvcc.isLocallyOwned(lockVer);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean hasLockCandidateUnsafe(GridCacheVersion ver) {
        GridCacheMvcc<K> mvcc = mvccExtras();

        return mvcc != null && mvcc.hasCandidate(ver);
    }

    /** {@inheritDoc} */
    @Override public synchronized Collection<GridCacheMvccCandidate<K>> localCandidates(GridCacheVersion... exclude)
        throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc<K> mvcc = mvccExtras();

        return mvcc == null ? Collections.<GridCacheMvccCandidate<K>>emptyList() : mvcc.localCandidates(exclude);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheMvccCandidate<K>> remoteMvccSnapshot(GridCacheVersion... exclude) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Nullable @Override public synchronized GridCacheMvccCandidate<K> candidate(GridCacheVersion ver)
        throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc<K> mvcc = mvccExtras();

        return mvcc == null ? null : mvcc.candidate(ver);
    }

    /** {@inheritDoc} */
    @Override public synchronized GridCacheMvccCandidate<K> localCandidate(long threadId)
        throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc<K> mvcc = mvccExtras();

        return mvcc == null ? null : mvcc.localCandidate(threadId);
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvccCandidate<K> candidate(UUID nodeId, long threadId)
        throws GridCacheEntryRemovedException {
        boolean loc = cctx.nodeId().equals(nodeId);

        synchronized (this) {
            checkObsolete();

            GridCacheMvcc<K> mvcc = mvccExtras();

            return mvcc == null ? null : loc ? mvcc.localCandidate(threadId) :
                mvcc.remoteCandidate(nodeId, threadId);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized GridCacheMvccCandidate<K> localOwner() throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc<K> mvcc = mvccExtras();

        return mvcc == null ? null : mvcc.localOwner();
    }

    /** {@inheritDoc} */
    @Override public synchronized long rawExpireTime() {
        return expireTimeExtras();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional"})
    @Override public long expireTime() throws GridCacheEntryRemovedException {
        GridCacheTxLocalAdapter<K, V> tx;

        if (cctx.isDht())
            tx = cctx.dht().near().context().tm().localTx();
        else
            tx = cctx.tm().localTx();

        if (tx != null) {
            long time = tx.entryExpireTime(key);

            if (time > 0)
                return time;
        }

        synchronized (this) {
            checkObsolete();

            return expireTimeExtras();
        }
    }

    /** {@inheritDoc} */
    @Override public long expireTimeUnlocked() {
        assert Thread.holdsLock(this);

        return expireTimeExtras();
    }

    /** {@inheritDoc} */
    @Override public boolean onTtlExpired(GridCacheVersion obsoleteVer) {
        boolean obsolete = false;
        boolean deferred = false;

        try {
            synchronized (this) {
                V expiredVal = val;

                boolean hasOldBytes = valBytes != null || valPtr != 0;

                boolean expired = checkExpired();

                if (expired) {
                    if (cctx.deferredDelete() && !detached() && !isInternal()) {
                        if (!deletedUnlocked()) {
                            update(null, null, 0L, 0L, ver);

                            deletedUnlocked(true);

                            deferred = true;
                        }
                    }
                    else {
                        if (markObsolete0(obsoleteVer, true))
                            obsolete = true; // Success, will return "true".
                    }

                    if (cctx.events().isRecordable(EVT_CACHE_OBJECT_EXPIRED))
                        cctx.events().addEvent(partition(), key, cctx.localNodeId(), null, EVT_CACHE_OBJECT_EXPIRED,
                            null, false, expiredVal, expiredVal != null || hasOldBytes, null, null, null);
                }


            }
        }
        catch (GridException e) {
            U.error(log, "Failed to clean up expired cache entry: " + this, e);
        }
        finally {
            if (obsolete)
                onMarkedObsolete();

            if (deferred)
                cctx.onDeferredDelete(this, obsoleteVer);
        }

        return obsolete;
    }

    /** {@inheritDoc} */
    @Override public synchronized long rawTtl() {
        return ttlExtras();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional"})
    @Override public long ttl() throws GridCacheEntryRemovedException {
        GridCacheTxLocalAdapter<K, V> tx;

        if (cctx.isDht())
            tx = cctx.dht().near().context().tm().localTx();
        else
            tx = cctx.tm().localTx();

        if (tx != null) {
            long entryTtl = tx.entryTtl(key);

            if (entryTtl > 0)
                return entryTtl;
        }

        synchronized (this) {
            checkObsolete();

            return ttlExtras();
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void keyBytes(byte[] keyBytes) throws GridCacheEntryRemovedException {
        checkObsolete();

        if (keyBytes != null)
            this.keyBytes = keyBytes;
    }

    /** {@inheritDoc} */
    @Override public synchronized byte[] keyBytes() {
        return keyBytes;
    }

    /** {@inheritDoc} */
    @Override public byte[] getOrMarshalKeyBytes() throws GridException {
        byte[] bytes = keyBytes();

        if (bytes != null)
            return bytes;

        bytes = CU.marshal(cctx, key);

        synchronized (this) {
            keyBytes = bytes;
        }

        return bytes;
    }

    /** {@inheritDoc} */
    @Override public synchronized GridCacheValueBytes valueBytes() throws GridCacheEntryRemovedException {
        checkObsolete();

        return valueBytesUnlocked();
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheValueBytes valueBytes(@Nullable GridCacheVersion ver)
        throws GridException, GridCacheEntryRemovedException {
        V val = null;
        GridCacheValueBytes valBytes = GridCacheValueBytes.nil();

        synchronized (this) {
            checkObsolete();

            if (ver == null || this.ver.equals(ver)) {
                val = this.val;
                ver = this.ver;
                valBytes = valueBytesUnlocked();
            }
            else
                ver = null;
        }

        if (valBytes.isNull()) {
            if (val != null)
                valBytes = (val instanceof byte[]) ? GridCacheValueBytes.plain(val) :
                    GridCacheValueBytes.marshaled(CU.marshal(cctx, val));

            if (ver != null && !isOffHeapValuesOnly()) {
                synchronized (this) {
                    checkObsolete();

                    if (this.val == val)
                        this.valBytes = isStoreValueBytes() ? valBytes.getIfMarshaled() : null;
                }
            }
        }

        return valBytes;
    }

    /**
     * Updates cache index.
     *
     * @param val Value.
     * @param valBytes Value bytes.
     * @param expireTime Expire time.
     * @param ver New entry version.
     * @param prevVal Previous value.
     * @throws GridException If update failed.
     */
    protected void updateIndex(@Nullable V val, @Nullable byte[] valBytes, long expireTime, GridCacheVersion ver,
        @Nullable V prevVal) throws GridException {
        assert Thread.holdsLock(this);
        assert val != null || valBytes != null : "null values in update index for key: " + key;

        try {
            GridCacheQueryManager<K, V> qryMgr = cctx.queries();

            if (qryMgr != null)
                qryMgr.store(key, keyBytes, val, valBytes, ver, expireTime);
        }
        catch (GridException e) {
            throw new GridCacheIndexUpdateException(e);
        }
    }

    /**
     * Clears index.
     *
     * @param prevVal Previous value (if needed for index update).
     * @throws GridException If failed.
     */
    protected void clearIndex(@Nullable V prevVal) throws GridException {
        assert Thread.holdsLock(this);

        try {
            GridCacheQueryManager<K, V> qryMgr = cctx.queries();

            if (qryMgr != null)
                qryMgr.remove(key(), keyBytes());
        }
        catch (GridException e) {
            throw new GridCacheIndexUpdateException(e);
        }
    }

    /**
     * This method will return current value only if clearIndex(V) will require previous value (this is the case
     * for Mongo caches). If previous value is not required, this method will return {@code null}.
     *
     * @return Previous value or {@code null}.
     * @throws GridException If failed to retrieve previous value.
     */
    protected V saveValueForIndexUnlocked() throws GridException {
        assert Thread.holdsLock(this);

        if (!cctx.cache().isMongoDataCache() && !cctx.cache().isMongoMetaCache())
            return null;

        return rawGetOrUnmarshalUnlocked();
    }

    /**
     * Wraps this map entry into cache entry.
     *
     * @param prjAware {@code true} if entry should inherit projection properties.
     * @return Wrapped entry.
     */
    @Override public GridCacheEntry<K, V> wrap(boolean prjAware) {
        GridCacheProjectionImpl<K, V> prjPerCall = null;

        if (prjAware)
            prjPerCall = cctx.projectionPerCall();

        return new GridCacheEntryImpl<>(prjPerCall, cctx, key, this);
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntry<K, V> wrapFilterLocked() throws GridException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntry<K, V> evictWrap() {
        return new GridCacheEvictionEntry<>(this);
    }

    /** {@inheritDoc} */
    @Override public boolean evictInternal(boolean swap, GridCacheVersion obsoleteVer,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        boolean marked = false;

        try {
            if (F.isEmptyOrNulls(filter)) {
                synchronized (this) {
                    V prev = saveValueForIndexUnlocked();

                    if (!hasReaders() && markObsolete0(obsoleteVer, false)) {
                        if (swap) {
                            if (!isStartVersion()) {
                                try {
                                    // Write to swap.
                                    swap();
                                }
                                catch (GridException e) {
                                    U.error(log, "Failed to write entry to swap storage: " + this, e);
                                }
                            }
                        }
                        else
                            clearIndex(prev);

                        // Nullify value after swap.
                        value(null, null);

                        marked = true;

                        return true;
                    }
                }
            }
            else {
                // For optimistic check.
                while (true) {
                    GridCacheVersion v;

                    synchronized (this) {
                        v = ver;
                    }

                    if (!cctx.isAll(this, filter))
                        return false;

                    synchronized (this) {
                        if (!v.equals(ver))
                            // Version has changed since entry passed the filter. Do it again.
                            continue;

                        V prevVal = saveValueForIndexUnlocked();

                        if (!hasReaders() && markObsolete0(obsoleteVer, false)) {
                            if (swap) {
                                if (!isStartVersion()) {
                                    try {
                                        // Write to swap.
                                        swap();
                                    }
                                    catch (GridException e) {
                                        U.error(log, "Failed to write entry to swap storage: " + this, e);
                                    }
                                }
                            }
                            else
                                clearIndex(prevVal);

                            // Nullify value after swap.
                            value(null, null);

                            marked = true;

                            return true;
                        }
                        else
                            return false;
                    }
                }
            }
        }
        catch (GridCacheEntryRemovedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Got removed entry when evicting (will simply return): " + this);

            return true;
        }
        finally {
            if (marked)
                onMarkedObsolete();
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public GridCacheBatchSwapEntry<K, V> evictInBatchInternal(GridCacheVersion obsoleteVer)
        throws GridException {
        assert Thread.holdsLock(this);
        assert cctx.isSwapOrOffheapEnabled();

        GridCacheBatchSwapEntry<K, V> ret = null;

        try {
            if (!hasReaders() && markObsolete0(obsoleteVer, false)) {
                if (!isStartVersion()) {
                    V val = rawGetOrUnmarshalUnlocked();

                    GridCacheValueBytes valBytes = valueBytesUnlocked();

                    if (valBytes.isNull())
                        valBytes = createValueBytes(val);

                    GridUuid valClsLdrId = null;

                    if (val != null)
                        valClsLdrId = cctx.deploy().getClassLoaderId(U.detectObjectClassLoader(val));

                    ret = new GridCacheBatchSwapEntry<>(key(), getOrMarshalKeyBytes(), hash, partition(),
                        valBytes.get(), valBytes.isPlain(), ver, ttlExtras(), expireTimeExtras(),
                        cctx.deploy().getClassLoaderId(U.detectObjectClassLoader(key)), valClsLdrId);
                }

                value(null, null);
            }
        }
        catch (GridCacheEntryRemovedException ignored) {
            if (log.isDebugEnabled())
                log.debug("Got removed entry when evicting (will simply return): " + this);
        }

        return ret;
    }

    /**
     * Create value bytes wrapper from the given object.
     *
     * @param val Value.
     * @return Value bytes wrapper.
     * @throws GridException If failed.
     */
    protected GridCacheValueBytes createValueBytes(@Nullable V val) throws GridException {
        return (val != null && val instanceof byte[]) ? GridCacheValueBytes.plain(val) :
            GridCacheValueBytes.marshaled(CU.marshal(cctx, val));
    }

    /**
     * @param filter Entry filter.
     * @return {@code True} if entry is visitable.
     */
    public boolean visitable(GridPredicate<GridCacheEntry<K, V>>[] filter) {
        try {
            if (obsoleteOrDeleted() || (filter != CU.<K, V>empty() && !cctx.isAll(wrap(false), filter)))
                return false;
        }
        catch (GridException e) {
            U.error(log, "An exception was thrown while filter checking.", e);

            RuntimeException ex = e.getCause(RuntimeException.class);

            if (ex != null)
                throw ex;

            Error err = e.getCause(Error.class);

            if (err != null)
                throw err;

            return false;
        }

        GridCacheTxEx<K, V> tx = cctx.tm().localTxx();

        return tx == null || !tx.removed(key);
    }

    /**
     * Ensures that internal data storage is created.
     *
     * @param size Amount of data to ensure.
     * @return {@code true} if data storage was created.
     */
    private boolean ensureData(int size) {
        if (attributeDataExtras() == null) {
            attributeDataExtras(new GridLeanMap<String, Object>(size));

            return true;
        }
        else
            return false;
    }

    /** {@inheritDoc} */
    @Override public void copyMeta(GridMetadataAware from) {
        A.notNull(from, "from");

        synchronized (this) {
            Map m = from.allMeta();

            ensureData(m.size());

            attributeDataExtras().putAll(from.allMeta());
        }
    }

    /** {@inheritDoc} */
    @Override public void copyMeta(Map<String, ?> attrData) {
        A.notNull(attrData, "data");

        synchronized (this) {
            ensureData(attrData.size());

            attributeDataExtras().putAll(attrData);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Nullable @Override public <V1> V1 addMeta(String name, V1 val) {
        A.notNull(name, "name", val, "val");

        synchronized (this) {
            ensureData(1);

            return (V1)attributeDataExtras().put(name, val);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Nullable @Override public <V1> V1 meta(String name) {
        A.notNull(name, "name");

        synchronized (this) {
            GridLeanMap<String, Object> attrData = attributeDataExtras();

            return attrData == null ? null : (V1)attrData.get(name);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Nullable
    @Override public <V1> V1 removeMeta(String name) {
        A.notNull(name, "name");

        synchronized (this) {
            GridLeanMap<String, Object> attrData = attributeDataExtras();

            if (attrData == null)
                return null;

            V1 old = (V1)attrData.remove(name);

            if (attrData.isEmpty())
                attributeDataExtras(null);

            return old;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <V1> boolean removeMeta(String name, V1 val) {
        A.notNull(name, "name", val, "val");

        synchronized (this) {
            GridLeanMap<String, Object> attrData = attributeDataExtras();

            if (attrData == null)
                return false;

            V1 old = (V1)attrData.get(name);

            if (old != null && old.equals(val)) {
                attrData.remove(name);

                if (attrData.isEmpty())
                    attributeDataExtras(null);

                return true;
            }

            return false;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"unchecked", "RedundantCast"})
    @Override public synchronized  <V1> Map<String, V1> allMeta() {
        GridLeanMap<String, Object> attrData = attributeDataExtras();

        if (attrData == null)
            return Collections.emptyMap();

        if (attrData.size() <= 5)
            // This is a singleton unmodifiable map.
            return (Map<String, V1>)attrData;

        // Return a copy.
        return new HashMap<>((Map<String, V1>)attrData);
    }

    /** {@inheritDoc} */
    @Override public boolean hasMeta(String name) {
        return meta(name) != null;
    }

    /** {@inheritDoc} */
    @Override public <V1> boolean hasMeta(String name, V1 val) {
        A.notNull(name, "name");

        Object v = meta(name);

        return v != null && v.equals(val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Nullable @Override public <V1> V1 putMetaIfAbsent(String name, V1 val) {
        A.notNull(name, "name", val, "val");

        synchronized (this) {
            V1 v = meta(name);

            if (v == null)
                return addMeta(name, val);

            return v;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "ClassReferencesSubclass"})
    @Nullable @Override public <V1> V1 putMetaIfAbsent(String name, Callable<V1> c) {
        A.notNull(name, "name", c, "c");

        synchronized (this) {
            V1 v = meta(name);

            if (v == null)
                try {
                    return addMeta(name, c.call());
                }
                catch (Exception e) {
                    throw F.wrap(e);
                }

            return v;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <V1> V1 addMetaIfAbsent(String name, V1 val) {
        A.notNull(name, "name", val, "val");

        synchronized (this) {
            V1 v = meta(name);

            if (v == null)
                addMeta(name, v = val);

            return v;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Nullable @Override public <V1> V1 addMetaIfAbsent(String name, @Nullable Callable<V1> c) {
        A.notNull(name, "name", c, "c");

        synchronized (this) {
            V1 v = meta(name);

            if (v == null && c != null)
                try {
                    addMeta(name, v = c.call());
                }
                catch (Exception e) {
                    throw F.wrap(e);
                }

            return v;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Override public <V1> boolean replaceMeta(String name, V1 curVal, V1 newVal) {
        A.notNull(name, "name", newVal, "newVal", curVal, "curVal");

        synchronized (this) {
            if (hasMeta(name)) {
                V1 val = this.<V1>meta(name);

                if (val != null && val.equals(curVal)) {
                    addMeta(name, newVal);

                    return true;
                }
            }

            return false;
        }
    }

    /**
     * Convenience way for super-classes which implement {@link Externalizable} to
     * serialize metadata. Super-classes must call this method explicitly from
     * within {@link Externalizable#writeExternal(ObjectOutput)} methods implementation.
     *
     * @param out Output to write to.
     * @throws IOException If I/O error occurred.
     */
    @SuppressWarnings({"TooBroadScope"})
    protected void writeExternalMeta(ObjectOutput out) throws IOException {
        Map<String, Object> cp;

        // Avoid code warning (suppressing is bad here, because we need this warning for other places).
        synchronized (this) {
            cp = new GridLeanMap<>(attributeDataExtras());
        }

        out.writeObject(cp);
    }

    /**
     * Convenience way for super-classes which implement {@link Externalizable} to
     * serialize metadata. Super-classes must call this method explicitly from
     * within {@link Externalizable#readExternal(ObjectInput)} methods implementation.
     *
     * @param in Input to read from.
     * @throws IOException If I/O error occurred.
     * @throws ClassNotFoundException If some class could not be found.
     */
    @SuppressWarnings({"unchecked"})
    protected void readExternalMeta(ObjectInput in) throws IOException, ClassNotFoundException {
        GridLeanMap<String, Object> cp = (GridLeanMap<String, Object>)in.readObject();

        synchronized (this) {
            attributeDataExtras(cp);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean deleted() {
        if (!cctx.deferredDelete())
            return false;

        synchronized (this) {
            return deletedUnlocked();
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean obsoleteOrDeleted() {
        return obsoleteVersionExtras() != null ||
            (cctx.deferredDelete() && (deletedUnlocked() || !hasValueUnlocked()));
    }

    /**
     * @return {@code True} if deleted.
     */
    @SuppressWarnings("SimplifiableIfStatement")
    protected boolean deletedUnlocked() {
        assert Thread.holdsLock(this);

        if (!cctx.deferredDelete())
            return false;

        return (flags & IS_DELETED_MASK) != 0;
    }

    /**
     * @param deleted {@code True} if deleted.
     */
    protected void deletedUnlocked(boolean deleted) {
        assert Thread.holdsLock(this);
        assert cctx.deferredDelete();

        if (deleted) {
            assert !deletedUnlocked();

            flags |= IS_DELETED_MASK;

            cctx.decrementPublicSize(this);
        }
        else {
            assert deletedUnlocked();

            flags &= ~IS_DELETED_MASK;

            cctx.incrementPublicSize(this);
        }
    }

    /**
     * @return {@code True} if refreshing.
     */
    protected boolean refreshingUnlocked() {
        assert Thread.holdsLock(this);

        return (flags & IS_REFRESHING_MASK) != 0;
    }

    /**
     * @param refreshing {@code True} if refreshing.
     */
    protected void refreshingLocked(boolean refreshing) {
        assert Thread.holdsLock(this);

        if (refreshing)
            flags |= IS_REFRESHING_MASK;
        else
            flags &= ~IS_REFRESHING_MASK;
    }

    /**
     * @return Attribute data.
     */
    @Nullable private GridLeanMap<String, Object> attributeDataExtras() {
        return extras != null ? extras.attributesData() : null;
    }

    /**
     * @param attrData Attribute data.
     */
    private void attributeDataExtras(@Nullable GridLeanMap<String, Object> attrData) {
        extras = (extras != null) ? extras.attributesData(attrData) : attrData != null ?
            new GridCacheAttributesEntryExtras<K>(attrData) : null;
    }

    /**
     * @return MVCC.
     */
    @Nullable protected GridCacheMvcc<K> mvccExtras() {
        return extras != null ? extras.mvcc() : null;
    }

    /**
     * @param mvcc MVCC.
     */
    protected void mvccExtras(@Nullable GridCacheMvcc<K> mvcc) {
        extras = (extras != null) ? extras.mvcc(mvcc) : mvcc != null ? new GridCacheMvccEntryExtras<>(mvcc) : null;
    }

    /**
     * @return Obsolete version.
     */
    @Nullable protected GridCacheVersion obsoleteVersionExtras() {
        return extras != null ? extras.obsoleteVersion() : null;
    }

    /**
     * @param obsoleteVer Obsolete version.
     */
    protected void obsoleteVersionExtras(@Nullable GridCacheVersion obsoleteVer) {
        extras = (extras != null) ? extras.obsoleteVersion(obsoleteVer) : obsoleteVer != null ?
            new GridCacheObsoleteEntryExtras<K>(obsoleteVer) : null;
    }

    /**
     * @return TTL.
     */
    protected long ttlExtras() {
        return extras != null ? extras.ttl() : 0;
    }

    /**
     * @return Expire time.
     */
    protected long expireTimeExtras() {
        return extras != null ? extras.expireTime() : 0L;
    }

    /**
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    protected void ttlAndExpireTimeExtras(long ttl, long expireTime) {
        extras = (extras != null) ? extras.ttlAndExpireTime(ttl, expireTime) : ttl != 0 ?
            new GridCacheTtlEntryExtras<K>(ttl, expireTime) : null;
    }

    /**
     * @return Size of extras object.
     */
    private int extrasSize() {
        return extras != null ? extras.size() : 0;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        // Identity comparison left on purpose.
        return o == this;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return hash;
    }

    /** {@inheritDoc} */
    @Override public synchronized String toString() {
        return S.toString(GridCacheMapEntry.class, this);
    }
}

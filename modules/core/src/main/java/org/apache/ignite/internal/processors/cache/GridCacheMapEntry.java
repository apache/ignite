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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.pagemem.wal.StorageException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheUpdateAtomicResult.UpdateOutcome;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicAbstractUpdateFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheEntry;
import org.apache.ignite.internal.processors.cache.extras.GridCacheEntryExtras;
import org.apache.ignite.internal.processors.cache.extras.GridCacheMvccEntryExtras;
import org.apache.ignite.internal.processors.cache.extras.GridCacheObsoleteEntryExtras;
import org.apache.ignite.internal.processors.cache.extras.GridCacheTtlEntryExtras;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryListener;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.version.GridCacheLazyPlainVersionedEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheFilter;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.GridMetadataAwareAdapter;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_EXPIRED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_LOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_UNLOCKED;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;

/**
 * Adapter for cache entry.
 */
@SuppressWarnings({"TooBroadScope"})
public abstract class GridCacheMapEntry extends GridMetadataAwareAdapter implements GridCacheEntryEx {
    /** */
    private static final byte IS_DELETED_MASK = 0x01;

    /** */
    private static final byte IS_UNSWAPPED_MASK = 0x02;

    /** */
    private static final byte IS_EVICT_DISABLED = 0x04;

    /** */
    public static final GridCacheAtomicVersionComparator ATOMIC_VER_COMPARATOR = new GridCacheAtomicVersionComparator();

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
    // 7 * 8 /*references*/  + 2 * 8 /*long*/  + 1 * 4 /*int*/ + 1 * 1 /*byte*/ + array at parent = 85
    private static final int SIZE_OVERHEAD = 85 /*entry*/ + 32 /* version */ + 4 * 7 /* key + val */;

    /** Static logger to avoid re-creation. Made static for test purpose. */
    protected static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static volatile IgniteLogger log;

    /** Cache registry. */
    @GridToStringExclude
    protected final GridCacheContext<?, ?> cctx;

    /** Key. */
    @GridToStringInclude
    protected final KeyCacheObject key;

    /** Value. */
    @GridToStringInclude
    protected CacheObject val;

    /** Version. */
    @GridToStringInclude
    protected GridCacheVersion ver;

    /** Key hash code. */
    @GridToStringInclude
    private final int hash;

    /** Extras */
    @GridToStringInclude
    private GridCacheEntryExtras extras;

    /** */
    @GridToStringExclude
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Flags:
     * <ul>
     *     <li>Deleted flag - mask {@link #IS_DELETED_MASK}</li>
     *     <li>Unswapped flag - mask {@link #IS_UNSWAPPED_MASK}</li>
     * </ul>
     */
    @GridToStringInclude
    protected byte flags;

    /**
     * @param cctx Cache context.
     * @param key Cache key.
     */
    protected GridCacheMapEntry(
        GridCacheContext<?, ?> cctx,
        KeyCacheObject key
    ) {
        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridCacheMapEntry.class);

        key = (KeyCacheObject)cctx.kernalContext().cacheObjects().prepareForCache(key, cctx);

        assert key != null;

        this.key = key;
        this.hash = key.hashCode();
        this.cctx = cctx;

        ver = GridCacheVersionManager.START_VER;
    }

    /**
     * Sets entry value. If off-heap value storage is enabled, will serialize value to off-heap.
     *
     * @param val Value to store.
     */
    protected void value(@Nullable CacheObject val) {
        assert lock.isHeldByCurrentThread();

        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public int memorySize() throws IgniteCheckedException {
        byte[] kb;
        byte[] vb = null;

        int extrasSize;

        lockEntry();

        try {
            key.prepareMarshal(cctx.cacheObjectContext());

            kb = key.valueBytes(cctx.cacheObjectContext());

            if (val != null) {
                val.prepareMarshal(cctx.cacheObjectContext());

                vb = val.valueBytes(cctx.cacheObjectContext());
            }

            extrasSize = extrasSize();
        }
        finally {
            unlockEntry();
        }

        return SIZE_OVERHEAD + extrasSize + kb.length + (vb == null ? 1 : vb.length);
    }

    /** {@inheritDoc} */
    @Override public boolean isInternal() {
        return key.internal();
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
    @Override public <K, V> GridCacheContext<K, V> context() {
        return (GridCacheContext<K, V>)cctx;
    }

    /** {@inheritDoc} */
    @Override public boolean isNew() throws GridCacheEntryRemovedException {
        assert lock.isHeldByCurrentThread();

        checkObsolete();

        return isStartVersion();
    }

    /** {@inheritDoc} */
    @Override public boolean isNewLocked() throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            return isStartVersion();
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * @return {@code True} if start version.
     */
    public boolean isStartVersion() {
        return ver == GridCacheVersionManager.START_VER;
    }

    /** {@inheritDoc} */
    @Override public boolean valid(AffinityTopologyVersion topVer) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return 0;
    }

    /**
     * @return Local partition that owns this entry.
     */
    protected GridDhtLocalPartition localPartition() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean partitionValid() {
        return true;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheEntryInfo info() {
        GridCacheEntryInfo info = null;

        lockEntry();

        try {
            if (!obsolete()) {
                info = new GridCacheEntryInfo();

                info.key(key);
                info.cacheId(cctx.cacheId());

                long expireTime = expireTimeExtras();

                boolean expired = expireTime != 0 && expireTime <= U.currentTimeMillis();

                info.ttl(ttlExtras());
                info.expireTime(expireTime);
                info.version(ver);
                info.setNew(isStartVersion());
                info.setDeleted(deletedUnlocked());

                if (!expired)
                    info.value(val);
            }
        }
        finally {
            unlockEntry();
        }

        return info;
    }

    /** {@inheritDoc} */
    @Override public final CacheObject unswap() throws IgniteCheckedException, GridCacheEntryRemovedException {
        return unswap(true);
    }

    /** {@inheritDoc} */
    @Override public final CacheObject unswap(CacheDataRow row) throws IgniteCheckedException, GridCacheEntryRemovedException {
        row = unswap(row, true);

        return row != null ? row.value() : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public final CacheObject unswap(boolean needVal)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        CacheDataRow row = unswap(null, true);

        return row != null ? row.value() : null;
    }

    /**
     * Unswaps an entry.
     *
     * @param row Already extracted cache data.
     * @param checkExpire If {@code true} checks for expiration, as result entry can be obsoleted or marked deleted.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    @Nullable protected CacheDataRow unswap(@Nullable CacheDataRow row, boolean checkExpire)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        boolean obsolete = false;
        boolean deferred = false;
        GridCacheVersion ver0 = null;

        lockEntry();

        try {
            checkObsolete();

            if (isStartVersion() && ((flags & IS_UNSWAPPED_MASK) == 0)) {
                assert row == null || row.key() == key : "Unexpected row key";

                CacheDataRow read = row == null ? cctx.offheap().read(this) : row;

                flags |= IS_UNSWAPPED_MASK;

                if (read != null) {
                    CacheObject val = read.value();

                    update(val, read.expireTime(), 0, read.version(), false);

                    long delta = checkExpire && read.expireTime() > 0 ? read.expireTime() - U.currentTimeMillis() : 0;

                    if (delta >= 0)
                        return read;
                    else {
                        if (onExpired(this.val, null)) {
                            if (cctx.deferredDelete()) {
                                deferred = true;
                                ver0 = ver;
                            }
                            else
                                obsolete = true;
                        }
                    }
                }
            }
        }
        finally {
            unlockEntry();
        }

        if (obsolete) {
            onMarkedObsolete();

            cctx.cache().removeEntry(this);
        }

        if (deferred) {
            assert ver0 != null;

            cctx.onDeferredDelete(this, ver0);
        }

        return null;
    }

    /**
     * @return Value bytes and flag indicating whether value is byte array.
     */
    protected IgniteBiTuple<byte[], Byte> valueBytes0() {
        assert lock.isHeldByCurrentThread();

        assert val != null;

        try {
            byte[] bytes = val.valueBytes(cctx.cacheObjectContext());

            return new IgniteBiTuple<>(bytes, val.cacheObjectType());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param tx Transaction.
     * @param key Key.
     * @param reload flag.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @return Read value.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Nullable protected Object readThrough(@Nullable IgniteInternalTx tx, KeyCacheObject key, boolean reload, UUID subjId,
        String taskName) throws IgniteCheckedException {
        return cctx.store().load(tx, key);
    }

    /** {@inheritDoc} */
    @Nullable @Override public final CacheObject innerGet(
        @Nullable GridCacheVersion ver,
        @Nullable IgniteInternalTx tx,
        boolean readThrough,
        boolean updateMetrics,
        boolean evt,
        UUID subjId,
        Object transformClo,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expirePlc,
        boolean keepBinary)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        return (CacheObject)innerGet0(
            ver,
            tx,
            readThrough,
            evt,
            updateMetrics,
            subjId,
            transformClo,
            taskName,
            expirePlc,
            false,
            keepBinary,
            false,
            null);
    }

    /** {@inheritDoc} */
    @Override public EntryGetResult innerGetAndReserveForLoad(boolean updateMetrics,
        boolean evt,
        UUID subjId,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean keepBinary,
        @Nullable ReaderArguments readerArgs) throws IgniteCheckedException, GridCacheEntryRemovedException {
        return (EntryGetResult)innerGet0(
            /*ver*/null,
            /*tx*/null,
            /*readThrough*/false,
            evt,
            updateMetrics,
            subjId,
            /*transformClo*/null,
            taskName,
            expiryPlc,
            true,
            keepBinary,
            /*reserve*/true,
            readerArgs);
    }

    /** {@inheritDoc} */
    @Override public EntryGetResult innerGetVersioned(
        @Nullable GridCacheVersion ver,
        IgniteInternalTx tx,
        boolean updateMetrics,
        boolean evt,
        UUID subjId,
        Object transformClo,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean keepBinary,
        @Nullable ReaderArguments readerArgs)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        return (EntryGetResult)innerGet0(
            ver,
            tx,
            false,
            evt,
            updateMetrics,
            subjId,
            transformClo,
            taskName,
            expiryPlc,
            true,
            keepBinary,
            false,
            readerArgs);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "RedundantTypeArguments", "TooBroadScope"})
    private Object innerGet0(
        GridCacheVersion nextVer,
        IgniteInternalTx tx,
        boolean readThrough,
        boolean evt,
        boolean updateMetrics,
        UUID subjId,
        Object transformClo,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean retVer,
        boolean keepBinary,
        boolean reserveForLoad,
        @Nullable ReaderArguments readerArgs
    ) throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert !(retVer && readThrough);
        assert !(reserveForLoad && readThrough);

        // Disable read-through if there is no store.
        if (readThrough && !cctx.readThrough())
            readThrough = false;

        GridCacheVersion startVer;
        GridCacheVersion resVer = null;

        boolean obsolete = false;
        boolean deferred = false;
        GridCacheVersion ver0 = null;

        Object res = null;

        lockEntry();

        try {
            checkObsolete();

            boolean valid = valid(tx != null ? tx.topologyVersion() : cctx.affinity().affinityTopologyVersion());

            CacheObject val;

            if (valid) {
                val = this.val;

                if (val == null) {
                    if (isStartVersion()) {
                        unswap(null, false);

                        val = this.val;
                    }
                }

                if (val != null) {
                    long expireTime = expireTimeExtras();

                    if (expireTime > 0 && (expireTime < U.currentTimeMillis())) {
                        if (onExpired((CacheObject)cctx.unwrapTemporary(val), null)) {
                            val = null;
                            evt = false;

                            if (cctx.deferredDelete()) {
                                deferred = true;
                                ver0 = ver;
                            }
                            else
                                obsolete = true;
                        }
                    }
                }
            }
            else
                val = null;

            CacheObject ret = val;

            if (ret == null) {
                if (updateMetrics && cctx.statisticsEnabled())
                    cctx.cache().metrics0().onRead(false);
            }
            else {
                if (updateMetrics && cctx.statisticsEnabled())
                    cctx.cache().metrics0().onRead(true);
            }

            if (evt && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ)) {
                transformClo = EntryProcessorResourceInjectorProxy.unwrap(transformClo);

                GridCacheMvcc mvcc = mvccExtras();

                cctx.events().addEvent(
                    partition(),
                    key,
                    tx,
                    mvcc != null ? mvcc.anyOwner() : null,
                    EVT_CACHE_OBJECT_READ,
                    ret,
                    ret != null,
                    ret,
                    ret != null,
                    subjId,
                    transformClo != null ? transformClo.getClass().getName() : null,
                    taskName,
                    keepBinary);

                // No more notifications.
                evt = false;
            }

            if (ret != null && expiryPlc != null)
                updateTtl(expiryPlc);

            if (retVer) {
                resVer = (isNear() && cctx.transactional()) ? ((GridNearCacheEntry)this).dhtVersion() : this.ver;

                if (resVer == null)
                    ret = null;
            }

            // Cache version for optimistic check.
            startVer = ver;

            addReaderIfNeed(readerArgs);

            if (ret != null) {
                assert !obsolete;
                assert !deferred;

                // If return value is consistent, then done.
                res = retVer ? entryGetResult(ret, resVer, false) : ret;
            }
            else if (reserveForLoad && !obsolete) {
                assert !readThrough;
                assert retVer;

                boolean reserve = !evictionDisabled();

                if (reserve)
                    flags |= IS_EVICT_DISABLED;

                res = entryGetResult(null, resVer, reserve);
            }
        }
        finally {
            unlockEntry();
        }

        if (obsolete) {
            onMarkedObsolete();

            throw new GridCacheEntryRemovedException();
        }

        if (deferred)
            cctx.onDeferredDelete(this, ver0);

        if (res != null)
            return res;

        CacheObject ret = null;

        if (readThrough) {
            IgniteInternalTx tx0 = null;

            if (tx != null && tx.local()) {
                if (cctx.isReplicated() || cctx.isColocated() || tx.near())
                    tx0 = tx;
                else if (tx.dht()) {
                    GridCacheVersion ver = tx.nearXidVersion();

                    tx0 = cctx.dht().near().context().tm().tx(ver);
                }
            }

            Object storeVal = readThrough(tx0, key, false, subjId, taskName);

            ret = cctx.toCacheObject(storeVal);
        }

        if (ret == null && !evt)
            return null;

        lockEntry();

        try {
            long ttl = ttlExtras();

            // If version matched, set value.
            if (startVer.equals(ver)) {
                if (ret != null) {
                    // Detach value before index update.
                    ret = cctx.kernalContext().cacheObjects().prepareForCache(ret, cctx);

                    nextVer = nextVer != null ? nextVer : nextVersion();

                    long expTime = CU.toExpireTime(ttl);

                    // Update indexes before actual write to entry.
                    storeValue(ret, expTime, nextVer);

                    update(ret, expTime, ttl, nextVer, true);

                    if (cctx.deferredDelete() && deletedUnlocked() && !isInternal() && !detached())
                        deletedUnlocked(false);

                    assert readerArgs == null;
                }

                if (evt && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ)) {
                    transformClo = EntryProcessorResourceInjectorProxy.unwrap(transformClo);

                    GridCacheMvcc mvcc = mvccExtras();

                    cctx.events().addEvent(
                        partition(),
                        key,
                        tx,
                        mvcc != null ? mvcc.anyOwner() : null,
                        EVT_CACHE_OBJECT_READ,
                        ret,
                        ret != null,
                        null,
                        false,
                        subjId,
                        transformClo != null ? transformClo.getClass().getName() : null,
                        taskName,
                        keepBinary);
                }
            }
        }
        finally {
            unlockEntry();
        }

        assert ret == null || !retVer;

        return ret;
    }

    /**
     * Creates EntryGetResult or EntryGetWithTtlResult if expire time information exists.
     *
     * @param val Value.
     * @param ver Version.
     * @param reserve Reserve flag.
     * @return EntryGetResult.
     */
    private EntryGetResult entryGetResult(CacheObject val, GridCacheVersion ver, boolean reserve) {
        return extras == null || extras.expireTime() == 0
            ? new EntryGetResult(val, ver, reserve)
            : new EntryGetWithTtlResult(val, ver, reserve, rawExpireTime(), rawTtl());
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "TooBroadScope"})
    @Nullable @Override public final CacheObject innerReload()
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        CU.checkStore(cctx);

        GridCacheVersion startVer;

        boolean wasNew;

        lockEntry();

        try {
            checkObsolete();

            // Cache version for optimistic check.
            startVer = ver;

            wasNew = isNew();
        }
        finally {
            unlockEntry();
        }

        String taskName = cctx.kernalContext().job().currentTaskName();

        // Check before load.
        CacheObject ret = cctx.toCacheObject(readThrough(null, key, true, cctx.localNodeId(), taskName));

        boolean touch = false;

        try {
            ensureFreeSpace();

            lockEntry();

            try {
                long ttl = ttlExtras();

                // Generate new version.
                GridCacheVersion nextVer = cctx.versions().nextForLoad(ver);

                // If entry was loaded during read step.
                if (wasNew && !isNew())
                    // Map size was updated on entry creation.
                    return ret;

                // If version matched, set value.
                if (startVer.equals(ver)) {
                    long expTime = CU.toExpireTime(ttl);

                    // Detach value before index update.
                    ret = cctx.kernalContext().cacheObjects().prepareForCache(ret, cctx);

                    // Update indexes.
                    if (ret != null) {
                        storeValue(ret, expTime, nextVer);

                        if (cctx.deferredDelete() && !isInternal() && !detached() && deletedUnlocked())
                            deletedUnlocked(false);
                    }
                    else {
                        removeValue();

                        if (cctx.deferredDelete() && !isInternal() && !detached() && !deletedUnlocked())
                            deletedUnlocked(true);
                    }

                    update(ret, expTime, ttl, nextVer, true);

                    touch = true;

                    // If value was set - return, otherwise try again.
                    return ret;
                }
            }
            finally {
                unlockEntry();
            }

            touch = true;

            return ret;
        }
        finally {
            if (touch)
                cctx.evicts().touch(this, cctx.affinity().affinityTopologyVersion());
        }
    }

    /**
     * @param nodeId Node ID.
     */
    protected void recordNodeId(UUID nodeId, AffinityTopologyVersion topVer) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public final GridCacheUpdateTxResult innerSet(
        @Nullable IgniteInternalTx tx,
        UUID evtNodeId,
        UUID affNodeId,
        CacheObject val,
        boolean writeThrough,
        boolean retval,
        long ttl,
        boolean evt,
        boolean metrics,
        boolean keepBinary,
        boolean oldValPresent,
        @Nullable CacheObject oldVal,
        AffinityTopologyVersion topVer,
        CacheEntryPredicate[] filter,
        GridDrType drType,
        long drExpireTime,
        @Nullable GridCacheVersion explicitVer,
        @Nullable UUID subjId,
        String taskName,
        @Nullable GridCacheVersion dhtVer,
        @Nullable Long updateCntr
    ) throws IgniteCheckedException, GridCacheEntryRemovedException {
        CacheObject old;

        boolean valid = valid(tx != null ? tx.topologyVersion() : topVer);

        // Lock should be held by now.
        if (!cctx.isAll(this, filter))
            return new GridCacheUpdateTxResult(false, null, null);

        final GridCacheVersion newVer;

        boolean intercept = cctx.config().getInterceptor() != null;

        Object key0 = null;
        Object val0 = null;
        WALPointer logPtr = null;

        long updateCntr0;

        ensureFreeSpace();

        lockEntry();

        try {
            checkObsolete();

            if (isNear()) {
                assert dhtVer != null;

                // It is possible that 'get' could load more recent value.
                if (!((GridNearCacheEntry)this).recordDhtVersion(dhtVer))
                    return new GridCacheUpdateTxResult(false, null, logPtr);
            }

            assert tx == null || (!tx.local() && tx.onePhaseCommit()) || tx.ownsLock(this) :
                "Transaction does not own lock for update [entry=" + this + ", tx=" + tx + ']';

            // Load and remove from swap if it is new.
            boolean startVer = isStartVersion();

            boolean internal = isInternal() || !context().userCache();

            Map<UUID, CacheContinuousQueryListener> lsnrCol =
                notifyContinuousQueries(tx) ? cctx.continuousQueries().updateListeners(internal, false) : null;

            if (startVer && (retval || intercept || lsnrCol != null))
                unswap(retval);

            newVer = explicitVer != null ? explicitVer : tx == null ?
                nextVersion() : tx.writeVersion();

            assert newVer != null : "Failed to get write version for tx: " + tx;

            old = oldValPresent ? oldVal : this.val;

            if (intercept) {
                val0 = cctx.unwrapBinaryIfNeeded(val, keepBinary, false);

                CacheLazyEntry e = new CacheLazyEntry(cctx, key, old, keepBinary);

                Object interceptorVal = cctx.config().getInterceptor().onBeforePut(
                    new CacheLazyEntry(cctx, key, old, keepBinary),
                    val0);

                key0 = e.key();

                if (interceptorVal == null)
                    return new GridCacheUpdateTxResult(false, (CacheObject)cctx.unwrapTemporary(old), logPtr);
                else if (interceptorVal != val0)
                    val0 = cctx.unwrapTemporary(interceptorVal);

                val = cctx.toCacheObject(val0);
            }

            // Determine new ttl and expire time.
            long expireTime;

            if (drExpireTime >= 0) {
                assert ttl >= 0 : ttl;

                expireTime = drExpireTime;
            }
            else {
                if (ttl == -1L) {
                    ttl = ttlExtras();
                    expireTime = expireTimeExtras();
                }
                else
                    expireTime = CU.toExpireTime(ttl);
            }

            assert ttl >= 0 : ttl;
            assert expireTime >= 0 : expireTime;

            // Detach value before index update.
            val = cctx.kernalContext().cacheObjects().prepareForCache(val, cctx);

            assert val != null;

            storeValue(val, expireTime, newVer);

            if (cctx.deferredDelete() && deletedUnlocked() && !isInternal() && !detached())
                deletedUnlocked(false);

            updateCntr0 = nextPartitionCounter(topVer, tx == null || tx.local(), updateCntr);

            if (updateCntr != null && updateCntr != 0)
                updateCntr0 = updateCntr;

            if (tx != null && cctx.group().persistenceEnabled() && cctx.group().walEnabled())
                logPtr = logTxUpdate(tx, val, expireTime, updateCntr0);

            update(val, expireTime, ttl, newVer, true);

            drReplicate(drType, val, newVer, topVer);

            recordNodeId(affNodeId, topVer);

            if (metrics && cctx.statisticsEnabled())
                cctx.cache().metrics0().onWrite();

            if (evt && newVer != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_PUT)) {
                CacheObject evtOld = cctx.unwrapTemporary(old);

                cctx.events().addEvent(partition(),
                    key,
                    evtNodeId,
                    tx == null ? null : tx.xid(),
                    newVer,
                    EVT_CACHE_OBJECT_PUT,
                    val,
                    val != null,
                    evtOld,
                    evtOld != null || hasValueUnlocked(),
                    subjId, null, taskName,
                    keepBinary);
            }

            if (lsnrCol != null) {
                cctx.continuousQueries().onEntryUpdated(
                    lsnrCol,
                    key,
                    val,
                    old,
                    internal,
                    partition(),
                    tx.local(),
                    false,
                    updateCntr0,
                    null,
                    topVer);
            }

            cctx.dataStructures().onEntryUpdated(key, false, keepBinary);
        }
        finally {
            unlockEntry();
        }

        onUpdateFinished(updateCntr0);

        if (log.isDebugEnabled())
            log.debug("Updated cache entry [val=" + val + ", old=" + old + ", entry=" + this + ']');

        // Persist outside of synchronization. The correctness of the
        // value will be handled by current transaction.
        if (writeThrough)
            cctx.store().put(tx, key, val, newVer);

        if (intercept)
            cctx.config().getInterceptor().onAfterPut(new CacheLazyEntry(cctx, key, key0, val, val0, keepBinary, updateCntr0));

        return valid ? new GridCacheUpdateTxResult(true, retval ? old : null, updateCntr0, logPtr) :
            new GridCacheUpdateTxResult(false, null, logPtr);
    }

    /**
     * @param cpy Copy flag.
     * @return Key value.
     */
    protected Object keyValue(boolean cpy) {
        return key.value(cctx.cacheObjectContext(), cpy);
    }

    /** {@inheritDoc} */
    @Override public final GridCacheUpdateTxResult innerRemove(
        @Nullable IgniteInternalTx tx,
        UUID evtNodeId,
        UUID affNodeId,
        boolean retval,
        boolean evt,
        boolean metrics,
        boolean keepBinary,
        boolean oldValPresent,
        @Nullable CacheObject oldVal,
        AffinityTopologyVersion topVer,
        CacheEntryPredicate[] filter,
        GridDrType drType,
        @Nullable GridCacheVersion explicitVer,
        @Nullable UUID subjId,
        String taskName,
        @Nullable GridCacheVersion dhtVer,
        @Nullable Long updateCntr
    ) throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert cctx.transactional();

        CacheObject old;

        GridCacheVersion newVer;

        boolean valid = valid(tx != null ? tx.topologyVersion() : topVer);

        // Lock should be held by now.
        if (!cctx.isAll(this, filter))
            return new GridCacheUpdateTxResult(false, null, null);

        GridCacheVersion obsoleteVer = null;

        boolean intercept = cctx.config().getInterceptor() != null;

        IgniteBiTuple<Boolean, Object> interceptRes = null;

        CacheLazyEntry entry0 = null;

        long updateCntr0;

        WALPointer logPtr = null;

        boolean deferred;

        boolean marked = false;

        lockEntry();

        try {
            checkObsolete();

            if (isNear()) {
                assert dhtVer != null;

                // It is possible that 'get' could load more recent value.
                if (!((GridNearCacheEntry)this).recordDhtVersion(dhtVer))
                    return new GridCacheUpdateTxResult(false, null, logPtr);
            }

            assert tx == null || (!tx.local() && tx.onePhaseCommit()) || tx.ownsLock(this) :
                "Transaction does not own lock for remove[entry=" + this + ", tx=" + tx + ']';

            boolean startVer = isStartVersion();

            newVer = explicitVer != null ? explicitVer : tx == null ? nextVersion() : tx.writeVersion();

            boolean internal = isInternal() || !context().userCache();

            Map<UUID, CacheContinuousQueryListener> lsnrCol =
                notifyContinuousQueries(tx) ? cctx.continuousQueries().updateListeners(internal, false) : null;

            if (startVer && (retval || intercept || lsnrCol != null))
                unswap();

            old = oldValPresent ? oldVal : val;

            if (intercept) {
                entry0 = new CacheLazyEntry(cctx, key, old, keepBinary);

                interceptRes = cctx.config().getInterceptor().onBeforeRemove(entry0);

                if (cctx.cancelRemove(interceptRes)) {
                    CacheObject ret = cctx.toCacheObject(cctx.unwrapTemporary(interceptRes.get2()));

                    return new GridCacheUpdateTxResult(false, ret, logPtr);
                }
            }

            removeValue();

            update(null, 0, 0, newVer, true);

            if (cctx.deferredDelete() && !detached() && !isInternal()) {
                if (!deletedUnlocked()) {
                    deletedUnlocked(true);

                    if (tx != null) {
                        GridCacheMvcc mvcc = mvccExtras();

                        if (mvcc == null || mvcc.isEmpty(tx.xidVersion()))
                            clearReaders();
                        else
                            clearReader(tx.originatingNodeId());
                    }
                }
            }

            updateCntr0 = nextPartitionCounter(topVer, tx == null || tx.local(), updateCntr);

            if (updateCntr != null && updateCntr != 0)
                updateCntr0 = updateCntr;

            if (tx != null && cctx.group().persistenceEnabled() && cctx.group().walEnabled())
                logPtr = logTxUpdate(tx, null, 0, updateCntr0);

            drReplicate(drType, null, newVer, topVer);

            if (metrics && cctx.statisticsEnabled())
                cctx.cache().metrics0().onRemove();

            if (tx == null)
                obsoleteVer = newVer;
            else {
                // Only delete entry if the lock is not explicit.
                if (lockedBy(tx.xidVersion()))
                    obsoleteVer = tx.xidVersion();
                else if (log.isDebugEnabled())
                    log.debug("Obsolete version was not set because lock was explicit: " + this);
            }

            if (evt && newVer != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_REMOVED)) {
                CacheObject evtOld = cctx.unwrapTemporary(old);

                cctx.events().addEvent(partition(),
                    key,
                    evtNodeId,
                    tx == null ? null : tx.xid(), newVer,
                    EVT_CACHE_OBJECT_REMOVED,
                    null,
                    false,
                    evtOld,
                    evtOld != null || hasValueUnlocked(),
                    subjId,
                    null,
                    taskName,
                    keepBinary);
            }

            if (lsnrCol != null) {
                cctx.continuousQueries().onEntryUpdated(
                    lsnrCol,
                    key,
                    null,
                    old,
                    internal,
                    partition(),
                    tx.local(),
                    false,
                    updateCntr0,
                    null,
                    topVer);
            }

            cctx.dataStructures().onEntryUpdated(key, true, keepBinary);

            deferred = cctx.deferredDelete() && !detached() && !isInternal();

            if (intercept)
                entry0.updateCounter(updateCntr0);

            if (!deferred) {
                // If entry is still removed.
                assert newVer == ver;

                if (obsoleteVer == null || !(marked = markObsolete0(obsoleteVer, true, null))) {
                    if (log.isDebugEnabled())
                        log.debug("Entry could not be marked obsolete (it is still used): " + this);
                }
                else {
                    recordNodeId(affNodeId, topVer);

                    if (log.isDebugEnabled())
                        log.debug("Entry was marked obsolete: " + this);
                }
            }
        }
        finally {
            unlockEntry();
        }

        if (deferred)
            cctx.onDeferredDelete(this, newVer);

        if (marked) {
            assert !deferred;

            onMarkedObsolete();
        }

        onUpdateFinished(updateCntr0);

        if (intercept)
            cctx.config().getInterceptor().onAfterRemove(entry0);

        if (valid) {
            CacheObject ret;

            if (interceptRes != null)
                ret = cctx.toCacheObject(cctx.unwrapTemporary(interceptRes.get2()));
            else
                ret = old;

            return new GridCacheUpdateTxResult(true, ret, updateCntr0, logPtr);
        }
        else
            return new GridCacheUpdateTxResult(false, null, logPtr);
    }

    /**
     * @param tx Transaction.
     * @return {@code True} if should notify continuous query manager.
     */
    private boolean notifyContinuousQueries(@Nullable IgniteInternalTx tx) {
        return cctx.isLocal() ||
            cctx.isReplicated() ||
            (!isNear() && !(tx != null && tx.onePhaseCommit() && !tx.local()));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridTuple3<Boolean, Object, EntryProcessorResult<Object>> innerUpdateLocal(
        GridCacheVersion ver,
        GridCacheOperation op,
        @Nullable Object writeObj,
        @Nullable Object[] invokeArgs,
        boolean writeThrough,
        boolean readThrough,
        boolean retval,
        boolean keepBinary,
        @Nullable ExpiryPolicy expiryPlc,
        boolean evt,
        boolean metrics,
        @Nullable CacheEntryPredicate[] filter,
        boolean intercept,
        @Nullable UUID subjId,
        String taskName
    ) throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert cctx.isLocal() && cctx.atomic();

        CacheObject old;

        boolean res = true;

        IgniteBiTuple<Boolean, ?> interceptorRes = null;

        EntryProcessorResult<Object> invokeRes = null;

        lockEntry();

        try {
            checkObsolete();

            boolean internal = isInternal() || !context().userCache();

            Map<UUID, CacheContinuousQueryListener> lsnrCol =
                cctx.continuousQueries().updateListeners(internal, false);

            boolean needVal = retval ||
                intercept ||
                op == GridCacheOperation.TRANSFORM ||
                !F.isEmpty(filter) ||
                lsnrCol != null;

            // Load and remove from swap if it is new.
            if (isNew())
                unswap(null, false);

            old = val;

            if (expireTimeExtras() > 0 && expireTimeExtras() < U.currentTimeMillis()) {
                if (onExpired(val, null)) {
                    assert !deletedUnlocked();

                    update(null, CU.TTL_ETERNAL, CU.EXPIRE_TIME_ETERNAL, ver, true);

                    old = null;
                }
            }

            boolean readFromStore = false;

            Object old0 = null;

            if (readThrough && needVal && old == null &&
                (cctx.readThrough() && (op == GridCacheOperation.TRANSFORM || cctx.loadPreviousValue()))) {
                old0 = readThrough(null, key, false, subjId, taskName);

                old = cctx.toCacheObject(old0);

                long ttl = CU.TTL_ETERNAL;
                long expireTime = CU.EXPIRE_TIME_ETERNAL;

                if (expiryPlc != null && old != null) {
                    ttl = CU.toTtl(expiryPlc.getExpiryForCreation());

                    if (ttl == CU.TTL_ZERO) {
                        ttl = CU.TTL_MINIMUM;
                        expireTime = CU.expireTimeInPast();
                    }
                    else if (ttl == CU.TTL_NOT_CHANGED)
                        ttl = CU.TTL_ETERNAL;
                    else
                        expireTime = CU.toExpireTime(ttl);
                }

                // Detach value before index update.
                old = cctx.kernalContext().cacheObjects().prepareForCache(old, cctx);

                if (old != null)
                    storeValue(old, expireTime, ver);
                else
                    removeValue();

                update(old, expireTime, ttl, ver, true);
            }

            // Apply metrics.
            if (metrics && cctx.statisticsEnabled() && needVal)
                cctx.cache().metrics0().onRead(old != null);

            // Check filter inside of synchronization.
            if (!F.isEmpty(filter)) {
                boolean pass = cctx.isAllLocked(this, filter);

                if (!pass) {
                    if (expiryPlc != null && !readFromStore && !cctx.putIfAbsentFilter(filter) && hasValueUnlocked())
                        updateTtl(expiryPlc);

                    Object val = retval ?
                        cctx.cacheObjectContext().unwrapBinaryIfNeeded(CU.value(old, cctx, false), keepBinary, false)
                        : null;

                    return new T3<>(false, val, null);
                }
            }

            String transformCloClsName = null;

            CacheObject updated;

            Object key0 = null;
            Object updated0 = null;

            // Calculate new value.
            if (op == GridCacheOperation.TRANSFORM) {
                transformCloClsName = EntryProcessorResourceInjectorProxy.unwrap(writeObj).getClass().getName();

                EntryProcessor<Object, Object, ?> entryProcessor = (EntryProcessor<Object, Object, ?>)writeObj;

                assert entryProcessor != null;

                CacheInvokeEntry<Object, Object> entry = new CacheInvokeEntry<>(key, old, version(), keepBinary, this);

                try {
                    Object computed = entryProcessor.process(entry, invokeArgs);

                    if (entry.modified()) {
                        updated0 = cctx.unwrapTemporary(entry.getValue());

                        updated = cctx.toCacheObject(updated0);

                        cctx.validateKeyAndValue(key, updated);
                    }
                    else
                        updated = old;

                    key0 = entry.key();

                    invokeRes = computed != null ? CacheInvokeResult.fromResult(cctx.unwrapTemporary(computed)) : null;
                }
                catch (Exception e) {
                    updated = old;

                    invokeRes = CacheInvokeResult.fromError(e);
                }

                if (!entry.modified()) {
                    if (expiryPlc != null && !readFromStore && hasValueUnlocked())
                        updateTtl(expiryPlc);

                    return new GridTuple3<>(false, null, invokeRes);
                }
            }
            else
                updated = (CacheObject)writeObj;

            op = updated == null ? GridCacheOperation.DELETE : GridCacheOperation.UPDATE;

            if (intercept) {
                CacheLazyEntry e;

                if (op == GridCacheOperation.UPDATE) {
                    updated0 = value(updated0, updated, keepBinary, false);

                    e = new CacheLazyEntry(cctx, key, key0, old, old0, keepBinary);

                    Object interceptorVal = cctx.config().getInterceptor().onBeforePut(e, updated0);

                    if (interceptorVal == null)
                        return new GridTuple3<>(false, cctx.unwrapTemporary(value(old0, old, keepBinary, false)), invokeRes);
                    else {
                        updated0 = cctx.unwrapTemporary(interceptorVal);

                        updated = cctx.toCacheObject(updated0);
                    }
                }
                else {
                    e = new CacheLazyEntry(cctx, key, key0, old, old0, keepBinary);

                    interceptorRes = cctx.config().getInterceptor().onBeforeRemove(e);

                    if (cctx.cancelRemove(interceptorRes))
                        return new GridTuple3<>(false, cctx.unwrapTemporary(interceptorRes.get2()), invokeRes);
                }

                key0 = e.key();
                old0 = e.value();
            }

            boolean hadVal = hasValueUnlocked();

            long ttl = CU.TTL_ETERNAL;
            long expireTime = CU.EXPIRE_TIME_ETERNAL;

            if (op == GridCacheOperation.UPDATE) {
                if (expiryPlc != null) {
                    ttl = CU.toTtl(hadVal ? expiryPlc.getExpiryForUpdate() : expiryPlc.getExpiryForCreation());

                    if (ttl == CU.TTL_NOT_CHANGED) {
                        ttl = ttlExtras();
                        expireTime = expireTimeExtras();
                    }
                    else if (ttl != CU.TTL_ZERO)
                        expireTime = CU.toExpireTime(ttl);
                }
                else {
                    ttl = ttlExtras();
                    expireTime = expireTimeExtras();
                }
            }

            if (ttl == CU.TTL_ZERO)
                op = GridCacheOperation.DELETE;

            // Try write-through.
            if (op == GridCacheOperation.UPDATE) {
                // Detach value before index update.
                updated = cctx.kernalContext().cacheObjects().prepareForCache(updated, cctx);

                if (writeThrough)
                    // Must persist inside synchronization in non-tx mode.
                    cctx.store().put(null, key, updated, ver);

                storeValue(updated, expireTime, ver);

                assert ttl != CU.TTL_ZERO;

                update(updated, expireTime, ttl, ver, true);

                if (evt) {
                    CacheObject evtOld = null;

                    if (transformCloClsName != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ)) {
                        evtOld = cctx.unwrapTemporary(old);

                        cctx.events().addEvent(partition(), key, cctx.localNodeId(), null,
                            (GridCacheVersion)null, EVT_CACHE_OBJECT_READ, evtOld, evtOld != null || hadVal, evtOld,
                            evtOld != null || hadVal, subjId, transformCloClsName, taskName, keepBinary);
                    }

                    if (cctx.events().isRecordable(EVT_CACHE_OBJECT_PUT)) {
                        if (evtOld == null)
                            evtOld = cctx.unwrapTemporary(old);

                        cctx.events().addEvent(partition(), key, cctx.localNodeId(), null,
                            (GridCacheVersion)null, EVT_CACHE_OBJECT_PUT, updated, updated != null, evtOld,
                            evtOld != null || hadVal, subjId, null, taskName, keepBinary);
                    }
                }
            }
            else {
                if (writeThrough)
                    // Must persist inside synchronization in non-tx mode.
                    cctx.store().remove(null, key);

                removeValue();

                update(null, CU.TTL_ETERNAL, CU.EXPIRE_TIME_ETERNAL, ver, true);

                if (evt) {
                    CacheObject evtOld = null;

                    if (transformCloClsName != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ))
                        cctx.events().addEvent(partition(), key, cctx.localNodeId(), null,
                            (GridCacheVersion)null, EVT_CACHE_OBJECT_READ, evtOld, evtOld != null || hadVal, evtOld,
                            evtOld != null || hadVal, subjId, transformCloClsName, taskName, keepBinary);

                    if (cctx.events().isRecordable(EVT_CACHE_OBJECT_REMOVED)) {
                        if (evtOld == null)
                            evtOld = cctx.unwrapTemporary(old);

                        cctx.events().addEvent(partition(), key, cctx.localNodeId(), null, (GridCacheVersion)null,
                            EVT_CACHE_OBJECT_REMOVED, null, false, evtOld, evtOld != null || hadVal, subjId, null,
                            taskName, keepBinary);
                    }
                }

                res = hadVal;
            }

            if (res)
                updateMetrics(op, metrics);

            if (lsnrCol != null) {
                long updateCntr = nextPartitionCounter(AffinityTopologyVersion.NONE, true, null);

                cctx.continuousQueries().onEntryUpdated(
                    lsnrCol,
                    key,
                    val,
                    old,
                    internal,
                    partition(),
                    true,
                    false,
                    updateCntr,
                    null,
                    AffinityTopologyVersion.NONE);

                onUpdateFinished(updateCntr);
            }

            cctx.dataStructures().onEntryUpdated(key, op == GridCacheOperation.DELETE, keepBinary);

            if (intercept) {
                if (op == GridCacheOperation.UPDATE)
                    cctx.config().getInterceptor().onAfterPut(new CacheLazyEntry(cctx, key, key0, updated, updated0, keepBinary, 0L));
                else
                    cctx.config().getInterceptor().onAfterRemove(new CacheLazyEntry(cctx, key, key0, old, old0, keepBinary, 0L));
            }
        }
        finally {
            unlockEntry();
        }

        return new GridTuple3<>(res,
            cctx.unwrapTemporary(interceptorRes != null ?
                interceptorRes.get2() :
                cctx.cacheObjectContext().unwrapBinaryIfNeeded(old, keepBinary, false)),
            invokeRes);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridCacheUpdateAtomicResult innerUpdate(
        final GridCacheVersion newVer,
        final UUID evtNodeId,
        final UUID affNodeId,
        final GridCacheOperation op,
        @Nullable final Object writeObj,
        @Nullable final Object[] invokeArgs,
        final boolean writeThrough,
        final boolean readThrough,
        final boolean retval,
        final boolean keepBinary,
        @Nullable final IgniteCacheExpiryPolicy expiryPlc,
        final boolean evt,
        final boolean metrics,
        final boolean primary,
        final boolean verCheck,
        final AffinityTopologyVersion topVer,
        @Nullable final CacheEntryPredicate[] filter,
        final GridDrType drType,
        final long explicitTtl,
        final long explicitExpireTime,
        @Nullable final GridCacheVersion conflictVer,
        final boolean conflictResolve,
        final boolean intercept,
        @Nullable final UUID subjId,
        final String taskName,
        @Nullable final CacheObject prevVal,
        @Nullable final Long updateCntr,
        @Nullable final GridDhtAtomicAbstractUpdateFuture fut
    ) throws IgniteCheckedException, GridCacheEntryRemovedException, GridClosureException {
        assert cctx.atomic() && !detached();

        AtomicCacheUpdateClosure c;

        if (!primary && !isNear())
            ensureFreeSpace();

        lockEntry();

        try {
            checkObsolete();

            boolean internal = isInternal() || !context().userCache();

            Map<UUID, CacheContinuousQueryListener> lsnrs = cctx.continuousQueries().updateListeners(internal, false);

            boolean needVal = lsnrs != null || intercept || retval || op == GridCacheOperation.TRANSFORM
                || !F.isEmptyOrNulls(filter);

            // Possibly read value from store.
            boolean readFromStore = readThrough && needVal && (cctx.readThrough() &&
                (op == GridCacheOperation.TRANSFORM || cctx.loadPreviousValue()));

            c = new AtomicCacheUpdateClosure(this,
                topVer,
                newVer,
                op,
                writeObj,
                invokeArgs,
                readFromStore,
                writeThrough,
                keepBinary,
                expiryPlc,
                primary,
                verCheck,
                filter,
                explicitTtl,
                explicitExpireTime,
                conflictVer,
                conflictResolve,
                intercept,
                updateCntr);

            key.valueBytes(cctx.cacheObjectContext());

            if (isNear()) {
                CacheDataRow dataRow = val != null ? new CacheDataRowAdapter(key, val, ver, expireTimeExtras()) : null;

                c.call(dataRow);
            }
            else
                cctx.offheap().invoke(cctx, key, localPartition(), c);

            GridCacheUpdateAtomicResult updateRes = c.updateRes;

            assert updateRes != null : c;

            // We should ignore expired old row. Expired oldRow instance is needed for correct row replacement\deletion only.
            CacheObject oldVal = c.oldRow != null && !c.oldRowExpiredFlag ? c.oldRow.value() : null;
            CacheObject updateVal = null;
            GridCacheVersion updateVer = c.newVer;

            // Apply metrics.
            if (metrics &&
                updateRes.outcome().updateReadMetrics() &&
                cctx.statisticsEnabled() &&
                needVal)
                    cctx.cache().metrics0().onRead(oldVal != null);

            switch (updateRes.outcome()) {
                case VERSION_CHECK_FAILED: {
                    if (!cctx.isNear()) {
                        CacheObject evtVal;

                        if (op == GridCacheOperation.TRANSFORM) {
                            EntryProcessor<Object, Object, ?> entryProcessor =
                                (EntryProcessor<Object, Object, ?>)writeObj;

                            CacheInvokeEntry<Object, Object> entry =
                                new CacheInvokeEntry<>(key, prevVal, version(), keepBinary, this);

                            try {
                                entryProcessor.process(entry, invokeArgs);

                                evtVal = entry.modified() ?
                                    cctx.toCacheObject(cctx.unwrapTemporary(entry.getValue())) : prevVal;
                            }
                            catch (Exception ignore) {
                                evtVal = prevVal;
                            }
                        }
                        else
                            evtVal = (CacheObject)writeObj;

                        long updateCntr0 = nextPartitionCounter(topVer, primary, updateCntr);

                        if (updateCntr != null)
                            updateCntr0 = updateCntr;

                        onUpdateFinished(updateCntr0);

                        cctx.continuousQueries().onEntryUpdated(
                            key,
                            evtVal,
                            prevVal,
                            isInternal() || !context().userCache(),
                            partition(),
                            primary,
                            false,
                            updateCntr0,
                            null,
                            topVer);
                    }

                    return updateRes;
                }

                case CONFLICT_USE_OLD:
                case FILTER_FAILED:
                case INVOKE_NO_OP:
                case INTERCEPTOR_CANCEL:
                    return updateRes;
            }

            assert updateRes.outcome() == UpdateOutcome.SUCCESS || updateRes.outcome() == UpdateOutcome.REMOVE_NO_VAL;

            CacheObject evtOld = null;

            if (evt && op == TRANSFORM && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ)) {
                assert writeObj instanceof EntryProcessor : writeObj;

                evtOld = cctx.unwrapTemporary(oldVal);

                Object transformClo = EntryProcessorResourceInjectorProxy.unwrap(writeObj);

                cctx.events().addEvent(partition(),
                    key,
                    evtNodeId,
                    null,
                    newVer,
                    EVT_CACHE_OBJECT_READ,
                    evtOld, evtOld != null,
                    evtOld, evtOld != null,
                    subjId,
                    transformClo.getClass().getName(),
                    taskName,
                    keepBinary);
            }

            if (c.op == GridCacheOperation.UPDATE) {
                updateVal = val;

                assert updateVal != null : c;

                drReplicate(drType, updateVal, updateVer, topVer);

                recordNodeId(affNodeId, topVer);

                if (evt && cctx.events().isRecordable(EVT_CACHE_OBJECT_PUT)) {
                    if (evtOld == null)
                        evtOld = cctx.unwrapTemporary(oldVal);

                    cctx.events().addEvent(partition(),
                        key,
                        evtNodeId,
                        null,
                        newVer,
                        EVT_CACHE_OBJECT_PUT,
                        updateVal,
                        true,
                        evtOld,
                        evtOld != null,
                        subjId,
                        null,
                        taskName,
                        keepBinary);
                }
            }
            else {
                assert c.op == GridCacheOperation.DELETE : c.op;

                clearReaders();

                drReplicate(drType, null, newVer, topVer);

                recordNodeId(affNodeId, topVer);

                if (evt && cctx.events().isRecordable(EVT_CACHE_OBJECT_REMOVED)) {
                    if (evtOld == null)
                        evtOld = cctx.unwrapTemporary(oldVal);

                    cctx.events().addEvent(partition(),
                        key,
                        evtNodeId,
                        null, newVer,
                        EVT_CACHE_OBJECT_REMOVED,
                        null, false,
                        evtOld, evtOld != null,
                        subjId,
                        null,
                        taskName,
                        keepBinary);
                }
            }

            if (updateRes.success())
                updateMetrics(c.op, metrics);

            // Continuous query filter should be perform under lock.
            if (lsnrs != null) {
                CacheObject evtVal = cctx.unwrapTemporary(updateVal);
                CacheObject evtOldVal = cctx.unwrapTemporary(oldVal);

                cctx.continuousQueries().onEntryUpdated(lsnrs,
                    key,
                    evtVal,
                    evtOldVal,
                    internal,
                    partition(),
                    primary,
                    false,
                    c.updateRes.updateCounter(),
                    fut,
                    topVer);
            }

            cctx.dataStructures().onEntryUpdated(key, c.op == GridCacheOperation.DELETE, keepBinary);

            if (intercept) {
                if (c.op == GridCacheOperation.UPDATE) {
                    cctx.config().getInterceptor().onAfterPut(new CacheLazyEntry(
                        cctx,
                        key,
                        null,
                        updateVal,
                        null,
                        keepBinary,
                        c.updateRes.updateCounter()));
                }
                else {
                    assert c.op == GridCacheOperation.DELETE : c.op;

                    cctx.config().getInterceptor().onAfterRemove(new CacheLazyEntry(
                        cctx,
                        key,
                        null,
                        oldVal,
                        null,
                        keepBinary,
                        c.updateRes.updateCounter()));
                }
            }
        }
        finally {
            unlockEntry();
        }

        onUpdateFinished(c.updateRes.updateCounter());

        return c.updateRes;
    }

    /**
     * @param val Value.
     * @param cacheObj Cache object.
     * @param keepBinary Keep binary flag.
     * @param cpy Copy flag.
     * @return Cache object value.
     */
    @Nullable private Object value(@Nullable Object val, @Nullable CacheObject cacheObj, boolean keepBinary, boolean cpy) {
        if (val != null)
            return val;

        return cctx.unwrapBinaryIfNeeded(cacheObj, keepBinary, cpy);
    }

    /**
     * @param expiry Expiration policy.
     * @return Tuple holding initial TTL and expire time with the given expiry.
     */
    private static IgniteBiTuple<Long, Long> initialTtlAndExpireTime(IgniteCacheExpiryPolicy expiry) {
        assert expiry != null;

        long initTtl = expiry.forCreate();
        long initExpireTime;

        if (initTtl == CU.TTL_ZERO) {
            initTtl = CU.TTL_MINIMUM;
            initExpireTime = CU.expireTimeInPast();
        }
        else if (initTtl == CU.TTL_NOT_CHANGED) {
            initTtl = CU.TTL_ETERNAL;
            initExpireTime = CU.EXPIRE_TIME_ETERNAL;
        }
        else
            initExpireTime = CU.toExpireTime(initTtl);

        return F.t(initTtl, initExpireTime);
    }

    /**
     * Get TTL, expire time and remove flag for the given entry, expiration policy and explicit TTL and expire time.
     *
     * @param expiry Expiration policy.
     * @param ttl Explicit TTL.
     * @param expireTime Explicit expire time.
     * @return Result.
     */
    private GridTuple3<Long, Long, Boolean> ttlAndExpireTime(IgniteCacheExpiryPolicy expiry, long ttl, long expireTime) {
        assert !obsolete();

        boolean rmv = false;

        // 1. If TTL is not changed, then calculate it based on expiry.
        if (ttl == CU.TTL_NOT_CHANGED) {
            if (expiry != null)
                ttl = hasValueUnlocked() ? expiry.forUpdate() : expiry.forCreate();
        }

        // 2. If TTL is zero, then set delete marker.
        if (ttl == CU.TTL_ZERO) {
            rmv = true;

            ttl = CU.TTL_ETERNAL;
        }

        // 3. If TTL is still not changed, then either use old entry TTL or set it to "ETERNAL".
        if (ttl == CU.TTL_NOT_CHANGED) {
            if (isStartVersion())
                ttl = CU.TTL_ETERNAL;
            else {
                ttl = ttlExtras();
                expireTime = expireTimeExtras();
            }
        }

        // 4 If expire time was not set explicitly, then calculate it.
        if (expireTime == CU.EXPIRE_TIME_CALCULATE)
            expireTime = CU.toExpireTime(ttl);

        return F.t(ttl, expireTime, rmv);
    }

    /**
     * Perform DR if needed.
     *
     * @param drType DR type.
     * @param val Value.
     * @param ver Version.
     * @param topVer Topology version.
     * @throws IgniteCheckedException In case of exception.
     */
    private void drReplicate(GridDrType drType, @Nullable CacheObject val, GridCacheVersion ver, AffinityTopologyVersion topVer)
        throws IgniteCheckedException {
        if (cctx.isDrEnabled() && drType != DR_NONE && !isInternal())
            cctx.dr().replicate(key, val, rawTtl(), rawExpireTime(), ver.conflictVersion(), drType, topVer);
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

    /**
     * @param nodeId Node ID to clear.
     * @throws GridCacheEntryRemovedException If removed.
     */
    protected void clearReader(UUID nodeId) throws GridCacheEntryRemovedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean clear(GridCacheVersion ver, boolean readers) throws IgniteCheckedException {
        lockEntry();

        try {
            if (obsolete())
                return false;

            try {
                if ((!hasReaders() || readers)) {
                    // markObsolete will clear the value.
                    if (!(markObsolete0(ver, true, null))) {
                        if (log.isDebugEnabled())
                            log.debug("Entry could not be marked obsolete (it is still used): " + this);

                        return false;
                    }

                    clearReaders();
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Entry could not be marked obsolete (it still has readers): " + this);

                    return false;
                }
            }
            catch (GridCacheEntryRemovedException ignore) {
                assert false;

                return false;
            }

            if (log.isTraceEnabled()) {
                log.trace("entry clear [key=" + key +
                    ", entry=" + System.identityHashCode(this) +
                    ", val=" + val + ']');
            }

            removeValue();
        }
        finally {
            unlockEntry();
        }

        onMarkedObsolete();

        cctx.cache().removeEntry(this); // Clear cache.

        return true;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion obsoleteVersion() {
        lockEntry();

        try {
            return obsoleteVersionExtras();
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean markObsolete(GridCacheVersion ver) {
        boolean obsolete;

        lockEntry();

        try {
            obsolete = markObsolete0(ver, true, null);
        }
        finally {
            unlockEntry();
        }

        if (obsolete)
            onMarkedObsolete();

        return obsolete;
    }

    /** {@inheritDoc} */
    @Override public boolean markObsoleteIfEmpty(@Nullable GridCacheVersion obsoleteVer) throws IgniteCheckedException {
        boolean obsolete = false;
        boolean deferred = false;
        GridCacheVersion ver0 = null;

        lockEntry();

        try {
            if (obsoleteVersionExtras() != null)
                return false;

            if (hasValueUnlocked()) {
                long expireTime = expireTimeExtras();

                if (expireTime > 0 && (expireTime < U.currentTimeMillis())) {
                    if (obsoleteVer == null)
                        obsoleteVer = nextVersion();

                    if (onExpired(this.val, obsoleteVer)) {
                        if (cctx.deferredDelete()) {
                            deferred = true;
                            ver0 = ver;
                        }
                        else
                            obsolete = true;
                    }
                }
            }
            else {
                if (cctx.deferredDelete() && !isStartVersion() && !detached() && !isInternal()) {
                    if (!deletedUnlocked()) {
                        update(null, 0L, 0L, ver, true);

                        deletedUnlocked(true);

                        deferred = true;
                        ver0 = ver;
                    }
                }
                else {
                    if (obsoleteVer == null)
                        obsoleteVer = nextVersion();

                    obsolete = markObsolete0(obsoleteVer, true, null);
                }
            }
        }
        finally {
            unlockEntry();

            if (obsolete)
                onMarkedObsolete();

            if (deferred)
                cctx.onDeferredDelete(this, ver0);
        }

        return obsolete;
    }

    /** {@inheritDoc} */
    @Override public boolean markObsoleteVersion(GridCacheVersion ver) {
        assert cctx.deferredDelete();

        boolean marked;

        lockEntry();

        try {
            if (obsoleteVersionExtras() != null)
                return true;

            if (!this.ver.equals(ver))
                return false;

            marked = markObsolete0(ver, true, null);
        }
        finally {
            unlockEntry();
        }

        if (marked)
            onMarkedObsolete();

        return marked;
    }

    /**
     * @return {@code True} if this entry should not be evicted from cache.
     */
    protected boolean evictionDisabled() {
        return (flags & IS_EVICT_DISABLED) != 0;
    }

    /**
     * <p>
     * Note that {@link #onMarkedObsolete()} should always be called after this method returns {@code true}.
     *
     * @param ver Version.
     * @param clear {@code True} to clear.
     * @param extras Predefined extras.
     * @return {@code True} if entry is obsolete, {@code false} if entry is still used by other threads or nodes.
     */
    protected final boolean markObsolete0(GridCacheVersion ver, boolean clear, GridCacheObsoleteEntryExtras extras) {
        assert lock.isHeldByCurrentThread();

        if (evictionDisabled()) {
            assert !obsolete() : this;

            return false;
        }

        GridCacheVersion obsoleteVer = obsoleteVersionExtras();

        if (ver != null) {
            // If already obsolete, then do nothing.
            if (obsoleteVer != null)
                return true;

            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc == null || mvcc.isEmpty(ver)) {
                obsoleteVer = ver;

                obsoleteVersionExtras(obsoleteVer, extras);

                if (clear)
                    value(null);

                if (log.isTraceEnabled()) {
                    log.trace("markObsolete0 [key=" + key +
                        ", entry=" + System.identityHashCode(this) +
                        ", clear=" + clear +
                        ']');
                }
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
    @Override public final boolean obsolete() {
        lockEntry();

        try {
            return obsoleteVersionExtras() != null;
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean obsolete(GridCacheVersion exclude) {
        lockEntry();

        try {
            GridCacheVersion obsoleteVer = obsoleteVersionExtras();

            return obsoleteVer != null && !obsoleteVer.equals(exclude);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean invalidate(GridCacheVersion newVer)
        throws IgniteCheckedException {
        lockEntry();

        try {
            assert newVer != null;

            value(null);

            ver = newVer;
            flags &= ~IS_EVICT_DISABLED;

            removeValue();

            onInvalidate();

            return obsoleteVersionExtras() != null;
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * Called when entry invalidated.
     */
    protected void onInvalidate() {
        // No-op.
    }

    /**
     * @param val New value.
     * @param expireTime Expiration time.
     * @param ttl Time to live.
     * @param ver Update version.
     */
    protected final void update(@Nullable CacheObject val, long expireTime, long ttl, GridCacheVersion ver, boolean addTracked) {
        assert ver != null;
        assert lock.isHeldByCurrentThread();
        assert ttl != CU.TTL_ZERO && ttl != CU.TTL_NOT_CHANGED && ttl >= 0 : ttl;

        boolean trackNear = addTracked && isNear() && cctx.config().isEagerTtl();

        long oldExpireTime = expireTimeExtras();

        if (trackNear && oldExpireTime != 0 && (expireTime != oldExpireTime || isStartVersion()))
            cctx.ttl().removeTrackedEntry((GridNearCacheEntry)this);

        value(val);

        ttlAndExpireTimeExtras(ttl, expireTime);

        this.ver = ver;
        flags &= ~IS_EVICT_DISABLED;

        if (trackNear && expireTime != 0 && (expireTime != oldExpireTime || isStartVersion()))
            cctx.ttl().addTrackedEntry((GridNearCacheEntry)this);
    }

    /**
     * Update TTL if it is changed.
     *
     * @param expiryPlc Expiry policy.
     */
    private void updateTtl(ExpiryPolicy expiryPlc) throws IgniteCheckedException, GridCacheEntryRemovedException {
        long ttl = CU.toTtl(expiryPlc.getExpiryForAccess());

        if (ttl != CU.TTL_NOT_CHANGED)
            updateTtl(ttl);
    }

    /**
     * Update TTL is it is changed.
     *
     * @param expiryPlc Expiry policy.
     * @throws GridCacheEntryRemovedException If failed.
     */
    private void updateTtl(IgniteCacheExpiryPolicy expiryPlc) throws GridCacheEntryRemovedException,
        IgniteCheckedException {
        long ttl = expiryPlc.forAccess();

        if (ttl != CU.TTL_NOT_CHANGED) {
            updateTtl(ttl);

            expiryPlc.ttlUpdated(key(), version(), hasReaders() ? ((GridDhtCacheEntry)this).readers() : null);
        }
    }

    /**
     * @param ttl Time to live.
     */
    private void updateTtl(long ttl) throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert ttl >= 0 || ttl == CU.TTL_ZERO : ttl;
        assert lock.isHeldByCurrentThread();

        long expireTime;

        if (ttl == CU.TTL_ZERO) {
            ttl = CU.TTL_MINIMUM;
            expireTime = CU.expireTimeInPast();
        }
        else
            expireTime = CU.toExpireTime(ttl);

        ttlAndExpireTimeExtras(ttl, expireTime);

        cctx.shared().database().checkpointReadLock();

        try {
            storeValue(val, expireTime, ver);
        }
        finally {
            cctx.shared().database().checkpointReadUnlock();
        }
    }

    /**
     * @throws GridCacheEntryRemovedException If entry is obsolete.
     */
    protected void checkObsolete() throws GridCacheEntryRemovedException {
        assert lock.isHeldByCurrentThread();

        if (obsoleteVersionExtras() != null)
            throw new GridCacheEntryRemovedException();
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxKey txKey() {
        return cctx.txKey(key);
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            return ver;
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean checkSerializableReadVersion(GridCacheVersion serReadVer)
        throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            if (!serReadVer.equals(ver)) {
                boolean empty = isStartVersion() || deletedUnlocked();

                if (serReadVer.equals(IgniteTxEntry.SER_READ_EMPTY_ENTRY_VER))
                    return empty;
                else if (serReadVer.equals(IgniteTxEntry.SER_READ_NOT_EMPTY_VER))
                    return !empty;

                return false;
            }

            return true;
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * Gets hash value for the entry key.
     *
     * @return Hash value.
     */
    int hash() {
        return hash;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject peek(
        boolean heap,
        boolean offheap,
        AffinityTopologyVersion topVer,
        @Nullable IgniteCacheExpiryPolicy expiryPlc)
        throws GridCacheEntryRemovedException, IgniteCheckedException {
        assert heap || offheap;

        boolean rmv = false;

        try {
            boolean deferred;
            GridCacheVersion ver0;

            lockEntry();

            try {
                checkObsolete();

                if (!valid(topVer))
                    return null;

                if (val == null && offheap)
                    unswap(null, false);

                if (checkExpired()) {
                    if (cctx.deferredDelete()) {
                        deferred = true;
                        ver0 = ver;
                    }
                    else {
                        rmv = markObsolete0(cctx.versions().next(this.ver), true, null);

                        return null;
                    }
                }
                else {
                    CacheObject val = this.val;

                    if (val != null && expiryPlc != null)
                        updateTtl(expiryPlc);

                    return val;
                }
            }
            finally {
                unlockEntry();
            }

            if (deferred) {
                assert ver0 != null;

                cctx.onDeferredDelete(this, ver0);
            }

            return null;
        }
        finally {
            if (rmv) {
                onMarkedObsolete();

                cctx.cache().removeEntry(this);
            }
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject peek(@Nullable IgniteCacheExpiryPolicy plc)
        throws GridCacheEntryRemovedException, IgniteCheckedException {
        IgniteInternalTx tx = cctx.tm().localTx();

        AffinityTopologyVersion topVer = tx != null ? tx.topologyVersion() : cctx.affinity().affinityTopologyVersion();

        return peek(true, false, topVer, plc);
    }

    /**
     * TODO: IGNITE-3500: do we need to generate event and invalidate value?
     *
     * @return {@code true} if expired.
     * @throws IgniteCheckedException In case of failure.
     */
    private boolean checkExpired() throws IgniteCheckedException {
        assert lock.isHeldByCurrentThread();

        long expireTime = expireTimeExtras();

        if (expireTime > 0) {
            long delta = expireTime - U.currentTimeMillis();

            if (delta <= 0) {
                removeValue();

                return true;
            }
        }

        return false;
    }

    /**
     * @return Value.
     */
    @Override public CacheObject rawGet() {
        lockEntry();

        try {
            return val;
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean hasValue() {
        lockEntry();

        try {
            return hasValueUnlocked();
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * @return {@code True} if this entry has value.
     */
    protected final boolean hasValueUnlocked() {
        assert lock.isHeldByCurrentThread();

        return val != null;
    }

    /** {@inheritDoc} */
    @Override public CacheObject rawPut(CacheObject val, long ttl) {
        lockEntry();

        try {
            CacheObject old = this.val;

            update(val, CU.toExpireTime(ttl), ttl, nextVersion(), true);

            return old;
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Override public boolean initialValue(
        CacheObject val,
        GridCacheVersion ver,
        long ttl,
        long expireTime,
        boolean preload,
        AffinityTopologyVersion topVer,
        GridDrType drType,
        boolean fromStore
    ) throws IgniteCheckedException, GridCacheEntryRemovedException {
        ensureFreeSpace();

        boolean deferred = false;
        boolean obsolete = false;

        GridCacheVersion oldVer = null;

        lockEntry();

        try {
            checkObsolete();

            boolean walEnabled = !cctx.isNear() && cctx.group().persistenceEnabled() && cctx.group().walEnabled();

            long expTime = expireTime < 0 ? CU.toExpireTime(ttl) : expireTime;

            val = cctx.kernalContext().cacheObjects().prepareForCache(val, cctx);

            final boolean unswapped = ((flags & IS_UNSWAPPED_MASK) != 0);

            boolean update;

            IgnitePredicate<CacheDataRow> p = new IgnitePredicate<CacheDataRow>() {
                @Override public boolean apply(@Nullable CacheDataRow row) {
                    boolean update0;

                    GridCacheVersion currentVer = row != null ? row.version() : GridCacheMapEntry.this.ver;

                    boolean isStartVer = currentVer == GridCacheVersionManager.START_VER;

                    if (cctx.group().persistenceEnabled()) {
                        if (!isStartVer) {
                            if (cctx.atomic())
                                update0 = ATOMIC_VER_COMPARATOR.compare(currentVer, ver) < 0;
                            else
                                update0 = currentVer.compareTo(ver) < 0;
                        }
                        else
                            update0 = true;
                    }
                    else
                        update0 = isStartVer;

                    update0 |= (!preload && deletedUnlocked());

                    return update0;
                }
            };

            if (unswapped) {
                update = p.apply(null);

                if (update) {
                    // If entry is already unswapped and we are modifying it, we must run deletion callbacks for old value.
                    long oldExpTime = expireTimeUnlocked();

                    if (oldExpTime > 0 && oldExpTime < U.currentTimeMillis()) {
                        if (onExpired(this.val, null)) {
                            if (cctx.deferredDelete()) {
                                deferred = true;
                                oldVer = this.ver;
                            }
                            else if (val == null)
                                obsolete = true;
                        }
                    }

                    storeValue(val, expTime, ver);
                }
            }
            else // Optimization to access storage only once.
                update = storeValue(val, expTime, ver, p);

            if (update) {
                update(val, expTime, ttl, ver, true);

                boolean skipQryNtf = false;

                if (val == null) {
                    skipQryNtf = true;

                    if (cctx.deferredDelete() && !deletedUnlocked() && !isInternal())
                        deletedUnlocked(true);
                }
                else if (deletedUnlocked())
                    deletedUnlocked(false);

                long updateCntr = 0;

                if (!preload)
                    updateCntr = nextPartitionCounter(topVer, true, null);

                if (walEnabled) {
                    cctx.shared().wal().log(new DataRecord(new DataEntry(
                        cctx.cacheId(),
                        key,
                        val,
                        val == null ? GridCacheOperation.DELETE : GridCacheOperation.CREATE,
                        null,
                        ver,
                        expireTime,
                        partition(),
                        updateCntr
                    )));
                }

                drReplicate(drType, val, ver, topVer);

                if (!skipQryNtf) {
                    cctx.continuousQueries().onEntryUpdated(
                        key,
                        val,
                        null,
                        this.isInternal() || !this.context().userCache(),
                        this.partition(),
                        true,
                        preload,
                        updateCntr,
                        null,
                        topVer);

                    cctx.dataStructures().onEntryUpdated(key, false, true);
                }

                onUpdateFinished(updateCntr);

                if (!fromStore && cctx.store().isLocal()) {
                    if (val != null)
                        cctx.store().put(null, key, val, ver);
                }

                return true;
            }

            return false;
        }
        finally {
            unlockEntry();

            // It is necessary to execute these callbacks outside of lock to avoid deadlocks.

            if (obsolete) {
                onMarkedObsolete();

                cctx.cache().removeEntry(this);
            }

            if (deferred) {
                assert oldVer != null;

                cctx.onDeferredDelete(this, oldVer);
            }
        }
    }

    /**
     * @param cntr Updated partition counter.
     */
    protected void onUpdateFinished(long cntr) {
        // No-op.
    }

    /**
     * @param topVer Topology version for current operation.
     * @param primary Primary node update flag.
     * @param primaryCntr Counter assigned on primary node.
     * @return Update counter.
     */
    protected long nextPartitionCounter(AffinityTopologyVersion topVer, boolean primary, @Nullable Long primaryCntr) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersionedEntryEx versionedEntry(final boolean keepBinary)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        lockEntry();

        try {
            boolean isNew = isStartVersion();

            if (isNew)
                unswap(null, false);

            CacheObject val = this.val;

            return new GridCacheLazyPlainVersionedEntry<>(cctx,
                key,
                val,
                ttlExtras(),
                expireTimeExtras(),
                ver.conflictVersion(),
                isNew,
                keepBinary);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public void clearReserveForLoad(GridCacheVersion ver) {
        lockEntry();

        try {
            if (obsoleteVersionExtras() != null)
                return;

            if (ver.equals(this.ver)) {
                assert evictionDisabled() : this;

                flags &= ~IS_EVICT_DISABLED;
            }
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public EntryGetResult versionedValue(CacheObject val,
        GridCacheVersion curVer,
        GridCacheVersion newVer,
        @Nullable IgniteCacheExpiryPolicy loadExpiryPlc,
        @Nullable ReaderArguments readerArgs
    ) throws IgniteCheckedException, GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            addReaderIfNeed(readerArgs);

            if (curVer == null || curVer.equals(ver)) {
                if (val != this.val) {
                    GridCacheMvcc mvcc = mvccExtras();

                    if (mvcc != null && !mvcc.isEmpty())
                        return entryGetResult(this.val, ver, false);

                    if (newVer == null)
                        newVer = cctx.versions().next();

                    long ttl;
                    long expTime;

                    if (loadExpiryPlc != null) {
                        IgniteBiTuple<Long, Long> initTtlAndExpireTime = initialTtlAndExpireTime(loadExpiryPlc);

                        ttl = initTtlAndExpireTime.get1();
                        expTime = initTtlAndExpireTime.get2();
                    }
                    else {
                        ttl = ttlExtras();
                        expTime = expireTimeExtras();
                    }

                    // Detach value before index update.
                    val = cctx.kernalContext().cacheObjects().prepareForCache(val, cctx);

                    if (val != null) {
                        storeValue(val, expTime, newVer);

                        if (deletedUnlocked())
                            deletedUnlocked(false);
                    }

                    // Version does not change for load ops.
                    update(val, expTime, ttl, newVer, true);

                    return entryGetResult(val, newVer, false);
                }

                assert !evictionDisabled() : this;
            }

            return entryGetResult(this.val, ver, false);
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * @param readerArgs Reader arguments
     */
    private void addReaderIfNeed(@Nullable ReaderArguments readerArgs) {
        if (readerArgs != null) {
            assert this instanceof GridDhtCacheEntry : this;
            assert lock.isHeldByCurrentThread();

            try {
                ((GridDhtCacheEntry)this).addReader(readerArgs.reader(),
                    readerArgs.messageId(),
                    readerArgs.topologyVersion());
            }
            catch (GridCacheEntryRemovedException e) {
                assert false : this;
            }
        }
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
    @Override public boolean hasLockCandidate(GridCacheVersion ver) throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.hasCandidate(ver);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasLockCandidate(long threadId) throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.localCandidate(threadId) != null;
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByAny(GridCacheVersion... exclude)
        throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && !mvcc.isEmpty(exclude);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByThread() throws GridCacheEntryRemovedException {
        return lockedByThread(Thread.currentThread().getId());
    }

    /** {@inheritDoc} */
    @Override public boolean lockedLocally(GridCacheVersion lockVer)
        throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.isLocallyOwned(lockVer);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByThread(long threadId, GridCacheVersion exclude)
        throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.isLocallyOwnedByThread(threadId, false, exclude);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockedLocallyByIdOrThread(GridCacheVersion lockVer, long threadId)
        throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.isLocallyOwnedByIdOrThread(lockVer, threadId);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByThread(long threadId) throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.isLocallyOwnedByThread(threadId, true);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockedBy(GridCacheVersion ver) throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.isOwnedBy(ver);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByThreadUnsafe(long threadId) {
        lockEntry();

        try {
            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.isLocallyOwnedByThread(threadId, true);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByUnsafe(GridCacheVersion ver) {
        lockEntry();

        try {
            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.isOwnedBy(ver);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lockedLocallyUnsafe(GridCacheVersion lockVer) {
        lockEntry();

        try {
            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.isLocallyOwned(lockVer);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasLockCandidateUnsafe(GridCacheVersion ver) {
        lockEntry();

        try {
            GridCacheMvcc mvcc = mvccExtras();

            return mvcc != null && mvcc.hasCandidate(ver);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheMvccCandidate> localCandidates(GridCacheVersion... exclude)
        throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc == null ? Collections.<GridCacheMvccCandidate>emptyList() : mvcc.localCandidates(exclude);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheMvccCandidate> remoteMvccSnapshot(GridCacheVersion... exclude) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheMvccCandidate candidate(GridCacheVersion ver)
        throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc == null ? null : mvcc.candidate(ver);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvccCandidate localCandidate(long threadId)
        throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc == null ? null : mvcc.localCandidate(threadId);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvccCandidate candidate(UUID nodeId, long threadId)
        throws GridCacheEntryRemovedException {
        boolean loc = cctx.nodeId().equals(nodeId);

        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc == null ? null : loc ? mvcc.localCandidate(threadId) :
                mvcc.remoteCandidate(nodeId, threadId);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvccCandidate localOwner() throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc == null ? null : mvcc.localOwner();
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public long rawExpireTime() {
        lockEntry();

        try {
            return expireTimeExtras();
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public long expireTimeUnlocked() {
        assert lock.isHeldByCurrentThread();

        return expireTimeExtras();
    }

    /** {@inheritDoc} */
    @Override public boolean onTtlExpired(GridCacheVersion obsoleteVer) throws GridCacheEntryRemovedException {
        assert obsoleteVer != null;

        boolean obsolete = false;
        boolean deferred = false;
        GridCacheVersion ver0 = null;

        lockEntry();

        try {
            checkObsolete();

            if (isStartVersion())
                unswap(null, false);

            long expireTime = expireTimeExtras();

            if (!(expireTime > 0 && expireTime < U.currentTimeMillis()))
                return false;

            CacheObject expiredVal = this.val;

            if (expiredVal == null)
                return false;

            if (onExpired(expiredVal, obsoleteVer)) {
                if (cctx.deferredDelete()) {
                    deferred = true;
                    ver0 = ver;
                }
                else
                    obsolete = true;
            }
        }
        catch (NodeStoppingException ignore) {
            if (log.isDebugEnabled())
                log.warning("Node is stopping while removing expired value.", ignore);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to clean up expired cache entry: " + this, e);
        }
        finally {
            unlockEntry();

            if (obsolete) {
                onMarkedObsolete();

                cctx.cache().removeEntry(this);
            }

            if (deferred) {
                assert ver0 != null;

                cctx.onDeferredDelete(this, ver0);
            }

            if ((obsolete || deferred) && cctx.statisticsEnabled())
                cctx.cache().metrics0().onEvict();
        }

        return true;
    }

    /**
     * @param expiredVal Expired value.
     * @param obsoleteVer Version.
     * @return {@code True} if entry was marked as removed.
     * @throws IgniteCheckedException If failed.
     */
    private boolean onExpired(CacheObject expiredVal, GridCacheVersion obsoleteVer) throws IgniteCheckedException {
        assert expiredVal != null;

        boolean rmvd = false;

        if (mvccExtras() != null)
            return false;

        if (cctx.deferredDelete() && !detached() && !isInternal()) {
            if (!deletedUnlocked() && !isStartVersion()) {
                update(null, 0L, 0L, ver, true);

                deletedUnlocked(true);

                rmvd = true;
            }
        }
        else {
            if (obsoleteVer == null)
                obsoleteVer = nextVersion();

            if (markObsolete0(obsoleteVer, true, null))
                rmvd = true;
        }

        if (log.isTraceEnabled())
            log.trace("onExpired clear [key=" + key + ", entry=" + System.identityHashCode(this) + ']');

        cctx.shared().database().checkpointReadLock();

        try {
            removeValue();
        }
        finally {
            cctx.shared().database().checkpointReadUnlock();
        }

        if (cctx.events().isRecordable(EVT_CACHE_OBJECT_EXPIRED)) {
            cctx.events().addEvent(partition(),
                key,
                cctx.localNodeId(),
                null,
                EVT_CACHE_OBJECT_EXPIRED,
                null,
                false,
                expiredVal,
                expiredVal != null,
                null,
                null,
                null,
                true);
        }

        cctx.continuousQueries().onEntryExpired(this, key, expiredVal);

        return rmvd;
    }

    /** {@inheritDoc} */
    @Override public long rawTtl() {
        lockEntry();

        try {
            return ttlExtras();
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional"})
    @Override public long expireTime() throws GridCacheEntryRemovedException {
        IgniteTxLocalAdapter tx = currentTx();

        if (tx != null) {
            long time = tx.entryExpireTime(txKey());

            if (time > 0)
                return time;
        }

        lockEntry();

        try {
            checkObsolete();

            return expireTimeExtras();
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional"})
    @Override public long ttl() throws GridCacheEntryRemovedException {
        IgniteTxLocalAdapter tx = currentTx();

        if (tx != null) {
            long entryTtl = tx.entryTtl(txKey());

            if (entryTtl > 0)
                return entryTtl;
        }

        lockEntry();

        try {
            checkObsolete();

            return ttlExtras();
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * @return Current transaction.
     */
    private IgniteTxLocalAdapter currentTx() {
        return cctx.tm().localTx();
    }

    /** {@inheritDoc} */
    @Override public void updateTtl(@Nullable GridCacheVersion ver, long ttl) throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            if (hasValueUnlocked()) {
                try {
                    updateTtl(ttl);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to update TTL: " + e, e);
                }
            }

            /*
            TODO IGNITE-305.
            try {
                if (var == null || ver.equals(version()))
                    updateTtl(ttl);
            }
            catch (GridCacheEntryRemovedException ignored) {
                // No-op.
            }
            */
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public CacheObject valueBytes() throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            return this.val;
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject valueBytes(@Nullable GridCacheVersion ver)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        CacheObject val = null;

        lockEntry();

        try {
            checkObsolete();

            if (ver == null || this.ver.equals(ver))
                val = this.val;
        }
        finally {
            unlockEntry();
        }

        return val;
    }

    /**
     * Stores value in offheap.
     *
     * @param val Value.
     * @param expireTime Expire time.
     * @param ver New entry version.
     * @throws IgniteCheckedException If update failed.
     */
    protected boolean storeValue(@Nullable CacheObject val,
        long expireTime,
        GridCacheVersion ver) throws IgniteCheckedException {
        return storeValue(val, expireTime, ver, null);
    }

    /**
     * Stores value in offheap.
     *
     * @param val Value.
     * @param expireTime Expire time.
     * @param ver New entry version.
     * @param predicate Optional predicate.
     *
     * @return {@code True} if storage was modified.
     * @throws IgniteCheckedException If update failed.
     */
    protected boolean storeValue(
        @Nullable CacheObject val,
        long expireTime,
        GridCacheVersion ver,
        @Nullable IgnitePredicate<CacheDataRow> predicate) throws IgniteCheckedException {
        assert lock.isHeldByCurrentThread();

        UpdateClosure closure = new UpdateClosure(this, val, ver, expireTime, predicate);

        cctx.offheap().invoke(cctx, key, localPartition(), closure);

        return closure.treeOp != IgniteTree.OperationType.NOOP;
    }

    /**
     * @param op Update operation.
     * @param val Write value.
     * @param writeVer Write version.
     * @param expireTime Expire time.
     * @param updCntr Update counter.
     */
    protected void logUpdate(GridCacheOperation op, CacheObject val, GridCacheVersion writeVer, long expireTime, long updCntr)
        throws IgniteCheckedException {
        // We log individual updates only in ATOMIC cache.
        assert cctx.atomic();

        try {
            if (cctx.group().persistenceEnabled() && cctx.group().walEnabled())
                cctx.shared().wal().log(new DataRecord(new DataEntry(
                    cctx.cacheId(),
                    key,
                    val,
                    op,
                    null,
                    writeVer,
                    expireTime,
                    partition(),
                    updCntr)));
        }
        catch (StorageException e) {
            throw new IgniteCheckedException("Failed to log ATOMIC cache update [key=" + key + ", op=" + op +
                ", val=" + val + ']', e);
        }
    }

    /**
     * @param tx Transaction.
     * @param val Value.
     * @param expireTime Expire time (or 0 if not applicable).
     * @param updCntr Update counter.
     * @throws IgniteCheckedException In case of log failure.
     */
    protected WALPointer logTxUpdate(IgniteInternalTx tx, CacheObject val, long expireTime, long updCntr)
        throws IgniteCheckedException {
        assert cctx.transactional();

        if (tx.local()) { // For remote tx we log all updates in batch: GridDistributedTxRemoteAdapter.commitIfLocked()
            GridCacheOperation op;
            if (val == null)
                op = GridCacheOperation.DELETE;
            else
                op = this.val == null ? GridCacheOperation.CREATE : GridCacheOperation.UPDATE;

            return cctx.shared().wal().log(new DataRecord(new DataEntry(
                cctx.cacheId(),
                key,
                val,
                op,
                tx.nearXidVersion(),
                tx.writeVersion(),
                expireTime,
                key.partition(),
                updCntr)));
        }
        else
            return null;
    }

    /**
     * Removes value from offheap.
     *
     * @throws IgniteCheckedException If failed.
     */
    protected void removeValue() throws IgniteCheckedException {
        assert lock.isHeldByCurrentThread();

        cctx.offheap().remove(cctx, key, partition(), localPartition());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> Cache.Entry<K, V> wrap() {
        try {
            IgniteInternalTx tx = cctx.tm().userTx();

            CacheObject val;

            if (tx != null) {
                GridTuple<CacheObject> peek = tx.peek(cctx, false, key);

                val = peek == null ? rawGet() : peek.get();
            }
            else
                val = rawGet();

            return new CacheEntryImpl<>(key.<K>value(cctx.cacheObjectContext(), false),
                CU.<V>value(val, cctx, false), ver);
        }
        catch (GridCacheFilterFailedException ignored) {
            throw new IgniteException("Should never happen.");
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> Cache.Entry<K, V> wrapLazyValue(boolean keepBinary) {
        return new LazyValueEntry<>(key, keepBinary);
    }

    /** {@inheritDoc} */
    @Override @Nullable public CacheObject peekVisibleValue() {
        try {
            IgniteInternalTx tx = cctx.tm().userTx();

            if (tx != null) {
                GridTuple<CacheObject> peek = tx.peek(cctx, false, key);

                if (peek != null)
                    return peek.get();
            }

            if (detached())
                return rawGet();

            for (;;) {
                GridCacheEntryEx e = cctx.cache().peekEx(key);

                if (e == null)
                    return null;

                try {
                    return e.peek(null);
                }
                catch (GridCacheEntryRemovedException ignored) {
                    // No-op.
                }
                catch (IgniteCheckedException ex) {
                    throw new IgniteException(ex);
                }
            }
        }
        catch (GridCacheFilterFailedException ignored) {
            throw new IgniteException("Should never happen.");
        }
    }

    /** {@inheritDoc} */
    @Override public void updateIndex(SchemaIndexCacheFilter filter, SchemaIndexCacheVisitorClosure clo)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        lockEntry();

        try {
            if (isInternal())
                return;

            checkObsolete();

            CacheDataRow row = cctx.offheap().read(this);

            if (row != null && (filter == null || filter.apply(row)))
                clo.apply(row);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> EvictableEntry<K, V> wrapEviction() {
        return new CacheEvictableEntryImpl<>(this);
    }

    /** {@inheritDoc} */
    @Override public <K, V> CacheEntryImplEx<K, V> wrapVersioned() {
        lockEntry();

        try {
            return new CacheEntryImplEx<>(key.<K>value(cctx.cacheObjectContext(), false), null, ver);
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * Evicts necessary number of data pages if per-page eviction is configured in current {@link DataRegion}.
     */
    private void ensureFreeSpace() throws IgniteCheckedException {
        // Deadlock alert: evicting data page causes removing (and locking) all entries on the page one by one.
        assert !lock.isHeldByCurrentThread();

        cctx.shared().database().ensureFreeSpace(cctx.dataRegion());
    }

    /**
     * @return Entry which holds key, value and version.
     */
    private <K, V> CacheEntryImplEx<K, V> wrapVersionedWithValue() {
        lockEntry();

        try {
            V val = this.val == null ? null : this.val.<V>value(cctx.cacheObjectContext(), false);

            return new CacheEntryImplEx<>(key.<K>value(cctx.cacheObjectContext(), false), val, ver);
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean evictInternal(
        GridCacheVersion obsoleteVer,
        @Nullable CacheEntryPredicate[] filter,
        boolean evictOffheap)
        throws IgniteCheckedException {

        boolean marked = false;

        try {
            if (F.isEmptyOrNulls(filter)) {
                lockEntry();

                try {
                    if (evictionDisabled()) {
                        assert !obsolete();

                        return false;
                    }

                    if (obsoleteVersionExtras() != null)
                        return true;

                    // TODO IGNITE-5286: need keep removed entries in heap map, otherwise removes can be lost.
                    if (cctx.deferredDelete() && deletedUnlocked())
                        return false;

                    if (!hasReaders() && markObsolete0(obsoleteVer, false, null)) {
                        // Nullify value after swap.
                        value(null);

                        if (evictOffheap)
                            removeValue();

                        marked = true;

                        return true;
                    }
                }
                finally {
                    unlockEntry();
                }
            }
            else {
                // For optimistic check.
                while (true) {
                    GridCacheVersion v;

                    lockEntry();

                    try {
                        v = ver;
                    }
                    finally {
                        unlockEntry();
                    }

                    if (!cctx.isAll(/*version needed for sync evicts*/this, filter))
                        return false;

                    lockEntry();

                    try {
                        if (evictionDisabled()) {
                            assert !obsolete();

                            return false;
                        }

                        if (obsoleteVersionExtras() != null)
                            return true;

                        if (!v.equals(ver))
                            // Version has changed since entry passed the filter. Do it again.
                            continue;

                        // TODO IGNITE-5286: need keep removed entries in heap map, otherwise removes can be lost.
                        if (cctx.deferredDelete() && deletedUnlocked())
                            return false;

                        if (!hasReaders() && markObsolete0(obsoleteVer, false, null)) {
                            // Nullify value after swap.
                            value(null);

                            if (evictOffheap)
                                removeValue();

                            marked = true;

                            return true;
                        }
                        else
                            return false;
                    }
                    finally {
                        unlockEntry();
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

    /**
     * @param filter Entry filter.
     * @return {@code True} if entry is visitable.
     */
    public final boolean visitable(CacheEntryPredicate[] filter) {
        boolean rmv = false;

        try {
            lockEntry();

            try {
                if (obsoleteOrDeleted())
                    return false;

                if (checkExpired()) {
                    rmv = markObsolete0(cctx.versions().next(this.ver), true, null);

                    return false;
                }
            }
            finally {
                unlockEntry();
            }

            if (filter != CU.empty0() && !cctx.isAll(this, filter))
                return false;
        }
        catch (IgniteCheckedException e) {
            U.error(log, "An exception was thrown while filter checking.", e);

            RuntimeException ex = e.getCause(RuntimeException.class);

            if (ex != null)
                throw ex;

            Error err = e.getCause(Error.class);

            if (err != null)
                throw err;

            return false;
        }
        finally {
            if (rmv) {
                onMarkedObsolete();

                cctx.cache().removeEntry(this);
            }
        }

        IgniteInternalTx tx = cctx.tm().localTx();

        if (tx != null) {
            IgniteTxEntry e = tx.entry(txKey());

            boolean rmvd = e != null && e.op() == DELETE;

            return !rmvd;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public final boolean deleted() {
        if (!cctx.deferredDelete())
            return false;

        lockEntry();

        try {
            return deletedUnlocked();
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean obsoleteOrDeleted() {
        lockEntry();

        try {
            return obsoleteVersionExtras() != null ||
                (cctx.deferredDelete() && (deletedUnlocked() || !hasValueUnlocked()));
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * @return {@code True} if deleted.
     */
    @SuppressWarnings("SimplifiableIfStatement")
    protected final boolean deletedUnlocked() {
        assert lock.isHeldByCurrentThread();

        if (!cctx.deferredDelete())
            return false;

        return (flags & IS_DELETED_MASK) != 0;
    }

    /**
     * @param deleted {@code True} if deleted.
     */
    protected final void deletedUnlocked(boolean deleted) {
        assert lock.isHeldByCurrentThread();
        assert cctx.deferredDelete();

        if (deleted) {
            assert !deletedUnlocked() : this;

            flags |= IS_DELETED_MASK;

            decrementMapPublicSize();
        }
        else {
            assert deletedUnlocked() : this;

            flags &= ~IS_DELETED_MASK;

            incrementMapPublicSize();
        }
    }

    /**
     * Increments public size of map.
     */
    protected void incrementMapPublicSize() {
        GridDhtLocalPartition locPart = localPartition();

        if (locPart != null)
            locPart.incrementPublicSize(null, this);
        else
            cctx.incrementPublicSize(this);
    }

    /**
     * Decrements public size of map.
     */
    protected void decrementMapPublicSize() {
        GridDhtLocalPartition locPart = localPartition();

        if (locPart != null)
            locPart.decrementPublicSize(null, this);
        else
            cctx.decrementPublicSize(this);
    }

    /**
     * @return MVCC.
     */
    @Nullable protected final GridCacheMvcc mvccExtras() {
        return extras != null ? extras.mvcc() : null;
    }

    /**
     * @return All MVCC local and non near candidates.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Nullable public final List<GridCacheMvccCandidate> mvccAllLocal() {
        lockEntry();

        try {
            GridCacheMvcc mvcc = extras != null ? extras.mvcc() : null;

            if (mvcc == null)
                return null;

            List<GridCacheMvccCandidate> allLocs = mvcc.allLocal();

            if (allLocs == null || allLocs.isEmpty())
                return null;

            List<GridCacheMvccCandidate> locs = new ArrayList<>(allLocs.size());

            for (int i = 0; i < allLocs.size(); i++) {
                GridCacheMvccCandidate loc = allLocs.get(i);

                if (!loc.nearLocal())
                    locs.add(loc);
            }

            return locs.isEmpty() ? null : locs;
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * @param mvcc MVCC.
     */
    protected final void mvccExtras(@Nullable GridCacheMvcc mvcc) {
        extras = (extras != null) ? extras.mvcc(mvcc) : mvcc != null ? new GridCacheMvccEntryExtras(mvcc) : null;
    }

    /**
     * @return Obsolete version.
     */
    @Nullable protected final GridCacheVersion obsoleteVersionExtras() {
        return extras != null ? extras.obsoleteVersion() : null;
    }

    /**
     * @param obsoleteVer Obsolete version.
     * @param ext Extras.
     */
    private void obsoleteVersionExtras(@Nullable GridCacheVersion obsoleteVer, GridCacheObsoleteEntryExtras ext) {
        extras = (extras != null) ?
            extras.obsoleteVersion(obsoleteVer) :
            obsoleteVer != null ?
                (ext != null) ? ext : new GridCacheObsoleteEntryExtras(obsoleteVer) :
                null;
    }

    /**
     * @param prevOwners Previous owners.
     * @param owners Current owners.
     * @param val Entry value.
     */
    protected final void checkOwnerChanged(@Nullable CacheLockCandidates prevOwners,
        @Nullable CacheLockCandidates owners,
        CacheObject val) {
        assert !lock.isHeldByCurrentThread();

        if (prevOwners != null && owners == null) {
            cctx.mvcc().callback().onOwnerChanged(this, null);

            if (cctx.events().isRecordable(EVT_CACHE_OBJECT_UNLOCKED)) {
                boolean hasVal = hasValue();

                GridCacheMvccCandidate cand = prevOwners.candidate(0);

                cctx.events().addEvent(partition(),
                    key,
                    cand.nodeId(),
                    cand,
                    EVT_CACHE_OBJECT_UNLOCKED,
                    val,
                    hasVal,
                    val,
                    hasVal,
                    null,
                    null,
                    null,
                    true);
            }
        }

        if (owners != null) {
            for (int i = 0; i < owners.size(); i++) {
                GridCacheMvccCandidate owner = owners.candidate(i);

                boolean locked = prevOwners == null || !prevOwners.hasCandidate(owner.version());

                if (locked) {
                    cctx.mvcc().callback().onOwnerChanged(this, owner);

                    if (owner.local())
                        checkThreadChain(owner);

                    if (cctx.events().isRecordable(EVT_CACHE_OBJECT_LOCKED)) {
                        boolean hasVal = hasValue();

                        // Event notification.
                        cctx.events().addEvent(partition(),
                            key,
                            owner.nodeId(),
                            owner,
                            EVT_CACHE_OBJECT_LOCKED,
                            val,
                            hasVal,
                            val,
                            hasVal,
                            null,
                            null,
                            null,
                            true);
                    }
                }
            }
        }
    }

    /**
     * @param owner Starting candidate in the chain.
     */
    protected abstract void checkThreadChain(GridCacheMvccCandidate owner);

    /**
     * Updates metrics.
     *
     * @param op Operation.
     * @param metrics Update merics flag.
     */
    private void updateMetrics(GridCacheOperation op, boolean metrics) {
        if (metrics && cctx.statisticsEnabled()) {
            if (op == GridCacheOperation.DELETE)
                cctx.cache().metrics0().onRemove();
            else
                cctx.cache().metrics0().onWrite();
        }
    }

    /**
     * @return TTL.
     */
    public long ttlExtras() {
        return extras != null ? extras.ttl() : 0;
    }

    /**
     * @return Expire time.
     */
    public long expireTimeExtras() {
        return extras != null ? extras.expireTime() : 0L;
    }

    /**
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    protected void ttlAndExpireTimeExtras(long ttl, long expireTime) {
        assert ttl != CU.TTL_NOT_CHANGED && ttl != CU.TTL_ZERO;

        extras = (extras != null) ? extras.ttlAndExpireTime(ttl, expireTime) : expireTime != CU.EXPIRE_TIME_ETERNAL ?
            new GridCacheTtlEntryExtras(ttl, expireTime) : null;
    }

    /**
     * @return Size of extras object.
     */
    private int extrasSize() {
        return extras != null ? extras.size() : 0;
    }

    /** {@inheritDoc} */
    @Override public void onUnlock() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void lockEntry() {
        lock.lock();
    }

    /** {@inheritDoc} */
    @Override public void unlockEntry() {
        lock.unlock();
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByCurrentThread() {
        return lock.isHeldByCurrentThread();
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
    @Override public String toString() {
        lockEntry();

        try {
            return S.toString(GridCacheMapEntry.class, this);
        }
        finally {
            unlockEntry();
        }
    }

    /**
     *
     */
    private class LazyValueEntry<K, V> implements Cache.Entry<K, V> {
        /** */
        private final KeyCacheObject key;

        /** */
        private boolean keepBinary;

        /**
         * @param key Key.
         * @param keepBinary Keep binary flag.
         */
        private LazyValueEntry(KeyCacheObject key, boolean keepBinary) {
            this.key = key;
            this.keepBinary = keepBinary;
        }

        /** {@inheritDoc} */
        @Override public K getKey() {
            return (K)cctx.cacheObjectContext().unwrapBinaryIfNeeded(key, keepBinary, true);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public V getValue() {
            return (V)cctx.cacheObjectContext().unwrapBinaryIfNeeded(peekVisibleValue(), keepBinary, true);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public <T> T unwrap(Class<T> cls) {
            if (cls.isAssignableFrom(IgniteCache.class))
                return (T)cctx.grid().cache(cctx.name());

            if (cls.isAssignableFrom(getClass()))
                return (T)this;

            if (cls.isAssignableFrom(EvictableEntry.class))
                return (T)wrapEviction();

            if (cls.isAssignableFrom(CacheEntryImplEx.class))
                return cls == CacheEntryImplEx.class ? (T)wrapVersioned() : (T)wrapVersionedWithValue();

            if (cls.isAssignableFrom(GridCacheVersion.class))
                return (T)ver;

            if (cls.isAssignableFrom(GridCacheMapEntry.this.getClass()))
                return (T)GridCacheMapEntry.this;

            throw new IllegalArgumentException("Unwrapping to class is not supported: " + cls);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "IteratorEntry [key=" + key + ']';
        }
    }

    /**
     *
     */
    private static class UpdateClosure implements IgniteCacheOffheapManager.OffheapInvokeClosure {
        /** */
        private final GridCacheMapEntry entry;

        /** */
        @Nullable private final CacheObject val;

        /** */
        private final GridCacheVersion ver;

        /** */
        private final long expireTime;

        /** */
        @Nullable private final IgnitePredicate<CacheDataRow> predicate;

        /** */
        private CacheDataRow newRow;

        /** */
        private CacheDataRow oldRow;

        /** */
        private IgniteTree.OperationType treeOp = IgniteTree.OperationType.PUT;

        /**
         * @param entry Entry.
         * @param val New value.
         * @param ver New version.
         * @param expireTime New expire time.
         * @param predicate Optional predicate.
         */
        UpdateClosure(GridCacheMapEntry entry, @Nullable CacheObject val, GridCacheVersion ver, long expireTime,
            @Nullable IgnitePredicate<CacheDataRow> predicate) {
            this.entry = entry;
            this.val = val;
            this.ver = ver;
            this.expireTime = expireTime;
            this.predicate = predicate;
        }

        /** {@inheritDoc} */
        @Override public void call(@Nullable CacheDataRow oldRow) throws IgniteCheckedException {
            if (oldRow != null) {
                oldRow.key(entry.key);

                oldRow = checkRowExpired(oldRow);
            }

            this.oldRow = oldRow;

            if (predicate != null && !predicate.apply(oldRow)) {
                treeOp = IgniteTree.OperationType.NOOP;

                return;
            }

            if (val != null) {
                newRow = entry.cctx.offheap().dataStore(entry.localPartition()).createRow(
                    entry.cctx,
                    entry.key,
                    val,
                    ver,
                    expireTime,
                    oldRow);

                treeOp = oldRow != null && oldRow.link() == newRow.link() ?
                    IgniteTree.OperationType.NOOP : IgniteTree.OperationType.PUT;
            }
            else
                treeOp = oldRow != null ? IgniteTree.OperationType.REMOVE : IgniteTree.OperationType.NOOP;
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow newRow() {
            return newRow;
        }

        /** {@inheritDoc} */
        @Override public IgniteTree.OperationType operationType() {
            return treeOp;
        }

        /** {@inheritDoc} */
        @Nullable @Override public CacheDataRow oldRow() {
            return oldRow;
        }

        /**
         * Checks row for expiration and fire expire events if needed.
         *
         * @param row old row.
         * @return {@code Null} if row was expired, row itself otherwise.
         * @throws IgniteCheckedException
         */
        private CacheDataRow checkRowExpired(CacheDataRow row) throws IgniteCheckedException {
            assert row != null;

            if (!(row.expireTime() > 0 && row.expireTime() < U.currentTimeMillis()))
                return row;

            GridCacheContext cctx = entry.context();

            CacheObject expiredVal = row.value();

            if (cctx.deferredDelete() && !entry.detached() && !entry.isInternal()) {
                entry.update(null, CU.TTL_ETERNAL, CU.EXPIRE_TIME_ETERNAL, entry.ver, true);

                if (!entry.deletedUnlocked() && !entry.isStartVersion())
                    entry.deletedUnlocked(true);
            }
            else
                entry.markObsolete0(cctx.versions().next(), true, null);

            if (cctx.events().isRecordable(EVT_CACHE_OBJECT_EXPIRED)) {
                cctx.events().addEvent(entry.partition(),
                    entry.key(),
                    cctx.localNodeId(),
                    null,
                    EVT_CACHE_OBJECT_EXPIRED,
                    null,
                    false,
                    expiredVal,
                    expiredVal != null,
                    null,
                    null,
                    null,
                    true);
            }

            cctx.continuousQueries().onEntryExpired(entry, entry.key(), expiredVal);

            return null;
        }
    }

    /**
     *
     */
    private static class AtomicCacheUpdateClosure implements IgniteCacheOffheapManager.OffheapInvokeClosure {
        /** */
        private final GridCacheMapEntry entry;

        /** */
        private final AffinityTopologyVersion topVer;

        /** */
        private GridCacheVersion newVer;

        /** */
        private GridCacheOperation op;

        /** */
        private Object writeObj;

        /** */
        private Object[] invokeArgs;

        /** */
        private final boolean readThrough;

        /** */
        private final boolean writeThrough;

        /** */
        private final boolean keepBinary;

        /** */
        private final IgniteCacheExpiryPolicy expiryPlc;

        /** */
        private final boolean primary;

        /** */
        private final boolean verCheck;

        /** */
        private final CacheEntryPredicate[] filter;

        /** */
        private final long explicitTtl;

        /** */
        private final long explicitExpireTime;

        /** */
        private GridCacheVersion conflictVer;

        /** */
        private final boolean conflictResolve;

        /** */
        private final boolean intercept;

        /** */
        private final Long updateCntr;

        /** */
        private GridCacheUpdateAtomicResult updateRes;

        /** */
        private IgniteTree.OperationType treeOp;

        /** */
        private CacheDataRow newRow;

        /** */
        private CacheDataRow oldRow;

        /** OldRow expiration flag. */
        private boolean oldRowExpiredFlag = false;

        AtomicCacheUpdateClosure(
            GridCacheMapEntry entry,
            AffinityTopologyVersion topVer,
            GridCacheVersion newVer,
            GridCacheOperation op,
            Object writeObj,
            Object[] invokeArgs,
            boolean readThrough,
            boolean writeThrough,
            boolean keepBinary,
            @Nullable IgniteCacheExpiryPolicy expiryPlc,
            boolean primary,
            boolean verCheck,
            @Nullable CacheEntryPredicate[] filter,
            long explicitTtl,
            long explicitExpireTime,
            @Nullable GridCacheVersion conflictVer,
            boolean conflictResolve,
            boolean intercept,
            @Nullable Long updateCntr) {
            assert op == UPDATE || op == DELETE || op == TRANSFORM : op;

            this.entry = entry;
            this.topVer = topVer;
            this.newVer = newVer;
            this.op = op;
            this.writeObj = writeObj;
            this.invokeArgs = invokeArgs;
            this.readThrough = readThrough;
            this.writeThrough = writeThrough;
            this.keepBinary = keepBinary;
            this.expiryPlc = expiryPlc;
            this.primary = primary;
            this.verCheck = verCheck;
            this.filter = filter;
            this.explicitTtl = explicitTtl;
            this.explicitExpireTime = explicitExpireTime;
            this.conflictVer = conflictVer;
            this.conflictResolve = conflictResolve;
            this.intercept = intercept;
            this.updateCntr = updateCntr;

            switch (op) {
                case UPDATE:
                    treeOp = IgniteTree.OperationType.PUT;

                    break;

                case DELETE:
                    treeOp = IgniteTree.OperationType.REMOVE;

                    break;
            }
        }

        /** {@inheritDoc} */
        @Nullable @Override public CacheDataRow oldRow() {
            return oldRow;
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow newRow() {
            return newRow;
        }

        /** {@inheritDoc} */
        @Override public IgniteTree.OperationType operationType() {
            return treeOp;
        }

        /** {@inheritDoc} */
        @Override public void call(@Nullable CacheDataRow oldRow) throws IgniteCheckedException {
            assert entry.isNear() || oldRow == null || oldRow.link() != 0 : oldRow;

            GridCacheContext cctx = entry.context();

            CacheObject oldVal;
            CacheObject storeLoadedVal = null;

            this.oldRow = oldRow;

            if (oldRow != null) {
                oldRow.key(entry.key());

                // unswap
                entry.update(oldRow.value(), oldRow.expireTime(), 0, oldRow.version(), false);

                if (checkRowExpired(oldRow)) {
                    oldRowExpiredFlag = true;

                    oldRow = null;
                }
            }

            oldVal = (oldRow != null) ? oldRow.value() : null;

            if (oldVal == null && readThrough) {
                storeLoadedVal = cctx.toCacheObject(cctx.store().load(null, entry.key));

                if (storeLoadedVal != null) {
                    oldVal = cctx.kernalContext().cacheObjects().prepareForCache(storeLoadedVal, cctx);

                    entry.val = oldVal;

                    if (entry.deletedUnlocked())
                        entry.deletedUnlocked(false);
                }
            }

            CacheInvokeEntry<Object, Object> invokeEntry = null;
            IgniteBiTuple<Object, Exception> invokeRes = null;

            boolean invoke = op == TRANSFORM;

            if (invoke) {
                invokeEntry = new CacheInvokeEntry<>(entry.key, oldVal, entry.ver, keepBinary, entry);

                invokeRes = runEntryProcessor(invokeEntry);

                op = writeObj == null ? DELETE : UPDATE;
            }

            CacheObject newVal = (CacheObject)writeObj;

            GridCacheVersionConflictContext<?, ?> conflictCtx = null;

            if (conflictResolve) {
                conflictCtx = resolveConflict(newVal, invokeRes);

                if (updateRes != null) {
                    assert conflictCtx != null && conflictCtx.isUseOld() : conflictCtx;
                    assert treeOp == IgniteTree.OperationType.NOOP : treeOp;

                    return;
                }
            }

            if (conflictCtx == null) {
                // Perform version check only in case there was no explicit conflict resolution.
                versionCheck(invokeRes);

                if (updateRes != null) {
                    assert treeOp == IgniteTree.OperationType.NOOP : treeOp;

                    return;
                }
            }

            if (!F.isEmptyOrNulls(filter)) {
                boolean pass = cctx.isAllLocked(entry, filter);

                if (!pass) {
                    initResultOnCancelUpdate(storeLoadedVal, !cctx.putIfAbsentFilter(filter));

                    updateRes = new GridCacheUpdateAtomicResult(UpdateOutcome.FILTER_FAILED,
                        oldVal,
                        null,
                        invokeRes,
                        CU.TTL_ETERNAL,
                        CU.EXPIRE_TIME_ETERNAL,
                        null,
                        null,
                        0);

                    return;
                }
            }

            if (invoke) {
                if (!invokeEntry.modified()) {
                    initResultOnCancelUpdate(storeLoadedVal, true);

                    updateRes = new GridCacheUpdateAtomicResult(UpdateOutcome.INVOKE_NO_OP,
                        oldVal,
                        null,
                        invokeRes,
                        CU.TTL_ETERNAL,
                        CU.EXPIRE_TIME_ETERNAL,
                        null,
                        null,
                        0);

                    return;
                }
                else if ((invokeRes == null || invokeRes.getValue() == null) && writeObj != null) {
                    try {
                        cctx.validateKeyAndValue(entry.key, (CacheObject)writeObj);
                    }
                    catch (Exception e) {
                        initResultOnCancelUpdate(null, true);

                        updateRes = new GridCacheUpdateAtomicResult(UpdateOutcome.INVOKE_NO_OP,
                            oldVal,
                            null,
                            new IgniteBiTuple<>(null, e),
                            CU.TTL_ETERNAL,
                            CU.EXPIRE_TIME_ETERNAL,
                            null,
                            null,
                            0);

                        return;
                    }
                }

                op = writeObj == null ? DELETE : UPDATE;
            }

            // Incorporate conflict version into new version if needed.
            if (conflictVer != null && conflictVer != newVer) {
                newVer = new GridCacheVersionEx(newVer.topologyVersion(),
                    newVer.order(),
                    newVer.nodeOrder(),
                    newVer.dataCenterId(),
                    conflictVer);
            }

            if (op == UPDATE) {
                assert writeObj != null;

                update(conflictCtx, invokeRes, storeLoadedVal != null);
            }
            else {
                assert op == DELETE && writeObj == null : op;

                remove(conflictCtx, invokeRes, storeLoadedVal != null);
            }

            assert updateRes != null && treeOp != null;
        }

        /**
         * Check row expiration and fire expire events if needed.
         *
         * @param row Old row.
         * @return {@code True} if row was expired, {@code False} otherwise.
         * @throws IgniteCheckedException if failed.
         */
        private boolean checkRowExpired(CacheDataRow row) throws IgniteCheckedException {
            assert row != null;

            if (!(row.expireTime() > 0 && row.expireTime() < U.currentTimeMillis()))
                return false;

            GridCacheContext cctx = entry.context();

            CacheObject expiredVal = row.value();

            if (cctx.deferredDelete() && !entry.detached() && !entry.isInternal()) {
                entry.update(null, CU.TTL_ETERNAL, CU.EXPIRE_TIME_ETERNAL, entry.ver, true);

                if (!entry.deletedUnlocked())
                    entry.deletedUnlocked(true);
            }
            else
                entry.markObsolete0(cctx.versions().next(), true, null);

            if (cctx.events().isRecordable(EVT_CACHE_OBJECT_EXPIRED)) {
                cctx.events().addEvent(entry.partition(),
                    entry.key(),
                    cctx.localNodeId(),
                    null,
                    EVT_CACHE_OBJECT_EXPIRED,
                    null,
                    false,
                    expiredVal,
                    expiredVal != null,
                    null,
                    null,
                    null,
                    true);
            }

            cctx.continuousQueries().onEntryExpired(entry, entry.key(), expiredVal);

            return true;
        }

        /**
         * @param storeLoadedVal Value loaded from store.
         * @param updateExpireTime {@code True} if need update expire time.
         * @throws IgniteCheckedException If failed.
         */
        private void initResultOnCancelUpdate(@Nullable CacheObject storeLoadedVal, boolean updateExpireTime)
            throws IgniteCheckedException {
            boolean needUpdate = false;

            if (storeLoadedVal != null) {
                long initTtl;
                long initExpireTime;

                if (expiryPlc != null) {
                    IgniteBiTuple<Long, Long> initTtlAndExpireTime = initialTtlAndExpireTime(expiryPlc);

                    initTtl = initTtlAndExpireTime.get1();
                    initExpireTime = initTtlAndExpireTime.get2();
                }
                else {
                    initTtl = CU.TTL_ETERNAL;
                    initExpireTime = CU.EXPIRE_TIME_ETERNAL;
                }

                entry.update(storeLoadedVal, initExpireTime, initTtl, entry.ver, true);

                needUpdate = true;
            }
            else if (updateExpireTime && expiryPlc != null && entry.val != null) {
                long ttl = expiryPlc.forAccess();

                if (ttl != CU.TTL_NOT_CHANGED) {
                    long expireTime;

                    if (ttl == CU.TTL_ZERO) {
                        ttl = CU.TTL_MINIMUM;
                        expireTime = CU.expireTimeInPast();
                    }
                    else
                        expireTime = CU.toExpireTime(ttl);

                    if (entry.expireTimeExtras() != expireTime) {
                        entry.update(entry.val, expireTime, ttl, entry.ver, true);

                        expiryPlc.ttlUpdated(entry.key, entry.ver, null);

                        needUpdate = true;
                        storeLoadedVal = entry.val;
                    }
                }
            }

            if (needUpdate) {
                newRow = entry.localPartition().dataStore().createRow(
                    entry.cctx,
                    entry.key,
                    storeLoadedVal,
                    newVer,
                    entry.expireTimeExtras(),
                    oldRow);

                treeOp = IgniteTree.OperationType.PUT;
            }
            else
                treeOp = IgniteTree.OperationType.NOOP;
        }

        /**
         * @param conflictCtx Conflict context.
         * @param invokeRes Entry processor result (for invoke operation).
         * @param readFromStore {@code True} if initial entry value was {@code null} and it was read from store.
         * @throws IgniteCheckedException If failed.
         */
        private void update(@Nullable GridCacheVersionConflictContext<?, ?> conflictCtx,
            @Nullable IgniteBiTuple<Object, Exception> invokeRes,
            boolean readFromStore)
            throws IgniteCheckedException
        {
            GridCacheContext cctx = entry.context();

            final CacheObject oldVal = entry.val;
            CacheObject updated = (CacheObject)writeObj;

            long newSysTtl;
            long newSysExpireTime;

            long newTtl;
            long newExpireTime;

            // Conflict context is null if there were no explicit conflict resolution.
            if (conflictCtx == null) {
                // Calculate TTL and expire time for local update.
                if (explicitTtl != CU.TTL_NOT_CHANGED) {
                    // If conflict existed, expire time must be explicit.
                    assert conflictVer == null || explicitExpireTime != CU.EXPIRE_TIME_CALCULATE;

                    newSysTtl = newTtl = explicitTtl;
                    newSysExpireTime = explicitExpireTime;

                    newExpireTime = explicitExpireTime != CU.EXPIRE_TIME_CALCULATE ?
                        explicitExpireTime : CU.toExpireTime(explicitTtl);
                }
                else {
                    newSysTtl = expiryPlc == null ? CU.TTL_NOT_CHANGED :
                        entry.val != null ? expiryPlc.forUpdate() : expiryPlc.forCreate();

                    if (newSysTtl == CU.TTL_NOT_CHANGED) {
                        newSysExpireTime = CU.EXPIRE_TIME_CALCULATE;
                        newTtl = entry.ttlExtras();
                        newExpireTime = entry.expireTimeExtras();
                    }
                    else if (newSysTtl == CU.TTL_ZERO) {
                        op = GridCacheOperation.DELETE;

                        writeObj = null;

                        remove(conflictCtx, invokeRes, readFromStore);

                        return;
                    }
                    else {
                        newSysExpireTime = CU.EXPIRE_TIME_CALCULATE;
                        newTtl = newSysTtl;
                        newExpireTime = CU.toExpireTime(newTtl);
                    }
                }
            }
            else {
                newSysTtl = newTtl = conflictCtx.ttl();
                newSysExpireTime = newExpireTime = conflictCtx.expireTime();
            }

            if (intercept) {
                Object updated0 = cctx.unwrapBinaryIfNeeded(updated, keepBinary, false);

                CacheLazyEntry<Object, Object> interceptEntry = new CacheLazyEntry<>(cctx,
                    entry.key,
                    null,
                    oldVal,
                    null,
                    keepBinary);

                Object interceptorVal = cctx.config().getInterceptor().onBeforePut(interceptEntry, updated0);

                if (interceptorVal == null) {
                    treeOp = IgniteTree.OperationType.NOOP;

                    updateRes = new GridCacheUpdateAtomicResult(UpdateOutcome.INTERCEPTOR_CANCEL,
                        oldVal,
                        null,
                        invokeRes,
                        CU.TTL_ETERNAL,
                        CU.EXPIRE_TIME_ETERNAL,
                        null,
                        null,
                        0);

                    return;
                }
                else if (interceptorVal != updated0) {
                    updated0 = cctx.unwrapTemporary(interceptorVal);

                    updated = cctx.toCacheObject(updated0);
                }
            }

            updated = cctx.kernalContext().cacheObjects().prepareForCache(updated, cctx);

            if (writeThrough)
                // Must persist inside synchronization in non-tx mode.
                cctx.store().put(null, entry.key, updated, newVer);

            if (entry.val == null) {
                boolean new0 = entry.isStartVersion();

                assert entry.deletedUnlocked() || new0 || entry.isInternal() : "Invalid entry [entry=" + entry +
                    ", locNodeId=" + cctx.localNodeId() + ']';

                if (!new0 && !entry.isInternal())
                    entry.deletedUnlocked(false);
            }
            else {
                assert !entry.deletedUnlocked() : "Invalid entry [entry=" + this +
                    ", locNodeId=" + cctx.localNodeId() + ']';
            }

            long updateCntr0 = entry.nextPartitionCounter(topVer, primary, updateCntr);

            if (updateCntr != null)
                updateCntr0 = updateCntr;

            entry.logUpdate(op, updated, newVer, newExpireTime, updateCntr0);

            if (!entry.isNear()) {
                newRow = entry.localPartition().dataStore().createRow(
                    entry.cctx,
                    entry.key,
                    updated,
                    newVer,
                    newExpireTime,
                    oldRow);

                treeOp = oldRow != null && oldRow.link() == newRow.link() ?
                    IgniteTree.OperationType.NOOP : IgniteTree.OperationType.PUT;
            }
            else
                treeOp = IgniteTree.OperationType.PUT;

            entry.update(updated, newExpireTime, newTtl, newVer, true);

            updateRes = new GridCacheUpdateAtomicResult(UpdateOutcome.SUCCESS,
                oldVal,
                updated,
                invokeRes,
                newSysTtl,
                newSysExpireTime,
                null,
                conflictCtx,
                updateCntr0);
        }

        /**
         * @param conflictCtx Conflict context.
         * @param invokeRes Entry processor result (for invoke operation).
         * @param readFromStore {@code True} if initial entry value was {@code null} and it was read from store.
         * @throws IgniteCheckedException If failed.
         */
        @SuppressWarnings("unchecked")
        private void remove(@Nullable GridCacheVersionConflictContext<?, ?> conflictCtx,
            @Nullable IgniteBiTuple<Object, Exception> invokeRes,
            boolean readFromStore)
            throws IgniteCheckedException
        {
            GridCacheContext cctx = entry.context();

            CacheObject oldVal = entry.val;

            IgniteBiTuple<Boolean, Object> interceptRes = null;

            if (intercept) {
                CacheLazyEntry<Object, Object> intercepEntry = new CacheLazyEntry<>(cctx,
                    entry.key,
                    null,
                    oldVal,
                    null,
                    keepBinary);

                interceptRes = cctx.config().getInterceptor().onBeforeRemove(intercepEntry);

                if (cctx.cancelRemove(interceptRes)) {
                    treeOp = IgniteTree.OperationType.NOOP;

                    updateRes = new GridCacheUpdateAtomicResult(UpdateOutcome.INTERCEPTOR_CANCEL,
                        cctx.toCacheObject(cctx.unwrapTemporary(interceptRes.get2())),
                        null,
                        invokeRes,
                        CU.TTL_ETERNAL,
                        CU.EXPIRE_TIME_ETERNAL,
                        null,
                        null,
                        0);

                    return;
                }
            }

            if (writeThrough)
                // Must persist inside synchronization in non-tx mode.
                cctx.store().remove(null, entry.key);

            long updateCntr0 = entry.nextPartitionCounter(topVer, primary, updateCntr);

            if (updateCntr != null)
                updateCntr0 = updateCntr;

            entry.logUpdate(op, null, newVer, 0, updateCntr0);

            if (oldVal != null) {
                assert !entry.deletedUnlocked();

                if (!entry.isInternal())
                    entry.deletedUnlocked(true);
            }
            else {
                boolean new0 = entry.isStartVersion();

                assert entry.deletedUnlocked() || new0 || entry.isInternal() : "Invalid entry [entry=" + this +
                    ", locNodeId=" + cctx.localNodeId() + ']';

                if (new0) {
                    if (!entry.isInternal())
                        entry.deletedUnlocked(true);
                }
            }

            GridCacheVersion enqueueVer = newVer;

            entry.update(null, CU.TTL_ETERNAL, CU.EXPIRE_TIME_ETERNAL, newVer, true);

            treeOp = (oldRow == null || readFromStore) ? IgniteTree.OperationType.NOOP :
                IgniteTree.OperationType.REMOVE;

            UpdateOutcome outcome = oldVal != null ? UpdateOutcome.SUCCESS : UpdateOutcome.REMOVE_NO_VAL;

            if (interceptRes != null)
                oldVal = cctx.toCacheObject(cctx.unwrapTemporary(interceptRes.get2()));

            updateRes = new GridCacheUpdateAtomicResult(outcome,
                oldVal,
                null,
                invokeRes,
                CU.TTL_NOT_CHANGED,
                CU.EXPIRE_TIME_CALCULATE,
                enqueueVer,
                conflictCtx,
                updateCntr0);
        }

        /**
         * @param newVal New entry value.
         * @param invokeRes Entry processor result (for invoke operation).
         * @return Conflict context.
         * @throws IgniteCheckedException If failed.
         */
        private GridCacheVersionConflictContext<?, ?> resolveConflict(
            CacheObject newVal,
            @Nullable IgniteBiTuple<Object, Exception> invokeRes)
            throws IgniteCheckedException
        {
            GridCacheContext cctx = entry.context();

            // Cache is conflict-enabled.
            if (cctx.conflictNeedResolve()) {
                GridCacheVersion oldConflictVer = entry.ver.conflictVersion();

                // Prepare old and new entries for conflict resolution.
                GridCacheVersionedEntryEx oldEntry = new GridCacheLazyPlainVersionedEntry<>(cctx,
                    entry.key,
                    entry.val,
                    entry.ttlExtras(),
                    entry.expireTimeExtras(),
                    entry.ver.conflictVersion(),
                    entry.isStartVersion(),
                    keepBinary);

                GridTuple3<Long, Long, Boolean> expiration = entry.ttlAndExpireTime(expiryPlc,
                    explicitTtl,
                    explicitExpireTime);

                GridCacheVersionedEntryEx newEntry = new GridCacheLazyPlainVersionedEntry<>(
                    cctx,
                    entry.key,
                    newVal,
                    expiration.get1(),
                    expiration.get2(),
                    conflictVer != null ? conflictVer : newVer,
                    keepBinary);

                // Resolve conflict.
                GridCacheVersionConflictContext<?, ?> conflictCtx = cctx.conflictResolve(oldEntry, newEntry, verCheck);

                assert conflictCtx != null;

                // Use old value?
                if (conflictCtx.isUseOld()) {
                    GridCacheVersion newConflictVer = conflictVer != null ? conflictVer : newVer;

                    // Handle special case with atomic comparator.
                    if (!entry.isStartVersion() &&                                                        // Not initial value,
                        verCheck &&                                                                       // and atomic version check,
                        oldConflictVer.dataCenterId() == newConflictVer.dataCenterId() &&                 // and data centers are equal,
                        ATOMIC_VER_COMPARATOR.compare(oldConflictVer, newConflictVer) == 0 && // and both versions are equal,
                        cctx.writeThrough() &&                                                            // and store is enabled,
                        primary)                                                                          // and we are primary.
                    {
                        CacheObject val = entry.val;

                        if (val == null) {
                            assert entry.deletedUnlocked();

                            cctx.store().remove(null, entry.key);
                        }
                        else
                            cctx.store().put(null, entry.key, val, entry.ver);
                    }

                    treeOp = IgniteTree.OperationType.NOOP;

                    updateRes = new GridCacheUpdateAtomicResult(UpdateOutcome.CONFLICT_USE_OLD,
                        entry.val,
                        null,
                        invokeRes,
                        CU.TTL_ETERNAL,
                        CU.EXPIRE_TIME_ETERNAL,
                        null,
                        null,
                        0);
                }
                // Will update something.
                else {
                    // Merge is a local update which override passed value bytes.
                    if (conflictCtx.isMerge()) {
                        writeObj = cctx.toCacheObject(conflictCtx.mergeValue());

                        conflictVer = null;
                    }
                    else
                        assert conflictCtx.isUseNew();

                    // Update value is known at this point, so update operation type.
                    op = writeObj != null ? GridCacheOperation.UPDATE : GridCacheOperation.DELETE;
                }

                return conflictCtx;
            }
            else
                // Nullify conflict version on this update, so that we will use regular version during next updates.
                conflictVer = null;

            return null;
        }

        /**
         * @param invokeRes Entry processor result (for invoke operation).
         * @throws IgniteCheckedException If failed.
         */
        private void versionCheck(@Nullable IgniteBiTuple<Object, Exception> invokeRes) throws IgniteCheckedException {
            GridCacheContext cctx = entry.context();

            if (verCheck) {
                if (!entry.isStartVersion() && ATOMIC_VER_COMPARATOR.compare(entry.ver, newVer) >= 0) {
                    if (ATOMIC_VER_COMPARATOR.compare(entry.ver, newVer) == 0 && cctx.writeThrough() && primary) {
                        if (log.isDebugEnabled())
                            log.debug("Received entry update with same version as current (will update store) " +
                                "[entry=" + this + ", newVer=" + newVer + ']');

                        CacheObject val = entry.val;

                        if (val == null) {
                            assert entry.deletedUnlocked();

                            cctx.store().remove(null, entry.key);
                        }
                        else
                            cctx.store().put(null, entry.key, val, entry.ver);
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Received entry update with smaller version than current (will ignore) " +
                                "[entry=" + this + ", newVer=" + newVer + ']');
                    }

                    treeOp = IgniteTree.OperationType.NOOP;

                    updateRes = new GridCacheUpdateAtomicResult(UpdateOutcome.VERSION_CHECK_FAILED,
                        entry.val,
                        null,
                        invokeRes,
                        CU.TTL_ETERNAL,
                        CU.EXPIRE_TIME_ETERNAL,
                        null,
                        null,
                        0);
                }
            }
            else
                assert entry.isStartVersion() || ATOMIC_VER_COMPARATOR.compare(entry.ver, newVer) <= 0 :
                    "Invalid version for inner update [isNew=" + entry.isStartVersion() + ", entry=" + entry + ", newVer=" + newVer + ']';
        }

        /**
         * @param invokeEntry Entry for {@link EntryProcessor}.
         * @return Entry processor return value.
         */
        @SuppressWarnings("unchecked")
        private IgniteBiTuple<Object, Exception> runEntryProcessor(CacheInvokeEntry<Object, Object> invokeEntry) {
            EntryProcessor<Object, Object, ?> entryProcessor = (EntryProcessor<Object, Object, ?>)writeObj;

            try {
                Object computed = entryProcessor.process(invokeEntry, invokeArgs);

                if (invokeEntry.modified()) {
                    GridCacheContext cctx = entry.context();

                    writeObj = cctx.toCacheObject(cctx.unwrapTemporary(invokeEntry.getValue()));
                }
                else
                    writeObj = invokeEntry.valObj;

                if (computed != null)
                    return new IgniteBiTuple<>(entry.cctx.unwrapTemporary(computed), null);

                return null;
            }
            catch (Exception e) {
                writeObj = invokeEntry.valObj;

                return new IgniteBiTuple<>(null, e);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AtomicCacheUpdateClosure.class, this);
        }
    }
}

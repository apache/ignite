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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.internal.binary.BinaryObjectOffheapImpl;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicAbstractUpdateFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheEntry;
import org.apache.ignite.internal.processors.cache.extras.GridCacheEntryExtras;
import org.apache.ignite.internal.processors.cache.extras.GridCacheMvccEntryExtras;
import org.apache.ignite.internal.processors.cache.extras.GridCacheObsoleteEntryExtras;
import org.apache.ignite.internal.processors.cache.extras.GridCacheTtlEntryExtras;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryListener;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.version.GridCacheLazyPlainVersionedEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.GridMetadataAwareAdapter;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_EXPIRED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_LOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_UNLOCKED;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;

/**
 * Adapter for cache entry.
 */
@SuppressWarnings({
    "NonPrivateFieldAccessedInSynchronizedContext", "TooBroadScope", "FieldAccessedSynchronizedAndUnsynchronized"})
public abstract class GridCacheMapEntry extends GridMetadataAwareAdapter implements GridCacheEntryEx {
    /** */
    private static final byte IS_DELETED_MASK = 0x01;

    /** */
    private static final byte IS_UNSWAPPED_MASK = 0x02;

    /** */
    private static final byte IS_OFFHEAP_PTR_MASK = 0x04;

    /** */
    private static final byte IS_SWAPPING_REQUIRED = 0x08;

    /** */
    private static final byte IS_EVICT_DISABLED = 0x10;

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

    /** Start version. */
    @GridToStringInclude
    protected final long startVer;

    /** Version. */
    @GridToStringInclude
    protected GridCacheVersion ver;

    /** Key hash code. */
    @GridToStringInclude
    private final int hash;

    /** Extras */
    @GridToStringInclude
    private GridCacheEntryExtras extras;

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
     * @param hash Key hash value.
     * @param val Entry value.
     */
    protected GridCacheMapEntry(
        GridCacheContext<?, ?> cctx,
        KeyCacheObject key,
        int hash,
        CacheObject val
    ) {
        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridCacheMapEntry.class);

        key = (KeyCacheObject)cctx.kernalContext().cacheObjects().prepareForCache(key, cctx);

        assert key != null;

        this.key = key;
        this.hash = hash;
        this.cctx = cctx;

        val = cctx.kernalContext().cacheObjects().prepareForCache(val, cctx);

        synchronized (this) {
            value(val);
        }

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
     */
    protected void value(@Nullable CacheObject val) {
        assert Thread.holdsLock(this);

        // In case we deal with IGFS cache, count updated data
        if (cctx.cache().isIgfsDataCache() &&
            cctx.kernalContext().igfsHelper().isIgfsBlockKey(keyValue(false))) {
            int newSize = valueLength0(val, null);
            int oldSize = valueLength0(this.val, (this.val == null && hasOffHeapPointer()) ? valueBytes0() : null);

            int delta = newSize - oldSize;

            if (delta != 0 && !cctx.isNear())
                cctx.cache().onIgfsDataSizeChanged(delta);
        }

        if (!isOffHeapValuesOnly()) {
            this.val = val;

            offHeapPointer(0);
        }
        else {
            try {
                if (cctx.deploymentEnabled()) {
                    Object val0 = null;

                    if (val != null && val.cacheObjectType() != CacheObject.TYPE_BYTE_ARR) {
                        val0 = cctx.cacheObjects().unmarshal(cctx.cacheObjectContext(),
                            val.valueBytes(cctx.cacheObjectContext()), cctx.deploy().globalLoader());

                        if (val0 != null)
                            cctx.gridDeploy().deploy(val0.getClass(), val0.getClass().getClassLoader());
                    }

                    if (U.p2pLoader(val0)) {
                        cctx.deploy().addDeploymentContext(
                            new GridDeploymentInfoBean((GridDeploymentInfo)val0.getClass().getClassLoader()));
                    }
                }

                GridUnsafeMemory mem = cctx.unsafeMemory();

                assert mem != null;

                if (val != null) {
                    byte type = val.cacheObjectType();

                    offHeapPointer(mem.putOffHeap(offHeapPointer(), val.valueBytes(cctx.cacheObjectContext()), type));
                }
                else {
                    mem.removeOffHeap(offHeapPointer());

                    offHeapPointer(0);
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to deserialize value [entry=" + this + ", val=" + val + ']');

                throw new IgniteException(e);
            }
        }
    }

    /**
     * Isolated method to get length of IGFS block.
     *
     * @param val Value.
     * @param valBytes Value bytes.
     * @return Length of value.
     */
    private int valueLength0(@Nullable CacheObject val, @Nullable IgniteBiTuple<byte[], Byte> valBytes) {
        byte[] bytes = val != null ? (byte[])val.value(cctx.cacheObjectContext(), false) : null;

        if (bytes != null)
            return bytes.length;

        if (valBytes == null)
            return 0;

        return valBytes.get1().length - (((valBytes.get2() == CacheObject.TYPE_BYTE_ARR) ? 0 : 6));
    }

    /**
     * @return Value bytes.
     */
    protected CacheObject valueBytesUnlocked() {
        assert Thread.holdsLock(this);

        CacheObject val0 = val;

        if (val0 == null && hasOffHeapPointer()) {
            IgniteBiTuple<byte[], Byte> t = valueBytes0();

            return cctx.cacheObjects().toCacheObject(cctx.cacheObjectContext(), t.get2(), t.get1());
        }

        return val0;
    }

    /** {@inheritDoc} */
    @Override public int memorySize() throws IgniteCheckedException {
        byte[] kb;
        byte[] vb = null;

        int extrasSize;

        synchronized (this) {
            key.prepareMarshal(cctx.cacheObjectContext());

            kb = key.valueBytes(cctx.cacheObjectContext());

            if (val != null) {
                val.prepareMarshal(cctx.cacheObjectContext());

                vb = val.valueBytes(cctx.cacheObjectContext());
            }

            extrasSize = extrasSize();
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
    public boolean isStartVersion() {
        return ver.nodeOrder() == cctx.localNode().order() && ver.order() == startVer;
    }

    /** {@inheritDoc} */
    @Override public boolean valid(AffinityTopologyVersion topVer) {
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
    @Nullable @Override public GridCacheEntryInfo info() {
        GridCacheEntryInfo info = null;

        long time = U.currentTimeMillis();

        synchronized (this) {
            if (!obsolete()) {
                info = new GridCacheEntryInfo();

                info.key(key);
                info.cacheId(cctx.cacheId());

                long expireTime = expireTimeExtras();

                boolean expired = expireTime != 0 && expireTime <= time;

                info.ttl(ttlExtras());
                info.expireTime(expireTime);
                info.version(ver);
                info.setNew(isStartVersion());
                info.setDeleted(deletedUnlocked());

                if (!expired)
                    info.value(valueBytesUnlocked());
            }
        }

        return info;
    }

    /** {@inheritDoc} */
    @Override public boolean onOffheapEvict(byte[] entry, GridCacheVersion evictVer, GridCacheVersion obsoleteVer)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert cctx.swap().offHeapEnabled() && (cctx.swap().swapEnabled() || cctx.queries().enabled()) : this;

        boolean obsolete;

        synchronized (this) {
            checkObsolete();

            if (hasReaders() || !isStartVersion())
                return false;

            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc != null && !mvcc.isEmpty(obsoleteVer))
                return false;

            if (cctx.swap().onOffheapEvict(key, entry, partition(), evictVer)) {
                assert !hasValueUnlocked() : this;

                if (log.isTraceEnabled()) {
                    log.trace("onOffheapEvict evicted [key=" + key +
                        ", entry=" + System.identityHashCode(this) +
                        ", ptr=" + offHeapPointer() +
                        ']');
                }

                obsolete = markObsolete0(obsoleteVer, false, null);

                assert obsolete : this;

                if (!cctx.swap().swapEnabled()) {
                    CacheObject val = cctx.swap().unmarshalSwapEntryValue(entry);

                    clearIndex(val);
                }
            }
            else {
                obsolete = false;

                if (log.isTraceEnabled()) {
                    log.trace("onOffheapEvict not evicted [key=" + key +
                        ", entry=" + System.identityHashCode(this) +
                        ", ptr=" + offHeapPointer() +
                        ']');
                }
            }
        }

        if (obsolete)
            onMarkedObsolete();

        return obsolete;
    }

    /** {@inheritDoc} */
    @Override public final CacheObject unswap() throws IgniteCheckedException, GridCacheEntryRemovedException {
        return unswap(true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public final CacheObject unswap(boolean needVal)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        return unswap(needVal, true);
    }

    /**
     * Unswaps an entry.
     *
     * @param needVal If {@code false} then do not to deserialize value during unswap.
     * @param checkExpire If {@code true} checks for expiration, as result entry can be obsoleted or marked deleted.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    @Nullable protected CacheObject unswap(boolean needVal, boolean checkExpire)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        if (!cctx.isSwapOrOffheapEnabled())
            return null;

        assert !detached() : this;

        boolean obsolete = false;
        boolean deferred = false;
        GridCacheVersion ver0 = null;

        synchronized (this) {
            checkObsolete();

            if (isStartVersion() && ((flags & IS_UNSWAPPED_MASK) == 0)) {
                GridCacheSwapEntry e;

                if (cctx.offheapTiered()) {
                    e = cctx.swap().readOffheapPointer(this);

                    if (log.isTraceEnabled()) {
                        if (e != null) {
                            log.trace("Read offheap pointer [key=" + key +
                                ", entry=" + System.identityHashCode(this) +
                                ", ptr=" + e.offheapPointer() +
                                ']');
                        }
                        else {
                            log.trace("Read offheap pointer [key=" + key +
                                ", entry=" + System.identityHashCode(this) +
                                ", val=" + null +
                                ']');
                        }
                    }

                    if (e != null) {
                        if (e.offheapPointer() > 0) {
                            offHeapPointer(e.offheapPointer());

                            flags |= IS_OFFHEAP_PTR_MASK;

                            if (needVal) {
                                CacheObject val = cctx.fromOffheap(e.offheapPointer(), false);

                                e.value(val);
                            }
                        }
                        else // Read from swap.
                            offHeapPointer(0);
                    }
                }
                else {
                    e = cctx.swap().readAndRemove(this);

                    if (log.isTraceEnabled()) {
                        log.trace("unswap readAndRemove [key=" + key +
                            ", entry=" + System.identityHashCode(this) +
                            ", found=" + (e != null) +
                            ']');
                    }
                }

                flags |= IS_UNSWAPPED_MASK;

                // If there is a value.
                if (e != null) {
                    long delta = checkExpire ? (e.expireTime() == 0 ? 0 : e.expireTime() - U.currentTimeMillis()) : 0;

                    CacheObject val = e.value();

                    val = cctx.kernalContext().cacheObjects().prepareForCache(val, cctx);

                    // Set unswapped value.
                    update(val, e.expireTime(), e.ttl(), e.version(), false);

                    // Must update valPtr again since update() will reset it.
                    if (cctx.offheapTiered() && e.offheapPointer() > 0)
                        offHeapPointer(e.offheapPointer());

                    if (delta >= 0)
                        return val;
                    else {
                        CacheObject expiredVal = rawGetOrUnmarshal(false);

                        if (onExpired(expiredVal, null)) {
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
     * @throws IgniteCheckedException If failed.
     */
    private void swap() throws IgniteCheckedException {
        boolean swapNeeded = (flags & IS_SWAPPING_REQUIRED) != 0;

        if (cctx.isSwapOrOffheapEnabled() && !deletedUnlocked() && (hasValueUnlocked() || swapNeeded) && !detached()) {
            assert Thread.holdsLock(this);

            boolean offheapPtr = hasOffHeapPointer();

            if (cctx.offheapTiered() && offheapPtr && !swapNeeded) {
                if (cctx.swap().offheapEvictionEnabled()) {
                    if (log.isTraceEnabled()) {
                        log.trace("enableOffheapEviction [key=" + key +
                            ", entry=" + System.identityHashCode(this) +
                            ", ptr=" + offHeapPointer() +
                            ']');
                    }

                    cctx.swap().enableOffheapEviction(key(), partition());
                }
                else {
                    if (log.isTraceEnabled())
                        log.trace("Value did not change, skip write swap entry: " + this);
                }

                return;
            }

            IgniteUuid valClsLdrId = null;
            IgniteUuid keyClsLdrId = null;

            if (cctx.deploymentEnabled()) {
                if (val != null) {
                    valClsLdrId = cctx.deploy().getClassLoaderId(
                        U.detectObjectClassLoader(val.value(cctx.cacheObjectContext(), false)));
                }

                keyClsLdrId = cctx.deploy().getClassLoaderId(
                    U.detectObjectClassLoader(keyValue(false)));
            }

            IgniteBiTuple<byte[], Byte> valBytes = valueBytes0();

            if (log.isTraceEnabled()) {
                log.trace("writeToOffheap [key=" + key +
                    ", entry=" + System.identityHashCode(this) +
                    ", ptr=" + offHeapPointer() +
                    ']');
            }

            cctx.swap().write(key(),
                ByteBuffer.wrap(valBytes.get1()),
                valBytes.get2(),
                ver,
                ttlExtras(),
                expireTimeExtras(),
                keyClsLdrId,
                valClsLdrId,
                !offheapPtr);

            flags &= ~IS_SWAPPING_REQUIRED;
        }
    }

    /**
     * @return Value bytes and flag indicating whether value is byte array.
     */
    protected IgniteBiTuple<byte[], Byte> valueBytes0() {
        assert Thread.holdsLock(this);

        if (hasOffHeapPointer()) {
            assert isOffHeapValuesOnly() || cctx.offheapTiered();

            return cctx.unsafeMemory().get(offHeapPointer());
        }
        else {
            assert val != null;

            try {
                byte[] bytes = val.valueBytes(cctx.cacheObjectContext());

                return new IgniteBiTuple<>(bytes, val.cacheObjectType());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    protected final void releaseSwap() throws IgniteCheckedException {
        if (cctx.isSwapOrOffheapEnabled()) {
            assert Thread.holdsLock(this);

            if (log.isTraceEnabled()) {
                log.trace("removeFromSwap [key=" + key +
                    ", entry=" + System.identityHashCode(this) +
                    ", ptr=" + offHeapPointer() +
                    ']');
            }

            cctx.swap().remove(key(), partition());
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
    @Nullable @Override public final CacheObject innerGet(@Nullable GridCacheVersion ver,
        @Nullable IgniteInternalTx tx,
        boolean readSwap,
        boolean readThrough,
        boolean updateMetrics,
        boolean evt,
        boolean tmp,
        UUID subjId,
        Object transformClo,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expirePlc,
        boolean keepBinary)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        return (CacheObject)innerGet0(ver,
            tx,
            readSwap,
            readThrough,
            evt,
            updateMetrics,
            tmp,
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
    @Override public EntryGetResult innerGetAndReserveForLoad(boolean readSwap,
        boolean updateMetrics,
        boolean evt,
        UUID subjId,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean keepBinary,
        @Nullable ReaderArguments readerArgs) throws IgniteCheckedException, GridCacheEntryRemovedException {
        return (EntryGetResult)innerGet0(
            /*ver*/null,
            /*tx*/null,
            readSwap,
            /*readThrough*/false,
            evt,
            updateMetrics,
            /*tmp*/false,
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
        boolean readSwap,
        boolean unmarshal,
        boolean updateMetrics,
        boolean evt,
        UUID subjId,
        Object transformClo,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean keepBinary,
        @Nullable ReaderArguments readerArgs)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        return (EntryGetResult)innerGet0(ver,
            tx,
            readSwap,
            false,
            evt,
            updateMetrics,
            false,
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
        boolean readSwap,
        boolean readThrough,
        boolean evt,
        boolean updateMetrics,
        boolean tmp,
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

        synchronized (this) {
            checkObsolete();

            boolean valid = valid(tx != null ? tx.topologyVersion() : cctx.affinity().affinityTopologyVersion());

            CacheObject val;

            if (valid) {
                val = rawGetOrUnmarshalUnlocked(tmp);

                if (val == null && readSwap) {
                    if (isStartVersion()) {
                        if (tmp) {
                            unswap(false, false);

                            val = rawGetOrUnmarshalUnlocked(true);
                        }
                        else
                            val = unswap(true, false);
                    }
                }

                if (val != null) {
                    long expireTime = expireTimeExtras();

                    if (expireTime > 0 && (expireTime - U.currentTimeMillis() <= 0)) {
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
                if (updateMetrics && cctx.cache().configuration().isStatisticsEnabled())
                    cctx.cache().metrics0().onRead(false);
            }
            else {
                if (updateMetrics && cctx.cache().configuration().isStatisticsEnabled())
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
                assert tmp || !(ret instanceof BinaryObjectOffheapImpl);
                assert !obsolete;
                assert !deferred;

                // If return value is consistent, then done.
                res = retVer ? new EntryGetResult(ret, resVer, false) : ret;
            }
            else if (reserveForLoad && !obsolete) {
                assert !readThrough;
                assert retVer;

                boolean reserve = !evictionDisabled();

                if (reserve)
                    flags |= IS_EVICT_DISABLED;

                res = new EntryGetResult(null, resVer, reserve);
            }
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

        synchronized (this) {
            long ttl = ttlExtras();

            // If version matched, set value.
            if (startVer.equals(ver)) {
                if (ret != null) {
                    // Detach value before index update.
                    ret = cctx.kernalContext().cacheObjects().prepareForCache(ret, cctx);

                    nextVer = nextVer != null ? nextVer : nextVersion();

                    CacheObject prevVal = rawGetOrUnmarshalUnlocked(false);

                    long expTime = CU.toExpireTime(ttl);

                    // Update indexes before actual write to entry.
                    updateIndex(ret, expTime, nextVer, prevVal);

                    boolean hadValPtr = hasOffHeapPointer();

                    update(ret, expTime, ttl, nextVer, true);

                    if (hadValPtr && cctx.offheapTiered()) {
                        if (log.isTraceEnabled()) {
                            log.trace("innerGet removeOffheap [key=" + key +
                                ", entry=" + System.identityHashCode(this) +
                                ", ptr=" + offHeapPointer() + ']');
                        }

                        cctx.swap().removeOffheap(key);
                    }

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

        assert ret == null || !retVer;
        assert tmp || !(ret instanceof BinaryObjectOffheapImpl);

        return ret;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "TooBroadScope"})
    @Nullable @Override public final CacheObject innerReload()
        throws IgniteCheckedException, GridCacheEntryRemovedException {
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
        CacheObject ret = cctx.toCacheObject(readThrough(null, key, true, cctx.localNodeId(), taskName));

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
                    CacheObject old = rawGetOrUnmarshalUnlocked(false);

                    if (log.isTraceEnabled()) {
                        log.trace("innerReload releaseSwap [key=" + key +
                            ", entry=" + System.identityHashCode(this) +
                            ", old=" + old +
                            ", ptr=" + offHeapPointer() + ']');
                    }

                    releaseSwap();

                    long expTime = CU.toExpireTime(ttl);

                    // Detach value before index update.
                    ret = cctx.kernalContext().cacheObjects().prepareForCache(ret, cctx);

                    // Update indexes.
                    if (ret != null) {
                        updateIndex(ret, expTime, nextVer, old);

                        if (cctx.deferredDelete() && !isInternal() && !detached() && deletedUnlocked())
                            deletedUnlocked(false);
                    }
                    else {
                        clearIndex(old);

                        if (cctx.deferredDelete() && !isInternal() && !detached() && !deletedUnlocked())
                            deletedUnlocked(true);
                    }

                    update(ret, expTime, ttl, nextVer, true);

                    touch = true;

                    // If value was set - return, otherwise try again.
                    return ret;
                }
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
            return new GridCacheUpdateTxResult(false, null);

        final GridCacheVersion newVer;

        boolean intercept = cctx.config().getInterceptor() != null;

        Object key0 = null;
        Object val0 = null;

        long updateCntr0;

        synchronized (this) {
            checkObsolete();

            if (isNear()) {
                assert dhtVer != null;

                // It is possible that 'get' could load more recent value.
                if (!((GridNearCacheEntry)this).recordDhtVersion(dhtVer))
                    return new GridCacheUpdateTxResult(false, null);
            }

            assert tx == null || (!tx.local() && tx.onePhaseCommit()) || tx.ownsLock(this) :
                "Transaction does not own lock for update [entry=" + this + ", tx=" + tx + ']';

            // Load and remove from swap if it is new.
            boolean startVer = isStartVersion();

            if (startVer)
                unswap(retval, false);

            newVer = explicitVer != null ? explicitVer : tx == null ?
                nextVersion() : tx.writeVersion();

            assert newVer != null : "Failed to get write version for tx: " + tx;

            boolean internal = isInternal() || !context().userCache();

            Map<UUID, CacheContinuousQueryListener> lsnrCol =
                notifyContinuousQueries(tx) ? cctx.continuousQueries().updateListeners(internal, false) : null;

            old = oldValPresent ? oldVal :
                (retval || intercept || lsnrCol != null) ?
                rawGetOrUnmarshalUnlocked(!retval && !isOffHeapValuesOnly()) : this.val;

            if (intercept) {
                val0 = cctx.unwrapBinaryIfNeeded(val, keepBinary, false);

                CacheLazyEntry e = new CacheLazyEntry(cctx, key, old, keepBinary);

                Object interceptorVal = cctx.config().getInterceptor().onBeforePut(
                    new CacheLazyEntry(cctx, key, old, keepBinary),
                    val0);

                key0 = e.key();

                if (interceptorVal == null)
                    return new GridCacheUpdateTxResult(false, (CacheObject)cctx.unwrapTemporary(old));
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

            // Update index inside synchronization since it can be updated
            // in load methods without actually holding entry lock.
            if (val != null) {
                updateIndex(val, expireTime, newVer, old);

                if (cctx.deferredDelete() && deletedUnlocked() && !isInternal() && !detached())
                    deletedUnlocked(false);
            }

            updateCntr0 = nextPartCounter(topVer);

            if (updateCntr != null && updateCntr != 0)
                updateCntr0 = updateCntr;

            update(val, expireTime, ttl, newVer, true);

            drReplicate(drType, val, newVer, topVer);

            recordNodeId(affNodeId, topVer);

            if (metrics && cctx.cache().configuration().isStatisticsEnabled())
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

        if (log.isDebugEnabled())
            log.debug("Updated cache entry [val=" + val + ", old=" + old + ", entry=" + this + ']');

        // Persist outside of synchronization. The correctness of the
        // value will be handled by current transaction.
        if (writeThrough)
            cctx.store().put(tx, key, val, newVer);

        if (intercept)
            cctx.config().getInterceptor().onAfterPut(new CacheLazyEntry(cctx, key, key0, val, val0, keepBinary, updateCntr0));

        return valid ? new GridCacheUpdateTxResult(true, retval ? old : null, updateCntr0) :
            new GridCacheUpdateTxResult(false, null);
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
            return new GridCacheUpdateTxResult(false, null);

        GridCacheVersion obsoleteVer = null;

        boolean intercept = cctx.config().getInterceptor() != null;

        IgniteBiTuple<Boolean, Object> interceptRes = null;

        CacheLazyEntry entry0 = null;

        Long updateCntr0;

        boolean deferred;

        boolean marked = false;

        synchronized (this) {
            checkObsolete();

            if (isNear()) {
                assert dhtVer != null;

                // It is possible that 'get' could load more recent value.
                if (!((GridNearCacheEntry)this).recordDhtVersion(dhtVer))
                    return new GridCacheUpdateTxResult(false, null);
            }

            assert tx == null || (!tx.local() && tx.onePhaseCommit()) || tx.ownsLock(this) :
                "Transaction does not own lock for remove[entry=" + this + ", tx=" + tx + ']';

            boolean startVer = isStartVersion();

            if (startVer) {
                if (log.isTraceEnabled()) {
                    log.trace("innerRemove releaseSwap [key=" + key +
                        ", entry=" + System.identityHashCode(this) +
                        ", ptr=" + offHeapPointer() +
                        ']');
                }

                // Release swap.
                releaseSwap();
            }

            newVer = explicitVer != null ? explicitVer : tx == null ? nextVersion() : tx.writeVersion();

            boolean internal = isInternal() || !context().userCache();

            Map<UUID, CacheContinuousQueryListener> lsnrCol =
                notifyContinuousQueries(tx) ? cctx.continuousQueries().updateListeners(internal, false) : null;

            old = oldValPresent ? oldVal : (retval || intercept || lsnrCol != null) ?
                rawGetOrUnmarshalUnlocked(!retval && !isOffHeapValuesOnly()) : val;

            if (intercept) {
                entry0 = new CacheLazyEntry(cctx, key, old, keepBinary);

                interceptRes = cctx.config().getInterceptor().onBeforeRemove(entry0);

                if (cctx.cancelRemove(interceptRes)) {
                    CacheObject ret = cctx.toCacheObject(cctx.unwrapTemporary(interceptRes.get2()));

                    return new GridCacheUpdateTxResult(false, ret);
                }
            }

            if (old == null)
                old = saveValueForIndexUnlocked();

            // Clear indexes inside of synchronization since indexes
            // can be updated without actually holding entry lock.
            clearIndex(old);

            boolean hadValPtr = hasOffHeapPointer();

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

            updateCntr0 = nextPartCounter(topVer);

            if (updateCntr != null && updateCntr != 0)
                updateCntr0 = updateCntr;

            drReplicate(drType, null, newVer, topVer);

            if (metrics && cctx.cache().configuration().isStatisticsEnabled())
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

            if (intercept) {
                entry0.updateCounter(updateCntr0);

                cctx.config().getInterceptor().onAfterRemove(entry0);
            }

            if (cctx.offheapTiered() && hadValPtr) {
                boolean rmv = cctx.swap().removeOffheap(key);

                if (log.isTraceEnabled()) {
                    log.trace("innerRemove remove offheap [key=" + key +
                        ", entry=" + System.identityHashCode(this) +
                        ", rmv=" + rmv +
                        ", ptr=" + offHeapPointer() +
                        ']');
                }

                assert rmv;
            }

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

        if (deferred)
            cctx.onDeferredDelete(this, newVer);

        if (marked) {
            assert !deferred;

            onMarkedObsolete();
        }

        if (valid) {
            CacheObject ret;

            if (interceptRes != null)
                ret = cctx.toCacheObject(cctx.unwrapTemporary(interceptRes.get2()));
            else
                ret = old;

            return new GridCacheUpdateTxResult(true, ret, updateCntr0);
        }
        else
            return new GridCacheUpdateTxResult(false, null);
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

        synchronized (this) {
            boolean internal = isInternal() || !context().userCache();

            Map<UUID, CacheContinuousQueryListener> lsnrCol =
                cctx.continuousQueries().updateListeners(internal, false);

            boolean needVal = retval ||
                intercept ||
                op == GridCacheOperation.TRANSFORM ||
                !F.isEmpty(filter) ||
                lsnrCol != null;

            checkObsolete();

            // Load and remove from swap if it is new.
            if (isNew())
                unswap(retval, false);

            // Possibly get old value form store.
            old = needVal ? rawGetOrUnmarshalUnlocked(!retval && !isOffHeapValuesOnly()) : val;

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
                    updateIndex(old, expireTime, ver, null);
                else
                    clearIndex(null);

                update(old, expireTime, ttl, ver, true);
            }

            // Apply metrics.
            if (metrics && cctx.cache().configuration().isStatisticsEnabled() && needVal) {
                // PutIfAbsent methods mustn't update hit/miss statistics
                if (op != GridCacheOperation.UPDATE || F.isEmpty(filter) || !cctx.putIfAbsentFilter(filter))
                    cctx.cache().metrics0().onRead(old != null);
            }

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

            boolean hasValPtr = false;

            // Try write-through.
            if (op == GridCacheOperation.UPDATE) {
                // Detach value before index update.
                updated = cctx.kernalContext().cacheObjects().prepareForCache(updated, cctx);

                if (writeThrough)
                    // Must persist inside synchronization in non-tx mode.
                    cctx.store().put(null, key, updated, ver);

                // Update index inside synchronization since it can be updated
                // in load methods without actually holding entry lock.
                updateIndex(updated, expireTime, ver, old);

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

                hasValPtr = hasOffHeapPointer();

                if (old == null)
                    old = saveValueForIndexUnlocked();

                // Update index inside synchronization since it can be updated
                // in load methods without actually holding entry lock.
                clearIndex(old);

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
                long updateCntr = nextPartCounter(AffinityTopologyVersion.NONE);

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
            }

            cctx.dataStructures().onEntryUpdated(key, op == GridCacheOperation.DELETE, keepBinary);

            if (intercept) {
                if (op == GridCacheOperation.UPDATE)
                    cctx.config().getInterceptor().onAfterPut(new CacheLazyEntry(cctx, key, key0, updated, updated0, keepBinary, 0L));
                else
                    cctx.config().getInterceptor().onAfterRemove(new CacheLazyEntry(cctx, key, key0, old, old0, keepBinary, 0L));
            }

            if (op != GridCacheOperation.UPDATE && cctx.offheapTiered() && hasValPtr) {
                boolean rmv = cctx.swap().removeOffheap(key);

                if (log.isTraceEnabled()) {
                    log.trace("innerUpdateLocal remove offheap [key=" + key +
                        ", entry=" + System.identityHashCode(this) +
                        ", rmv=" + rmv +
                        ", ptr=" + offHeapPointer() +
                        ']');
                }

                assert rmv;
            }
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
        GridCacheVersion newVer,
        final UUID evtNodeId,
        final UUID affNodeId,
        GridCacheOperation op,
        @Nullable Object writeObj,
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
        @Nullable GridCacheVersion conflictVer,
        final boolean conflictResolve,
        final boolean intercept,
        @Nullable final UUID subjId,
        final String taskName,
        @Nullable final CacheObject prevVal,
        @Nullable final Long updateCntr,
        @Nullable GridDhtAtomicAbstractUpdateFuture fut
    ) throws IgniteCheckedException, GridCacheEntryRemovedException, GridClosureException {
        assert cctx.atomic();

        boolean res = true;

        CacheObject oldVal;
        CacheObject updated;

        GridCacheVersion enqueueVer = null;

        GridCacheVersionConflictContext<?, ?> conflictCtx = null;

        IgniteBiTuple<Object, Exception> invokeRes = null;

        // System TTL/ET which may have special values.
        long newSysTtl;
        long newSysExpireTime;

        // TTL/ET which will be passed to entry on update.
        long newTtl;
        long newExpireTime;

        Object key0 = null;
        Object updated0 = null;

        Long updateCntr0 = null;

        synchronized (this) {
            boolean internal = isInternal() || !context().userCache();

            Map<UUID, CacheContinuousQueryListener> lsnrs = cctx.continuousQueries().updateListeners(internal, false);

            boolean needVal = lsnrs != null || intercept || retval || op == GridCacheOperation.TRANSFORM
                || !F.isEmptyOrNulls(filter);

            checkObsolete();

            // Load and remove from swap if it is new.
            if (isStartVersion())
                unswap(retval, false);

            // Prepare old value.
            oldVal = needVal ? rawGetOrUnmarshalUnlocked(!retval && !isOffHeapValuesOnly()) : val;

            // Possibly read value from store.
            boolean readFromStore = false;

            Object old0 = null;

            if (readThrough && needVal && oldVal == null && (cctx.readThrough() &&
                (op == GridCacheOperation.TRANSFORM || cctx.loadPreviousValue()))) {
                old0 = readThrough(null, key, false, subjId, taskName);

                oldVal = cctx.toCacheObject(old0);

                readFromStore = true;

                // Detach value before index update.
                oldVal = cctx.kernalContext().cacheObjects().prepareForCache(oldVal, cctx);

                // Calculate initial TTL and expire time.
                long initTtl;
                long initExpireTime;

                if (expiryPlc != null && oldVal != null) {
                    IgniteBiTuple<Long, Long> initTtlAndExpireTime = initialTtlAndExpireTime(expiryPlc);

                    initTtl = initTtlAndExpireTime.get1();
                    initExpireTime = initTtlAndExpireTime.get2();
                }
                else {
                    initTtl = CU.TTL_ETERNAL;
                    initExpireTime = CU.EXPIRE_TIME_ETERNAL;
                }

                if (oldVal != null)
                    updateIndex(oldVal, initExpireTime, ver, null);
                else
                    clearIndex(null);

                update(oldVal, initExpireTime, initTtl, ver, true);

                if (deletedUnlocked() && oldVal != null && !isInternal())
                    deletedUnlocked(false);
            }

            Object transformClo = null;

            // Request-level conflict resolution is needed, i.e. we do not know who will win in advance.
            if (conflictResolve) {
                GridCacheVersion oldConflictVer = version().conflictVersion();

                // Cache is conflict-enabled.
                if (cctx.conflictNeedResolve()) {
                    GridCacheVersionedEntryEx newEntry;

                    GridTuple3<Long, Long, Boolean> expiration = ttlAndExpireTime(expiryPlc,
                        explicitTtl,
                        explicitExpireTime);

                    // Prepare old and new entries for conflict resolution.
                    GridCacheVersionedEntryEx oldEntry = versionedEntry(keepBinary);

                    if (op == GridCacheOperation.TRANSFORM) {
                        transformClo = writeObj;

                        EntryProcessor<Object, Object, ?> entryProcessor = (EntryProcessor<Object, Object, ?>)writeObj;

                        oldVal = rawGetOrUnmarshalUnlocked(true);

                        CacheInvokeEntry<Object, Object> entry = new CacheInvokeEntry(key, oldVal, version(),
                            keepBinary, this);

                        try {
                            Object computed = entryProcessor.process(entry, invokeArgs);

                            if (entry.modified())
                                writeObj = cctx.toCacheObject(cctx.unwrapTemporary(entry.getValue()));
                            else
                                writeObj = oldVal;

                            key0 = entry.key();

                            if (computed != null)
                                invokeRes = new IgniteBiTuple(cctx.unwrapTemporary(computed), null);
                        }
                        catch (Exception e) {
                            invokeRes = new IgniteBiTuple(null, e);

                            writeObj = oldVal;
                        }
                    }

                    newEntry = new GridCacheLazyPlainVersionedEntry<>(
                        cctx,
                        key,
                        (CacheObject)writeObj,
                        expiration.get1(),
                        expiration.get2(),
                        conflictVer != null ? conflictVer : newVer,
                        keepBinary);

                    // Resolve conflict.
                    conflictCtx = cctx.conflictResolve(oldEntry, newEntry, verCheck);

                    assert conflictCtx != null;

                    boolean ignoreTime = cctx.config().getAtomicWriteOrderMode() == CacheAtomicWriteOrderMode.PRIMARY;

                    // Use old value?
                    if (conflictCtx.isUseOld()) {
                        GridCacheVersion newConflictVer = conflictVer != null ? conflictVer : newVer;

                        // Handle special case with atomic comparator.
                        if (!isNew() &&                                                                       // Not initial value,
                            verCheck &&                                                                       // and atomic version check,
                            oldConflictVer.dataCenterId() == newConflictVer.dataCenterId() &&                 // and data centers are equal,
                            ATOMIC_VER_COMPARATOR.compare(oldConflictVer, newConflictVer, ignoreTime) == 0 && // and both versions are equal,
                            cctx.writeThrough() &&                                                            // and store is enabled,
                            primary)                                                                          // and we are primary.
                        {
                            CacheObject val = rawGetOrUnmarshalUnlocked(false);

                            if (val == null) {
                                assert deletedUnlocked();

                                cctx.store().remove(null, key);
                            }
                            else
                                cctx.store().put(null, key, val, ver);
                        }

                        return new GridCacheUpdateAtomicResult(false,
                            retval ? rawGetOrUnmarshalUnlocked(false) : null,
                            null,
                            invokeRes,
                            CU.TTL_ETERNAL,
                            CU.EXPIRE_TIME_ETERNAL,
                            null,
                            null,
                            false,
                            updateCntr0 == null ? 0 : updateCntr0);
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
                }
                else
                    // Nullify conflict version on this update, so that we will use regular version during next updates.
                    conflictVer = null;
            }

            boolean ignoreTime = cctx.config().getAtomicWriteOrderMode() == CacheAtomicWriteOrderMode.PRIMARY;

            // Perform version check only in case there was no explicit conflict resolution.
            if (conflictCtx == null) {
                if (verCheck) {
                    if (!isNew() && ATOMIC_VER_COMPARATOR.compare(ver, newVer, ignoreTime) >= 0) {
                        if (ATOMIC_VER_COMPARATOR.compare(ver, newVer, ignoreTime) == 0 && cctx.writeThrough() && primary) {
                            if (log.isDebugEnabled())
                                log.debug("Received entry update with same version as current (will update store) " +
                                    "[entry=" + this + ", newVer=" + newVer + ']');

                            CacheObject val = rawGetOrUnmarshalUnlocked(false);

                            if (val == null) {
                                assert deletedUnlocked();

                                cctx.store().remove(null, key);
                            }
                            else
                                cctx.store().put(null, key, val, ver);
                        }
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Received entry update with smaller version than current (will ignore) " +
                                    "[entry=" + this + ", newVer=" + newVer + ']');
                        }

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

                            updateCntr0 = nextPartCounter(topVer);

                            if (updateCntr != null)
                                updateCntr0 = updateCntr;

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

                        return new GridCacheUpdateAtomicResult(false,
                            retval ? rawGetOrUnmarshalUnlocked(false) : null,
                            null,
                            invokeRes,
                            CU.TTL_ETERNAL,
                            CU.EXPIRE_TIME_ETERNAL,
                            null,
                            null,
                            false,
                            0);
                    }
                }
                else
                    assert isNew() || ATOMIC_VER_COMPARATOR.compare(ver, newVer, ignoreTime) <= 0 :
                        "Invalid version for inner update [isNew=" + isNew() + ", entry=" + this + ", newVer=" + newVer + ']';
            }

            // Apply metrics.
            if (metrics && cctx.cache().configuration().isStatisticsEnabled() && needVal) {
                // PutIfAbsent methods mustn't update hit/miss statistics
                if (op != GridCacheOperation.UPDATE || F.isEmpty(filter) || !cctx.putIfAbsentFilter(filter))
                    cctx.cache().metrics0().onRead(oldVal != null);
            }

            // Check filter inside of synchronization.
            if (!F.isEmptyOrNulls(filter)) {
                boolean pass = cctx.isAllLocked(this, filter);

                if (!pass) {
                    if (expiryPlc != null && !readFromStore && hasValueUnlocked() && !cctx.putIfAbsentFilter(filter))
                        updateTtl(expiryPlc);

                    return new GridCacheUpdateAtomicResult(false,
                        retval ? oldVal : null,
                        null,
                        invokeRes,
                        CU.TTL_ETERNAL,
                        CU.EXPIRE_TIME_ETERNAL,
                        null,
                        null,
                        false,
                        updateCntr0 == null ? 0 : updateCntr0);
                }
            }

            // Calculate new value in case we met transform.
            if (op == GridCacheOperation.TRANSFORM) {
                assert conflictCtx == null : "Cannot be TRANSFORM here if conflict resolution was performed earlier.";

                transformClo = writeObj;

                EntryProcessor<Object, Object, ?> entryProcessor = (EntryProcessor<Object, Object, ?>)writeObj;

                CacheInvokeEntry<Object, Object> entry = new CacheInvokeEntry(key, oldVal, version(), keepBinary, this);

                try {
                    Object computed = entryProcessor.process(entry, invokeArgs);

                    if (entry.modified()) {
                        updated0 = cctx.unwrapTemporary(entry.getValue());
                        updated = cctx.toCacheObject(updated0);
                    }
                    else
                        updated = oldVal;

                    key0 = entry.key();

                    if (computed != null)
                        invokeRes = new IgniteBiTuple(cctx.unwrapTemporary(computed), null);
                }
                catch (Exception e) {
                    invokeRes = new IgniteBiTuple(null, e);

                    updated = oldVal;
                }

                if (!entry.modified()) {
                    if (expiryPlc != null && !readFromStore && hasValueUnlocked())
                        updateTtl(expiryPlc);

                    return new GridCacheUpdateAtomicResult(false,
                        retval ? oldVal : null,
                        null,
                        invokeRes,
                        CU.TTL_ETERNAL,
                        CU.EXPIRE_TIME_ETERNAL,
                        null,
                        null,
                        false,
                        updateCntr0 == null ? 0 : updateCntr0);
                }
            }
            else
                updated = (CacheObject)writeObj;

            op = updated == null ? GridCacheOperation.DELETE : GridCacheOperation.UPDATE;

            assert op == GridCacheOperation.UPDATE || (op == GridCacheOperation.DELETE && updated == null);

            boolean hadVal = hasValueUnlocked();

            // Incorporate conflict version into new version if needed.
            if (conflictVer != null && conflictVer != newVer)
                newVer = new GridCacheVersionEx(newVer.topologyVersion(),
                    newVer.globalTime(),
                    newVer.order(),
                    newVer.nodeOrder(),
                    newVer.dataCenterId(),
                    conflictVer);

            if (op == GridCacheOperation.UPDATE) {
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
                            hadVal ? expiryPlc.forUpdate() : expiryPlc.forCreate();

                        if (newSysTtl == CU.TTL_NOT_CHANGED) {
                            newSysExpireTime = CU.EXPIRE_TIME_CALCULATE;
                            newTtl = ttlExtras();
                            newExpireTime = expireTimeExtras();
                        }
                        else if (newSysTtl == CU.TTL_ZERO) {
                            op = GridCacheOperation.DELETE;

                            newSysTtl = CU.TTL_NOT_CHANGED;
                            newSysExpireTime = CU.EXPIRE_TIME_CALCULATE;

                            newTtl = CU.TTL_ETERNAL;
                            newExpireTime = CU.EXPIRE_TIME_ETERNAL;

                            updated = null;
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
            }
            else {
                assert op == GridCacheOperation.DELETE;

                newSysTtl = CU.TTL_NOT_CHANGED;
                newSysExpireTime = CU.EXPIRE_TIME_CALCULATE;

                newTtl = CU.TTL_ETERNAL;
                newExpireTime = CU.EXPIRE_TIME_ETERNAL;
            }

            // TTL and expire time must be resolved at this point.
            assert newTtl != CU.TTL_NOT_CHANGED && newTtl != CU.TTL_ZERO && newTtl >= 0;
            assert newExpireTime != CU.EXPIRE_TIME_CALCULATE && newExpireTime >= 0;

            IgniteBiTuple<Boolean, Object> interceptRes = null;

            boolean hasValPtr = false;

            // Actual update.
            if (op == GridCacheOperation.UPDATE) {
                if (log.isTraceEnabled()) {
                    log.trace("innerUpdate [key=" + key +
                        ", entry=" + System.identityHashCode(this) +
                        ", ptr=" + offHeapPointer() +
                        ']');
                }

                if (intercept) {
                    updated0 = value(updated0, updated, keepBinary, false);

                    Object interceptorVal = cctx.config().getInterceptor()
                        .onBeforePut(new CacheLazyEntry(cctx, key, key0, oldVal, old0, keepBinary), updated0);

                    if (interceptorVal == null)
                        return new GridCacheUpdateAtomicResult(false,
                            retval ? oldVal : null,
                            null,
                            invokeRes,
                            CU.TTL_ETERNAL,
                            CU.EXPIRE_TIME_ETERNAL,
                            null,
                            null,
                            false,
                            updateCntr0 == null ? 0 : updateCntr0);
                    else if (interceptorVal != updated0) {
                        updated0 = cctx.unwrapTemporary(interceptorVal);

                        updated = cctx.toCacheObject(updated0);
                    }
                }

                // Try write-through.
                if (writeThrough)
                    // Must persist inside synchronization in non-tx mode.
                    cctx.store().put(null, key, updated, newVer);

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

                    // Do not change size.
                }

                updated = cctx.kernalContext().cacheObjects().prepareForCache(updated, cctx);

                // Update index inside synchronization since it can be updated
                // in load methods without actually holding entry lock.
                updateIndex(updated, newExpireTime, newVer, oldVal);

                update(updated, newExpireTime, newTtl, newVer, true);

                updateCntr0 = nextPartCounter(topVer);

                if (updateCntr != null)
                    updateCntr0 = updateCntr;

                drReplicate(drType, updated, newVer, topVer);

                recordNodeId(affNodeId, topVer);

                if (evt) {
                    CacheObject evtOld = null;

                    if (transformClo != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ)) {
                        evtOld = cctx.unwrapTemporary(oldVal);

                        transformClo = EntryProcessorResourceInjectorProxy.unwrap(transformClo);

                        cctx.events().addEvent(partition(), key, evtNodeId, null,
                            newVer, EVT_CACHE_OBJECT_READ, evtOld, evtOld != null || hadVal, evtOld,
                            evtOld != null || hadVal, subjId, transformClo.getClass().getName(), taskName,
                            keepBinary);
                    }

                    if (newVer != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_PUT)) {
                        if (evtOld == null)
                            evtOld = cctx.unwrapTemporary(oldVal);

                        cctx.events().addEvent(partition(), key, evtNodeId, null,
                            newVer, EVT_CACHE_OBJECT_PUT, updated, updated != null, evtOld,
                            evtOld != null || hadVal, subjId, null, taskName, keepBinary);
                    }
                }
            }
            else {
                if (intercept) {
                    interceptRes = cctx.config().getInterceptor().onBeforeRemove(new CacheLazyEntry(cctx, key, key0,
                        oldVal, old0, keepBinary, updateCntr0));

                    if (cctx.cancelRemove(interceptRes))
                        return new GridCacheUpdateAtomicResult(false,
                            cctx.toCacheObject(cctx.unwrapTemporary(interceptRes.get2())),
                            null,
                            invokeRes,
                            CU.TTL_ETERNAL,
                            CU.EXPIRE_TIME_ETERNAL,
                            null,
                            null,
                            false,
                            updateCntr0 == null ? 0 : updateCntr0);
                }

                if (writeThrough)
                    // Must persist inside synchronization in non-tx mode.
                    cctx.store().remove(null, key);

                if (oldVal == null)
                    oldVal = saveValueForIndexUnlocked();

                // Update index inside synchronization since it can be updated
                // in load methods without actually holding entry lock.
                clearIndex(oldVal);

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

                hasValPtr = hasOffHeapPointer();

                // Clear value on backup. Entry will be removed from cache when it got evicted from queue.
                update(null, CU.TTL_ETERNAL, CU.EXPIRE_TIME_ETERNAL, newVer, true);

                assert newSysTtl == CU.TTL_NOT_CHANGED;
                assert newSysExpireTime == CU.EXPIRE_TIME_CALCULATE;

                clearReaders();

                recordNodeId(affNodeId, topVer);

                updateCntr0 = nextPartCounter(topVer);

                if (updateCntr != null)
                    updateCntr0 = updateCntr;

                drReplicate(drType, null, newVer, topVer);

                if (evt) {
                    CacheObject evtOld = null;

                    if (transformClo != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ)) {
                        evtOld = cctx.unwrapTemporary(oldVal);

                        transformClo = EntryProcessorResourceInjectorProxy.unwrap(transformClo);

                        cctx.events().addEvent(partition(), key, evtNodeId, null,
                            newVer, EVT_CACHE_OBJECT_READ, evtOld, evtOld != null || hadVal, evtOld,
                            evtOld != null || hadVal, subjId, transformClo.getClass().getName(), taskName,
                            keepBinary);
                    }

                    if (newVer != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_REMOVED)) {
                        if (evtOld == null)
                            evtOld = cctx.unwrapTemporary(oldVal);

                        cctx.events().addEvent(partition(), key, evtNodeId, null, newVer,
                            EVT_CACHE_OBJECT_REMOVED, null, false, evtOld, evtOld != null || hadVal,
                            subjId, null, taskName, keepBinary);
                    }
                }

                res = hadVal;
            }

            if (res)
                updateMetrics(op, metrics);

            // Continuous query filter should be perform under lock.
            if (lsnrs != null) {
                CacheObject evtVal = cctx.unwrapTemporary(updated);
                CacheObject evtOldVal = cctx.unwrapTemporary(oldVal);

                cctx.continuousQueries().onEntryUpdated(lsnrs, key, evtVal, evtOldVal, internal,
                    partition(), primary, false, updateCntr0, fut, topVer);
            }

            cctx.dataStructures().onEntryUpdated(key, op == GridCacheOperation.DELETE, keepBinary);

            if (intercept) {
                if (op == GridCacheOperation.UPDATE)
                    cctx.config().getInterceptor().onAfterPut(new CacheLazyEntry(
                        cctx,
                        key,
                        key0,
                        updated,
                        updated0,
                        keepBinary,
                        updateCntr0));
                else
                    cctx.config().getInterceptor().onAfterRemove(new CacheLazyEntry(
                        cctx,
                        key,
                        key0,
                        oldVal,
                        old0,
                        keepBinary,
                        updateCntr0));

                if (interceptRes != null)
                    oldVal = cctx.toCacheObject(cctx.unwrapTemporary(interceptRes.get2()));
            }

            if (op != GridCacheOperation.UPDATE && cctx.offheapTiered() && hasValPtr) {
                boolean rmv = cctx.swap().removeOffheap(key);

                if (log.isTraceEnabled()) {
                    log.trace("innerUpdate remove offheap [key=" + key +
                        ", entry=" + System.identityHashCode(this) +
                        ", rmv=" + rmv +
                        ", ptr=" + offHeapPointer() +
                        ']');
                }

                assert rmv;
            }
        }

        if (log.isDebugEnabled())
            log.debug("Updated cache entry [val=" + val + ", old=" + oldVal + ", entry=" + this + ']');

        return new GridCacheUpdateAtomicResult(res,
            oldVal,
            updated,
            invokeRes,
            newSysTtl,
            newSysExpireTime,
            enqueueVer,
            conflictCtx,
            true,
            updateCntr0 == null ? 0 : updateCntr0);
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
    private GridTuple3<Long, Long, Boolean> ttlAndExpireTime(IgniteCacheExpiryPolicy expiry, long ttl, long expireTime)
        throws GridCacheEntryRemovedException {
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
            if (isNew())
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
        synchronized (this) {
            if (obsolete())
                return false;

            CacheObject val = saveValueForIndexUnlocked();

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
                    ", val=" + val +
                    ", ptr=" + offHeapPointer() + ']');
            }

            clearIndex(val);

            releaseSwap();
        }

        onMarkedObsolete();

        cctx.cache().removeEntry(this); // Clear cache.

        return true;
    }

    /** {@inheritDoc} */
    @Override public synchronized GridCacheVersion obsoleteVersion() {
        return obsoleteVersionExtras();
    }

    /** {@inheritDoc} */
    @Override public boolean markObsolete(GridCacheVersion ver) {
        boolean obsolete;

        synchronized (this) {
            obsolete = markObsolete0(ver, true, null);
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

        try {
            synchronized (this) {
                if (obsoleteVersionExtras() != null)
                    return false;

                if (hasValueUnlocked()) {
                    long expireTime = expireTimeExtras();

                    if (expireTime > 0 && (expireTime - U.currentTimeMillis() <= 0)) {
                        if (onExpired(rawGetOrUnmarshal(false), obsoleteVer)) {
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
        }
        finally {
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

        synchronized (this) {
            if (obsoleteVersionExtras() != null)
                return true;

            if (!this.ver.equals(ver))
                return false;

            marked = markObsolete0(ver, true, null);
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
     * Note that {@link #onMarkedObsolete()} should always be called after this method
     * returns {@code true}.
     *
     * @param ver Version.
     * @param clear {@code True} to clear.
     * @param extras Predefined extras.
     * @return {@code True} if entry is obsolete, {@code false} if entry is still used by other threads or nodes.
     */
    protected final boolean markObsolete0(GridCacheVersion ver, boolean clear, GridCacheObsoleteEntryExtras extras) {
        assert Thread.holdsLock(this);

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
                        ", ptr=" + offHeapPointer() +
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
        throws IgniteCheckedException {
        assert newVer != null;

        if (curVer == null || ver.equals(curVer)) {
            CacheObject val = saveValueForIndexUnlocked();

            value(null);

            ver = newVer;
            flags &= ~IS_EVICT_DISABLED;

            if (log.isTraceEnabled()) {
                log.trace("invalidate releaseSwap [key=" + key +
                    ", entry=" + System.identityHashCode(this) +
                    ", val=" + val +
                    ", ptr=" + offHeapPointer() +
                    ']');
            }

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
    @Override public boolean invalidate(@Nullable CacheEntryPredicate[] filter)
        throws GridCacheEntryRemovedException, IgniteCheckedException {
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

            synchronized (this) {
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

    /**
     *
     * @param val New value.
     * @param expireTime Expiration time.
     * @param ttl Time to live.
     * @param ver Update version.
     */
    protected final void update(@Nullable CacheObject val, long expireTime, long ttl, GridCacheVersion ver, boolean addTracked) {
        assert ver != null;
        assert Thread.holdsLock(this);
        assert ttl != CU.TTL_ZERO && ttl != CU.TTL_NOT_CHANGED && ttl >= 0 : ttl;

        long oldExpireTime = expireTimeExtras();

        if (addTracked && oldExpireTime != 0 && (expireTime != oldExpireTime || isStartVersion()) && cctx.config().isEagerTtl())
            cctx.ttl().removeTrackedEntry(this);

        value(val);

        ttlAndExpireTimeExtras(ttl, expireTime);

        this.ver = ver;
        flags &= ~IS_EVICT_DISABLED;

        if (addTracked && expireTime != 0 && (expireTime != oldExpireTime || isStartVersion()) && cctx.config().isEagerTtl())
            cctx.ttl().addTrackedEntry(this);
    }

    /**
     * Update TTL if it is changed.
     *
     * @param expiryPlc Expiry policy.
     */
    private void updateTtl(ExpiryPolicy expiryPlc) {
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
    private void updateTtl(IgniteCacheExpiryPolicy expiryPlc) throws GridCacheEntryRemovedException {
        long ttl = expiryPlc.forAccess();

        if (ttl != CU.TTL_NOT_CHANGED) {
            updateTtl(ttl);

            expiryPlc.ttlUpdated(key(),
                version(),
                hasReaders() ? ((GridDhtCacheEntry)this).readers() : null);
        }
    }

    /**
     * @param ttl Time to live.
     */
    protected void updateTtl(long ttl) {
        assert ttl >= 0 || ttl == CU.TTL_ZERO : ttl;
        assert Thread.holdsLock(this);

        long expireTime;

        if (ttl == CU.TTL_ZERO) {
            ttl = CU.TTL_MINIMUM;
            expireTime = CU.expireTimeInPast();
        }
        else
            expireTime = CU.toExpireTime(ttl);

        long oldExpireTime = expireTimeExtras();

        if (oldExpireTime != 0 && expireTime != oldExpireTime && cctx.config().isEagerTtl())
            cctx.ttl().removeTrackedEntry(this);

        ttlAndExpireTimeExtras(ttl, expireTime);

        if (cctx.isSwapOrOffheapEnabled())
            flags |= IS_SWAPPING_REQUIRED;

        if (expireTime != 0 && expireTime != oldExpireTime && cctx.config().isEagerTtl())
            cctx.ttl().addTrackedEntry(this);
    }

    /**
     * @return {@code True} if values should be stored off-heap.
     */
    protected final boolean isOffHeapValuesOnly() {
        return cctx.config().getMemoryMode() == CacheMemoryMode.OFFHEAP_VALUES;
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
    @Override public KeyCacheObject key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxKey txKey() {
        return cctx.txKey(key);
    }

    /** {@inheritDoc} */
    @Override public synchronized GridCacheVersion version() throws GridCacheEntryRemovedException {
        checkObsolete();

        return ver;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean checkSerializableReadVersion(GridCacheVersion serReadVer)
        throws GridCacheEntryRemovedException {
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

    /**
     * Gets hash value for the entry key.
     *
     * @return Hash value.
     */
    int hash() {
        return hash;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject peek(boolean heap,
        boolean offheap,
        boolean swap,
        AffinityTopologyVersion topVer,
        @Nullable IgniteCacheExpiryPolicy expiryPlc)
        throws GridCacheEntryRemovedException, IgniteCheckedException {
        assert heap || offheap || swap;

        if (heap) {
            GridTuple<CacheObject> val = peekGlobal(topVer, null, expiryPlc);

            if (val != null)
                return val.get();
        }

        if (offheap || swap) {
            GridCacheSwapEntry e = cctx.swap().read(this, false, offheap, swap, true);

            return e != null ? e.value() : null;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject peek(
        boolean heap,
        boolean offheap,
        boolean swap,
        @Nullable IgniteCacheExpiryPolicy plc)
        throws GridCacheEntryRemovedException, IgniteCheckedException {
        IgniteInternalTx tx = cctx.tm().localTxx();

        AffinityTopologyVersion topVer = tx != null ? tx.topologyVersion() : cctx.affinity().affinityTopologyVersion();

        return peek(heap, offheap, swap, topVer, plc);
    }

    /**
     * @param topVer Topology version.
     * @param filter Filter.
     * @param expiryPlc Optional expiry policy.
     * @return Peeked value.
     * @throws GridCacheEntryRemovedException If entry got removed.
     * @throws IgniteCheckedException If unexpected cache failure occurred.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Nullable private GridTuple<CacheObject> peekGlobal(
        AffinityTopologyVersion topVer,
        CacheEntryPredicate[] filter,
        @Nullable IgniteCacheExpiryPolicy expiryPlc)
        throws GridCacheEntryRemovedException, IgniteCheckedException {
        if (!valid(topVer))
            return null;

        boolean rmv = false;

        try {
            while (true) {
                GridCacheVersion ver;
                CacheObject val;

                synchronized (this) {
                    if (checkExpired()) {
                        rmv = markObsolete0(cctx.versions().next(this.ver), true, null);

                        return null;
                    }

                    checkObsolete();

                    ver = this.ver;
                    val = rawGetOrUnmarshalUnlocked(false);

                    if (val != null && expiryPlc != null)
                        updateTtl(expiryPlc);
                }

                if (val == null)
                    return null;

                if (!cctx.isAll(this, filter))
                    return null;

                if (F.isEmptyOrNulls(filter) || ver.equals(version()))
                    return F.t(val);
            }
        }
        finally {
            if (rmv) {
                onMarkedObsolete();

                cctx.cache().removeEntry(this);
            }
        }
    }

    /**
     * TODO: GG-4009: do we need to generate event and invalidate value?
     *
     * @return {@code true} if expired.
     * @throws IgniteCheckedException In case of failure.
     */
    private boolean checkExpired() throws IgniteCheckedException {
        assert Thread.holdsLock(this);

        long expireTime = expireTimeExtras();

        if (expireTime > 0) {
            long delta = expireTime - U.currentTimeMillis();

            if (delta <= 0) {
                if (log.isTraceEnabled()) {
                    log.trace("checkExpired clear [key=" + key +
                        ", entry=" + System.identityHashCode(this) +
                        ", ptr=" + offHeapPointer() + ']');
                }

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
    @Override public synchronized CacheObject rawGet() {
        return val;
    }

    /** {@inheritDoc} */
    @Nullable @Override public synchronized CacheObject rawGetOrUnmarshal(boolean tmp) throws IgniteCheckedException {
        return rawGetOrUnmarshalUnlocked(tmp);
    }

    /**
     * @param tmp If {@code true} can return temporary instance.
     * @return Value (unmarshalled if needed).
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public CacheObject rawGetOrUnmarshalUnlocked(boolean tmp) throws IgniteCheckedException {
        assert Thread.holdsLock(this);

        CacheObject val = this.val;

        if (val != null)
            return val;

        if (hasOffHeapPointer()) {
            CacheObject val0 = cctx.fromOffheap(offHeapPointer(), tmp);

            if (!tmp && cctx.kernalContext().config().isPeerClassLoadingEnabled())
                val0.finishUnmarshal(cctx.cacheObjectContext(), cctx.deploy().globalLoader());

            return val0;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public final synchronized boolean hasValue() {
        return hasValueUnlocked();
    }

    /**
     * @return {@code True} if this entry has value.
     */
    protected final boolean hasValueUnlocked() {
        assert Thread.holdsLock(this);

        return val != null || hasOffHeapPointer();
    }

    /** {@inheritDoc} */
    @Override public synchronized CacheObject rawPut(CacheObject val, long ttl) {
        CacheObject old = this.val;

        update(val, CU.toExpireTime(ttl), ttl, nextVersion(), true);

        return old;
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
        boolean fromStore)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        synchronized (this) {
            checkObsolete();

            if ((isNew() && !cctx.swap().containsKey(key, partition())) || (!preload && deletedUnlocked())) {
                long expTime = expireTime < 0 ? CU.toExpireTime(ttl) : expireTime;

                val = cctx.kernalContext().cacheObjects().prepareForCache(val, cctx);

                if (val != null)
                    updateIndex(val, expTime, ver, null);

                // Version does not change for load ops.
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
                    updateCntr = nextPartCounter(topVer);

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

                if (!fromStore && cctx.store().isLocal()) {
                    if (val != null)
                        cctx.store().put(null, key, val, ver);
                }

                return true;
            }

            return false;
        }
    }

    /**
     * @param topVer Topology version.
     * @return Update counter.
     */
    private long nextPartCounter(AffinityTopologyVersion topVer) {
        long updateCntr;

        if (!cctx.isLocal() && !isNear()) {
            GridDhtLocalPartition locPart = cctx.topology().localPartition(partition(), topVer, false);

            if (locPart == null)
                return 0;

            updateCntr = locPart.nextUpdateCounter();
        }
        else
            updateCntr = 0;

        return updateCntr;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean initialValue(KeyCacheObject key, GridCacheSwapEntry unswapped) throws
        IgniteCheckedException,
        GridCacheEntryRemovedException {
        checkObsolete();

        if (isNew()) {
            CacheObject val = unswapped.value();

            val = cctx.kernalContext().cacheObjects().prepareForCache(val, cctx);

            // Version does not change for load ops.
            update(val,
                unswapped.expireTime(),
                unswapped.ttl(),
                unswapped.version(),
                true
            );

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public synchronized GridCacheVersionedEntryEx versionedEntry(final boolean keepBinary)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        boolean isNew = isStartVersion();

        CacheObject val = isNew ? unswap(true, false) : rawGetOrUnmarshalUnlocked(false);

        return new GridCacheLazyPlainVersionedEntry<>(cctx,
            key,
            val,
            ttlExtras(),
            expireTimeExtras(),
            ver.conflictVersion(),
            isNew,
            keepBinary);
    }

    /** {@inheritDoc} */
    @Override public synchronized void clearReserveForLoad(GridCacheVersion ver) throws IgniteCheckedException {
        if (obsoleteVersionExtras() != null)
            return;

        if (ver.equals(this.ver)) {
            assert evictionDisabled() : this;

            flags &= ~IS_EVICT_DISABLED;
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized T2<CacheObject, GridCacheVersion> versionedValue(CacheObject val,
        GridCacheVersion curVer,
        GridCacheVersion newVer,
        @Nullable IgniteCacheExpiryPolicy loadExpiryPlc,
        @Nullable ReaderArguments readerArgs)
        throws IgniteCheckedException, GridCacheEntryRemovedException {

        checkObsolete();

        addReaderIfNeed(readerArgs);

        if (curVer == null || curVer.equals(ver)) {
            if (val != this.val) {
                GridCacheMvcc mvcc = mvccExtras();

                if (mvcc != null && !mvcc.isEmpty())
                    return new T2<>(this.val, ver);

                if (newVer == null)
                    newVer = cctx.versions().next();

                CacheObject old = rawGetOrUnmarshalUnlocked(false);

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
                    updateIndex(val, expTime, newVer, old);

                    if (deletedUnlocked())
                        deletedUnlocked(false);
                }

                // Version does not change for load ops.
                update(val, expTime, ttl, newVer, true);

                return new T2<>(val, newVer);
            }

            assert !evictionDisabled() : this;
        }

        return new T2<>(this.val, ver);
    }

    /**
     * @param readerArgs Reader arguments
     */
    private void addReaderIfNeed(@Nullable ReaderArguments readerArgs) {
        if (readerArgs != null) {
            assert this instanceof GridDhtCacheEntry : this;
            assert Thread.holdsLock(this);

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
    @Override public synchronized boolean hasLockCandidate(GridCacheVersion ver) throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc mvcc = mvccExtras();

        return mvcc != null && mvcc.hasCandidate(ver);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean hasLockCandidate(long threadId) throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc mvcc = mvccExtras();

        return mvcc != null && mvcc.localCandidate(threadId) != null;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean lockedByAny(GridCacheVersion... exclude)
        throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc mvcc = mvccExtras();

        return mvcc != null && !mvcc.isEmpty(exclude);
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByThread() throws GridCacheEntryRemovedException {
        return lockedByThread(Thread.currentThread().getId());
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean lockedLocally(GridCacheVersion lockVer)
        throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc mvcc = mvccExtras();

        return mvcc != null && mvcc.isLocallyOwned(lockVer);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean lockedByThread(long threadId, GridCacheVersion exclude)
        throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc mvcc = mvccExtras();

        return mvcc != null && mvcc.isLocallyOwnedByThread(threadId, false, exclude);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean lockedLocallyByIdOrThread(GridCacheVersion lockVer, long threadId)
        throws GridCacheEntryRemovedException {
        GridCacheMvcc mvcc = mvccExtras();

        return mvcc != null && mvcc.isLocallyOwnedByIdOrThread(lockVer, threadId);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean lockedByThread(long threadId) throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc mvcc = mvccExtras();

        return mvcc != null && mvcc.isLocallyOwnedByThread(threadId, true);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean lockedBy(GridCacheVersion ver) throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc mvcc = mvccExtras();

        return mvcc != null && mvcc.isOwnedBy(ver);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean lockedByThreadUnsafe(long threadId) {
        GridCacheMvcc mvcc = mvccExtras();

        return mvcc != null && mvcc.isLocallyOwnedByThread(threadId, true);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean lockedByUnsafe(GridCacheVersion ver) {
        GridCacheMvcc mvcc = mvccExtras();

        return mvcc != null && mvcc.isOwnedBy(ver);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean lockedLocallyUnsafe(GridCacheVersion lockVer) {
        GridCacheMvcc mvcc = mvccExtras();

        return mvcc != null && mvcc.isLocallyOwned(lockVer);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean hasLockCandidateUnsafe(GridCacheVersion ver) {
        GridCacheMvcc mvcc = mvccExtras();

        return mvcc != null && mvcc.hasCandidate(ver);
    }

    /** {@inheritDoc} */
    @Override public synchronized Collection<GridCacheMvccCandidate> localCandidates(GridCacheVersion... exclude)
        throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc mvcc = mvccExtras();

        return mvcc == null ? Collections.<GridCacheMvccCandidate>emptyList() : mvcc.localCandidates(exclude);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheMvccCandidate> remoteMvccSnapshot(GridCacheVersion... exclude) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Nullable @Override public synchronized GridCacheMvccCandidate candidate(GridCacheVersion ver)
        throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc mvcc = mvccExtras();

        return mvcc == null ? null : mvcc.candidate(ver);
    }

    /** {@inheritDoc} */
    @Override public synchronized GridCacheMvccCandidate localCandidate(long threadId)
        throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc mvcc = mvccExtras();

        return mvcc == null ? null : mvcc.localCandidate(threadId);
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvccCandidate candidate(UUID nodeId, long threadId)
        throws GridCacheEntryRemovedException {
        boolean loc = cctx.nodeId().equals(nodeId);

        synchronized (this) {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            return mvcc == null ? null : loc ? mvcc.localCandidate(threadId) :
                mvcc.remoteCandidate(nodeId, threadId);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized GridCacheMvccCandidate localOwner() throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc mvcc = mvccExtras();

        return mvcc == null ? null : mvcc.localOwner();
    }

    /** {@inheritDoc} */
    @Override public synchronized long rawExpireTime() {
        return expireTimeExtras();
    }

    /** {@inheritDoc} */
    @Override public long expireTimeUnlocked() {
        assert Thread.holdsLock(this);

        return expireTimeExtras();
    }

    /** {@inheritDoc} */
    @Override public boolean onTtlExpired(GridCacheVersion obsoleteVer) throws GridCacheEntryRemovedException {
        assert obsoleteVer != null;

        boolean obsolete = false;
        boolean deferred = false;
        GridCacheVersion ver0 = null;

        try {
            synchronized (this) {
                checkObsolete();

                if (isStartVersion())
                    unswap(true, false);

                long expireTime = expireTimeExtras();

                if (expireTime == 0 || (expireTime - U.currentTimeMillis() > 0))
                    return false;

                CacheObject expiredVal = rawGetOrUnmarshal(false);

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
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to clean up expired cache entry: " + this, e);
        }
        finally {
            if (obsolete) {
                onMarkedObsolete();

                cctx.cache().removeEntry(this);
            }

            if (deferred) {
                assert ver0 != null;

                cctx.onDeferredDelete(this, ver0);
            }

            if ((obsolete || deferred) && cctx.cache().configuration().isStatisticsEnabled())
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

        if (log.isTraceEnabled()) {
            log.trace("onExpired clear [key=" + key +
                ", entry=" + System.identityHashCode(this) +
                ", ptr=" + offHeapPointer() + ']');
        }

        clearIndex(expiredVal);

        releaseSwap();

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
    @Override public synchronized long rawTtl() {
        return ttlExtras();
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

        synchronized (this) {
            checkObsolete();

            return expireTimeExtras();
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

        synchronized (this) {
            checkObsolete();

            return ttlExtras();
        }
    }

    /**
     * @return Current transaction.
     */
    private IgniteTxLocalAdapter currentTx() {
        if (cctx.isDht())
            return cctx.dht().near().context().tm().localTx();
        else
            return cctx.tm().localTx();
    }

    /** {@inheritDoc} */
    @Override public void updateTtl(@Nullable GridCacheVersion ver, long ttl) throws GridCacheEntryRemovedException {
        synchronized (this) {
            checkObsolete();

            if (hasValueUnlocked())
                updateTtl(ttl);

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
    }

    /** {@inheritDoc} */
    @Override public synchronized CacheObject valueBytes() throws GridCacheEntryRemovedException {
        checkObsolete();

        return valueBytesUnlocked();
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject valueBytes(@Nullable GridCacheVersion ver)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        CacheObject val = null;

        synchronized (this) {
            checkObsolete();

            if (ver == null || this.ver.equals(ver))
                val = valueBytesUnlocked();
        }

        return val;
    }

    /**
     * Updates cache index.
     *
     * @param val Value.
     * @param expireTime Expire time.
     * @param ver New entry version.
     * @param prevVal Previous value.
     * @throws IgniteCheckedException If update failed.
     */
    protected void updateIndex(@Nullable CacheObject val,
        long expireTime,
        GridCacheVersion ver,
        @Nullable CacheObject prevVal) throws IgniteCheckedException {
        assert Thread.holdsLock(this);
        assert val != null : "null values in update index for key: " + key;

        try {
            GridCacheQueryManager qryMgr = cctx.queries();

            if (qryMgr.enabled())
                qryMgr.store(key, val, ver, expireTime);
        }
        catch (IgniteCheckedException e) {
            throw new GridCacheIndexUpdateException(e);
        }
    }

    /**
     * Clears index.
     *
     * @param prevVal Previous value (if needed for index update).
     * @throws IgniteCheckedException If failed.
     */
    protected void clearIndex(CacheObject prevVal) throws IgniteCheckedException {
        assert Thread.holdsLock(this);

        try {
            GridCacheQueryManager<?, ?> qryMgr = cctx.queries();

            if (qryMgr.enabled())
                qryMgr.remove(key(), (CacheObject)cctx.unwrapTemporary(prevVal));
        }
        catch (IgniteCheckedException e) {
            throw new GridCacheIndexUpdateException(e);
        }
    }

    /**
     * This method will return current value only if clearIndex(V) will require previous value.
     * If previous value is not required, this method will return {@code null}.
     *
     * @return Previous value or {@code null}.
     * @throws IgniteCheckedException If failed to retrieve previous value.
     */
    protected final CacheObject saveValueForIndexUnlocked() throws IgniteCheckedException {
        return saveOldValueUnlocked(true);
    }

    /**
     * @param qryOnly If {@code true} reads old value only if query indexing is enabled.
     * @return Previous value or {@code null}.
     * @throws IgniteCheckedException If failed to retrieve previous value.
     */
    private CacheObject saveOldValueUnlocked(boolean qryOnly) throws IgniteCheckedException {
        assert Thread.holdsLock(this);

        if (qryOnly && !cctx.queries().enabled())
            return null;

        CacheObject val = rawGetOrUnmarshalUnlocked(false);

        if (val == null)
            val = cctx.swap().readValue(key, true, true);

        return val;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> Cache.Entry<K, V> wrap() {
        try {
            IgniteInternalTx tx = cctx.tm().userTx();

            CacheObject val;

            if (tx != null) {
                GridTuple<CacheObject> peek = tx.peek(cctx, false, key);

                val = peek == null ? rawGetOrUnmarshal(false) : peek.get();
            }
            else
                val = rawGetOrUnmarshal(false);

            return new CacheEntryImpl<>(key.<K>value(cctx.cacheObjectContext(), false),
                CU.<V>value(val, cctx, false), ver);
        }
        catch (GridCacheFilterFailedException ignored) {
            throw new IgniteException("Should never happen.");
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to wrap entry: " + this, e);
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> Cache.Entry<K, V> wrapLazyValue(boolean keepBinary) {
        CacheOperationContext opCtx = cctx.operationContextPerCall();

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
                    return e.peek(true, false, false, null);
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
    @Override public <K, V> EvictableEntry<K, V> wrapEviction() {
        return new CacheEvictableEntryImpl<>(this);
    }

    /** {@inheritDoc} */
    @Override public synchronized <K, V> CacheEntryImplEx<K, V> wrapVersioned() {
        return new CacheEntryImplEx<>(key.<K>value(cctx.cacheObjectContext(), false), null, ver);
    }

    /**
     * @return Entry which holds key, value and version.
     */
    private synchronized <K, V> CacheEntryImplEx<K, V> wrapVersionedWithValue() {
        V val = this.val == null ? null : this.val.<V>value(cctx.cacheObjectContext(), false);

        return new CacheEntryImplEx<>(key.<K>value(cctx.cacheObjectContext(), false), val, ver);
    }

    /** {@inheritDoc} */
    @Override public boolean evictInternal(boolean swap, GridCacheVersion obsoleteVer,
        @Nullable CacheEntryPredicate[] filter) throws IgniteCheckedException {
        boolean marked = false;

        try {
            if (F.isEmptyOrNulls(filter)) {
                synchronized (this) {
                    if (evictionDisabled()) {
                        assert !obsolete();

                        return false;
                    }

                    if (obsoleteVersionExtras() != null)
                        return true;

                    CacheObject prev = saveOldValueUnlocked(false);

                    if (!hasReaders() && markObsolete0(obsoleteVer, false, null)) {
                        if (swap) {
                            if (!isStartVersion()) {
                                try {
                                    // Write to swap.
                                    swap();
                                }
                                catch (IgniteCheckedException e) {
                                    U.error(log, "Failed to write entry to swap storage: " + this, e);
                                }
                            }
                        }
                        else {
                            if (this.expireTimeUnlocked() > 0)
                                cctx.ttl().removeTrackedEntry(this);

                            clearIndex(prev);
                        }

                        // Nullify value after swap.
                        value(null);

                        marked = true;

                        return true;
                    }
                    else
                        evictFailed(prev);
                }
            }
            else {
                // For optimistic check.
                while (true) {
                    GridCacheVersion v;

                    synchronized (this) {
                        v = ver;
                    }

                    if (!cctx.isAll(/*version needed for sync evicts*/this, filter))
                        return false;

                    synchronized (this) {
                        if (evictionDisabled()) {
                            assert !obsolete();

                            return false;
                        }

                        if (obsoleteVersionExtras() != null)
                            return true;

                        if (!v.equals(ver))
                            // Version has changed since entry passed the filter. Do it again.
                            continue;

                        CacheObject prevVal = saveValueForIndexUnlocked();

                        if (!hasReaders() && markObsolete0(obsoleteVer, false, null)) {
                            if (swap) {
                                if (!isStartVersion()) {
                                    try {
                                        // Write to swap.
                                        swap();
                                    }
                                    catch (IgniteCheckedException e) {
                                        U.error(log, "Failed to write entry to swap storage: " + this, e);
                                    }
                                }
                            }
                            else {
                                if (this.expireTimeUnlocked() > 0)
                                    cctx.ttl().removeTrackedEntry(this);

                                clearIndex(prevVal);
                            }

                            // Nullify value after swap.
                            value(null);

                            marked = true;

                            return true;
                        }
                        else {
                            evictFailed(prevVal);

                            return false;
                        }
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
     * @param prevVal Previous value.
     * @throws IgniteCheckedException If failed.
     */
    private void evictFailed(@Nullable CacheObject prevVal) throws IgniteCheckedException {
        if (cctx.offheapTiered() && ((flags & IS_OFFHEAP_PTR_MASK) != 0)) {
            flags &= ~IS_OFFHEAP_PTR_MASK;

            if (prevVal != null) {
                if (log.isTraceEnabled()) {
                    log.trace("evictFailed [key=" + key +
                        ", entry=" + System.identityHashCode(this) +
                        ", ptr=" + offHeapPointer() +
                        ']');
                }

                cctx.swap().removeOffheap(key());

                value(prevVal);

                GridCacheQueryManager qryMgr = cctx.queries();

                if (qryMgr.enabled())
                    qryMgr.onUnswap(key, prevVal);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public final GridCacheBatchSwapEntry evictInBatchInternal(GridCacheVersion obsoleteVer)
        throws IgniteCheckedException {
        assert Thread.holdsLock(this);
        assert cctx.isSwapOrOffheapEnabled();
        assert !obsolete();

        GridCacheBatchSwapEntry ret = null;

        try {
            if (!hasReaders() && markObsolete0(obsoleteVer, false, null)) {
                if (!isStartVersion() && hasValueUnlocked()) {
                    if (cctx.offheapTiered() && hasOffHeapPointer()) {
                        if (cctx.swap().offheapEvictionEnabled()) {
                            if (log.isTraceEnabled()) {
                                log.trace("enableOffheapEviction evictInBatchInternal [key=" + key +
                                    ", entry=" + System.identityHashCode(this) +
                                    ", ptr=" + offHeapPointer() +
                                    ']');
                            }

                            cctx.swap().enableOffheapEviction(key(), partition());
                        }

                        return null;
                    }

                    IgniteUuid valClsLdrId = null;
                    IgniteUuid keyClsLdrId = null;

                    if (cctx.deploymentEnabled()) {
                        if (val != null) {
                            valClsLdrId = cctx.deploy().getClassLoaderId(
                                U.detectObjectClassLoader(val.value(cctx.cacheObjectContext(), false)));
                        }

                        keyClsLdrId = cctx.deploy().getClassLoaderId(
                            U.detectObjectClassLoader(keyValue(false)));
                    }

                    IgniteBiTuple<byte[], Byte> valBytes = valueBytes0();

                    ret = new GridCacheBatchSwapEntry(key(),
                        partition(),
                        ByteBuffer.wrap(valBytes.get1()),
                        valBytes.get2(),
                        ver,
                        ttlExtras(),
                        expireTimeExtras(),
                        keyClsLdrId,
                        valClsLdrId);
                }

                value(null);
            }
        }
        catch (GridCacheEntryRemovedException ignored) {
            if (log.isDebugEnabled())
                log.debug("Got removed entry when evicting (will simply return): " + this);
        }

        return ret;
    }

    /**
     * @param filter Entry filter.
     * @return {@code True} if entry is visitable.
     */
    public final boolean visitable(CacheEntryPredicate[] filter) {
        boolean rmv = false;

        try {
            synchronized (this) {
                if (obsoleteOrDeleted())
                    return false;

                if (checkExpired()) {
                    rmv = markObsolete0(cctx.versions().next(this.ver), true, null);

                    return false;
                }
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

        IgniteInternalTx tx = cctx.tm().localTxx();

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

        synchronized (this) {
            return deletedUnlocked();
        }
    }

    /** {@inheritDoc} */
    @Override public final synchronized boolean obsoleteOrDeleted() {
        return obsoleteVersionExtras() != null ||
            (cctx.deferredDelete() && (deletedUnlocked() || !hasValueUnlocked()));
    }

    /**
     * @return {@code True} if deleted.
     */
    @SuppressWarnings("SimplifiableIfStatement")
    protected final boolean deletedUnlocked() {
        assert Thread.holdsLock(this);

        if (!cctx.deferredDelete())
            return false;

        return (flags & IS_DELETED_MASK) != 0;
    }

    /**
     * @param deleted {@code True} if deleted.
     */
    protected final void deletedUnlocked(boolean deleted) {
        assert Thread.holdsLock(this);
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
     *  Increments public size of map.
     */
    protected void incrementMapPublicSize() {
        cctx.incrementPublicSize(this);
    }

    /**
     * Decrements public size of map.
     */
    protected void decrementMapPublicSize() {
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
    @Nullable public final synchronized List<GridCacheMvccCandidate> mvccAllLocal() {
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
        assert !Thread.holdsLock(this);

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
        if (metrics && cctx.cache().configuration().isStatisticsEnabled()) {
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

        extras = (extras != null) ? extras.ttlAndExpireTime(ttl, expireTime) : ttl != CU.TTL_ETERNAL ?
            new GridCacheTtlEntryExtras(ttl, expireTime) : null;
    }

    /**
     * @return True if entry has off-heap value pointer.
     */
    protected boolean hasOffHeapPointer() {
        return false;
    }

    /**
     * @return Off-heap value pointer.
     */
    protected long offHeapPointer() {
        return 0;
    }

    /**
     * @param valPtr Off-heap value pointer.
     */
    protected void offHeapPointer(long valPtr) {
        // No-op.
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
         */
        private LazyValueEntry(KeyCacheObject key, boolean keepBinary) {
            this.key = key;
            this.keepBinary = keepBinary;
        }

        /** {@inheritDoc} */
        @Override public K getKey() {
            return (K)cctx.cacheObjectContext().unwrapBinaryIfNeeded(key, keepBinary);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public V getValue() {
            return (V)cctx.cacheObjectContext().unwrapBinaryIfNeeded(peekVisibleValue(), keepBinary);
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
}

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

package org.apache.ignite.internal.processors.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.transactions.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import javax.cache.processor.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.*;
import static org.apache.ignite.internal.processors.cache.CacheFlag.*;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.DataStructureType.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 * Manager of data structures.
 */
public final class DataStructuresProcessor extends GridProcessorAdapter {
    /** */
    public static final CacheDataStructuresConfigurationKey DATA_STRUCTURES_KEY =
        new CacheDataStructuresConfigurationKey();

    /** Initial capacity. */
    private static final int INITIAL_CAPACITY = 10;

    /** */
    private static final int MAX_UPDATE_RETRIES = 100;

    /** */
    private static final long RETRY_DELAY = 1;

    /** Cache contains only {@code GridCacheInternal,GridCacheInternal}. */
    private CacheProjection<GridCacheInternal, GridCacheInternal> dsView;

    /** Internal storage of all dataStructures items (sequence, atomic long etc.). */
    private final ConcurrentMap<GridCacheInternal, GridCacheRemovable> dsMap;

    /** Cache contains only {@code GridCacheAtomicValue}. */
    private CacheProjection<GridCacheInternalKey, GridCacheAtomicLongValue> atomicLongView;

    /** Cache contains only {@code GridCacheCountDownLatchValue}. */
    private CacheProjection<GridCacheInternalKey, GridCacheCountDownLatchValue> cntDownLatchView;

    /** Cache contains only {@code GridCacheAtomicReferenceValue}. */
    private CacheProjection<GridCacheInternalKey, GridCacheAtomicReferenceValue> atomicRefView;

    /** Cache contains only {@code GridCacheAtomicStampedValue}. */
    private CacheProjection<GridCacheInternalKey, GridCacheAtomicStampedValue> atomicStampedView;

    /** Cache contains only entry {@code GridCacheSequenceValue}.  */
    private CacheProjection<GridCacheInternalKey, GridCacheAtomicSequenceValue> seqView;

    /** Cache context for atomic data structures. */
    private GridCacheContext dsCacheCtx;

    /** Atomic data structures configuration. */
    private final AtomicConfiguration atomicCfg;

    /** */
    private GridCacheProjectionEx<CacheDataStructuresConfigurationKey, Map<String, DataStructureInfo>> utilityCache;

    /**
     * @param ctx Context.
     */
    public DataStructuresProcessor(GridKernalContext ctx) {
        super(ctx);

        dsMap = new ConcurrentHashMap8<>(INITIAL_CAPACITY);

        atomicCfg = ctx.config().getAtomicConfiguration();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onKernalStart() {
        if (ctx.config().isDaemon())
            return;

        utilityCache = (GridCacheProjectionEx)ctx.cache().utilityCache();

        assert utilityCache != null;

        if (atomicCfg != null) {
            GridCache atomicsCache = ctx.cache().atomicsCache();

            assert atomicsCache != null;

            dsView = atomicsCache.flagsOn(CLONE);

            cntDownLatchView = atomicsCache.flagsOn(CLONE);

            atomicLongView = atomicsCache.flagsOn(CLONE);

            atomicRefView = atomicsCache.flagsOn(CLONE);

            atomicStampedView = atomicsCache.flagsOn(CLONE);

            seqView = atomicsCache.flagsOn(CLONE);

            dsCacheCtx = ctx.cache().internalCache(CU.ATOMICS_CACHE_NAME).context();
        }
    }

    /**
     * Gets a sequence from cache or creates one if it's not cached.
     *
     * @param name Sequence name.
     * @param initVal Initial value for sequence. If sequence already cached, {@code initVal} will be ignored.
     * @param create  If {@code true} sequence will be created in case it is not in cache.
     * @return Sequence.
     * @throws IgniteCheckedException If loading failed.
     */
    public final IgniteAtomicSequence sequence(final String name,
        final long initVal,
        final boolean create)
        throws IgniteCheckedException
    {
        A.notNull(name, "name");

        checkAtomicsConfiguration();

        return getAtomic(new IgniteOutClosureX<IgniteAtomicSequence>() {
            @Override public IgniteAtomicSequence applyx() throws IgniteCheckedException {
                GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

                dsCacheCtx.gate().enter();

                try (IgniteInternalTx tx = CU.txStartInternal(dsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheAtomicSequenceValue seqVal = cast(dsView.get(key), GridCacheAtomicSequenceValue.class);

                    // Check that sequence hasn't been created in other thread yet.
                    GridCacheAtomicSequenceEx seq = cast(dsMap.get(key), GridCacheAtomicSequenceEx.class);

                    if (seq != null) {
                        assert seqVal != null;

                        return seq;
                    }

                    if (seqVal == null && !create)
                        return null;

                    // We should use offset because we already reserved left side of range.
                    long off = atomicCfg.getAtomicSequenceReserveSize() > 1 ?
                        atomicCfg.getAtomicSequenceReserveSize() - 1 : 1;

                    long upBound;
                    long locCntr;

                    if (seqVal == null) {
                        locCntr = initVal;

                        upBound = locCntr + off;

                        // Global counter must be more than reserved region.
                        seqVal = new GridCacheAtomicSequenceValue(upBound + 1);
                    }
                    else {
                        locCntr = seqVal.get();

                        upBound = locCntr + off;

                        // Global counter must be more than reserved region.
                        seqVal.set(upBound + 1);
                    }

                    // Update global counter.
                    dsView.putx(key, seqVal);

                    // Only one thread can be in the transaction scope and create sequence.
                    seq = new GridCacheAtomicSequenceImpl(name,
                        key,
                        seqView,
                        dsCacheCtx,
                        atomicCfg.getAtomicSequenceReserveSize(),
                        locCntr,
                        upBound);

                    dsMap.put(key, seq);

                    tx.commit();

                    return seq;
                }
                catch (Error | Exception e) {
                    dsMap.remove(key);

                    U.error(log, "Failed to make atomic sequence: " + name, e);

                    throw e;
                }
                finally {
                    dsCacheCtx.gate().leave();
                }
            }
        }, new DataStructureInfo(name, ATOMIC_SEQ, null), create, IgniteAtomicSequence.class);
    }

    /**
     * Removes sequence from cache.
     *
     * @param name Sequence name.
     * @throws IgniteCheckedException If removing failed.
     */
    public final void removeSequence(final String name) throws IgniteCheckedException {
        assert name != null;

        checkAtomicsConfiguration();

        removeDataStructure(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                dsCacheCtx.gate().enter();

                try {
                    GridCacheInternal key = new GridCacheInternalKeyImpl(name);

                    removeInternal(key, GridCacheAtomicSequenceValue.class);
                }
                catch (Exception e) {
                    throw new IgniteCheckedException("Failed to remove sequence by name: " + name, e);
                }
                finally {
                    dsCacheCtx.gate().leave();
                }

                return null;
            }
        }, name, ATOMIC_SEQ, null);
    }

    /**
     * Gets an atomic long from cache or creates one if it's not cached.
     *
     * @param name Name of atomic long.
     * @param initVal Initial value for atomic long. If atomic long already cached, {@code initVal}
     *        will be ignored.
     * @param create If {@code true} atomic long will be created in case it is not in cache.
     * @return Atomic long.
     * @throws IgniteCheckedException If loading failed.
     */
    public final IgniteAtomicLong atomicLong(final String name,
        final long initVal,
        final boolean create) throws IgniteCheckedException {
        A.notNull(name, "name");

        checkAtomicsConfiguration();

        return getAtomic(new IgniteOutClosureX<IgniteAtomicLong>() {
            @Override public IgniteAtomicLong applyx() throws IgniteCheckedException {
                final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

                dsCacheCtx.gate().enter();

                try (IgniteInternalTx tx = CU.txStartInternal(dsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheAtomicLongValue val = cast(dsView.get(key), GridCacheAtomicLongValue.class);

                    // Check that atomic long hasn't been created in other thread yet.
                    GridCacheAtomicLongEx a = cast(dsMap.get(key), GridCacheAtomicLongEx.class);

                    if (a != null) {
                        assert val != null;

                        return a;
                    }

                    if (val == null && !create)
                        return null;

                    if (val == null) {
                        val = new GridCacheAtomicLongValue(initVal);

                        dsView.putx(key, val);
                    }

                    a = new GridCacheAtomicLongImpl(name, key, atomicLongView, dsCacheCtx);

                    dsMap.put(key, a);

                    tx.commit();

                    return a;
                }
                catch (Error | Exception e) {
                    dsMap.remove(key);

                    U.error(log, "Failed to make atomic long: " + name, e);

                    throw e;
                }
                finally {
                    dsCacheCtx.gate().leave();
                }
            }
        }, new DataStructureInfo(name, ATOMIC_LONG, null), create, IgniteAtomicLong.class);
    }

    /**
     * @param c Closure creating data structure instance.
     * @param dsInfo Data structure info.
     * @param create Create flag.
     * @param cls Expected data structure class.
     * @return Data structure instance.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private <T> T getAtomic(final IgniteOutClosureX<T> c,
        DataStructureInfo dsInfo,
        boolean create,
        Class<? extends T> cls)
        throws IgniteCheckedException
    {
        Map<String, DataStructureInfo> dsMap = utilityCache.get(DATA_STRUCTURES_KEY);

        if (!create && (dsMap == null || !dsMap.containsKey(dsInfo.name)))
            return null;

        IgniteCheckedException err = validateDataStructure(dsMap, dsInfo, create);

        if (err != null)
            throw err;

        final GridCacheInternalKey key = new GridCacheInternalKeyImpl(dsInfo.name);

        // Check type of structure received by key from local cache.
        T dataStructure = cast(this.dsMap.get(key), cls);

        if (dataStructure != null)
            return dataStructure;

        if (!create)
            return c.applyx();

        try (IgniteInternalTx tx = utilityCache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
            err = utilityCache.invoke(DATA_STRUCTURES_KEY, new AddAtomicProcessor(dsInfo)).get();

            if (err != null)
                throw err;

            dataStructure = ctx.closure().callLocalSafe(new Callable<T>() {
                @Override public T call() throws Exception {
                    return c.applyx();
                }
            }, false).get();

            tx.commit();
        }

        return dataStructure;
    }

    /**
     * Removes atomic long from cache.
     *
     * @param name Atomic long name.
     * @throws IgniteCheckedException If removing failed.
     */
    public final void removeAtomicLong(final String name) throws IgniteCheckedException {
        assert name != null;
        assert dsCacheCtx != null;

        removeDataStructure(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                dsCacheCtx.gate().enter();

                try {
                    removeInternal(new GridCacheInternalKeyImpl(name), GridCacheAtomicLongValue.class);
                }
                catch (Exception e) {
                    throw new IgniteCheckedException("Failed to remove atomic long by name: " + name, e);
                }
                finally {
                    dsCacheCtx.gate().leave();
                }

                return null;
            }
        }, name, ATOMIC_LONG, null);
    }

    /**
     * @param c Closure.
     * @param name Data structure name.
     * @param type Data structure type.
     * @param afterRmv Optional closure to run after data structure removed.
     * @throws IgniteCheckedException If failed.
     */
    private <T> void removeDataStructure(IgniteCallable<T> c,
        String name,
        DataStructureType type,
        @Nullable IgniteInClosureX<T> afterRmv)
        throws IgniteCheckedException
    {
        Map<String, DataStructureInfo> dsMap = utilityCache.get(DATA_STRUCTURES_KEY);

        if (dsMap == null || !dsMap.containsKey(name))
            return;

        DataStructureInfo dsInfo = new DataStructureInfo(name, type, null);

        IgniteCheckedException err = validateDataStructure(dsMap, dsInfo, false);

        if (err != null)
            throw err;

        T rmvInfo;

        try (IgniteInternalTx tx = utilityCache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
            T2<Boolean, IgniteCheckedException> res =
                utilityCache.invoke(DATA_STRUCTURES_KEY, new RemoveDataStructureProcessor(dsInfo)).get();

            err = res.get2();

            if (err != null)
                throw err;

            assert res.get1() != null;

            boolean exists = res.get1();

            if (!exists)
                return;

            rmvInfo = ctx.closure().callLocalSafe(c, false).get();

            tx.commit();
        }

        if (afterRmv != null && rmvInfo != null)
            afterRmv.applyx(rmvInfo);
    }

    /**
     * Gets an atomic reference from cache or creates one if it's not cached.
     *
     * @param name Name of atomic reference.
     * @param initVal Initial value for atomic reference. If atomic reference already cached, {@code initVal}
     *        will be ignored.
     * @param create If {@code true} atomic reference will be created in case it is not in cache.
     * @return Atomic reference.
     * @throws IgniteCheckedException If loading failed.
     */
    @SuppressWarnings("unchecked")
    public final <T> IgniteAtomicReference<T> atomicReference(final String name,
        final T initVal,
        final boolean create)
        throws IgniteCheckedException
    {
        A.notNull(name, "name");

        checkAtomicsConfiguration();

        return getAtomic(new IgniteOutClosureX<IgniteAtomicReference>() {
            @Override public IgniteAtomicReference<T> applyx() throws IgniteCheckedException {
                GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

                dsCacheCtx.gate().enter();

                try (IgniteInternalTx tx = CU.txStartInternal(dsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheAtomicReferenceValue val = cast(dsView.get(key),
                        GridCacheAtomicReferenceValue.class);

                    // Check that atomic reference hasn't been created in other thread yet.
                    GridCacheAtomicReferenceEx ref = cast(dsMap.get(key),
                        GridCacheAtomicReferenceEx.class);

                    if (ref != null) {
                        assert val != null;

                        return ref;
                    }

                    if (val == null && !create)
                        return null;

                    if (val == null) {
                        val = new GridCacheAtomicReferenceValue(initVal);

                        dsView.putx(key, val);
                    }

                    ref = new GridCacheAtomicReferenceImpl(name, key, atomicRefView, dsCacheCtx);

                    dsMap.put(key, ref);

                    tx.commit();

                    return ref;
                }
                catch (Error | Exception e) {
                    dsMap.remove(key);

                    U.error(log, "Failed to make atomic reference: " + name, e);

                    throw e;
                }
                finally {
                    dsCacheCtx.gate().leave();
                }
            }
        }, new DataStructureInfo(name, ATOMIC_REF, null), create, IgniteAtomicReference.class);
    }

    /**
     * Removes atomic reference from cache.
     *
     * @param name Atomic reference name.
     * @throws IgniteCheckedException If removing failed.
     */
    public final void removeAtomicReference(final String name) throws IgniteCheckedException {
        assert name != null;
        assert dsCacheCtx != null;

        removeDataStructure(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                dsCacheCtx.gate().enter();

                try {
                    GridCacheInternal key = new GridCacheInternalKeyImpl(name);

                    removeInternal(key, GridCacheAtomicReferenceValue.class);
                }
                catch (Exception e) {
                    throw new IgniteCheckedException("Failed to remove atomic reference by name: " + name, e);
                }
                finally {
                    dsCacheCtx.gate().leave();
                }

                return null;
            }
        }, name, ATOMIC_REF, null);
    }

    /**
     * Gets an atomic stamped from cache or creates one if it's not cached.
     *
     * @param name Name of atomic stamped.
     * @param initVal Initial value for atomic stamped. If atomic stamped already cached, {@code initVal}
     *        will be ignored.
     * @param initStamp Initial stamp for atomic stamped. If atomic stamped already cached, {@code initStamp}
     *        will be ignored.
     * @param create If {@code true} atomic stamped will be created in case it is not in cache.
     * @return Atomic stamped.
     * @throws IgniteCheckedException If loading failed.
     */
    @SuppressWarnings("unchecked")
    public final <T, S> IgniteAtomicStamped<T, S> atomicStamped(final String name, final T initVal,
        final S initStamp, final boolean create) throws IgniteCheckedException {
        A.notNull(name, "name");

        checkAtomicsConfiguration();

        return getAtomic(new IgniteOutClosureX<IgniteAtomicStamped>() {
            @Override public IgniteAtomicStamped<T, S> applyx() throws IgniteCheckedException {
                GridCacheInternalKeyImpl key = new GridCacheInternalKeyImpl(name);

                dsCacheCtx.gate().enter();

                try (IgniteInternalTx tx = CU.txStartInternal(dsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheAtomicStampedValue val = cast(dsView.get(key),
                        GridCacheAtomicStampedValue.class);

                    // Check that atomic stamped hasn't been created in other thread yet.
                    GridCacheAtomicStampedEx stmp = cast(dsMap.get(key),
                        GridCacheAtomicStampedEx.class);

                    if (stmp != null) {
                        assert val != null;

                        return stmp;
                    }

                    if (val == null && !create)
                        return null;

                    if (val == null) {
                        val = new GridCacheAtomicStampedValue(initVal, initStamp);

                        dsView.putx(key, val);
                    }

                    stmp = new GridCacheAtomicStampedImpl(name, key, atomicStampedView, dsCacheCtx);

                    dsMap.put(key, stmp);

                    tx.commit();

                    return stmp;
                }
                catch (Error | Exception e) {
                    dsMap.remove(key);

                    U.error(log, "Failed to make atomic stamped: " + name, e);

                    throw e;
                }
                finally {
                    dsCacheCtx.gate().leave();
                }
            }
        }, new DataStructureInfo(name, ATOMIC_STAMPED, null), create, IgniteAtomicStamped.class);
    }

    /**
     * Removes atomic stamped from cache.
     *
     * @param name Atomic stamped name.
     * @throws IgniteCheckedException If removing failed.
     */
    public final void removeAtomicStamped(final String name) throws IgniteCheckedException {
        assert name != null;
        assert dsCacheCtx != null;

        removeDataStructure(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                dsCacheCtx.gate().enter();

                try {
                    GridCacheInternal key = new GridCacheInternalKeyImpl(name);

                    removeInternal(key, GridCacheAtomicStampedValue.class);
                }
                catch (Exception e) {
                    throw new IgniteCheckedException("Failed to remove atomic stamped by name: " + name, e);
                }
                finally {
                    dsCacheCtx.gate().leave();
                }

                return null;
            }
        }, name, ATOMIC_STAMPED, null);
    }

    /**
     * Gets a queue from cache or creates one if it's not cached.
     *
     * @param name Name of queue.
     * @param cap Max size of queue.
     * @param cfg Non-null queue configuration if new queue should be created.
     * @return Instance of queue.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public final <T> IgniteQueue<T> queue(final String name,
        int cap,
        @Nullable final CollectionConfiguration cfg)
        throws IgniteCheckedException {
        A.notNull(name, "name");

        if (cfg != null) {
            if (cap <= 0)
                cap = Integer.MAX_VALUE;

            if (ctx.cache().publicCache(cfg.getCacheName()) == null)
                throw new IgniteCheckedException("Cache for collection is not configured: " + cfg.getCacheName());

            checkSupportsQueue(ctx.cache().internalCache(cfg.getCacheName()).context());
        }

        DataStructureInfo dsInfo = new DataStructureInfo(name,
            QUEUE,
            cfg != null ? new QueueInfo(cfg.getCacheName(), cfg.isCollocated(), cap) : null);

        final int cap0 = cap;

        final boolean create = cfg != null;

        return getCollection(new IgniteClosureX<GridCacheContext, IgniteQueue<T>>() {
            @Override public IgniteQueue<T> applyx(GridCacheContext ctx) throws IgniteCheckedException {
                return ctx.dataStructures().queue(name, cap0, create && cfg.isCollocated(), create);
            }
        }, dsInfo, create);
    }

    /**
     * @param name Queue name.
     * @param cctx Queue cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void removeQueue(final String name, final GridCacheContext cctx) throws IgniteCheckedException {
        assert name != null;
        assert cctx != null;

        IgniteCallable<GridCacheQueueHeader> rmv = new IgniteCallable<GridCacheQueueHeader>() {
            @Override public GridCacheQueueHeader call() throws Exception {
                return (GridCacheQueueHeader)retryRemove(cctx.cache(), new GridCacheQueueHeaderKey(name));
            }
        };

        CIX1<GridCacheQueueHeader> afterRmv = new CIX1<GridCacheQueueHeader>() {
            @Override public void applyx(GridCacheQueueHeader hdr) throws IgniteCheckedException {
                if (hdr.empty())
                    return;

                GridCacheQueueAdapter.removeKeys(cctx.cache(),
                    hdr.id(),
                    name,
                    hdr.collocated(),
                    hdr.head(),
                    hdr.tail(),
                    0);
            }
        };

        removeDataStructure(rmv, name, QUEUE, afterRmv);
    }

    /**
     * @param c Closure creating collection.
     * @param dsInfo Data structure info.
     * @param create Create flag.
     * @return Collection instance.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private <T> T getCollection(final IgniteClosureX<GridCacheContext, T> c,
        DataStructureInfo dsInfo,
        boolean create)
        throws IgniteCheckedException
    {
        Map<String, DataStructureInfo> dsMap = utilityCache.get(DATA_STRUCTURES_KEY);

        if (!create && (dsMap == null || !dsMap.containsKey(dsInfo.name)))
            return null;

        IgniteCheckedException err = validateDataStructure(dsMap, dsInfo, create);

        if (err != null)
            throw err;

        if (!create) {
            DataStructureInfo oldInfo = dsMap.get(dsInfo.name);

            assert oldInfo.info instanceof CollectionInfo : oldInfo.info;

            String cacheName = ((CollectionInfo)oldInfo.info).cacheName;

            GridCacheContext cacheCtx = ctx.cache().internalCache(cacheName).context();

            return c.applyx(cacheCtx);
        }

        T col;

        try (IgniteInternalTx tx = utilityCache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
            T2<String, IgniteCheckedException> res =
                utilityCache.invoke(DATA_STRUCTURES_KEY, new AddCollectionProcessor(dsInfo)).get();

            err = res.get2();

            if (err != null)
                throw err;

            String cacheName = res.get1();

            final GridCacheContext cacheCtx = ctx.cache().internalCache(cacheName).context();

            col = ctx.closure().callLocalSafe(new Callable<T>() {
                @Override public T call() throws Exception {
                    return c.applyx(cacheCtx);
                }
            }, false).get();

            tx.commit();
        }

        return col;
    }

    /**
     * @param dsMap Map with data structure information.
     * @param info New data structure information.
     * @param create Create flag.
     * @return {@link IgniteException} if validation failed.
     */
    @Nullable private static IgniteCheckedException validateDataStructure(
        @Nullable Map<String, DataStructureInfo> dsMap,
        DataStructureInfo info,
        boolean create)
    {
        if (dsMap == null)
            return null;

        DataStructureInfo oldInfo = dsMap.get(info.name);

        if (oldInfo != null)
            return oldInfo.validate(info, create);

        return null;
    }

    /**
     * Gets or creates count down latch. If count down latch is not found in cache,
     * it is created using provided name and count parameter.
     *
     * @param name Name of the latch.
     * @param cnt Initial count.
     * @param autoDel {@code True} to automatically delete latch from cache when
     *      its count reaches zero.
     * @param create If {@code true} latch will be created in case it is not in cache,
     *      if it is {@code false} all parameters except {@code name} are ignored.
     * @return Count down latch for the given name or {@code null} if it is not found and
     *      {@code create} is false.
     * @throws IgniteCheckedException If operation failed.
     */
    public IgniteCountDownLatch countDownLatch(final String name,
        final int cnt,
        final boolean autoDel,
        final boolean create)
        throws IgniteCheckedException
    {
        A.notNull(name, "name");

        if (create)
            A.ensure(cnt >= 0, "count can not be negative");

        checkAtomicsConfiguration();

        return getAtomic(new IgniteOutClosureX<IgniteCountDownLatch>() {
            @Override public IgniteCountDownLatch applyx() throws IgniteCheckedException {
                GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

                dsCacheCtx.gate().enter();

                try (IgniteInternalTx tx = CU.txStartInternal(dsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheCountDownLatchValue val = cast(dsView.get(key),
                        GridCacheCountDownLatchValue.class);

                    // Check that count down hasn't been created in other thread yet.
                    GridCacheCountDownLatchEx latch = cast(dsMap.get(key), GridCacheCountDownLatchEx.class);

                    if (latch != null) {
                        assert val != null;

                        return latch;
                    }

                    if (val == null && !create)
                        return null;

                    if (val == null) {
                        val = new GridCacheCountDownLatchValue(cnt, autoDel);

                        dsView.putx(key, val);
                    }

                    latch = new GridCacheCountDownLatchImpl(name, val.get(), val.initialCount(),
                        val.autoDelete(), key, cntDownLatchView, dsCacheCtx);

                    dsMap.put(key, latch);

                    tx.commit();

                    return latch;
                }
                catch (Error | Exception e) {
                    dsMap.remove(key);

                    U.error(log, "Failed to create count down latch: " + name, e);

                    throw e;
                }
                finally {
                    dsCacheCtx.gate().leave();
                }
            }
        }, new DataStructureInfo(name, COUNT_DOWN_LATCH, null), create, GridCacheCountDownLatchEx.class);
    }

    /**
     * Removes count down latch from cache.
     *
     * @param name Name of the latch.
     * @throws IgniteCheckedException If operation failed.
     */
    public void removeCountDownLatch(final String name) throws IgniteCheckedException {
        assert name != null;
        assert dsCacheCtx != null;

        removeDataStructure(new IgniteCallable<Void>() {
            @Override
            public Void call() throws Exception {
                GridCacheInternal key = new GridCacheInternalKeyImpl(name);

                dsCacheCtx.gate().enter();

                try (IgniteInternalTx tx = CU.txStartInternal(dsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                    // Check correctness type of removable object.
                    GridCacheCountDownLatchValue val =
                            cast(dsView.get(key), GridCacheCountDownLatchValue.class);

                    if (val != null) {
                        if (val.get() > 0) {
                            throw new IgniteCheckedException("Failed to remove count down latch " +
                                    "with non-zero count: " + val.get());
                        }

                        dsView.removex(key);

                        tx.commit();
                    } else
                        tx.setRollbackOnly();

                    return null;
                } catch (Error | Exception e) {
                    U.error(log, "Failed to remove data structure: " + key, e);

                    throw e;
                } finally {
                    dsCacheCtx.gate().leave();
                }
            }
        }, name, COUNT_DOWN_LATCH, null);
    }

    /**
     * Remove internal entry by key from cache.
     *
     * @param key Internal entry key.
     * @param cls Class of object which will be removed. If cached object has different type exception will be thrown.
     * @return Method returns true if sequence has been removed and false if it's not cached.
     * @throws IgniteCheckedException If removing failed or class of object is different to expected class.
     */
    private <R> boolean removeInternal(final GridCacheInternal key, final Class<R> cls) throws IgniteCheckedException {
        return CU.outTx(
            new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    try (IgniteInternalTx tx = CU.txStartInternal(dsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                        // Check correctness type of removable object.
                        R val = cast(dsView.get(key), cls);

                        if (val != null) {
                            dsView.removex(key);

                            tx.commit();
                        }
                        else
                            tx.setRollbackOnly();

                        return val != null;
                    }
                    catch (Error | Exception e) {
                        U.error(log, "Failed to remove data structure: " + key, e);

                        throw e;
                    }
                }
            },
            dsCacheCtx
        );
    }

    /**
     * Transaction committed callback for transaction manager.
     *
     * @param tx Committed transaction.
     */
    public <K, V> void onTxCommitted(IgniteInternalTx tx) {
        if (dsCacheCtx == null)
            return;

        if (!dsCacheCtx.isDht() && tx.internal() && (!dsCacheCtx.isColocated() || dsCacheCtx.isReplicated())) {
            Collection<IgniteTxEntry> entries = tx.writeEntries();

            if (log.isDebugEnabled())
                log.debug("Committed entries: " + entries);

            for (IgniteTxEntry entry : entries) {
                // Check updated or created GridCacheInternalKey keys.
                if ((entry.op() == CREATE || entry.op() == UPDATE) && entry.key().internal()) {
                    GridCacheInternal key = entry.key().value(entry.context().cacheObjectContext(), false);

                    Object val0 = CU.value(entry.value(), entry.context(), false);

                    if (val0 instanceof GridCacheCountDownLatchValue) {
                        // Notify latch on changes.
                        GridCacheRemovable latch = dsMap.get(key);

                        GridCacheCountDownLatchValue val = (GridCacheCountDownLatchValue)val0;

                        if (latch instanceof GridCacheCountDownLatchEx) {
                            GridCacheCountDownLatchEx latch0 = (GridCacheCountDownLatchEx)latch;

                            latch0.onUpdate(val.get());

                            if (val.get() == 0 && val.autoDelete()) {
                                entry.cached().markObsolete(dsCacheCtx.versions().next());

                                dsMap.remove(key);

                                latch.onRemoved();
                            }
                        }
                        else if (latch != null) {
                            U.error(log, "Failed to cast object " +
                                "[expected=" + IgniteCountDownLatch.class.getSimpleName() +
                                ", actual=" + latch.getClass() + ", value=" + latch + ']');
                        }
                    }
                }

                // Check deleted GridCacheInternal keys.
                if (entry.op() == DELETE && entry.key().internal()) {
                    GridCacheInternal key = entry.key().value(entry.context().cacheObjectContext(), false);

                    // Entry's val is null if entry deleted.
                    GridCacheRemovable obj = dsMap.remove(key);

                    if (obj != null)
                        obj.onRemoved();
                }
            }
        }
    }

    /**
     * Gets a set from cache or creates one if it's not cached.
     *
     * @param name Set name.
     * @param cfg Set configuration if new set should be created.
     * @return Set instance.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    @Nullable public <T> IgniteSet<T> set(final String name,
        @Nullable final CollectionConfiguration cfg)
        throws IgniteCheckedException {
        A.notNull(name, "name");

        if (cfg != null) {
            if (ctx.cache().publicCache(cfg.getCacheName()) == null)
                throw new IgniteCheckedException("Cache for collection is not configured: " + cfg.getCacheName());
        }

        DataStructureInfo dsInfo = new DataStructureInfo(name,
            SET,
            cfg != null ? new CollectionInfo(cfg.getCacheName(), cfg.isCollocated()) : null);

        final boolean create = cfg != null;

        return getCollection(new CX1<GridCacheContext, IgniteSet<T>>() {
            @Override public IgniteSet<T> applyx(GridCacheContext cctx) throws IgniteCheckedException {
                return cctx.dataStructures().set(name, create ? cfg.isCollocated() : false, create);
            }
        }, dsInfo, create);
    }

    /**
     * @param name Set name.
     * @param cctx Set cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void removeSet(final String name, final GridCacheContext cctx) throws IgniteCheckedException {
        assert name != null;
        assert cctx != null;

        IgniteCallable<GridCacheSetHeader> rmv = new IgniteCallable<GridCacheSetHeader>() {
            @Override public GridCacheSetHeader call() throws Exception {
                return (GridCacheSetHeader)retryRemove(cctx.cache(), new GridCacheSetHeaderKey(name));
            }
        };

        CIX1<GridCacheSetHeader> afterRmv = new CIX1<GridCacheSetHeader>() {
            @Override public void applyx(GridCacheSetHeader hdr) throws IgniteCheckedException {
                cctx.dataStructures().removeSetData(hdr.id());
            }
        };

        removeDataStructure(rmv, name, SET, afterRmv);
    }

    /**
     * @param cache Cache.
     * @param key Key to remove.
     * @throws IgniteCheckedException If failed.
     * @return Removed value.
     */
    @SuppressWarnings("unchecked")
    @Nullable private <T> T retryRemove(final GridCache cache, final Object key) throws IgniteCheckedException {
        return retry(log, new Callable<T>() {
            @Nullable @Override public T call() throws Exception {
                return (T)cache.remove(key);
            }
        });
    }

    /**
     * @param log Logger.
     * @param call Callable.
     * @return Callable result.
     * @throws IgniteCheckedException If all retries failed.
     */
    public static <R> R retry(IgniteLogger log, Callable<R> call) throws IgniteCheckedException {
        try {
            int cnt = 0;

            while (true) {
                try {
                    return call.call();
                }
                catch (ClusterGroupEmptyCheckedException e) {
                    throw new IgniteCheckedException(e);
                }
                catch (IgniteTxRollbackCheckedException | CachePartialUpdateCheckedException | ClusterTopologyCheckedException e) {
                    if (cnt++ == MAX_UPDATE_RETRIES)
                        throw e;
                    else {
                        U.warn(log, "Failed to execute data structure operation, will retry [err=" + e + ']');

                        U.sleep(RETRY_DELAY);
                    }
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Tries to cast the object to expected type.
     *
     * @param obj Object which will be casted.
     * @param cls Class
     * @param <R> Type of expected result.
     * @return Object has casted to expected type.
     * @throws IgniteCheckedException If {@code obj} has different to {@code cls} type.
     */
    @SuppressWarnings("unchecked")
    @Nullable private <R> R cast(@Nullable Object obj, Class<R> cls) throws IgniteCheckedException {
        if (obj == null)
            return null;

        if (cls.isInstance(obj))
            return (R)obj;
        else
            throw new IgniteCheckedException("Failed to cast object [expected=" + cls +
                ", actual=" + obj.getClass() + ']');
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Data structure processor memory stats [grid=" + ctx.gridName() +
            ", cache=" + (dsCacheCtx != null ? dsCacheCtx.name() : null) + ']');
        X.println(">>>   dsMapSize: " + dsMap.size());
    }

    /**
     * @throws IgniteException If atomics configuration is not provided.
     */
    private void checkAtomicsConfiguration() throws IgniteException {
        if (atomicCfg == null)
            throw new IgniteException("Atomic data structure can not be created, " +
                "need to provide IgniteAtomicConfiguration.");
    }

    /**
     * @param cctx Cache context.
     * @throws IgniteCheckedException If {@link IgniteQueue} can with given cache.
     */
    private void checkSupportsQueue(GridCacheContext cctx) throws IgniteCheckedException {
        if (cctx.atomic() && !cctx.isLocal() && cctx.config().getAtomicWriteOrderMode() == CLOCK)
            throw new IgniteCheckedException("IgniteQueue can not be used with ATOMIC cache with CLOCK write order mode" +
                " (change write order mode to PRIMARY in configuration)");
    }

    /**
     *
     */
    static enum DataStructureType {
        /** */
        ATOMIC_LONG(IgniteAtomicLong.class.getSimpleName()),

        /** */
        ATOMIC_REF(IgniteAtomicReference.class.getSimpleName()),

        /** */
        ATOMIC_SEQ(IgniteAtomicSequence.class.getSimpleName()),

        /** */
        ATOMIC_STAMPED(IgniteAtomicStamped.class.getSimpleName()),

        /** */
        COUNT_DOWN_LATCH(IgniteCountDownLatch.class.getSimpleName()),

        /** */
        QUEUE(IgniteQueue.class.getSimpleName()),

        /** */
        SET(IgniteSet.class.getSimpleName());

        /** */
        private static final DataStructureType[] VALS = values();

        /** */
        private String name;

        /**
         * @param name Name.
         */
        DataStructureType(String name) {
            this.name = name;
        }

        /**
         * @return Data structure public class name.
         */
        public String className() {
            return name;
        }

        /**
         * @param ord Ordinal value.
         * @return Enumerated value or {@code null} if ordinal out of range.
         */
        @Nullable public static DataStructureType fromOrdinal(int ord) {
            return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
        }
    }

    /**
     *
     */
    static class CollectionInfo implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private boolean collocated;

        /** */
        private String cacheName;

        /**
         * Required by {@link Externalizable}.
         */
        public CollectionInfo() {
            // No-op.
        }

        /**
         * @param cacheName Collection cache name.
         * @param collocated Collocated flag.
         */
        public CollectionInfo(String cacheName, boolean collocated) {
            this.cacheName = cacheName;
            this.collocated = collocated;
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            collocated = in.readBoolean();
            cacheName = U.readString(in);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeBoolean(collocated);
            U.writeString(out, cacheName);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CollectionInfo.class, this);
        }
    }

    /**
     *
     */
    static class QueueInfo extends CollectionInfo {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int cap;

        /**
         * Required by {@link Externalizable}.
         */
        public QueueInfo() {
            // No-op.
        }

        /**
         * @param collocated Collocated flag.
         * @param cap Queue capacity.
         * @param cacheName Cache name.
         */
        public QueueInfo(String cacheName, boolean collocated, int cap) {
            super(cacheName, collocated);

            this.cap = cap;
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);

            cap = in.readInt();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);

            out.writeInt(cap);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(QueueInfo.class, this, "super", super.toString());
        }
    }

    /**
     *
     */
    static class DataStructureInfo implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private String name;

        /** */
        private DataStructureType type;

        /** */
        private Object info;

        /**
         * Required by {@link Externalizable}.
         */
        public DataStructureInfo() {
            // No-op.
        }

        /**
         * @param name Data structure name.
         * @param type Data structure type.
         * @param info Data structure information.
         */
        DataStructureInfo(String name, DataStructureType type, Externalizable info) {
            this.name = name;
            this.type = type;
            this.info = info;
        }

        /**
         * @param dsInfo New data structure info.
         * @param create Create flag.
         * @return Exception if validation failed.
         */
        @Nullable IgniteCheckedException validate(DataStructureInfo dsInfo, boolean create) {
            if (type != dsInfo.type) {
                return new IgniteCheckedException("Another data structure with the same name already created " +
                    "[name=" + name +
                    ", newType=" + dsInfo.type.className() +
                    ", existingType=" + type.className() + ']');
            }

            if (create) {
                if (type == QUEUE || type == SET) {
                    CollectionInfo oldInfo = (CollectionInfo)info;
                    CollectionInfo newInfo = (CollectionInfo)dsInfo.info;

                    if (oldInfo.collocated != newInfo.collocated) {
                        return new IgniteCheckedException("Another collection with the same name but different " +
                            "configuration already created [name=" + name +
                            ", newCollocated=" + newInfo.collocated +
                            ", existingCollocated=" + newInfo.collocated + ']');
                    }

                    if (type == QUEUE) {
                        if (((QueueInfo)oldInfo).cap != ((QueueInfo)newInfo).cap) {
                            return new IgniteCheckedException("Another queue with the same name but different " +
                                "configuration already created [name=" + name +
                                ", newCapacity=" + ((QueueInfo)newInfo).cap +
                                ", existingCapacity=" + ((QueueInfo)oldInfo).cap + ']');
                        }
                    }
                }
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, name);
            U.writeEnum(out, type);
            out.writeObject(info);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            name = U.readString(in);
            type = DataStructureType.fromOrdinal(in.readByte());
            info = in.readObject();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DataStructureInfo.class, this);
        }
    }

    /**
     *
     */
    static class AddAtomicProcessor implements
        EntryProcessor<CacheDataStructuresConfigurationKey, Map<String, DataStructureInfo>, IgniteCheckedException>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private DataStructureInfo info;

        /**
         * @param info Data structure information.
         */
        AddAtomicProcessor(DataStructureInfo info) {
            assert info != null;

            this.info = info;
        }

        /**
         * Required by {@link Externalizable}.
         */
        public AddAtomicProcessor() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public IgniteCheckedException process(
            MutableEntry<CacheDataStructuresConfigurationKey, Map<String, DataStructureInfo>> entry,
            Object... args)
            throws EntryProcessorException
        {
            Map<String, DataStructureInfo> map = entry.getValue();

            if (map == null) {
                map = new HashMap<>();

                map.put(info.name, info);

                entry.setValue(map);

                return null;
            }

            DataStructureInfo oldInfo = map.get(info.name);

            if (oldInfo == null) {
                map = new HashMap<>(map);

                map.put(info.name, info);

                entry.setValue(map);

                return null;
            }

            return oldInfo.validate(info, true);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            info.writeExternal(out);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            info = new DataStructureInfo();

            info.readExternal(in);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AddAtomicProcessor.class, this);
        }
    }

    /**
     *
     */
    static class AddCollectionProcessor implements
        EntryProcessor<CacheDataStructuresConfigurationKey, Map<String, DataStructureInfo>,
            T2<String, IgniteCheckedException>>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private DataStructureInfo info;

        /**
         * @param info Data structure information.
         */
        AddCollectionProcessor(DataStructureInfo info) {
            assert info != null;
            assert info.info instanceof CollectionInfo;

            this.info = info;
        }

        /**
         * Required by {@link Externalizable}.
         */
        public AddCollectionProcessor() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public T2<String, IgniteCheckedException> process(
            MutableEntry<CacheDataStructuresConfigurationKey, Map<String, DataStructureInfo>> entry,
            Object... args)
        {
            Map<String, DataStructureInfo> map = entry.getValue();

            CollectionInfo colInfo = (CollectionInfo)info.info;

            if (map == null) {
                map = new HashMap<>();

                map.put(info.name, info);

                entry.setValue(map);

                return new T2<>(colInfo.cacheName, null);
            }

            DataStructureInfo oldInfo = map.get(info.name);

            if (oldInfo == null) {
                map = new HashMap<>(map);

                map.put(info.name, info);

                entry.setValue(map);

                return new T2<>(colInfo.cacheName, null);
            }

            return new T2<>(colInfo.cacheName, oldInfo.validate(info, true));
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            info.writeExternal(out);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            info = new DataStructureInfo();

            info.readExternal(in);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AddCollectionProcessor.class, this);
        }
    }

    /**
     *
     */
    static class RemoveDataStructureProcessor implements
        EntryProcessor<CacheDataStructuresConfigurationKey, Map<String, DataStructureInfo>,
            T2<Boolean, IgniteCheckedException>>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private DataStructureInfo info;

        /**
         * @param info Data structure information.
         */
        RemoveDataStructureProcessor(DataStructureInfo info) {
            assert info != null;

            this.info = info;
        }

        /**
         * Required by {@link Externalizable}.
         */
        public RemoveDataStructureProcessor() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public T2<Boolean, IgniteCheckedException> process(
            MutableEntry<CacheDataStructuresConfigurationKey, Map<String, DataStructureInfo>> entry,
            Object... args)
        {
            Map<String, DataStructureInfo> map = entry.getValue();

            if (map == null)
                return new T2<>(false, null);

            DataStructureInfo oldInfo = map.get(info.name);

            if (oldInfo == null)
                return new T2<>(false, null);

            IgniteCheckedException err = oldInfo.validate(info, false);

            if (err == null) {
                map = new HashMap<>(map);

                map.remove(info.name);

                entry.setValue(map);
            }

            return new T2<>(true, err);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            info.writeExternal(out);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            info = new DataStructureInfo();

            info.readExternal(in);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RemoveDataStructureProcessor.class, this);
        }
    }
}

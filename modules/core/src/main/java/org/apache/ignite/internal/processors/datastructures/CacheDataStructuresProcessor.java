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
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import javax.cache.processor.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.internal.processors.cache.CacheFlag.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;
import static org.apache.ignite.internal.GridClosureCallMode.*;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;

/**
 * Manager of data structures.
 */
public final class CacheDataStructuresProcessor extends GridProcessorAdapter {
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

    /** Queues map. */
    private final ConcurrentMap<IgniteUuid, GridCacheQueueProxy> queuesMap;

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

    /** Set keys used for set iteration. */
    private ConcurrentMap<IgniteUuid, GridConcurrentHashSet<GridCacheSetItemKey>> setDataMap
        = new ConcurrentHashMap8<>();

    /** Sets map. */
    private final ConcurrentMap<IgniteUuid, GridCacheSetProxy> setsMap;

    /** */
    private GridCacheContext atomicsCacheCtx;

    /** */
    private final IgniteAtomicConfiguration atomicCfg;

    /** */
    private IgniteCache<CacheDataStructuresConfigurationKey, Map<String, DataStructureInfo>> utilityCache;

    /**
     * @param ctx Context.
     */
    public CacheDataStructuresProcessor(GridKernalContext ctx) {
        super(ctx);

        dsMap = new ConcurrentHashMap8<>(INITIAL_CAPACITY);
        queuesMap = new ConcurrentHashMap8<>(INITIAL_CAPACITY);
        setsMap = new ConcurrentHashMap8<>(INITIAL_CAPACITY);

        atomicCfg = ctx.config().getAtomicConfiguration();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void start() throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        utilityCache = ctx.cache().jcache(CU.UTILITY_CACHE_NAME);

        assert utilityCache != null;

        if (atomicCfg != null) {
            GridCache atomicsCache = ctx.cache().atomicsCache();

            assert atomicsCache != null;

            dsView = atomicsCache.projection(GridCacheInternal.class, GridCacheInternal.class).flagsOn(CLONE);

            cntDownLatchView = atomicsCache.projection
                (GridCacheInternalKey.class, GridCacheCountDownLatchValue.class).flagsOn(CLONE);

            atomicLongView = atomicsCache.projection
                (GridCacheInternalKey.class, GridCacheAtomicLongValue.class).flagsOn(CLONE);

            atomicRefView = atomicsCache.projection
                (GridCacheInternalKey.class, GridCacheAtomicReferenceValue.class).flagsOn(CLONE);

            atomicStampedView = atomicsCache.projection
                (GridCacheInternalKey.class, GridCacheAtomicStampedValue.class).flagsOn(CLONE);

            seqView = atomicsCache.projection
                (GridCacheInternalKey.class, GridCacheAtomicSequenceValue.class).flagsOn(CLONE);

            atomicsCacheCtx = ctx.cache().internalCache(CU.ATOMICS_CACHE_NAME).context();
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        super.onKernalStop(cancel);

        for (GridCacheQueueProxy q : queuesMap.values())
            q.delegate().onKernalStop();
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

        atomicsCacheCtx.gate().enter();

        try {
            final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

            // Check type of structure received by key from local cache.
            IgniteAtomicSequence val = cast(dsMap.get(key), IgniteAtomicSequence.class);

            if (val != null)
                return val;

            return CU.outTx(new Callable<IgniteAtomicSequence>() {
                @Override public IgniteAtomicSequence call() throws Exception {
                    try (IgniteTx tx = CU.txStartInternal(atomicsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
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
                            atomicsCacheCtx,
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
                }
            }, atomicsCacheCtx);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to get sequence by name: " + name, e);
        }
        finally {
            atomicsCacheCtx.gate().leave();
        }
    }

    /**
     * Removes sequence from cache.
     *
     * @param name Sequence name.
     * @return Method returns {@code true} if sequence has been removed and {@code false} if it's not cached.
     * @throws IgniteCheckedException If removing failed.
     */
    public final boolean removeSequence(String name) throws IgniteCheckedException {
        assert name != null;

        checkAtomicsConfiguration();

        atomicsCacheCtx.gate().enter();

        try {
            GridCacheInternal key = new GridCacheInternalKeyImpl(name);

            return removeInternal(key, GridCacheAtomicSequenceValue.class);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to remove sequence by name: " + name, e);
        }
        finally {
            atomicsCacheCtx.gate().leave();
        }
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
    public final IgniteAtomicLong atomicLong(final String name, final long initVal,
        final boolean create) throws IgniteCheckedException {
        A.notNull(name, "name");

        checkAtomicsConfiguration();

        atomicsCacheCtx.gate().enter();

        try {
            final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

            // Check type of structure received by key from local cache.
            IgniteAtomicLong atomicLong = cast(dsMap.get(key), IgniteAtomicLong.class);

            if (atomicLong != null)
                return atomicLong;

            return CU.outTx(new Callable<IgniteAtomicLong>() {
                @Override public IgniteAtomicLong call() throws Exception {
                    try (IgniteTx tx = CU.txStartInternal(atomicsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                        GridCacheAtomicLongValue val = cast(dsView.get(key),
                            GridCacheAtomicLongValue.class);

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

                        a = new GridCacheAtomicLongImpl(name, key, atomicLongView, atomicsCacheCtx);

                        dsMap.put(key, a);

                        tx.commit();

                        return a;
                    }
                    catch (Error | Exception e) {
                        dsMap.remove(key);

                        U.error(log, "Failed to make atomic long: " + name, e);

                        throw e;
                    }
                }
            }, atomicsCacheCtx);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to get atomic long by name: " + name, e);
        }
        finally {
            atomicsCacheCtx.gate().leave();
        }
    }

    /**
     * Removes atomic long from cache.
     *
     * @param name Atomic long name.
     * @return Method returns {@code true} if atomic long has been removed and {@code false} if it's not cached.
     * @throws IgniteCheckedException If removing failed.
     */
    public final boolean removeAtomicLong(String name) throws IgniteCheckedException {
        assert name != null;

        checkAtomicsConfiguration();

        atomicsCacheCtx.gate().enter();

        try {
            GridCacheInternal key = new GridCacheInternalKeyImpl(name);

            return removeInternal(key, GridCacheAtomicLongValue.class);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to remove atomic long by name: " + name, e);
        }
        finally {
            atomicsCacheCtx.gate().leave();
        }
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

        atomicsCacheCtx.gate().enter();

        try {
            final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

            // Check type of structure received by key from local cache.
            IgniteAtomicReference atomicRef = cast(dsMap.get(key), IgniteAtomicReference.class);

            if (atomicRef != null)
                return atomicRef;

            return CU.outTx(new Callable<IgniteAtomicReference<T>>() {
                @Override public IgniteAtomicReference<T> call() throws Exception {
                    try (IgniteTx tx = CU.txStartInternal(atomicsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
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

                        ref = new GridCacheAtomicReferenceImpl(name, key, atomicRefView, atomicsCacheCtx);

                        dsMap.put(key, ref);

                        tx.commit();

                        return ref;
                    }
                    catch (Error | Exception e) {
                        dsMap.remove(key);

                        U.error(log, "Failed to make atomic reference: " + name, e);

                        throw e;
                    }
                }
            }, atomicsCacheCtx);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to get atomic reference by name: " + name, e);
        }
        finally {
            atomicsCacheCtx.gate().leave();

        }
    }

    /**
     * Removes atomic reference from cache.
     *
     * @param name Atomic reference name.
     * @return Method returns {@code true} if atomic reference has been removed and {@code false} if it's not cached.
     * @throws IgniteCheckedException If removing failed.
     */
    public final boolean removeAtomicReference(String name) throws IgniteCheckedException {
        assert name != null;

        checkAtomicsConfiguration();

        atomicsCacheCtx.gate().enter();

        try {
            GridCacheInternal key = new GridCacheInternalKeyImpl(name);

            return removeInternal(key, GridCacheAtomicReferenceValue.class);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to remove atomic reference by name: " + name, e);
        }
        finally {
            atomicsCacheCtx.gate().enter();

        }
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

        atomicsCacheCtx.gate().enter();

        try {
            final GridCacheInternalKeyImpl key = new GridCacheInternalKeyImpl(name);

            // Check type of structure received by key from local cache.
            IgniteAtomicStamped atomicStamped = cast(dsMap.get(key), IgniteAtomicStamped.class);

            if (atomicStamped != null)
                return atomicStamped;

            return CU.outTx(new Callable<IgniteAtomicStamped<T, S>>() {
                @Override public IgniteAtomicStamped<T, S> call() throws Exception {
                    try (IgniteTx tx = CU.txStartInternal(atomicsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
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

                        stmp = new GridCacheAtomicStampedImpl(name, key, atomicStampedView, atomicsCacheCtx);

                        dsMap.put(key, stmp);

                        tx.commit();

                        return stmp;
                    }
                    catch (Error | Exception e) {
                        dsMap.remove(key);

                        U.error(log, "Failed to make atomic stamped: " + name, e);

                        throw e;
                    }
                }
            }, atomicsCacheCtx);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to get atomic stamped by name: " + name, e);
        }
        finally {
            atomicsCacheCtx.gate().leave();

        }
    }

    /**
     * Removes atomic stamped from cache.
     *
     * @param name Atomic stamped name.
     * @return Method returns {@code true} if atomic stamped has been removed and {@code false} if it's not cached.
     * @throws IgniteCheckedException If removing failed.
     */
    public final boolean removeAtomicStamped(String name) throws IgniteCheckedException {
        assert name != null;

        checkAtomicsConfiguration();

        atomicsCacheCtx.gate().enter();

        try {
            GridCacheInternal key = new GridCacheInternalKeyImpl(name);

            return removeInternal(key, GridCacheAtomicStampedValue.class);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to remove atomic stamped by name: " + name, e);
        }
        finally {
            atomicsCacheCtx.gate().leave();
        }
    }

    /**
     * Gets a queue from cache or creates one if it's not cached.
     *
     * @param name Name of queue.
     * @param cfg Queue configuration.
     * @param cap Max size of queue.
     * @param create If {@code true} queue will be created in case it is not in cache.
     * @return Instance of queue.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public final <T> IgniteQueue<T> queue(final String name,
        @Nullable IgniteCollectionConfiguration cfg,
        int cap,
        boolean create)
        throws IgniteCheckedException {
        A.notNull(name, "name");

        if (create) {
            A.notNull(cfg, "cfg");

            if (cap <= 0)
                cap = Integer.MAX_VALUE;
        }

        GridCacheAdapter cache = cacheForCollection(cfg);

        GridCacheContext cctx = cache.context();

        // Non collocated mode enabled only for PARTITIONED cache.
        final boolean colloc =
            create && (cctx.cache().configuration().getCacheMode() != PARTITIONED || cfg.isCollocated());

        GridCacheQueueHeader hdr = cctx.dataStructures().queue(name, cap, colloc, create);

        if (hdr == null)
            return null;

        GridCacheQueueProxy queue = queuesMap.get(hdr.id());

        if (queue == null) {
            queue = new GridCacheQueueProxy(cctx, cctx.atomic() ? new GridAtomicCacheQueueImpl<>(name, hdr, cctx) :
                new GridTransactionalCacheQueueImpl<>(name, hdr, cctx));

            GridCacheQueueProxy old = queuesMap.putIfAbsent(hdr.id(), queue);

            if (old != null)
                queue = old;
        }

        return queue;
    }

    /**
     * @param key Queue header key.
     * @param hdr Current queue header.
     * @param oldHdr Previous queue header value.
     */
    public void onQueueUpdated(GridCacheQueueHeaderKey key,
        @Nullable GridCacheQueueHeader hdr,
        @Nullable GridCacheQueueHeader oldHdr) {
        for (final GridCacheQueueProxy queue : queuesMap.values()) {
            if (queue.name().equals(key.queueName())) {
                if (hdr == null) {
                    assert oldHdr != null;

                    if (oldHdr.id().equals(queue.delegate().id())) {
                        queue.delegate().onRemoved(false);

                        queuesMap.remove(queue.delegate().id());
                    }
                }
                else
                    queue.delegate().onHeaderChanged(hdr);
            }
        }
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

        A.ensure(cnt >= 0, "count can not be negative");

        checkAtomicsConfiguration();

        atomicsCacheCtx.gate().enter();

        try {
            final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

            // Check type of structure received by key from local cache.
            IgniteCountDownLatch latch = cast(dsMap.get(key), IgniteCountDownLatch.class);

            if (latch != null)
                return latch;

            return CU.outTx(new Callable<IgniteCountDownLatch>() {
                @Override public IgniteCountDownLatch call() throws Exception {
                    try (IgniteTx tx = CU.txStartInternal(atomicsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
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
                            val.autoDelete(), key, cntDownLatchView, atomicsCacheCtx);

                        dsMap.put(key, latch);

                        tx.commit();

                        return latch;
                    }
                    catch (Error | Exception e) {
                        dsMap.remove(key);

                        U.error(log, "Failed to create count down latch: " + name, e);

                        throw e;
                    }
                }
            }, atomicsCacheCtx);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to get count down latch by name: " + name, e);
        }
        finally {
            atomicsCacheCtx.gate().leave();
        }
    }

    /**
     * Removes count down latch from cache.
     *
     * @param name Name of the latch.
     * @return Count down latch for the given name.
     * @throws IgniteCheckedException If operation failed.
     */
    public boolean removeCountDownLatch(final String name) throws IgniteCheckedException {
        assert name != null;

        checkAtomicsConfiguration();

        atomicsCacheCtx.gate().enter();

        try {
            return CU.outTx(
                new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        GridCacheInternal key = new GridCacheInternalKeyImpl(name);

                        try (IgniteTx tx = CU.txStartInternal(atomicsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
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
                atomicsCacheCtx);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to remove count down latch by name: " + name, e);
        }
        finally {
            atomicsCacheCtx.gate().leave();
        }
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
                    try (IgniteTx tx = CU.txStartInternal(atomicsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
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
            atomicsCacheCtx
        );
    }

    /**
     * Transaction committed callback for transaction manager.
     *
     * @param tx Committed transaction.
     */
    public <K, V> void onTxCommitted(IgniteTxEx<K, V> tx) {
        if (atomicsCacheCtx == null)
            return;

        if (!atomicsCacheCtx.isDht() && tx.internal() && (!atomicsCacheCtx.isColocated() || atomicsCacheCtx.isReplicated())) {
            Collection<IgniteTxEntry<K, V>> entries = tx.writeEntries();

            if (log.isDebugEnabled())
                log.debug("Committed entries: " + entries);

            for (IgniteTxEntry<K, V> entry : entries) {
                // Check updated or created GridCacheInternalKey keys.
                if ((entry.op() == CREATE || entry.op() == UPDATE) && entry.key() instanceof GridCacheInternalKey) {
                    GridCacheInternal key = (GridCacheInternal)entry.key();

                    if (entry.value() instanceof GridCacheCountDownLatchValue) {
                        // Notify latch on changes.
                        GridCacheRemovable latch = dsMap.get(key);

                        GridCacheCountDownLatchValue val = (GridCacheCountDownLatchValue)entry.value();

                        if (latch instanceof GridCacheCountDownLatchEx) {
                            GridCacheCountDownLatchEx latch0 = (GridCacheCountDownLatchEx)latch;

                            latch0.onUpdate(val.get());

                            if (val.get() == 0 && val.autoDelete()) {
                                entry.cached().markObsolete(atomicsCacheCtx.versions().next());

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
                if (entry.op() == DELETE && entry.key() instanceof GridCacheInternal) {
                    GridCacheInternal key = (GridCacheInternal)entry.key();

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
     * @param cfg Set configuration.
     * @param create If {@code true} set will be created in case it is not in cache.
     * @return Set instance.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public <T> IgniteSet<T> set(final String name,
        @Nullable IgniteCollectionConfiguration cfg,
        final boolean create)
        throws IgniteCheckedException {
        A.notNull(name, "name");

        if (create)
            A.notNull(cfg, "cfg");

        GridCacheAdapter cache = cacheForCollection(cfg);

        final GridCacheContext cctx = cache.context();

        // Non collocated mode enabled only for PARTITIONED cache.
        final boolean colloc =
            create && (cctx.cache().configuration().getCacheMode() != PARTITIONED || cfg.isCollocated());

        if (cctx.atomic())
            return set0(cctx, name, colloc, create);

        return CU.outTx(new Callable<IgniteSet<T>>() {
            @Nullable @Override public IgniteSet<T> call() throws Exception {
                return set0(cctx, name, colloc, create);
            }
        }, cctx);
    }

    /**
     * @param cctx Cache context.
     * @param name Name of set.
     * @param collocated Collocation flag.
     * @param create If {@code true} set will be created in case it is not in cache.
     * @return Set.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    @Nullable private <T> IgniteSet<T> set0(GridCacheContext cctx,
        String name,
        boolean collocated,
        boolean create)
        throws IgniteCheckedException
    {
        cctx.gate().enter();

        try {
            GridCacheSetHeaderKey key = new GridCacheSetHeaderKey(name);

            GridCacheSetHeader hdr;

            GridCacheAdapter cache = cctx.cache();

            if (create) {
                hdr = new GridCacheSetHeader(IgniteUuid.randomUuid(), collocated);

                GridCacheSetHeader old = retryPutIfAbsent(cache, key, hdr);

                if (old != null)
                    hdr = old;
            }
            else
                hdr = (GridCacheSetHeader)cache.get(key);

            if (hdr == null)
                return null;

            GridCacheSetProxy<T> set = setsMap.get(hdr.id());

            if (set == null) {
                GridCacheSetProxy<T> old = setsMap.putIfAbsent(hdr.id(),
                    set = new GridCacheSetProxy<>(cctx, new GridCacheSetImpl<T>(cctx, name, hdr)));

                if (old != null)
                    set = old;
            }

            return set;
        }
        finally {
            cctx.gate().leave();
        }
    }

    /**
     * @param cctx Cache context.
     * @param name Set name.
     * @return {@code True} if set was removed.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public boolean removeSet(GridCacheContext cctx, String name) throws IgniteCheckedException {
        GridCacheSetHeaderKey key = new GridCacheSetHeaderKey(name);

        GridCache cache = cctx.cache();

        GridCacheSetHeader hdr = retryRemove(cache, key);

        if (hdr == null)
            return false;

        if (!cctx.isLocal()) {
            while (true) {
                long topVer = cctx.topologyVersionFuture().get();

                Collection<ClusterNode> nodes = CU.affinityNodes(cctx, topVer);

                try {
                    cctx.closures().callAsyncNoFailover(BROADCAST,
                        new BlockSetCallable(hdr.id()),
                        nodes,
                        true).get();
                }
                catch (ClusterTopologyException e) {
                    if (log.isDebugEnabled())
                        log.debug("BlockSet job failed, will retry: " + e);

                    continue;
                }

                try {
                    cctx.closures().callAsyncNoFailover(BROADCAST,
                        new RemoveSetDataCallable(cctx.name(), hdr.id(), topVer),
                        nodes,
                        true).get();
                }
                catch (ClusterTopologyException e) {
                    if (log.isDebugEnabled())
                        log.debug("RemoveSetData job failed, will retry: " + e);

                    continue;
                }

                if (cctx.topologyVersionFuture().get() == topVer)
                    break;
            }
        }
        else {
            blockSet(hdr.id());

            removeSetData(cctx, hdr.id(), 0);
        }

        return true;
    }

    /**
     * @param setId Set ID.
     */
    @SuppressWarnings("unchecked")
    private void blockSet(IgniteUuid setId) {
        GridCacheSetProxy set = setsMap.remove(setId);

        if (set != null)
            set.blockOnRemove();
    }

    /**
     * @param cctx Cache context.
     * @param setId Set ID.
     * @param topVer Topology version.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private void removeSetData(GridCacheContext cctx, IgniteUuid setId, long topVer) throws IgniteCheckedException {
        boolean loc = cctx.isLocal();

        GridCacheAffinityManager aff = cctx.affinity();

        if (!loc) {
            aff.affinityReadyFuture(topVer).get();

            cctx.preloader().syncFuture().get();
        }

        GridConcurrentHashSet<GridCacheSetItemKey> set = setDataMap.get(setId);

        if (set == null)
            return;

        GridCache cache = cctx.cache();

        final int BATCH_SIZE = 100;

        Collection<GridCacheSetItemKey> keys = new ArrayList<>(BATCH_SIZE);

        for (GridCacheSetItemKey key : set) {
           if (!loc && !aff.primary(cctx.localNode(), key, topVer))
               continue;

            keys.add(key);

            if (keys.size() == BATCH_SIZE) {
                retryRemoveAll(cache, keys);

                keys.clear();
            }
        }

        if (!keys.isEmpty())
            retryRemoveAll(cache, keys);

        setDataMap.remove(setId);
    }

    /**
     * @param call Callable.
     * @return Callable result.
     * @throws IgniteCheckedException If all retrys failed.
     */
    <R> R retry(Callable<R> call) throws IgniteCheckedException {
        try {
            int cnt = 0;

            while (true) {
                try {
                    return call.call();
                }
                catch (ClusterGroupEmptyException e) {
                    throw new IgniteException(e);
                }
                catch (IgniteTxRollbackException | CachePartialUpdateCheckedException | ClusterTopologyException e) {
                    if (cnt++ == MAX_UPDATE_RETRIES)
                        throw e;
                    else {
                        U.warn(log, "Failed to execute data structure operation, will retry [err=" + e + ']');

                        U.sleep(RETRY_DELAY);
                    }
                }
            }
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param cache Cache.
     * @param key Key to remove.
     * @throws IgniteCheckedException If failed.
     * @return Removed value.
     */
    @SuppressWarnings("unchecked")
    @Nullable private <T> T retryRemove(final GridCache cache, final Object key) throws IgniteCheckedException {
        return retry(new Callable<T>() {
            @Nullable @Override public T call() throws Exception {
                return (T)cache.remove(key);
            }
        });
    }

    /**
     * @param cache Cache.
     * @param keys Keys to remove.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private void retryRemoveAll(final GridCache cache, final Collection<GridCacheSetItemKey> keys)
        throws IgniteCheckedException {
        retry(new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.removeAll(keys);

                return null;
            }
        });
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @param val Value.
     * @throws IgniteCheckedException If failed.
     * @return Previous value.
     */
    @SuppressWarnings("unchecked")
    @Nullable private <T> T retryPutIfAbsent(final GridCache cache, final Object key, final T val)
        throws IgniteCheckedException {
        return retry(new Callable<T>() {
            @Nullable @Override public T call() throws Exception {
                return (T)cache.putIfAbsent(key, val);
            }
        });
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
            throw new IgniteCheckedException("Failed to cast object [expected=" + cls + ", actual=" + obj.getClass() + ']');
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Data structure processor memory stats [grid=" + ctx.gridName() +
            ", cache=" + (atomicsCacheCtx != null ? atomicsCacheCtx.name() : null) + ']');
        X.println(">>>   dsMapSize: " + dsMap.size());
    }

    /**
     * @param cfg Collection configuration.
     * @return Cache to use for collection.
     */
    private GridCacheAdapter cacheForCollection(IgniteCollectionConfiguration cfg) {
        // TODO IGNITE-29: start collection internal cache with required configuration or use existing one.
        GridCacheAdapter cache = ctx.cache().internalCache("TEST_COLLECTION_CACHE");

        if (cache == null)
            throw new IgniteException("TEST_COLLECTION_CACHE is not configured.");

        if (cfg != null) {
            CacheConfiguration ccfg = cache.configuration();

            assert ccfg.getCacheMode() == cfg.getCacheMode();
            assert ccfg.getAtomicityMode() == cfg.getAtomicityMode();
            assert ccfg.getMemoryMode() == cfg.getMemoryMode();
            assert ccfg.getDistributionMode() == cfg.getDistributionMode();
        }

        return cache;
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
     * Waits for completion of all started set operations and blocks all subsequent operations.
     */
    @GridInternal
    private static class BlockSetCallable implements Callable<Void>, Externalizable {
        /** */
        private static final long serialVersionUID = 0;

        /** Injected grid instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private IgniteUuid setId;

        /**
         * Required by {@link Externalizable}.
         */
        public BlockSetCallable() {
            // No-op.
        }

        /**
         * @param setId Set ID.
         */
        private BlockSetCallable(IgniteUuid setId) {
            this.setId = setId;
        }

        /** {@inheritDoc} */
        @Override public Void call() throws IgniteCheckedException {
            assert ignite != null;

            ((GridKernal)ignite).context().dataStructures().blockSet(setId);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeGridUuid(out, setId);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            setId = U.readGridUuid(in);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "BlockSetCallable [setId=" + setId + ']';
        }
    }

    /**
     * Removes set items.
     */
    @GridInternal
    private static class RemoveSetDataCallable implements Callable<Void>, Externalizable {
        /** */
        private static final long serialVersionUID = 5053205121218843148L;

        /** Injected grid instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private String cacheName;

        /** */
        private IgniteUuid setId;

        /** */
        private long topVer;

        /**
         * Required by {@link Externalizable}.
         */
        public RemoveSetDataCallable() {
            // No-op.
        }

        /**
         * @param cacheName Cache name.
         * @param setId Set ID.
         * @param topVer Topology version.
         */
        private RemoveSetDataCallable(String cacheName, IgniteUuid setId, long topVer) {
            this.cacheName = cacheName;
            this.setId = setId;
            this.topVer = topVer;
        }

        /** {@inheritDoc} */
        @Override public Void call() throws IgniteCheckedException {
            assert ignite != null;

            GridCacheAdapter cache = ((GridKernal)ignite).context().cache().internalCache(cacheName);

            assert cache != null;

            GridCacheGateway gate = cache.context().gate();

            gate.enter();

            try {
                ((GridKernal)ignite).context().dataStructures().removeSetData(cache.context(), setId, topVer);
            }
            finally {
                gate.leave();
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, cacheName);
            U.writeGridUuid(out, setId);
            out.writeLong(topVer);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            cacheName = U.readString(in);
            setId = U.readGridUuid(in);
            topVer = in.readLong();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "RemoveSetCallable [setId=" + setId + ']';
        }
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
    }

    /**
     * @param info New data structure information.
     */
    private void validateDataStructure(DataStructureInfo info) {
        Map<String, DataStructureInfo> map = utilityCache.get(DATA_STRUCTURES_KEY);

        if (map != null) {
            DataStructureInfo oldInfo = map.get(info.name);

            if (oldInfo != null) {
                IgniteException err = validateDataStructure(oldInfo, info);

                if (err != null)
                    throw err;
            }
        }
    }

    /**
     * @param oldInfo Existing data structure information.
     * @param info New data structure information.
     * @return {@link IgniteException} if validation failed.
     */
    @Nullable private static IgniteException validateDataStructure(DataStructureInfo oldInfo, DataStructureInfo info) {
        if (oldInfo.type != info.type) {
            return new IgniteException("Another data structure with the same name already created " +
                "[name= " + info.name +
                ", new= " + info.type.className() +
                ", existing=" + oldInfo.type.className() + ']');
        }

        return null;
    }

    /**
     *
     */
    static class AddAtomicProcessor implements
        EntryProcessor<CacheDataStructuresConfigurationKey, Map<String, DataStructureInfo>, IgniteException>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private DataStructureInfo info;

        /**
         * @param info Data structure information.
         */
        AddAtomicProcessor(DataStructureInfo info) {
            this.info = info;
        }

        /**
         * Required by {@link Externalizable}.
         */
        public AddAtomicProcessor() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public IgniteException process(
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

            return validateDataStructure(oldInfo, info);
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
    }
}

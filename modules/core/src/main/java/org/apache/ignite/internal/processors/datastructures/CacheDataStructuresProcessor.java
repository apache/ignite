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
import org.apache.ignite.transactions.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.internal.processors.cache.CacheFlag.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;
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
    private final IgniteAtomicConfiguration atomicCfg;

    /**
     * @param ctx Context.
     */
    public CacheDataStructuresProcessor(GridKernalContext ctx) {
        super(ctx);

        dsMap = new ConcurrentHashMap8<>(INITIAL_CAPACITY);

        atomicCfg = ctx.config().getAtomicConfiguration();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void start() throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

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

        dsCacheCtx.gate().enter();

        try {
            final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

            // Check type of structure received by key from local cache.
            IgniteAtomicSequence val = cast(dsMap.get(key), IgniteAtomicSequence.class);

            if (val != null)
                return val;

            return CU.outTx(new Callable<IgniteAtomicSequence>() {
                @Override public IgniteAtomicSequence call() throws Exception {
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
                }
            }, dsCacheCtx);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to get sequence by name: " + name, e);
        }
        finally {
            dsCacheCtx.gate().leave();
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

        dsCacheCtx.gate().enter();

        try {
            GridCacheInternal key = new GridCacheInternalKeyImpl(name);

            return removeInternal(key, GridCacheAtomicSequenceValue.class);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to remove sequence by name: " + name, e);
        }
        finally {
            dsCacheCtx.gate().leave();
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

        dsCacheCtx.gate().enter();

        try {
            final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

            // Check type of structure received by key from local cache.
            IgniteAtomicLong atomicLong = cast(dsMap.get(key), IgniteAtomicLong.class);

            if (atomicLong != null)
                return atomicLong;

            return CU.outTx(new Callable<IgniteAtomicLong>() {
                @Override public IgniteAtomicLong call() throws Exception {
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
                }
            }, dsCacheCtx);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to get atomic long by name: " + name, e);
        }
        finally {
            dsCacheCtx.gate().leave();
        }
    }

    /**
     * Removes atomic long from cache.
     *
     * @param name Atomic long name.
     * @throws IgniteCheckedException If removing failed.
     */
    public final void removeAtomicLong(String name) throws IgniteCheckedException {
        assert name != null;
        assert dsCacheCtx != null;

        dsCacheCtx.gate().enter();

        try {
            GridCacheInternal key = new GridCacheInternalKeyImpl(name);

            removeInternal(key, GridCacheAtomicLongValue.class);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to remove atomic long by name: " + name, e);
        }
        finally {
            dsCacheCtx.gate().leave();
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

        dsCacheCtx.gate().enter();

        try {
            final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

            // Check type of structure received by key from local cache.
            IgniteAtomicReference atomicRef = cast(dsMap.get(key), IgniteAtomicReference.class);

            if (atomicRef != null)
                return atomicRef;

            return CU.outTx(new Callable<IgniteAtomicReference<T>>() {
                @Override public IgniteAtomicReference<T> call() throws Exception {
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
                }
            }, dsCacheCtx);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to get atomic reference by name: " + name, e);
        }
        finally {
            dsCacheCtx.gate().leave();
        }
    }

    /**
     * Removes atomic reference from cache.
     *
     * @param name Atomic reference name.
     * @throws IgniteCheckedException If removing failed.
     */
    public final void removeAtomicReference(String name) throws IgniteCheckedException {
        assert name != null;
        assert dsCacheCtx != null;

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

        dsCacheCtx.gate().enter();

        try {
            final GridCacheInternalKeyImpl key = new GridCacheInternalKeyImpl(name);

            // Check type of structure received by key from local cache.
            IgniteAtomicStamped atomicStamped = cast(dsMap.get(key), IgniteAtomicStamped.class);

            if (atomicStamped != null)
                return atomicStamped;

            return CU.outTx(new Callable<IgniteAtomicStamped<T, S>>() {
                @Override public IgniteAtomicStamped<T, S> call() throws Exception {
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
                }
            }, dsCacheCtx);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to get atomic stamped by name: " + name, e);
        }
        finally {
            dsCacheCtx.gate().leave();
        }
    }

    /**
     * Removes atomic stamped from cache.
     *
     * @param name Atomic stamped name.
     * @throws IgniteCheckedException If removing failed.
     */
    public final void removeAtomicStamped(String name) throws IgniteCheckedException {
        assert name != null;
        assert dsCacheCtx != null;

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

        return cache.context().dataStructures().queue(name, cap, create && cfg.isCollocated(), create);
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

        dsCacheCtx.gate().enter();

        try {
            final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

            // Check type of structure received by key from local cache.
            IgniteCountDownLatch latch = cast(dsMap.get(key), IgniteCountDownLatch.class);

            if (latch != null)
                return latch;

            return CU.outTx(new Callable<IgniteCountDownLatch>() {
                @Override public IgniteCountDownLatch call() throws Exception {
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
                }
            }, dsCacheCtx);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to get count down latch by name: " + name, e);
        }
        finally {
            dsCacheCtx.gate().leave();
        }
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

        dsCacheCtx.gate().enter();

        try {
            CU.outTx(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    GridCacheInternal key = new GridCacheInternalKeyImpl(name);

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
            }, dsCacheCtx);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to remove count down latch by name: " + name, e);
        }
        finally {
            dsCacheCtx.gate().leave();
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
    public <K, V> void onTxCommitted(IgniteInternalTx<K, V> tx) {
        if (dsCacheCtx == null)
            return;

        if (!dsCacheCtx.isDht() && tx.internal() && (!dsCacheCtx.isColocated() || dsCacheCtx.isReplicated())) {
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
    @SuppressWarnings("unchecked")
    @Nullable public <T> IgniteSet<T> set(final String name,
        @Nullable IgniteCollectionConfiguration cfg,
        final boolean create)
        throws IgniteCheckedException {
        A.notNull(name, "name");

        if (create)
            A.notNull(cfg, "cfg");

        GridCacheAdapter cache = cacheForCollection(cfg);

        return cache.context().dataStructures().set(name, create ? cfg.isCollocated() : false, create);
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
     * @param cfg Collection configuration.
     * @return Cache to use for collection.
     * @throws IgniteCheckedException If cache is not configured.
     */
    private GridCacheAdapter cacheForCollection(IgniteCollectionConfiguration cfg) throws IgniteCheckedException {
        if (ctx.cache().publicCache(cfg.getCacheName()) == null)
            throw new IgniteCheckedException("Cache for collection is not configured: " + cfg.getCacheName());

        GridCacheAdapter cache = ctx.cache().internalCache(cfg.getCacheName());

        assert cache != null : cfg.getCacheName();

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
}

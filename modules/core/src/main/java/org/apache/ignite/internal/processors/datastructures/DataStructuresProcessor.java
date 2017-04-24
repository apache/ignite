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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheInternal;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.util.lang.IgniteClosureX;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.lang.IgniteOutClosureX;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.GPR;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.DataStructureType.ATOMIC_LONG;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.DataStructureType.ATOMIC_REF;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.DataStructureType.ATOMIC_SEQ;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.DataStructureType.ATOMIC_STAMPED;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.DataStructureType.COUNT_DOWN_LATCH;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.DataStructureType.QUEUE;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.DataStructureType.REENTRANT_LOCK;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.DataStructureType.SEMAPHORE;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.DataStructureType.SET;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Manager of data structures.
 */
public final class DataStructuresProcessor extends GridProcessorAdapter implements IgniteChangeGlobalStateSupport {
    /** */
    public static final CacheDataStructuresConfigurationKey DATA_STRUCTURES_KEY =
        new CacheDataStructuresConfigurationKey();

    /** */
    private static final CacheDataStructuresCacheKey DATA_STRUCTURES_CACHE_KEY =
        new CacheDataStructuresCacheKey();

    /** Initial capacity. */
    private static final int INITIAL_CAPACITY = 10;

    /** Initialization latch. */
    private volatile CountDownLatch initLatch = new CountDownLatch(1);

    /** Initialization failed flag. */
    private boolean initFailed;

    /** Cache contains only {@code GridCacheInternal,GridCacheInternal}. */
    private IgniteInternalCache<GridCacheInternal, GridCacheInternal> dsView;

    /** Internal storage of all dataStructures items (sequence, atomic long etc.). */
    private final ConcurrentMap<GridCacheInternal, GridCacheRemovable> dsMap;

    /** Cache contains only {@code GridCacheAtomicValue}. */
    private IgniteInternalCache<GridCacheInternalKey, GridCacheAtomicLongValue> atomicLongView;

    /** Cache contains only {@code GridCacheCountDownLatchValue}. */
    private IgniteInternalCache<GridCacheInternalKey, GridCacheCountDownLatchValue> cntDownLatchView;

    /** Cache contains only {@code GridCacheSemaphoreState}. */
    private IgniteInternalCache<GridCacheInternalKey, GridCacheSemaphoreState> semView;

    /** Cache contains only {@code GridCacheLockState}. */
    private IgniteInternalCache<GridCacheInternalKey, GridCacheLockState> reentrantLockView;

    /** Cache contains only {@code GridCacheAtomicReferenceValue}. */
    private IgniteInternalCache<GridCacheInternalKey, GridCacheAtomicReferenceValue> atomicRefView;

    /** Cache contains only {@code GridCacheAtomicStampedValue}. */
    private IgniteInternalCache<GridCacheInternalKey, GridCacheAtomicStampedValue> atomicStampedView;

    /** Cache contains only entry {@code GridCacheSequenceValue}. */
    private IgniteInternalCache<GridCacheInternalKey, GridCacheAtomicSequenceValue> seqView;

    /** Cache context for atomic data structures. */
    private GridCacheContext dsCacheCtx;

    /** Atomic data structures configuration. */
    private final AtomicConfiguration atomicCfg;

    /** */
    private IgniteInternalCache<CacheDataStructuresConfigurationKey, Map<String, DataStructureInfo>> utilityCache;

    /** */
    private IgniteInternalCache<CacheDataStructuresCacheKey, List<CacheCollectionInfo>> utilityDataCache;

    /** */
    private volatile UUID qryId;

    /** Listener. */
    private final GridLocalEventListener lsnr = new GridLocalEventListener() {
        @Override public void onEvent(final Event evt) {
            // This may require cache operation to execute,
            // therefore cannot use event notification thread.
            ctx.closure().callLocalSafe(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                        UUID leftNodeId = discoEvt.eventNode().id();

                        for (GridCacheRemovable ds : dsMap.values()) {
                            if (ds instanceof GridCacheSemaphoreEx)
                                ((GridCacheSemaphoreEx)ds).onNodeRemoved(leftNodeId);
                            else if (ds instanceof GridCacheLockEx)
                                ((GridCacheLockEx)ds).onNodeRemoved(leftNodeId);
                        }

                        return null;
                    }
                },
                false);
        }
    };

    /**
     * @param ctx Context.
     */
    public DataStructuresProcessor(GridKernalContext ctx) {
        super(ctx);

        dsMap = new ConcurrentHashMap8<>(INITIAL_CAPACITY);

        atomicCfg = ctx.config().getAtomicConfiguration();
    }

    /** {@inheritDoc} */
    @Override public void start(boolean activeOnStart) throws IgniteCheckedException {
        super.start(activeOnStart);

        if (!activeOnStart)
            return;

        ctx.event().addLocalEventListener(lsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onKernalStart(boolean activeOnStart) throws IgniteCheckedException {
        if (ctx.config().isDaemon() || !ctx.state().active())
            return;

        onKernalStart0(activeOnStart);
    }

    /**
     *
     */
    private void onKernalStart0(boolean activeOnStart){
        if (!activeOnStart && ctx.state().active())
            ctx.event().addLocalEventListener(lsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);

        utilityCache = (IgniteInternalCache)ctx.cache().utilityCache();

        utilityDataCache = (IgniteInternalCache)ctx.cache().utilityCache();

        assert utilityCache != null;

        if (atomicCfg != null) {
            IgniteInternalCache atomicsCache = ctx.cache().atomicsCache();

            assert atomicsCache != null;

            dsView = atomicsCache;

            cntDownLatchView = atomicsCache;

            semView = atomicsCache;

            reentrantLockView = atomicsCache;

            atomicLongView = atomicsCache;

            atomicRefView = atomicsCache;

            atomicStampedView = atomicsCache;

            seqView = atomicsCache;

            dsCacheCtx = atomicsCache.context();
        }

        initLatch.countDown();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void startQuery() throws IgniteCheckedException {
        if (qryId == null) {
            synchronized (this) {
                if (qryId == null) {
                    qryId = dsCacheCtx.continuousQueries().executeInternalQuery(
                        new DataStructuresEntryListener(),
                        new DataStructuresEntryFilter(),
                        dsCacheCtx.isReplicated() && dsCacheCtx.affinityNode(),
                        false,
                        false
                    );
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        super.onKernalStop(cancel);

        for (GridCacheRemovable ds : dsMap.values()) {
            if (ds instanceof GridCacheSemaphoreEx)
                ((GridCacheSemaphoreEx)ds).stop();

            if (ds instanceof GridCacheLockEx)
                ((GridCacheLockEx)ds).onStop();
        }

        if (initLatch.getCount() > 0) {
            initFailed = true;

            initLatch.countDown();
        }

        if (qryId != null)
            dsCacheCtx.continuousQueries().cancelInternalQuery(qryId);
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext ctx) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Activate data structure processor [nodeId=" + ctx.localNodeId() +
                " topVer=" + ctx.discovery().topologyVersionEx() + " ]");

        this.initFailed = false;

        this.initLatch = new CountDownLatch(1);

        this.qryId = null;

        ctx.event().addLocalEventListener(lsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);

        onKernalStart0(true);

        for (Map.Entry<GridCacheInternal, GridCacheRemovable> e : dsMap.entrySet()) {
            GridCacheRemovable v = e.getValue();

            if (v instanceof IgniteChangeGlobalStateSupport)
                ((IgniteChangeGlobalStateSupport)v).onActivate(ctx);
        }
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext ctx) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("DeActivate data structure processor [nodeId=" + ctx.localNodeId() +
                " topVer=" + ctx.discovery().topologyVersionEx() + " ]");

        ctx.event().removeLocalEventListener(lsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);

        onKernalStop(false);

        for (Map.Entry<GridCacheInternal, GridCacheRemovable> e : dsMap.entrySet()) {
            GridCacheRemovable v = e.getValue();

            if (v instanceof IgniteChangeGlobalStateSupport)
                ((IgniteChangeGlobalStateSupport)v).onDeActivate(ctx);
        }
    }

    /**
     * @param key Key.
     * @param obj Object.
     */
    void onRemoved(GridCacheInternal key, GridCacheRemovable obj) {
        dsMap.remove(key, obj);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> onReconnected(boolean clusterRestarted) throws IgniteCheckedException {
        for (Map.Entry<GridCacheInternal, GridCacheRemovable> e : dsMap.entrySet()) {
            GridCacheRemovable obj = e.getValue();

            if (clusterRestarted) {
                obj.onRemoved();

                dsMap.remove(e.getKey(), obj);
            }
            else
                obj.needCheckNotRemoved();
        }

        for (GridCacheContext cctx : ctx.cache().context().cacheContexts())
            cctx.dataStructures().onReconnected(clusterRestarted);

        return null;
    }

    /**
     * Gets a sequence from cache or creates one if it's not cached.
     *
     * @param name Sequence name.
     * @param initVal Initial value for sequence. If sequence already cached, {@code initVal} will be ignored.
     * @param create If {@code true} sequence will be created in case it is not in cache.
     * @return Sequence.
     * @throws IgniteCheckedException If loading failed.
     */
    public final IgniteAtomicSequence sequence(final String name,
        final long initVal,
        final boolean create)
        throws IgniteCheckedException
    {
        A.notNull(name, "name");

        awaitInitialization();

        checkAtomicsConfiguration();

        startQuery();

        return getAtomic(new IgniteOutClosureX<IgniteAtomicSequence>() {
            @Override public IgniteAtomicSequence applyx() throws IgniteCheckedException {
                GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

                dsCacheCtx.gate().enter();

                try (GridNearTxLocal tx = CU.txStartInternal(dsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
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
                    dsView.put(key, seqVal);

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
    final void removeSequence(final String name) throws IgniteCheckedException {
        assert name != null;

        awaitInitialization();

        checkAtomicsConfiguration();

        removeDataStructure(new IgniteOutClosureX<Void>() {
            @Override public Void applyx() throws IgniteCheckedException {
                dsCacheCtx.gate().enter();

                try {
                    dsView.remove(new GridCacheInternalKeyImpl(name));
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

        awaitInitialization();

        checkAtomicsConfiguration();

        startQuery();

        return getAtomic(new IgniteOutClosureX<IgniteAtomicLong>() {
            @Override public IgniteAtomicLong applyx() throws IgniteCheckedException {
                final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

                dsCacheCtx.gate().enter();

                try (GridNearTxLocal tx = CU.txStartInternal(dsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
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

                        dsView.put(key, val);
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
        final DataStructureInfo dsInfo,
        final boolean create,
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

        return retryTopologySafe(new IgniteOutClosureX<T>() {
            @Override public T applyx() throws IgniteCheckedException {
                if (!create)
                    return c.applyx();

                try (GridNearTxLocal tx = utilityCache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                    IgniteCheckedException err =
                        utilityCache.invoke(DATA_STRUCTURES_KEY, new AddAtomicProcessor(dsInfo)).get();

                    if (err != null)
                        throw err;

                    T dataStructure = c.applyx();

                    tx.commit();

                    return dataStructure;
                }
            }
        });
    }

    /**
     * Removes atomic long from cache.
     *
     * @param name Atomic long name.
     * @throws IgniteCheckedException If removing failed.
     */
    final void removeAtomicLong(final String name) throws IgniteCheckedException {
        assert name != null;
        assert dsCacheCtx != null;

        awaitInitialization();

        removeDataStructure(new IgniteOutClosureX<Void>() {
            @Override public Void applyx() throws IgniteCheckedException {
                dsCacheCtx.gate().enter();

                try {
                    dsView.remove(new GridCacheInternalKeyImpl(name));
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
    private <T> void removeDataStructure(final IgniteOutClosureX<T> c,
        String name,
        DataStructureType type,
        @Nullable final IgniteInClosureX<T> afterRmv)
        throws IgniteCheckedException
    {
        Map<String, DataStructureInfo> dsMap = utilityCache.get(DATA_STRUCTURES_KEY);

        if (dsMap == null || !dsMap.containsKey(name))
            return;

        final DataStructureInfo dsInfo = new DataStructureInfo(name, type, null);

        IgniteCheckedException err = validateDataStructure(dsMap, dsInfo, false);

        if (err != null)
            throw err;

        retryTopologySafe(new IgniteOutClosureX<Void>() {
            @Override public Void applyx() throws IgniteCheckedException {
                try (GridNearTxLocal tx = utilityCache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                    T2<Boolean, IgniteCheckedException> res =
                        utilityCache.invoke(DATA_STRUCTURES_KEY, new RemoveDataStructureProcessor(dsInfo)).get();

                    IgniteCheckedException err = res.get2();

                    if (err != null)
                        throw err;

                    assert res.get1() != null;

                    boolean exists = res.get1();

                    if (!exists)
                        return null;

                    T rmvInfo = c.applyx();

                    tx.commit();

                    if (afterRmv != null && rmvInfo != null)
                        afterRmv.applyx(rmvInfo);

                    return null;
                }
            }
        });
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

        awaitInitialization();

        checkAtomicsConfiguration();

        startQuery();

        return getAtomic(new IgniteOutClosureX<IgniteAtomicReference>() {
            @Override public IgniteAtomicReference<T> applyx() throws IgniteCheckedException {
                GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

                dsCacheCtx.gate().enter();

                try (GridNearTxLocal tx = CU.txStartInternal(dsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
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

                        dsView.put(key, val);
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
    final void removeAtomicReference(final String name) throws IgniteCheckedException {
        assert name != null;
        assert dsCacheCtx != null;

        awaitInitialization();

        removeDataStructure(new IgniteOutClosureX<Void>() {
            @Override public Void applyx() throws IgniteCheckedException {
                dsCacheCtx.gate().enter();

                try {
                    dsView.remove(new GridCacheInternalKeyImpl(name));
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

        awaitInitialization();

        checkAtomicsConfiguration();

        startQuery();

        return getAtomic(new IgniteOutClosureX<IgniteAtomicStamped>() {
            @Override public IgniteAtomicStamped<T, S> applyx() throws IgniteCheckedException {
                GridCacheInternalKeyImpl key = new GridCacheInternalKeyImpl(name);

                dsCacheCtx.gate().enter();

                try (GridNearTxLocal tx = CU.txStartInternal(dsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
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

                        dsView.put(key, val);
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
    final void removeAtomicStamped(final String name) throws IgniteCheckedException {
        assert name != null;
        assert dsCacheCtx != null;

        awaitInitialization();

        removeDataStructure(new IgniteOutClosureX<Void>() {
            @Override public Void applyx() throws IgniteCheckedException {
                dsCacheCtx.gate().enter();

                try {
                    dsView.remove(new GridCacheInternalKeyImpl(name));
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

        awaitInitialization();

        String cacheName = null;

        if (cfg != null) {
            if (cap <= 0)
                cap = Integer.MAX_VALUE;

            cacheName = compatibleConfiguration(cfg);
        }

        DataStructureInfo dsInfo = new DataStructureInfo(name,
            QUEUE,
            cfg != null ? new QueueInfo(cacheName, cfg.isCollocated(), cap) : null);

        final int cap0 = cap;

        final boolean create = cfg != null;

        return getCollection(new IgniteClosureX<GridCacheContext, IgniteQueue<T>>() {
            @Override public IgniteQueue<T> applyx(GridCacheContext ctx) throws IgniteCheckedException {
                return ctx.dataStructures().queue(name, cap0, create && cfg.isCollocated(), create);
            }
        }, dsInfo, create);
    }

    /**
     * @param cfg Collection configuration.
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(CollectionConfiguration cfg, String name) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setBackups(cfg.getBackups());
        ccfg.setCacheMode(cfg.getCacheMode());
        ccfg.setAtomicityMode(cfg.getAtomicityMode());
        ccfg.setNodeFilter(cfg.getNodeFilter());
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(SYNC);

        return ccfg;
    }

    /**
     * @param cfg Collection configuration.
     * @return Cache name.
     * @throws IgniteCheckedException If failed.
     */
    private String compatibleConfiguration(CollectionConfiguration cfg) throws IgniteCheckedException {
        List<CacheCollectionInfo> caches = utilityDataCache.context().affinityNode() ?
            utilityDataCache.localPeek(DATA_STRUCTURES_CACHE_KEY, null, null) :
            utilityDataCache.get(DATA_STRUCTURES_CACHE_KEY);

        String cacheName = findCompatibleConfiguration(cfg, caches);

        if (cacheName == null)
            cacheName = utilityDataCache.invoke(DATA_STRUCTURES_CACHE_KEY, new AddDataCacheProcessor(cfg)).get();

        assert cacheName != null;

        CacheConfiguration newCfg = cacheConfiguration(cfg, cacheName);

        if (ctx.cache().cache(cacheName) == null) {
            ctx.cache().dynamicStartCache(newCfg,
                cacheName,
                null,
                CacheType.INTERNAL,
                false,
                true,
                true).get();
        }

        assert ctx.cache().cache(cacheName) != null : cacheName;

        return cacheName;
    }

    /**
     * @param name Queue name.
     * @param cctx Queue cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void removeQueue(final String name, final GridCacheContext cctx) throws IgniteCheckedException {
        assert name != null;
        assert cctx != null;

        awaitInitialization();

        IgniteOutClosureX<GridCacheQueueHeader> rmv = new IgniteOutClosureX<GridCacheQueueHeader>() {
            @Override public GridCacheQueueHeader applyx() throws IgniteCheckedException {
                return (GridCacheQueueHeader)cctx.cache().withNoRetries().getAndRemove(new GridCacheQueueHeaderKey(name));
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
        final DataStructureInfo dsInfo,
        boolean create)
        throws IgniteCheckedException
    {
        awaitInitialization();

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

            GridCacheContext cacheCtx = ctx.cache().getOrStartCache(cacheName).context();

            return c.applyx(cacheCtx);
        }

        return retryTopologySafe(new IgniteOutClosureX<T>() {
            @Override public T applyx() throws IgniteCheckedException {
                try (GridNearTxLocal tx = utilityCache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                    T2<String, IgniteCheckedException> res =
                        utilityCache.invoke(DATA_STRUCTURES_KEY, new AddCollectionProcessor(dsInfo)).get();

                    IgniteCheckedException err = res.get2();

                    if (err != null)
                        throw err;

                    String cacheName = res.get1();

                    final GridCacheContext cacheCtx = ctx.cache().internalCache(cacheName).context();

                    T col = c.applyx(cacheCtx);

                    tx.commit();

                    return col;
                }
            }
        });
    }

    /**
     * Awaits for processor initialization.
     */
    private void awaitInitialization() {
        if (initLatch.getCount() > 0) {
            try {
                U.await(initLatch);

                if (initFailed)
                    throw new IllegalStateException("Failed to initialize data structures processor.");
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IllegalStateException("Failed to initialize data structures processor " +
                    "(thread has been interrupted).", e);
            }
        }
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

        awaitInitialization();

        if (create)
            A.ensure(cnt >= 0, "count can not be negative");

        checkAtomicsConfiguration();

        startQuery();

        return getAtomic(new IgniteOutClosureX<IgniteCountDownLatch>() {
            @Override public IgniteCountDownLatch applyx() throws IgniteCheckedException {
                GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

                dsCacheCtx.gate().enter();

                try (GridNearTxLocal tx = CU.txStartInternal(dsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheCountDownLatchValue val = cast(dsView.get(key), GridCacheCountDownLatchValue.class);

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

                        dsView.put(key, val);
                    }

                    latch = new GridCacheCountDownLatchImpl(name, val.initialCount(),
                        val.autoDelete(),
                        key,
                        cntDownLatchView,
                        dsCacheCtx);

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

        awaitInitialization();

        removeDataStructure(new IgniteOutClosureX<Void>() {
            @Override public Void applyx() throws IgniteCheckedException {
                GridCacheInternal key = new GridCacheInternalKeyImpl(name);

                dsCacheCtx.gate().enter();

                try (GridNearTxLocal tx = CU.txStartInternal(dsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                    // Check correctness type of removable object.
                    GridCacheCountDownLatchValue val =
                            cast(dsView.get(key), GridCacheCountDownLatchValue.class);

                    if (val != null) {
                        if (val.get() > 0) {
                            throw new IgniteCheckedException("Failed to remove count down latch " +
                                    "with non-zero count: " + val.get());
                        }

                        dsView.remove(key);

                        tx.commit();
                    }
                    else
                        tx.setRollbackOnly();

                    return null;
                }
                finally {
                    dsCacheCtx.gate().leave();
                }
            }
        }, name, COUNT_DOWN_LATCH, null);
    }

    /**
     * Gets or creates semaphore. If semaphore is not found in cache,
     * it is created using provided name and count parameter.
     *
     * @param name Name of the semaphore.
     * @param cnt Initial count.
     * @param failoverSafe {@code True} FailoverSafe parameter.
     * @param create If {@code true} semaphore will be created in case it is not in cache,
     *      if it is {@code false} all parameters except {@code name} are ignored.
     * @return Semaphore for the given name or {@code null} if it is not found and
     *      {@code create} is false.
     * @throws IgniteCheckedException If operation failed.
     */
    public IgniteSemaphore semaphore(final String name, final int cnt, final boolean failoverSafe, final boolean create)
        throws IgniteCheckedException {
        A.notNull(name, "name");

        awaitInitialization();

        checkAtomicsConfiguration();

        startQuery();

        return getAtomic(new IgniteOutClosureX<IgniteSemaphore>() {
            @Override public IgniteSemaphore applyx() throws IgniteCheckedException {
                GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

                dsCacheCtx.gate().enter();

                try (GridNearTxLocal tx = CU.txStartInternal(dsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheSemaphoreState val = cast(dsView.get(key), GridCacheSemaphoreState.class);

                    // Check that semaphore hasn't been created in other thread yet.
                    GridCacheSemaphoreEx sem = cast(dsMap.get(key), GridCacheSemaphoreEx.class);

                    if (sem != null) {
                        assert val != null;

                        return sem;
                    }

                    if (val == null && !create)
                        return null;

                    if (val == null) {
                        val = new GridCacheSemaphoreState(cnt, new HashMap<UUID, Integer>(), failoverSafe);

                        dsView.put(key, val);
                    }

                    GridCacheSemaphoreEx sem0 = new GridCacheSemaphoreImpl(
                        name,
                        key,
                        semView,
                        dsCacheCtx);

                    dsMap.put(key, sem0);

                    tx.commit();

                    return sem0;
                }
                catch (Error | Exception e) {
                    dsMap.remove(key);

                    U.error(log, "Failed to create semaphore: " + name, e);

                    throw e;
                }
                finally {
                    dsCacheCtx.gate().leave();
                }
            }
        }, new DataStructureInfo(name, SEMAPHORE, null), create, GridCacheSemaphoreEx.class);
    }

    /**
     * Removes semaphore from cache.
     *
     * @param name Name of the semaphore.
     * @throws IgniteCheckedException If operation failed.
     */
    public void removeSemaphore(final String name) throws IgniteCheckedException {
        assert name != null;
        assert dsCacheCtx != null;

        awaitInitialization();

        removeDataStructure(new IgniteOutClosureX<Void>() {
            @Override public Void applyx() throws IgniteCheckedException {
                GridCacheInternal key = new GridCacheInternalKeyImpl(name);

                dsCacheCtx.gate().enter();

                try (GridNearTxLocal tx = CU.txStartInternal(dsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                    // Check correctness type of removable object.
                    GridCacheSemaphoreState val = cast(dsView.get(key), GridCacheSemaphoreState.class);

                    if (val != null) {
                        if (val.getCount() < 0)
                            throw new IgniteCheckedException("Failed to remove semaphore with blocked threads. ");

                        dsView.remove(key);

                        tx.commit();
                    }
                    else
                        tx.setRollbackOnly();

                    return null;
                }
                finally {
                    dsCacheCtx.gate().leave();
                }
            }
        }, name, SEMAPHORE, null);
    }

    /**
     * Gets or creates reentrant lock. If reentrant lock is not found in cache,
     * it is created using provided name, failover mode, and fairness mode parameters.
     *
     * @param name Name of the reentrant lock.
     * @param failoverSafe Flag indicating behaviour in case of failure.
     * @param fair Flag indicating fairness policy of this lock.
     * @param create If {@code true} reentrant lock will be created in case it is not in cache.
     * @return ReentrantLock for the given name or {@code null} if it is not found and
     *      {@code create} is false.
     * @throws IgniteCheckedException If operation failed.
     */
    public IgniteLock reentrantLock(final String name, final boolean failoverSafe, final boolean fair, final boolean create)
        throws IgniteCheckedException {
        A.notNull(name, "name");

        awaitInitialization();

        checkAtomicsConfiguration();

        startQuery();

        return getAtomic(new IgniteOutClosureX<IgniteLock>() {
            @Override public IgniteLock applyx() throws IgniteCheckedException {
                GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

                dsCacheCtx.gate().enter();

                try (GridNearTxLocal tx = CU.txStartInternal(dsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheLockState val = cast(dsView.get(key), GridCacheLockState.class);

                    // Check that reentrant lock hasn't been created in other thread yet.
                    GridCacheLockEx reentrantLock = cast(dsMap.get(key), GridCacheLockEx.class);

                    if (reentrantLock != null) {
                        assert val != null;

                        return reentrantLock;
                    }

                    if (val == null && !create)
                        return null;

                    if (val == null) {
                        val = new GridCacheLockState(0, dsCacheCtx.nodeId(), 0, failoverSafe, fair);

                        dsView.put(key, val);
                    }

                    GridCacheLockEx reentrantLock0 = new GridCacheLockImpl(
                        name,
                        key,
                        reentrantLockView,
                        dsCacheCtx);

                    dsMap.put(key, reentrantLock0);

                    tx.commit();

                    return reentrantLock0;
                }
                catch (Error | Exception e) {
                    dsMap.remove(key);

                    U.error(log, "Failed to create reentrant lock: " + name, e);

                    throw e;
                }
                finally {
                    dsCacheCtx.gate().leave();
                }
            }
        }, new DataStructureInfo(name, REENTRANT_LOCK, null), create, GridCacheLockEx.class);
    }

    /**
     * Removes reentrant lock from cache.
     *
     * @param name Name of the reentrant lock.
     * @param broken Flag indicating the reentrant lock is broken and should be removed unconditionally.
     * @throws IgniteCheckedException If operation failed.
     */
    public void removeReentrantLock(final String name, final boolean broken) throws IgniteCheckedException {
        assert name != null;
        assert dsCacheCtx != null;

        awaitInitialization();

        removeDataStructure(new IgniteOutClosureX<Void>() {
            @Override public Void applyx() throws IgniteCheckedException {
                GridCacheInternal key = new GridCacheInternalKeyImpl(name);

                dsCacheCtx.gate().enter();

                try (GridNearTxLocal tx = CU.txStartInternal(dsCacheCtx, dsView, PESSIMISTIC, REPEATABLE_READ)) {
                    // Check correctness type of removable object.
                    GridCacheLockState val = cast(dsView.get(key), GridCacheLockState.class);

                    if (val != null) {
                        if (val.get() > 0 && !broken)
                            throw new IgniteCheckedException("Failed to remove reentrant lock with blocked threads. ");

                        dsView.remove(key);

                        tx.commit();
                    }
                    else
                        tx.setRollbackOnly();

                    return null;
                }
                finally {
                    dsCacheCtx.gate().leave();
                }
            }
        }, name, REENTRANT_LOCK, null);
    }

    /**
     *
     */
    static class DataStructuresEntryFilter implements CacheEntryEventSerializableFilter<Object, Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<?, ?> evt) throws CacheEntryListenerException {
            if (evt.getEventType() == EventType.CREATED || evt.getEventType() == EventType.UPDATED)
                return evt.getValue() instanceof GridCacheCountDownLatchValue ||
                    evt.getValue() instanceof GridCacheSemaphoreState ||
                    evt.getValue() instanceof GridCacheLockState;
            else {
                assert evt.getEventType() == EventType.REMOVED : evt;

                return true;
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DataStructuresEntryFilter.class, this);
        }
    }

    /**
     *
     */
    private class DataStructuresEntryListener implements
        CacheEntryUpdatedListener<GridCacheInternalKey, GridCacheInternal> {
        /** {@inheritDoc} */
        @Override public void onUpdated(
            Iterable<CacheEntryEvent<? extends GridCacheInternalKey, ? extends GridCacheInternal>> evts)
            throws CacheEntryListenerException
        {
            for (CacheEntryEvent<? extends GridCacheInternalKey, ? extends GridCacheInternal> evt : evts) {
                if (evt.getEventType() == EventType.CREATED || evt.getEventType() == EventType.UPDATED) {
                    GridCacheInternal val0 = evt.getValue();

                    if (val0 instanceof GridCacheCountDownLatchValue) {
                        GridCacheInternalKey key = evt.getKey();

                        // Notify latch on changes.
                        final GridCacheRemovable latch = dsMap.get(key);

                        GridCacheCountDownLatchValue val = (GridCacheCountDownLatchValue)val0;

                        if (latch instanceof GridCacheCountDownLatchEx) {
                            final GridCacheCountDownLatchEx latch0 = (GridCacheCountDownLatchEx)latch;

                            latch0.onUpdate(val.get());

                            if (val.get() == 0 && val.autoDelete()) {
                                dsMap.remove(key);

                                IgniteInternalFuture<?> removeFut = ctx.closure().runLocalSafe(new GPR() {
                                    @Override public void run() {
                                        try {
                                            removeCountDownLatch(latch0.name());
                                        }
                                        catch (IgniteCheckedException e) {
                                            U.error(log, "Failed to remove count down latch: " + latch0.name(), e);
                                        }
                                        finally {
                                            ctx.cache().context().txContextReset();
                                        }
                                    }
                                });

                                removeFut.listen(new CI1<IgniteInternalFuture<?>>() {
                                    @Override public void apply(IgniteInternalFuture<?> f) {
                                        try {
                                            f.get();
                                        }
                                        catch (IgniteCheckedException e) {
                                            U.error(log, "Failed to remove count down latch: " + latch0.name(), e);
                                        }

                                        latch.onRemoved();
                                    }
                                });
                            }
                        }
                        else if (latch != null) {
                            U.error(log, "Failed to cast object " +
                                "[expected=" + IgniteCountDownLatch.class.getSimpleName() +
                                ", actual=" + latch.getClass() + ", value=" + latch + ']');
                        }
                    }
                    else if (val0 instanceof GridCacheSemaphoreState) {
                        GridCacheInternalKey key = evt.getKey();

                        // Notify semaphore on changes.
                        final GridCacheRemovable sem = dsMap.get(key);

                        GridCacheSemaphoreState val = (GridCacheSemaphoreState)val0;

                        if (sem instanceof GridCacheSemaphoreEx) {
                            final GridCacheSemaphoreEx semaphore0 = (GridCacheSemaphoreEx)sem;

                            semaphore0.onUpdate(val);
                        }
                        else if (sem != null) {
                            U.error(log, "Failed to cast object " +
                                    "[expected=" + IgniteSemaphore.class.getSimpleName() +
                                    ", actual=" + sem.getClass() + ", value=" + sem + ']');
                        }
                    }
                    else if (val0 instanceof GridCacheLockState) {
                        GridCacheInternalKey key = evt.getKey();

                        // Notify reentrant lock on changes.
                        final GridCacheRemovable reentrantLock = dsMap.get(key);

                        GridCacheLockState val = (GridCacheLockState)val0;

                        if (reentrantLock instanceof GridCacheLockEx) {
                            final GridCacheLockEx lock0 = (GridCacheLockEx)reentrantLock;

                            lock0.onUpdate(val);
                        }
                        else if (reentrantLock != null) {
                            U.error(log, "Failed to cast object " +
                                "[expected=" + IgniteLock.class.getSimpleName() +
                                ", actual=" + reentrantLock.getClass() + ", value=" + reentrantLock + ']');
                        }
                    }
                }
                else {
                    assert evt.getEventType() == EventType.REMOVED : evt;

                    GridCacheInternal key = evt.getKey();

                    // Entry's val is null if entry deleted.
                    GridCacheRemovable obj = dsMap.remove(key);

                    if (obj != null)
                        obj.onRemoved();
                }
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DataStructuresEntryListener.class, this);
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

        awaitInitialization();

        String cacheName = null;

        if (cfg != null)
            cacheName = compatibleConfiguration(cfg);

        DataStructureInfo dsInfo = new DataStructureInfo(name,
            SET,
            cfg != null ? new CollectionInfo(cacheName, cfg.isCollocated()) : null);

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

        awaitInitialization();

        IgniteOutClosureX<GridCacheSetHeader> rmv = new IgniteOutClosureX<GridCacheSetHeader>() {
            @Override public GridCacheSetHeader applyx() throws IgniteCheckedException {
                return (GridCacheSetHeader)cctx.cache().withNoRetries().getAndRemove(new GridCacheSetHeaderKey(name));
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
     * @param log Logger.
     * @param call Callable.
     * @return Callable result.
     * @throws IgniteCheckedException If all retries failed.
     */
    public static <R> R retry(IgniteLogger log, Callable<R> call) throws IgniteCheckedException {
        try {
            return GridCacheUtils.retryTopologySafe(call);
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
        X.println(">>> Data structure processor memory stats [igniteInstanceName=" + ctx.igniteInstanceName() +
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
     * @param cfg Collection configuration.
     * @param infos Data structure caches.
     * @return Name of the cache with compatible configuration or null.
     */
    private static String findCompatibleConfiguration(CollectionConfiguration cfg, List<CacheCollectionInfo> infos) {
        if (infos == null)
            return null;

        for (CacheCollectionInfo col : infos) {
            if (col.cfg.getAtomicityMode() == cfg.getAtomicityMode() &&
                col.cfg.getCacheMode() == cfg.getCacheMode() &&
                col.cfg.getBackups() == cfg.getBackups() &&
                col.cfg.getOffHeapMaxMemory() == cfg.getOffHeapMaxMemory() &&
                ((col.cfg.getNodeFilter() == null && cfg.getNodeFilter() == null) ||
                (col.cfg.getNodeFilter() != null && col.cfg.getNodeFilter().equals(cfg.getNodeFilter()))))
                return col.cacheName;
        }

        return null;
    }

    /**
     * @param c Closure to run.
     * @throws IgniteCheckedException If failed.
     * @return Closure return value.
     */
    private static <T> T retryTopologySafe(IgniteOutClosureX<T> c) throws IgniteCheckedException {
        for (int i = 0; i < GridCacheAdapter.MAX_RETRIES; i++) {
            try {
                return c.applyx();
            }
            catch (IgniteCheckedException e) {
                if (i == GridCacheAdapter.MAX_RETRIES - 1)
                    throw e;

                ClusterTopologyCheckedException topErr = e.getCause(ClusterTopologyCheckedException.class);

                if (topErr == null || (topErr instanceof ClusterTopologyServerNotFoundException))
                    throw e;

                IgniteInternalFuture<?> fut = topErr.retryReadyFuture();

                if (fut != null)
                    fut.get();
            }
        }

        assert false;

        return null;
    }

    /**
     *
     */
    enum DataStructureType {
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
        SET(IgniteSet.class.getSimpleName()),

        /** */
        SEMAPHORE(IgniteSemaphore.class.getSimpleName()),

        /** */
        REENTRANT_LOCK(IgniteLock.class.getSimpleName());

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
    static class CacheCollectionInfo implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private String cacheName;

        /** */
        private CollectionConfiguration cfg;

        /**
         * Required by {@link Externalizable}.
         */
        public CacheCollectionInfo() {
            // No-op.
        }

        /**
         * @param cacheName Collection cache name.
         * @param cfg CollectionConfiguration.
         */
        public CacheCollectionInfo(String cacheName, CollectionConfiguration cfg) {
            this.cacheName = cacheName;
            this.cfg = cfg;
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            cfg = (CollectionConfiguration)in.readObject();
            cacheName = U.readString(in);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(cfg);
            U.writeString(out, cacheName);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheCollectionInfo.class, this);
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
    static class AddDataCacheProcessor implements
        EntryProcessor<CacheDataStructuresCacheKey, List<CacheCollectionInfo>, String>, Externalizable {
        /** Cache name prefix. */
        private static final String CACHE_NAME_PREFIX = "datastructures_";

        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private CollectionConfiguration cfg;

        /**
         * @param cfg Data structure information.
         */
        AddDataCacheProcessor(CollectionConfiguration cfg) {
            this.cfg = cfg;
        }

        /**
         * Required by {@link Externalizable}.
         */
        public AddDataCacheProcessor() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public String process(
            MutableEntry<CacheDataStructuresCacheKey, List<CacheCollectionInfo>> entry,
            Object... args)
        {
            List<CacheCollectionInfo> list = entry.getValue();

            if (list == null) {
                list = new ArrayList<>();

                String newName = CACHE_NAME_PREFIX + 0;

                list.add(new CacheCollectionInfo(newName, cfg));

                entry.setValue(list);

                return newName;
            }

            String oldName = findCompatibleConfiguration(cfg, list);

            if (oldName != null)
                return oldName;

            String newName = CACHE_NAME_PREFIX + list.size();

            List<CacheCollectionInfo> newList = new ArrayList<>(list);

            newList.add(new CacheCollectionInfo(newName, cfg));

            return newName;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(cfg);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            cfg = (CollectionConfiguration)in.readObject();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AddDataCacheProcessor.class, this);
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

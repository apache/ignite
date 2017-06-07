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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
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
import org.apache.ignite.internal.util.lang.IgnitePredicateX;
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
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.processors.datastructures.DataStructureType.*;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Manager of data structures.
 */
public final class DataStructuresProcessor extends GridProcessorAdapter implements IgniteChangeGlobalStateSupport {
    /** */
    public static final CacheDataStructuresConfigurationKey DATA_STRUCTURES_INFO_KEY =
        new CacheDataStructuresConfigurationKey();

    /** */
    private static final CacheDataStructuresCacheKey DATA_STRUCTURES_CACHE_KEY =
        new CacheDataStructuresCacheKey();

    private static final String DATA_STRUCTURES_CACHE_NAME_PREFIX = "datastructures_";

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

    /** Cache context for atomic data structures. */
    private GridCacheContext defaultDsCacheCtx;

    /** Atomic data structures configuration. */
    private final AtomicConfiguration defaultAtomicCfg;

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

        defaultAtomicCfg = ctx.config().getAtomicConfiguration();
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

        utilityCache = ctx.cache().utilityCache();

        utilityDataCache = ctx.cache().utilityCache();

        assert utilityCache != null;

        if (defaultAtomicCfg != null) {
            IgniteInternalCache atomicsCache = ctx.cache().atomicsCache();

            assert atomicsCache != null;

            dsView = atomicsCache;

            defaultDsCacheCtx = atomicsCache.context();
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
                    qryId = defaultDsCacheCtx.continuousQueries().executeInternalQuery(
                        new DataStructuresEntryListener(),
                        new DataStructuresEntryFilter(),
                        defaultDsCacheCtx.isReplicated() && defaultDsCacheCtx.affinityNode(),
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
            defaultDsCacheCtx.continuousQueries().cancelInternalQuery(qryId);
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
        @Nullable AtomicConfiguration cfg,
        final long initVal,
        final boolean create)
        throws IgniteCheckedException
    {
        return getAtomic(new AtomicBuilder<IgniteAtomicSequence>() {
            @Override public T2<IgniteAtomicSequence, AtomicDataStructureValue> build(AtomicDataStructureValue val, IgniteInternalCache cache) throws IgniteCheckedException {
                GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

                try {
                    GridCacheAtomicSequenceValue seqVal = cast(val, GridCacheAtomicSequenceValue.class);

                    // Check that sequence hasn't been created in other thread yet.
                    GridCacheAtomicSequenceEx seq = cast(dsMap.get(key), GridCacheAtomicSequenceEx.class);

                    if (seq != null) {
                        assert seqVal != null;

                        return new T2<IgniteAtomicSequence, AtomicDataStructureValue>(seq, null);
                    }

                    if (seqVal == null && !create)
                        return null;

                    // We should use offset because we already reserved left side of range.
                    long off = defaultAtomicCfg.getAtomicSequenceReserveSize() > 1 ?
                        defaultAtomicCfg.getAtomicSequenceReserveSize() - 1 : 1;

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

                    // Only one thread can be in the transaction scope and create sequence.
                    seq = new GridCacheAtomicSequenceImpl(name,
                        key,
                        cache,
                        defaultAtomicCfg.getAtomicSequenceReserveSize(),
                        locCntr,
                        upBound);

                    dsMap.put(key, seq);

                    return new T2<IgniteAtomicSequence, AtomicDataStructureValue>(seq, seqVal);
                }
                catch (Error | Exception e) {
                    dsMap.remove(key);

                    U.error(log, "Failed to make atomic sequence: " + name, e);

                    throw e;
                }
            }
        }, cfg, new DataStructureInfo(name, null, ATOMIC_SEQ, null), create, IgniteAtomicSequence.class);
    }

    /**
     * Removes sequence from cache.
     *
     * @param name Sequence name.
     * @throws IgniteCheckedException If removing failed.
     */
    final void removeSequence(final String name, @Nullable final String groupName) throws IgniteCheckedException {
        removeDataStructure(null, name, null, ATOMIC_SEQ, null);
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
        @Nullable AtomicConfiguration cfg,
        final long initVal,
        final boolean create) throws IgniteCheckedException {
        return getAtomic(new AtomicBuilder<IgniteAtomicLong>() {
            @Override public T2<IgniteAtomicLong, AtomicDataStructureValue> build(AtomicDataStructureValue val, IgniteInternalCache cache) throws IgniteCheckedException {
                final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

                try {
                    // Check that atomic long hasn't been created in other thread yet.
                    GridCacheAtomicLongEx a = cast(dsMap.get(key), GridCacheAtomicLongEx.class);

                    if (a != null) {
                        assert val != null;

                        return new T2<IgniteAtomicLong, AtomicDataStructureValue>(a, null);
                    }

                    if (val == null && !create)
                        return null;

                    GridCacheAtomicLongValue retVal = (val == null ? new GridCacheAtomicLongValue(initVal) : null);

                    a = new GridCacheAtomicLongImpl(name, key, cache);

                    dsMap.put(key, a);

                    return new T2<IgniteAtomicLong, AtomicDataStructureValue>(a, retVal);
                }
                catch (Error | Exception e) {
                    dsMap.remove(key);

                    U.error(log, "Failed to make atomic long: " + name, e);

                    throw e;
                }
            }
        }, cfg, new DataStructureInfo(name, null, ATOMIC_LONG, null), create, IgniteAtomicLong.class);
    }

    /**
     * @param c Closure creating data structure instance.
     * @param dsInfo Data structure info.
     * @param create Create flag.
     * @param cls Expected data structure class.
     * @return Data structure instance.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private <T> T getAtomic(final AtomicBuilder<T> c,
        @Nullable AtomicConfiguration cfg,
        final DataStructureInfo dsInfo,
        final boolean create,
        Class<? extends T> cls)
        throws IgniteCheckedException
    {
        A.notNull(dsInfo.name, "name");

        awaitInitialization();

        if (cfg == null) {
            checkAtomicsConfiguration();

            cfg = defaultAtomicCfg;
        }

        final String groupName = cfg.getGroupName();

        startQuery();

        // -----

        final GridCacheInternalKey key = new GridCacheInternalKeyImpl(dsInfo.name);

        // Check type of structure received by key from local cache.
        T dataStructure = cast(this.dsMap.get(key), cls);

        if (dataStructure != null)
            return dataStructure;

        String cacheName = CU.ATOMICS_CACHE_NAME + (cfg.getGroupName() != null ? "@" + cfg.getGroupName() : "");

        final IgniteInternalCache<GridCacheInternalKey, AtomicDataStructureValue> cache = create ?
            ctx.cache().<GridCacheInternalKey, AtomicDataStructureValue>getOrStartCache(cacheName, cacheConfiguration(cfg, cacheName)) :
            ctx.cache().<GridCacheInternalKey, AtomicDataStructureValue>cache(cacheName);

        if (cache == null) {
            assert !create;

            return null;
        }

        assert cache.context().groupId() == CU.cacheId(cfg.getGroupName() != null ? cfg.getGroupName() : CU.ATOMICS_CACHE_NAME);

        return retryTopologySafe(new IgniteOutClosureX<T>() {
            @Override public T applyx() throws IgniteCheckedException {
            cache.context().gate().enter();

            try (GridNearTxLocal tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                AtomicDataStructureValue val = cache.get(key);

                if (val == null && !create)
                    return null;

                // TODO : validate

                T2<T, ? extends AtomicDataStructureValue> ret = c.build(val, cache);

                if (ret.get2() != null)
                    cache.put(key, ret.get2());

                tx.commit();

                return ret.get1();
            }
            finally {
                cache.context().gate().leave();
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
    final void removeAtomicLong(final String name, @Nullable final String groupNane) throws IgniteCheckedException {
        removeDataStructure(null, name, groupNane, ATOMIC_LONG, null);
    }

    private <T> void removeDataStructure(@Nullable final IgnitePredicateX<AtomicDataStructureValue> predicate,
            String name,
            @Nullable String groupName,
            DataStructureType type,
            @Nullable final IgniteInClosureX<T> afterRmv) throws IgniteCheckedException {
        assert name != null;
        assert type != null;

        awaitInitialization();

        final String cacheName = CU.ATOMICS_CACHE_NAME + (groupName != null ? "@" + groupName : "");

        final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

        retryTopologySafe(new IgniteOutClosureX<Object>() {
            @Override public Object applyx() throws IgniteCheckedException {
                IgniteInternalCache<GridCacheInternalKey, AtomicDataStructureValue> cache = ctx.cache().cache(cacheName);

                if (cache != null && cache.context().gate().enterIfNotStopped()) {
                    try (GridNearTxLocal tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                        AtomicDataStructureValue val = cache.get(key);

                        if (val == null)
                            return null;

                        // TODO : validate

                        if (predicate == null || predicate.applyx(val)) {
                            cache.remove(key);

                            tx.commit();

                            if (afterRmv != null)
                                afterRmv.applyx(null);
                        }
                    }
                    finally {
                        cache.context().gate().leave();
                    }
                }

                return null;
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
        @Nullable AtomicConfiguration cfg,
        final T initVal,
        final boolean create)
        throws IgniteCheckedException
    {
        return getAtomic(new AtomicBuilder<IgniteAtomicReference>() {
            @Override public T2<IgniteAtomicReference, AtomicDataStructureValue> build(AtomicDataStructureValue val, IgniteInternalCache cache) throws IgniteCheckedException {
                GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

                try {
                    // Check that atomic reference hasn't been created in other thread yet.
                    GridCacheAtomicReferenceEx ref = cast(dsMap.get(key),
                        GridCacheAtomicReferenceEx.class);

                    if (ref != null) {
                        assert val != null;

                        return new T2<IgniteAtomicReference, AtomicDataStructureValue>(ref, null);
                    }

                    if (val == null && !create)
                        return null;

                    AtomicDataStructureValue retVal = (val == null ? new GridCacheAtomicReferenceValue<>(initVal) : null);

                    ref = new GridCacheAtomicReferenceImpl(name, key, cache);

                    dsMap.put(key, ref);

                    return new T2<IgniteAtomicReference, AtomicDataStructureValue>(ref, retVal);
                }
                catch (Error | Exception e) {
                    dsMap.remove(key);

                    U.error(log, "Failed to make atomic reference: " + name, e);

                    throw e;
                }
            }
        }, cfg, new DataStructureInfo(name, null, ATOMIC_REF, null), create, IgniteAtomicReference.class);
    }

    /**
     * Removes atomic reference from cache.
     *
     * @param name Atomic reference name.
     * @throws IgniteCheckedException If removing failed.
     */
    final void removeAtomicReference(final String name, @Nullable final String groupName) throws IgniteCheckedException {
        removeDataStructure(null, name, groupName, ATOMIC_REF, null);
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
    public final <T, S> IgniteAtomicStamped<T, S> atomicStamped(final String name, @Nullable AtomicConfiguration cfg,
        final T initVal, final S initStamp, final boolean create) throws IgniteCheckedException {
        return getAtomic(new AtomicBuilder<IgniteAtomicStamped>() {
            @Override public T2<IgniteAtomicStamped, AtomicDataStructureValue> build(AtomicDataStructureValue val, IgniteInternalCache cache) throws IgniteCheckedException {
                GridCacheInternalKeyImpl key = new GridCacheInternalKeyImpl(name);

                try {
                    // Check that atomic stamped hasn't been created in other thread yet.
                    GridCacheAtomicStampedEx stmp = cast(dsMap.get(key),
                        GridCacheAtomicStampedEx.class);

                    if (stmp != null) {
                        assert val != null;

                        return new T2(stmp, null);
                    }

                    if (val == null && !create)
                        return null;

                    AtomicDataStructureValue retVal = (val == null ? new GridCacheAtomicStampedValue(initVal, initStamp) : null);

                    stmp = new GridCacheAtomicStampedImpl(name, key, cache);

                    dsMap.put(key, stmp);

                    return new T2<IgniteAtomicStamped, AtomicDataStructureValue>(stmp, retVal);
                }
                catch (Error | Exception e) {
                    dsMap.remove(key);

                    U.error(log, "Failed to make atomic stamped: " + name, e);

                    throw e;
                }
            }
        }, cfg, new DataStructureInfo(name, null, ATOMIC_STAMPED, null), create, IgniteAtomicStamped.class);
    }

    /**
     * Removes atomic stamped from cache.
     *
     * @param name Atomic stamped name.
     * @throws IgniteCheckedException If removing failed.
     */
    final void removeAtomicStamped(final String name, @Nullable final String groupName) throws IgniteCheckedException {
        removeDataStructure(null, name, null, ATOMIC_STAMPED, null);
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
    public final <T> IgniteQueue<T> queue(final String name, int cap, @Nullable final CollectionConfiguration cfg)
        throws IgniteCheckedException {
        A.notNull(name, "name");

        if (cfg != null) {
            if (cap <= 0)
                cap = Integer.MAX_VALUE;
        }

        DataStructureInfo dsInfo = new DataStructureInfo(name,
            cfg != null ? cfg.getGroupName() : null,
            QUEUE,
            cfg != null ? cfg.isCollocated() : null);

        final int cap0 = cap;

        final boolean create = cfg != null;

        return getCollection(new IgniteClosureX<GridCacheContext, IgniteQueue<T>>() {
            @Override public IgniteQueue<T> applyx(GridCacheContext ctx) throws IgniteCheckedException {
                return ctx.dataStructures().queue(name, cap0, create && cfg.isCollocated(), create);
            }
        }, cfg, name, QUEUE, cfg != null && cfg.isCollocated(), create);
    }

    private CacheConfiguration cacheConfiguration(AtomicConfiguration cfg, String name) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setGroupName(cfg.getGroupName());
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setRebalanceMode(SYNC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setCacheMode(cfg.getCacheMode());
        ccfg.setNodeFilter(CacheConfiguration.ALL_NODES);
        ccfg.setAffinity(cfg.getAffinity());

        if (cfg.getCacheMode() == PARTITIONED)
            ccfg.setBackups(cfg.getBackups());

        return ccfg;
    }

    /**
     * @param cfg Collection configuration.
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(CollectionConfiguration cfg, String name) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setGroupName(cfg.getGroupName());
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
    private IgniteInternalCache compatibleCache(
        IgniteInternalCache<GridCacheInternalKey, AtomicDataStructureValue> metaCache,
        CollectionConfiguration cfg) throws IgniteCheckedException
    {
        Iterator<Cache.Entry<GridCacheInternalKey, AtomicDataStructureValue>> iterator = metaCache.scanIterator(false, new IgniteBiPredicate<Object, Object>() {
            @Override public boolean apply(Object key, Object value) {
                return key instanceof GridCacheInternalKey && value instanceof DistributedCollectionMetadata;
            }
        });

        String cacheName = findCompatibleConfiguration(cfg, iterator);

        if (cacheName == null)
            cacheName = DATA_STRUCTURES_CACHE_NAME_PREFIX + UUID.randomUUID();

        CacheConfiguration cacheCfg = cacheConfiguration(cfg, cacheName);

        IgniteInternalCache cache = ctx.cache().cache(cacheName);

        if (cache == null) {
            ctx.cache().dynamicStartCache(cacheCfg,
                cacheName,
                null,
                CacheType.INTERNAL,
                false,
                false,
                true,
                true).get();
        }

        cache = ctx.cache().cache(cacheName);

        assert cache != null;

        return cache;
    }

    /**
     * @param name Queue name.
     * @param cctx Queue cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void removeQueue(final String name, final GridCacheContext cctx, final IgniteUuid id) throws IgniteCheckedException {
        assert name != null;
        assert cctx != null;

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

        removeDataStructure(null, name, null, QUEUE, afterRmv);
    }

    /**
     * @param c Closure creating collection.
     * @param create Create flag.
     * @return Collection instance.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private <T> T getCollection(final IgniteClosureX<GridCacheContext, T> c,
        @Nullable CollectionConfiguration cfg,
        String name,
        final DataStructureType type,
        boolean colocated,
        boolean create)
        throws IgniteCheckedException
    {
        awaitInitialization();

        assert name != null;
        assert type == SET || type == QUEUE;

        final String groupName = cfg != null ? cfg.getGroupName() : null;

        final String metaCacheName = CU.ATOMICS_CACHE_NAME + (groupName != null ? "@" + groupName : "");

        assert !create || cfg != null;

        final IgniteInternalCache<GridCacheInternalKey, AtomicDataStructureValue> metaCache = create ?
            ctx.cache().<GridCacheInternalKey, AtomicDataStructureValue>getOrStartCache(metaCacheName,
                cacheConfiguration(new AtomicConfiguration(), metaCacheName)) :
            ctx.cache().<GridCacheInternalKey, AtomicDataStructureValue>cache(metaCacheName);

        if (!create && metaCache == null)
            return null;

//        IgniteCheckedException err = validateDataStructure(dsMap, dsInfo, create);
//
//        if (err != null)
//            throw err;

        assert metaCache != null;

        AtomicDataStructureValue oldVal;

        final IgniteInternalCache cache;

        if (create) {
            cache = compatibleCache(metaCache, cfg);

            DistributedCollectionMetadata newVal = new DistributedCollectionMetadata(type, cfg, cache.name());

            oldVal = metaCache.getAndPutIfAbsent(new GridCacheInternalKeyImpl(name), newVal);
        }
        else {
            oldVal = metaCache.get(new GridCacheInternalKeyImpl(name));

            if (oldVal == null || !(oldVal instanceof DistributedCollectionMetadata))
                return null;

            cache = ctx.cache().cache(((DistributedCollectionMetadata)oldVal).cacheName());

            if (cache == null)
                return null;
        }

        if (oldVal != null) {
            // TODO : validate
        }

        return retryTopologySafe(new IgniteOutClosureX<T>() {
            @Override public T applyx() throws IgniteCheckedException {
                try (GridNearTxLocal tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                    T col = c.applyx(cache.context());

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
     * @param oldInfo Old data structure information.
     * @param info New data structure information.
     * @param create Create flag.
     * @return {@link IgniteException} if validation failed.
     */
    @Nullable private static IgniteCheckedException validateDataStructure(
        DataStructureInfo oldInfo,
        DataStructureInfo info,
        boolean create)
    {
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
        @Nullable AtomicConfiguration cfg,
        final int cnt,
        final boolean autoDel,
        final boolean create)
        throws IgniteCheckedException
    {
        if (create)
            A.ensure(cnt >= 0, "count can not be negative");

        return getAtomic(new AtomicBuilder<IgniteCountDownLatch>() {
            @Override public T2<IgniteCountDownLatch, AtomicDataStructureValue> build(AtomicDataStructureValue val, IgniteInternalCache cache) throws IgniteCheckedException {
                GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

                try {
                    // Check that count down hasn't been created in other thread yet.
                    GridCacheCountDownLatchEx latch = cast(dsMap.get(key), GridCacheCountDownLatchEx.class);

                    if (latch != null) {
                        assert val != null;

                        return new T2<IgniteCountDownLatch, AtomicDataStructureValue>(latch, null);
                    }

                    if (val == null && !create)
                        return null;

                    GridCacheCountDownLatchValue retVal = (val == null ? new GridCacheCountDownLatchValue(cnt, autoDel) : null);

                    GridCacheCountDownLatchValue latchVal = retVal != null ? retVal : (GridCacheCountDownLatchValue) val;

                    assert latchVal != null;

                    latch = new GridCacheCountDownLatchImpl(name, latchVal.initialCount(),
                        latchVal.autoDelete(),
                        key,
                        cache);

                    dsMap.put(key, latch);

                    return new T2<IgniteCountDownLatch, AtomicDataStructureValue>(latch, retVal);
                }
                catch (Error | Exception e) {
                    dsMap.remove(key);

                    U.error(log, "Failed to create count down latch: " + name, e);

                    throw e;
                }
            }
        }, cfg, new DataStructureInfo(name, null, COUNT_DOWN_LATCH, null), create, GridCacheCountDownLatchEx.class);
    }

    /**
     * Removes count down latch from cache.
     *
     * @param name Name of the latch.
     * @throws IgniteCheckedException If operation failed.
     */
    public void removeCountDownLatch(final String name, @Nullable final String groupName) throws IgniteCheckedException {
        removeDataStructure(new IgnitePredicateX<AtomicDataStructureValue>() {
            @Override public boolean applyx(AtomicDataStructureValue val) throws IgniteCheckedException {
                assert val != null && val instanceof GridCacheCountDownLatchValue;

                GridCacheCountDownLatchValue latchVal = (GridCacheCountDownLatchValue) val;

                if (latchVal.get() > 0) {
                    throw new IgniteCheckedException("Failed to remove count down latch " +
                            "with non-zero count: " + latchVal.get());
                }

                return true;
            }
        }, name, null, COUNT_DOWN_LATCH, null);
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
    public IgniteSemaphore semaphore(final String name, @Nullable AtomicConfiguration cfg, final int cnt,
        final boolean failoverSafe, final boolean create)
        throws IgniteCheckedException {
        return getAtomic(new AtomicBuilder<IgniteSemaphore>() {
            @Override public T2<IgniteSemaphore, AtomicDataStructureValue> build(AtomicDataStructureValue val, IgniteInternalCache cache) throws IgniteCheckedException {
                GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

                try {
                    // Check that semaphore hasn't been created in other thread yet.
                    GridCacheSemaphoreEx sem = cast(dsMap.get(key), GridCacheSemaphoreEx.class);

                    if (sem != null) {
                        assert val != null;

                        return new T2<IgniteSemaphore, AtomicDataStructureValue>(sem, null);
                    }

                    if (val == null && !create)
                        return null;

                    AtomicDataStructureValue retVal = (val == null ? new GridCacheSemaphoreState(cnt, new HashMap<UUID, Integer>(), failoverSafe) : null);

                    GridCacheSemaphoreEx sem0 = new GridCacheSemaphoreImpl(name, key, cache);

                    dsMap.put(key, sem0);

                    return new T2<IgniteSemaphore, AtomicDataStructureValue>(sem0, retVal);
                }
                catch (Error | Exception e) {
                    dsMap.remove(key);

                    U.error(log, "Failed to create semaphore: " + name, e);

                    throw e;
                }
            }
        }, cfg, new DataStructureInfo(name, null, SEMAPHORE, null), create, GridCacheSemaphoreEx.class);
    }

    /**
     * Removes semaphore from cache.
     *
     * @param name Name of the semaphore.
     * @throws IgniteCheckedException If operation failed.
     */
    public void removeSemaphore(final String name, @Nullable final String groupName) throws IgniteCheckedException {
        removeDataStructure(new IgnitePredicateX<AtomicDataStructureValue>() {
            @Override public boolean applyx(AtomicDataStructureValue val) throws IgniteCheckedException {
                assert val != null && val instanceof GridCacheSemaphoreState;

                GridCacheSemaphoreState semVal = (GridCacheSemaphoreState) val;

                if (semVal.getCount() < 0)
                    throw new IgniteCheckedException("Failed to remove semaphore with blocked threads. ");

                return true;
            }
        }, name, groupName, SEMAPHORE, null);
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
    public IgniteLock reentrantLock(final String name, @Nullable AtomicConfiguration cfg, final boolean failoverSafe,
        final boolean fair, final boolean create) throws IgniteCheckedException {
        return getAtomic(new AtomicBuilder<IgniteLock>() {
            @Override public T2<IgniteLock, AtomicDataStructureValue> build(AtomicDataStructureValue val, IgniteInternalCache cache) throws IgniteCheckedException {
                GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

                try {
                    // Check that reentrant lock hasn't been created in other thread yet.
                    GridCacheLockEx reentrantLock = cast(dsMap.get(key), GridCacheLockEx.class);

                    if (reentrantLock != null) {
                        assert val != null;

                        return new T2<IgniteLock, AtomicDataStructureValue>(reentrantLock, null);
                    }

                    if (val == null && !create)
                        return null;

                    AtomicDataStructureValue retVal = (val == null ? new GridCacheLockState(0, defaultDsCacheCtx.nodeId(), 0, failoverSafe, fair) : null);

                    GridCacheLockEx reentrantLock0 = new GridCacheLockImpl(name, key, cache);

                    dsMap.put(key, reentrantLock0);

                    return new T2<IgniteLock, AtomicDataStructureValue>(reentrantLock0, retVal);
                }
                catch (Error | Exception e) {
                    dsMap.remove(key);

                    U.error(log, "Failed to create reentrant lock: " + name, e);

                    throw e;
                }
            }
        }, cfg, new DataStructureInfo(name, null, REENTRANT_LOCK, null), create, GridCacheLockEx.class);
    }

    /**
     * Removes reentrant lock from cache.
     *
     * @param name Name of the reentrant lock.
     * @param broken Flag indicating the reentrant lock is broken and should be removed unconditionally.
     * @throws IgniteCheckedException If operation failed.
     */
    public void removeReentrantLock(final String name, @Nullable final String groupName, final boolean broken) throws IgniteCheckedException {
        removeDataStructure(new IgnitePredicateX<AtomicDataStructureValue>() {
            @Override public boolean applyx(AtomicDataStructureValue val) throws IgniteCheckedException {
                assert val != null && val instanceof GridCacheLockState;

                GridCacheLockState lockVal = (GridCacheLockState) val;

                if (lockVal.get() > 0 && !broken)
                    throw new IgniteCheckedException("Failed to remove reentrant lock with blocked threads. ");

                return true;
            }
        }, name, null, REENTRANT_LOCK, null);
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
                                            removeCountDownLatch(latch0.name(), latch0.groupName());
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
    @Nullable public <T> IgniteSet<T> set(final String name, @Nullable final CollectionConfiguration cfg)
        throws IgniteCheckedException {
        A.notNull(name, "name");

        DataStructureInfo dsInfo = new DataStructureInfo(name,
            cfg != null ? cfg.getGroupName() : null,
            SET,
            cfg != null ? cfg.isCollocated() : null);

        final boolean create = cfg != null;

        return getCollection(new CX1<GridCacheContext, IgniteSet<T>>() {
            @Override public IgniteSet<T> applyx(GridCacheContext cctx) throws IgniteCheckedException {
                return cctx.dataStructures().set(name, create ? cfg.isCollocated() : false, create);
            }
        }, cfg, name, SET, cfg != null && cfg.isCollocated(), create);
    }

    /**
     * @param name Set name.
     * @param cctx Set cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void removeSet(final String name, final GridCacheContext cctx, final IgniteUuid id) throws IgniteCheckedException {
        assert name != null;
        assert cctx != null;

        CIX1<GridCacheSetHeader> afterRmv = new CIX1<GridCacheSetHeader>() {
            @Override public void applyx(GridCacheSetHeader hdr) throws IgniteCheckedException {
                cctx.dataStructures().removeSetData(id);
            }
        };

        removeDataStructure(null, name, null, SET, afterRmv);
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
            ", cache=" + (defaultDsCacheCtx != null ? defaultDsCacheCtx.name() : null) + ']');
        X.println(">>>   dsMapSize: " + dsMap.size());
    }

    /**
     * @throws IgniteException If atomics configuration is not provided.
     */
    private void checkAtomicsConfiguration() throws IgniteException {
        if (defaultAtomicCfg == null)
            throw new IgniteException("Atomic data structure can not be created, " +
                "need to provide IgniteAtomicConfiguration.");
    }

    /**
     * @param cfg Collection configuration.
     * @param iterator Data structure metadata iterator.
     * @return Name of the cache with compatible configuration or null.
     */
    private static String findCompatibleConfiguration(CollectionConfiguration cfg, Iterator<Cache.Entry<GridCacheInternalKey, AtomicDataStructureValue>> iterator) {
        if (iterator == null)
            return null;

        while (iterator.hasNext()) {
            Cache.Entry<GridCacheInternalKey, AtomicDataStructureValue> e = iterator.next();

            assert e.getValue() instanceof DistributedCollectionMetadata;

            CollectionConfiguration cfg2 = ((DistributedCollectionMetadata)e.getValue()).configuration();

            if (cfg2.getAtomicityMode() == cfg.getAtomicityMode() &&
                cfg2.getCacheMode() == cfg.getCacheMode() &&
                cfg2.getBackups() == cfg.getBackups() &&
                cfg2.getOffHeapMaxMemory() == cfg.getOffHeapMaxMemory() &&
                ((cfg2.getNodeFilter() == null && cfg.getNodeFilter() == null) ||
                (cfg2.getNodeFilter() != null && cfg2.getNodeFilter().equals(cfg.getNodeFilter()))))
                return ((DistributedCollectionMetadata)e.getValue()).cacheName();
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
        @Nullable private String groupName;

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
        DataStructureInfo(String name, @Nullable String groupName, DataStructureType type, Serializable info) {
            this.name = name;
            this.groupName = groupName;
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
            U.writeString(out, groupName);
            U.writeEnum(out, type);
            out.writeObject(info);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            name = U.readString(in);
            groupName = U.readString(in);
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
            throw new UnsupportedOperationException();

//            List<CacheCollectionInfo> list = entry.getValue();
//
//            if (list == null) {
//                list = new ArrayList<>();
//
//                String newName = CACHE_NAME_PREFIX + 0;
//
//                list.add(new CacheCollectionInfo(newName, cfg));
//
//                entry.setValue(list);
//
//                return newName;
//            }
//
//            String oldName = findCompatibleConfiguration(cfg, list);
//
//            if (oldName != null)
//                return oldName;
//
//            String newName = CACHE_NAME_PREFIX + list.size();
//
//            List<CacheCollectionInfo> newList = new ArrayList<>(list);
//
//            newList.add(new CacheCollectionInfo(newName, cfg));
//
//            return newName;
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

    private interface AtomicBuilder<T> {
        T2<T, AtomicDataStructureValue> build(AtomicDataStructureValue val, IgniteInternalCache cache) throws IgniteCheckedException;
    }
}

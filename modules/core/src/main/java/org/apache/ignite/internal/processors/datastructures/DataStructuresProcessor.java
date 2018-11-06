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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
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
import org.apache.ignite.cluster.ClusterNode;
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
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheInternal;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
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
import org.apache.ignite.internal.util.typedef.internal.GPR;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.processors.datastructures.DataStructureType.ATOMIC_LONG;
import static org.apache.ignite.internal.processors.datastructures.DataStructureType.ATOMIC_REF;
import static org.apache.ignite.internal.processors.datastructures.DataStructureType.ATOMIC_SEQ;
import static org.apache.ignite.internal.processors.datastructures.DataStructureType.ATOMIC_STAMPED;
import static org.apache.ignite.internal.processors.datastructures.DataStructureType.COUNT_DOWN_LATCH;
import static org.apache.ignite.internal.processors.datastructures.DataStructureType.QUEUE;
import static org.apache.ignite.internal.processors.datastructures.DataStructureType.REENTRANT_LOCK;
import static org.apache.ignite.internal.processors.datastructures.DataStructureType.SEMAPHORE;
import static org.apache.ignite.internal.processors.datastructures.DataStructureType.SET;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Manager of data structures.
 */
public final class DataStructuresProcessor extends GridProcessorAdapter implements IgniteChangeGlobalStateSupport {
    /** */
    public static final String DEFAULT_VOLATILE_DS_GROUP_NAME = "default-volatile-ds-group";

    /** */
    private static final String DEFAULT_DS_GROUP_NAME = "default-ds-group";

    /** */
    private static final String DS_CACHE_NAME_PREFIX = "datastructures_";

    /** Atomics system cache name. */
    public static final String ATOMICS_CACHE_NAME = "ignite-sys-atomic-cache";

    /** Non collocated IgniteSet will use separate cache if all nodes in cluster is not older then specified version. */
    private static final IgniteProductVersion SEPARATE_CACHE_PER_NON_COLLOCATED_SET_SINCE =
        IgniteProductVersion.fromString("2.7.0");

    /** Initial capacity. */
    private static final int INITIAL_CAPACITY = 10;

    /** Initialization latch. */
    private volatile CountDownLatch initLatch = new CountDownLatch(1);

    /** Initialization failed flag. */
    private boolean initFailed;

    /** Internal storage of all dataStructures items (sequence, atomic long etc.). */
    private final ConcurrentMap<GridCacheInternalKey, GridCacheRemovable> dsMap;

    /** Atomic data structures configuration. */
    private final AtomicConfiguration dfltAtomicCfg;

    /** Map of continuous query IDs. */
    private final ConcurrentHashMap<Integer, UUID> qryIdMap = new ConcurrentHashMap<>();

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

        dsMap = new ConcurrentHashMap<>(INITIAL_CAPACITY);

        dfltAtomicCfg = ctx.config().getAtomicConfiguration();
    }

    /** {@inheritDoc} */
    @Override public void start() {
        ctx.event().addLocalEventListener(lsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onKernalStart(boolean active) {
        if (ctx.config().isDaemon() || !active)
            return;

        onKernalStart0();
    }

    /**
     *
     */
    public void onBeforeActivate() {
        initLatch = new CountDownLatch(1);
    }

    /**
     *
     */
    private void onKernalStart0() {
        initLatch.countDown();
    }

    /**
     * @param cctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    private void startQuery(GridCacheContext cctx) throws IgniteCheckedException {
        if (!qryIdMap.containsKey(cctx.cacheId())) {
            synchronized (this) {
                if (!qryIdMap.containsKey(cctx.cacheId())) {
                    qryIdMap.put(cctx.cacheId(),
                        cctx.continuousQueries().executeInternalQuery(
                            new DataStructuresEntryListener(),
                            new DataStructuresEntryFilter(),
                            cctx.isReplicated() && cctx.affinityNode(),
                            false,
                            false,
                            true
                        ));
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

        CountDownLatch init0 = initLatch;

        if (init0 != null && init0.getCount() > 0) {
            initFailed = true;

            init0.countDown();

            initLatch = null;
        }

        Iterator<Map.Entry<Integer, UUID>> iter = qryIdMap.entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry<Integer, UUID> e = iter.next();

            iter.remove();

            GridCacheContext cctx = ctx.cache().context().cacheContext(e.getKey());

            cctx.continuousQueries().cancelInternalQuery(e.getValue());
        }
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext ctx) {
        if (log.isDebugEnabled())
            log.debug("Activating data structure processor [nodeId=" + ctx.localNodeId() +
                " topVer=" + ctx.discovery().topologyVersionEx() + " ]");

        initFailed = false;

        qryIdMap.clear();

        ctx.event().addLocalEventListener(lsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);

        restoreStructuresState(ctx);

        onKernalStart0();
    }

    /**
     * @param ctx Context.
     */
    public void restoreStructuresState(GridKernalContext ctx) {
        onKernalStart0();

        try {
            for (GridCacheRemovable v : dsMap.values()) {
                if (v instanceof IgniteChangeGlobalStateSupport)
                    ((IgniteChangeGlobalStateSupport)v).onActivate(ctx);
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed restore data structures state", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext ctx) {
        if (log.isDebugEnabled())
            log.debug("DeActivate data structure processor [nodeId=" + ctx.localNodeId() +
                ", topVer=" + ctx.discovery().topologyVersionEx() + "]");

        ctx.event().removeLocalEventListener(lsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);

        onKernalStop(false);

        initLatch = null;

        for (GridCacheRemovable v : dsMap.values()) {
            if (v instanceof IgniteChangeGlobalStateSupport)
                ((IgniteChangeGlobalStateSupport)v).onDeActivate(ctx);
        }
    }

    /**
     * @param key Key.
     * @param obj Object.
     */
    void onRemoved(GridCacheInternalKey key, GridCacheRemovable obj) {
        dsMap.remove(key, obj);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> onReconnected(boolean clusterRestarted) throws IgniteCheckedException {
        for (Map.Entry<GridCacheInternalKey, GridCacheRemovable> e : dsMap.entrySet()) {
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
     * @param cacheName Cache name.
     * @return {@code True} if cache with such name is used to store data structures.
     */
    public static boolean isDataStructureCache(String cacheName) {
        return cacheName != null && (cacheName.startsWith(ATOMICS_CACHE_NAME) ||
            cacheName.startsWith(DS_CACHE_NAME_PREFIX) ||
            cacheName.equals(DEFAULT_DS_GROUP_NAME) ||
            cacheName.equals(DEFAULT_VOLATILE_DS_GROUP_NAME));
    }

    /**
     * @param grpName Group name.
     * @return {@code True} if group name is reserved to store data structures.
     */
    public static boolean isReservedGroup(@Nullable String grpName) {
        return DEFAULT_DS_GROUP_NAME.equals(grpName) ||
            DEFAULT_VOLATILE_DS_GROUP_NAME.equals(grpName);
    }

    /**
     * Gets a sequence from cache or creates one if it's not cached.
     *
     * @param name Sequence name.
     * @param cfg Configuration.
     * @param initVal Initial value for sequence. If sequence already cached, {@code initVal} will be ignored.
     * @param create If {@code true} sequence will be created in case it is not in cache.
     * @return Sequence.
     * @throws IgniteCheckedException If loading failed.
     */
    public final IgniteAtomicSequence sequence(final String name,
        @Nullable final AtomicConfiguration cfg,
        final long initVal,
        final boolean create)
        throws IgniteCheckedException
    {
        return getAtomic(new AtomicAccessor<GridCacheAtomicSequenceEx>() {
            @Override public T2<GridCacheAtomicSequenceEx, AtomicDataStructureValue> get(GridCacheInternalKey key,
                AtomicDataStructureValue val, IgniteInternalCache cache) throws IgniteCheckedException {
                GridCacheAtomicSequenceValue seqVal = cast(val, GridCacheAtomicSequenceValue.class);

                // Check that sequence hasn't been created in other thread yet.
                GridCacheAtomicSequenceEx seq = cast(dsMap.get(key), GridCacheAtomicSequenceEx.class);

                if (seq != null) {
                    assert seqVal != null;

                    return new T2<>(seq, null);
                }

                if (seqVal == null && !create)
                    return null;

                AtomicConfiguration cfg0 = cfg != null ? cfg : dfltAtomicCfg;

                // We should use offset because we already reserved left side of range.
                long off = cfg0.getAtomicSequenceReserveSize() > 1 ? cfg0.getAtomicSequenceReserveSize() - 1 : 1;

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
                    cfg0.getAtomicSequenceReserveSize(),
                    locCntr,
                    upBound);

                return new T2<GridCacheAtomicSequenceEx, AtomicDataStructureValue>(seq, seqVal);
            }
        }, cfg, name, DataStructureType.ATOMIC_SEQ, create, GridCacheAtomicSequenceEx.class);
    }

    /**
     * Removes sequence from cache.
     *
     * @param name Sequence name.
     * @param grpName Group name.
     * @throws IgniteCheckedException If removing failed.
     */
    final void removeSequence(final String name, final String grpName) throws IgniteCheckedException {
        removeDataStructure(null, name, grpName, ATOMIC_SEQ, null);
    }

    /**
     * Gets an atomic long from cache or creates one if it's not cached.
     *
     * @param name Name of atomic long.
     * @param cfg Configuration.
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
        return getAtomic(new AtomicAccessor<GridCacheAtomicLongEx>() {
            @Override public T2<GridCacheAtomicLongEx, AtomicDataStructureValue> get(GridCacheInternalKey key, AtomicDataStructureValue val, IgniteInternalCache cache) throws IgniteCheckedException {
                // Check that atomic long hasn't been created in other thread yet.
                GridCacheAtomicLongEx a = cast(dsMap.get(key), GridCacheAtomicLongEx.class);

                if (a != null) {
                    assert val != null;

                    return new T2<>(a, null);
                }

                if (val == null && !create)
                    return null;

                GridCacheAtomicLongValue retVal = (val == null ? new GridCacheAtomicLongValue(initVal) : null);

                a = new GridCacheAtomicLongImpl(name, key, cache);

                return new T2<GridCacheAtomicLongEx, AtomicDataStructureValue>(a, retVal);
            }
        }, cfg, name, ATOMIC_LONG, create, GridCacheAtomicLongEx.class);
    }

    /**
     * @param c Closure creating data structure instance.
     * @param cfg Optional custom configuration or {@code null} to use default one.
     * @param name Data structure name.
     * @param type Data structure type.
     * @param create Create flag.
     * @param cls Expected data structure class.
     * @return Data structure instance.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private <T extends GridCacheRemovable> T getAtomic(final AtomicAccessor<T> c,
        @Nullable AtomicConfiguration cfg,
        final String name,
        final DataStructureType type,
        final boolean create,
        Class<? extends T> cls)
        throws IgniteCheckedException
    {
        A.notNull(name, "name");

        awaitInitialization();

        if (cfg == null) {
            checkAtomicsConfiguration();

            cfg = dfltAtomicCfg;
        }

        final String grpName;

        if (type.isVolatile())
            grpName = DEFAULT_VOLATILE_DS_GROUP_NAME;
        else if (cfg.getGroupName() != null)
            grpName = cfg.getGroupName();
        else
            grpName = DEFAULT_DS_GROUP_NAME;

        String cacheName = ATOMICS_CACHE_NAME + "@" + grpName;

        IgniteInternalCache<GridCacheInternalKey, AtomicDataStructureValue> cache0 = ctx.cache().cache(cacheName);

        if (cache0 == null) {
            if (!create && ctx.cache().cacheDescriptor(cacheName) == null)
                return null;

            ctx.cache().dynamicStartCache(cacheConfiguration(cfg, cacheName, grpName),
                cacheName,
                null,
                CacheType.DATA_STRUCTURES,
                false,
                false,
                true,
                true).get();

            cache0 = ctx.cache().cache(cacheName);

            assert cache0 != null;
        }

        final IgniteInternalCache<GridCacheInternalKey, AtomicDataStructureValue> cache = cache0;

        startQuery(cache.context());

        final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name, grpName);

        // Check type of structure received by key from local cache.
        T dataStructure = cast(dsMap.get(key), cls);

        if (dataStructure != null)
            return dataStructure;

        return retryTopologySafe(new IgniteOutClosureX<T>() {
            @Override public T applyx() throws IgniteCheckedException {
                cache.context().gate().enter();

                try (GridNearTxLocal tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                    AtomicDataStructureValue val = cache.get(key);

                    if (isObsolete(val))
                        val = null;

                    if (val == null && !create)
                        return null;

                    if (val != null) {
                        if (val.type() != type)
                            throw new IgniteCheckedException("Another data structure with the same name already created " +
                                "[name=" + name +
                                ", newType=" + type +
                                ", existingType=" + val.type() + ']');
                    }

                    T2<T, ? extends AtomicDataStructureValue> ret;

                    try {
                        ret = c.get(key, val, cache);

                        dsMap.put(key, ret.get1());

                        if (ret.get2() != null)
                            cache.put(key, ret.get2());

                        tx.commit();
                    }
                    catch (Error | Exception e) {
                        dsMap.remove(key);

                        U.error(log, "Failed to make datastructure: " + name, e);

                        throw e;
                    }

                    return ret.get1();
                }
                finally {
                    cache.context().gate().leave();
                }
            }
        });
    }

    /**
     * @param val Value.
     * @return {@code True} if value is obsolete.
     */
    private boolean isObsolete(AtomicDataStructureValue val) {
        return !(val == null || !(val instanceof VolatileAtomicDataStructureValue)) &&
            ((VolatileAtomicDataStructureValue)val).gridStartTime() != ctx.discovery().gridStartTime();

    }

    /**
     * Removes atomic long from cache.
     *
     * @param name Atomic long name.
     * @param grpName Group name.
     * @throws IgniteCheckedException If removing failed.
     */
    final void removeAtomicLong(final String name, @Nullable final String grpName) throws IgniteCheckedException {
        removeDataStructure(null, name, grpName, ATOMIC_LONG, null);
    }

    /**
     * @param pred Remove predicate.
     * @param name Data structure name.
     * @param grpName Group name.
     * @param type Data structure type.
     * @param afterRmv Optional closure to run after data structure removed.
     * @throws IgniteCheckedException If failed.
     */
    private <T> void removeDataStructure(
        @Nullable final IgnitePredicateX<AtomicDataStructureValue> pred,
        final String name,
        String grpName,
        final DataStructureType type,
        @Nullable final IgniteInClosureX<T> afterRmv
    ) throws IgniteCheckedException {
        assert name != null;
        assert grpName != null;
        assert type != null;

        awaitInitialization();

        final String cacheName = ATOMICS_CACHE_NAME + "@" + grpName;

        final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name, grpName);

        retryTopologySafe(new IgniteOutClosureX<Object>() {
            @Override public Object applyx() throws IgniteCheckedException {
                IgniteInternalCache<GridCacheInternalKey, AtomicDataStructureValue> cache = ctx.cache().cache(cacheName);

                if (cache != null && cache.context().gate().enterIfNotStopped()) {
                    boolean isInterrupted = Thread.interrupted();

                    try {
                        while(true) {
                            try {
                                try (GridNearTxLocal tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                                    AtomicDataStructureValue val = cache.get(key);

                                    if (val == null)
                                        return null;

                                    if (val.type() != type)
                                        throw new IgniteCheckedException("Data structure has different type " +
                                            "[name=" + name +
                                            ", expectedType=" + type +
                                            ", actualType=" + val.type() + ']');

                                    if (pred == null || pred.applyx(val)) {
                                        cache.remove(key);

                                        tx.commit();

                                        if (afterRmv != null)
                                            afterRmv.applyx(null);
                                    }
                                }

                                break;
                            }
                            catch (IgniteCheckedException e) {
                                if (X.hasCause(e, IgniteInterruptedCheckedException.class, InterruptedException.class))
                                    isInterrupted = Thread.interrupted();
                                else
                                    throw e;
                            }
                        }
                    }
                    finally {
                        cache.context().gate().leave();

                        if (isInterrupted)
                            Thread.currentThread().interrupt();
                    }
                }

                return null;
            }
        });
    }

    public void suspend(String cacheName) {
        for (Map.Entry<GridCacheInternalKey, GridCacheRemovable> e : dsMap.entrySet()) {
            String cacheName0 = ATOMICS_CACHE_NAME + "@" + e.getKey().groupName();

            if (cacheName0.equals(cacheName))
                e.getValue().suspend();
        }
    }

    public void restart(IgniteInternalCache cache) {
        for (Map.Entry<GridCacheInternalKey, GridCacheRemovable> e : dsMap.entrySet()) {
            String cacheName0 = ATOMICS_CACHE_NAME + "@" + e.getKey().groupName();

            if (cacheName0.equals(cache.name()))
                e.getValue().restart(cache);
        }
    }

    /**
     * Gets an atomic reference from cache or creates one if it's not cached.
     *
     * @param name Name of atomic reference.
     * @param cfg Configuration.
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
        return getAtomic(new AtomicAccessor<GridCacheAtomicReferenceEx>() {
            @Override public T2<GridCacheAtomicReferenceEx, AtomicDataStructureValue> get(GridCacheInternalKey key, AtomicDataStructureValue val, IgniteInternalCache cache) throws IgniteCheckedException {
                // Check that atomic reference hasn't been created in other thread yet.
                GridCacheAtomicReferenceEx ref = cast(dsMap.get(key),
                    GridCacheAtomicReferenceEx.class);

                if (ref != null) {
                    assert val != null;

                    return new T2<>(ref, null);
                }

                if (val == null && !create)
                    return null;

                AtomicDataStructureValue retVal = (val == null ? new GridCacheAtomicReferenceValue<>(initVal) : null);

                ref = new GridCacheAtomicReferenceImpl(name, key, cache);

                return new T2<>(ref, retVal);
            }
        }, cfg, name, ATOMIC_REF, create, GridCacheAtomicReferenceEx.class);
    }

    /**
     * Removes atomic reference from cache.
     *
     * @param name Atomic reference name.
     * @param grpName Group name.
     * @throws IgniteCheckedException If removing failed.
     */
    final void removeAtomicReference(final String name, @Nullable final String grpName) throws IgniteCheckedException {
        removeDataStructure(null, name, grpName, ATOMIC_REF, null);
    }

    /**
     * Gets an atomic stamped from cache or creates one if it's not cached.
     *
     * @param name Name of atomic stamped.
     * @param cfg Configuration.
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
        return getAtomic(new AtomicAccessor<GridCacheAtomicStampedEx>() {
            @Override public T2<GridCacheAtomicStampedEx, AtomicDataStructureValue> get(GridCacheInternalKey key, AtomicDataStructureValue val, IgniteInternalCache cache) throws IgniteCheckedException {
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

                return new T2<>(stmp, retVal);
            }
        }, cfg, name, ATOMIC_STAMPED, create, GridCacheAtomicStampedEx.class);
    }

    /**
     * Removes atomic stamped from cache.
     *
     * @param name Atomic stamped name.
     * @param grpName Group name.
     * @throws IgniteCheckedException If removing failed.
     */
    final void removeAtomicStamped(final String name, final String grpName) throws IgniteCheckedException {
        removeDataStructure(null, name, grpName, ATOMIC_STAMPED, null);
    }

    /**
     * Gets a queue from cache or creates one if it's not cached.
     *
     * @param name Name of queue.
     * @param grpName Group name. If present, will override groupName from configuration.
     * @param cap Max size of queue.
     * @param cfg Non-null queue configuration if new queue should be created.
     * @return Instance of queue.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public final <T> IgniteQueue<T> queue(final String name, @Nullable final String grpName, int cap,
        @Nullable final CollectionConfiguration cfg)
        throws IgniteCheckedException {
        A.notNull(name, "name");

        if (cfg != null) {
            if (cap <= 0)
                cap = Integer.MAX_VALUE;
        }

        final int cap0 = cap;

        final boolean create = cfg != null;

        return getCollection(new IgniteClosureX<GridCacheContext, IgniteQueue<T>>() {
            @Override public IgniteQueue<T> applyx(GridCacheContext ctx) throws IgniteCheckedException {
                return ctx.dataStructures().queue(name, cap0, isCollocated(cfg), create);
            }
        }, cfg, name, grpName, QUEUE, create, false);
    }

    /**
     * Non-collocated mode only makes sense for and is only supported for PARTITIONED caches, so
     * collocated mode should be enabled for non-partitioned cache by default.
     *
     * @param cfg Collection configuration.
     * @return {@code True} If collocated mode should be enabled.
     */
    private boolean isCollocated(CollectionConfiguration cfg) {
        return cfg != null && (cfg.isCollocated() || cfg.getCacheMode() != PARTITIONED);
    }

    /**
     * @param cfg Atomic configuration.
     * @param name Cache name.
     * @param grpName Group name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(AtomicConfiguration cfg, String name, String grpName) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setGroupName(grpName);
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
     * @param grpName Group name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(CollectionConfiguration cfg, String name, String grpName) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setGroupName(grpName);
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
     * @param name Cache name.
     * @param grpName Group name.
     * @return Meta cache configuration.
     */
    private CacheConfiguration metaCacheConfiguration(CollectionConfiguration cfg, String name, String grpName) {
        CacheConfiguration ccfg = cacheConfiguration(cfg, name, grpName);

        ccfg.setAtomicityMode(TRANSACTIONAL);

        return ccfg;
    }

    /**
     * Get compatible with collection configuration data structure cache.
     *
     * @param cfg Collection configuration.
     * @param grpName Group name.
     * @param dsType Data structure type.
     * @param dsName Data structure name.
     * @param separated Separated cache flag.
     * @return Data structure cache.
     * @throws IgniteCheckedException If failed.
     */
    private IgniteInternalCache compatibleCache(CollectionConfiguration cfg,
        String grpName,
        DataStructureType dsType,
        String dsName,
        boolean separated
    ) throws IgniteCheckedException {
        String cacheName = DS_CACHE_NAME_PREFIX + cfg.getAtomicityMode() + "_" + cfg.getCacheMode() + "_" +
            cfg.getBackups() + "@" + grpName;

        IgniteInternalCache cache = ctx.cache().cache(cacheName);

        if (separated && (cache == null || !cache.containsKey(new GridCacheSetHeaderKey(dsName)))) {
            cacheName += "#" + dsType.name() + "_" + dsName;

            cache = ctx.cache().cache(cacheName);
        }

        if (cache == null) {
            ctx.cache().dynamicStartCache(cacheConfiguration(cfg, cacheName, grpName),
                cacheName,
                null,
                CacheType.DATA_STRUCTURES,
                false,
                false,
                true,
                true).get();
        }
        else {
            IgnitePredicate<ClusterNode> cacheNodeFilter = cache.context().group().nodeFilter();

            String clsName1 = cacheNodeFilter != null ? cacheNodeFilter.getClass().getName() :
                CacheConfiguration.IgniteAllNodesPredicate.class.getName();
            String clsName2 = cfg.getNodeFilter() != null ? cfg.getNodeFilter().getClass().getName() :
                CacheConfiguration.IgniteAllNodesPredicate.class.getName();

            if (!clsName1.equals(clsName2))
                throw new IgniteCheckedException("Could not add collection to group " + grpName +
                    " because of different node filters [existing=" + clsName1 + ", new=" + clsName2 + "]");
        }

        cache = ctx.cache().getOrStartCache(cacheName);

        assert cache != null;

        return cache;
    }

    /**
     * @param name Queue name.
     * @param cctx Queue cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void removeQueue(final String name, final GridCacheContext cctx) throws IgniteCheckedException {
        assert name != null;
        assert cctx != null;

        CIX1<GridCacheQueueHeader> afterRmv = new CIX1<GridCacheQueueHeader>() {
            @Override public void applyx(GridCacheQueueHeader hdr) throws IgniteCheckedException {
                hdr = (GridCacheQueueHeader) cctx.cache().withNoRetries().getAndRemove(new GridCacheQueueHeaderKey(name));

                if (hdr == null || hdr.empty())
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

        removeDataStructure(null, name, cctx.group().name(), QUEUE, afterRmv);
    }

    /**
     * @param c Closure creating collection.
     * @param cfg Configuration.
     * @param name Collection name.
     * @param grpName Cache group name.
     * @param type Data structure type.
     * @param create Create flag.
     * @param separated Separated cache flag.
     * @return Collection instance.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private <T> T getCollection(final IgniteClosureX<GridCacheContext, T> c,
        @Nullable CollectionConfiguration cfg,
        String name,
        @Nullable String grpName,
        final DataStructureType type,
        boolean create,
        boolean separated)
        throws IgniteCheckedException
    {
        awaitInitialization();

        assert name != null;
        assert type.isCollection() : type;
        assert !create || cfg != null;

        if (grpName == null) {
            if (cfg != null && cfg.getGroupName() != null)
                grpName = cfg.getGroupName();
            else
                grpName = DEFAULT_DS_GROUP_NAME;
        }

        final String metaCacheName = ATOMICS_CACHE_NAME + "@" + grpName;

        IgniteInternalCache<GridCacheInternalKey, AtomicDataStructureValue> metaCache0 = ctx.cache().cache(metaCacheName);

        if (metaCache0 == null) {
            CacheConfiguration ccfg = null;

            if (!create) {
                DynamicCacheDescriptor desc = ctx.cache().cacheDescriptor(metaCacheName);

                if (desc == null)
                    return null;
            }
            else
                ccfg = metaCacheConfiguration(cfg, metaCacheName, grpName);

            ctx.cache().dynamicStartCache(ccfg,
                metaCacheName,
                null,
                CacheType.DATA_STRUCTURES,
                false,
                false,
                true,
                true).get();

            metaCache0 = ctx.cache().cache(metaCacheName);

            assert metaCache0 != null;
        }

        final IgniteInternalCache<GridCacheInternalKey, AtomicDataStructureValue> metaCache = metaCache0;

        AtomicDataStructureValue oldVal;

        final IgniteInternalCache cache;

        if (create) {
            cache = compatibleCache(cfg, grpName, type, name, separated);

            DistributedCollectionMetadata newVal = new DistributedCollectionMetadata(type, cfg, cache.name());

            oldVal = metaCache.getAndPutIfAbsent(new GridCacheInternalKeyImpl(name, grpName), newVal);
        }
        else {
            oldVal = metaCache.get(new GridCacheInternalKeyImpl(name, grpName));

            if (oldVal == null)
                return null;
            else if (!(oldVal instanceof DistributedCollectionMetadata))
                throw new IgniteCheckedException("Another data structure with the same name already created " +
                    "[name=" + name +
                    ", newType=" + type +
                    ", existingType=" + oldVal.type() + ']');

            cache = ctx.cache().getOrStartCache(((DistributedCollectionMetadata)oldVal).cacheName());

            if (cache == null)
                return null;
        }

        if (oldVal != null) {
            if (oldVal.type() != type)
                throw new IgniteCheckedException("Another data structure with the same name already created " +
                    "[name=" + name +
                    ", newType=" + type +
                    ", existingType=" + oldVal.type() + ']');

            assert oldVal instanceof DistributedCollectionMetadata;

            if (cfg != null && ((DistributedCollectionMetadata)oldVal).configuration().isCollocated() != cfg.isCollocated()) {
                throw new IgniteCheckedException("Another collection with the same name but different " +
                    "configuration already created [name=" + name +
                    ", newCollocated=" + cfg.isCollocated() +
                    ", existingCollocated=" + !cfg.isCollocated() + ']');
            }
        }

        return retryTopologySafe(new IgniteOutClosureX<T>() {
            @Override public T applyx() throws IgniteCheckedException {
                return c.applyx(cache.context());
            }
        });
    }

    /**
     * Awaits for processor initialization.
     */
    private void awaitInitialization() {
        CountDownLatch latch0 = initLatch;

        if (latch0 == null)
            throw new IllegalStateException("Ignite cluster is not active");

        if (latch0.getCount() > 0) {
            try {
                U.await(latch0);

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
     * Gets or creates count down latch. If count down latch is not found in cache,
     * it is created using provided name and count parameter.
     *
     * @param name Name of the latch.
     * @param cfg Configuration.
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

        return getAtomic(new AtomicAccessor<GridCacheCountDownLatchEx>() {
            @Override public T2<GridCacheCountDownLatchEx, AtomicDataStructureValue> get(GridCacheInternalKey key, AtomicDataStructureValue val, IgniteInternalCache cache) throws IgniteCheckedException {
                // Check that count down hasn't been created in other thread yet.
                GridCacheCountDownLatchEx latch = cast(dsMap.get(key), GridCacheCountDownLatchEx.class);

                if (latch != null) {
                    assert val != null;

                    return new T2<>(latch, null);
                }

                if (val == null && !create)
                    return null;

                GridCacheCountDownLatchValue retVal = (val == null ? new GridCacheCountDownLatchValue(cnt, autoDel,
                    ctx.discovery().gridStartTime()) : null);

                GridCacheCountDownLatchValue latchVal = retVal != null ? retVal : (GridCacheCountDownLatchValue) val;

                assert latchVal != null;

                latch = new GridCacheCountDownLatchImpl(name, latchVal.initialCount(),
                    latchVal.autoDelete(),
                    key,
                    cache);

                return new T2<GridCacheCountDownLatchEx, AtomicDataStructureValue>(latch, retVal);
            }
        }, cfg, name, COUNT_DOWN_LATCH, create, GridCacheCountDownLatchEx.class);
    }

    /**
     * Removes count down latch from cache.
     *
     * @param name Name of the latch.
     * @param grpName Cache group name.
     * @throws IgniteCheckedException If operation failed.
     */
    public void removeCountDownLatch(final String name, final String grpName) throws IgniteCheckedException {
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
        }, name, grpName, COUNT_DOWN_LATCH, null);
    }

    /**
     * Gets or creates semaphore. If semaphore is not found in cache,
     * it is created using provided name and count parameter.
     *
     * @param name Name of the semaphore.
     * @param cfg Configuration.
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
        return getAtomic(new AtomicAccessor<GridCacheSemaphoreEx>() {
            @Override public T2<GridCacheSemaphoreEx, AtomicDataStructureValue> get(GridCacheInternalKey key, AtomicDataStructureValue val, IgniteInternalCache cache) throws IgniteCheckedException {
                // Check that semaphore hasn't been created in other thread yet.
                GridCacheSemaphoreEx sem = cast(dsMap.get(key), GridCacheSemaphoreEx.class);

                if (sem != null) {
                    assert val != null;

                    return new T2<>(sem, null);
                }

                if (val == null && !create)
                    return null;

                AtomicDataStructureValue retVal = (val == null ? new GridCacheSemaphoreState(cnt,
                    new HashMap<UUID, Integer>(), failoverSafe, ctx.discovery().gridStartTime()) : null);

                GridCacheSemaphoreEx sem0 = new GridCacheSemaphoreImpl(name, key, cache);

                //check Cluster state against semaphore state
                if (val != null && failoverSafe) {
                    GridCacheSemaphoreState semState = (GridCacheSemaphoreState) val;

                    boolean updated = false;

                    Map<UUID,Integer> waiters = semState.getWaiters();

                    Integer permit = ((GridCacheSemaphoreState) val).getCount();

                    for (UUID nodeId : new HashSet<>(waiters.keySet())) {

                        ClusterNode node = ctx.cluster().get().node(nodeId);

                        if (node == null) {

                            permit += waiters.get(nodeId);

                            waiters.remove(nodeId);

                            updated = true;
                        }
                    }
                    if (updated) {
                        semState.setWaiters(waiters);
                        semState.setCount(permit);

                        retVal = semState;
                    }
                }

                return new T2<>(sem0, retVal);
            }
        }, cfg, name, SEMAPHORE, create, GridCacheSemaphoreEx.class);
    }

    /**
     * Removes semaphore from cache.
     *
     * @param name Name of the semaphore.
     * @param grpName Group name.
     * @throws IgniteCheckedException If operation failed.
     */
    public void removeSemaphore(final String name, final String grpName) throws IgniteCheckedException {
        removeDataStructure(new IgnitePredicateX<AtomicDataStructureValue>() {
            @Override public boolean applyx(AtomicDataStructureValue val) throws IgniteCheckedException {
                assert val != null && val instanceof GridCacheSemaphoreState;

                GridCacheSemaphoreState semVal = (GridCacheSemaphoreState) val;

                if (semVal.getCount() < 0)
                    throw new IgniteCheckedException("Failed to remove semaphore with blocked threads. ");

                return true;
            }
        }, name, grpName, SEMAPHORE, null);
    }

    /**
     * Gets or creates reentrant lock. If reentrant lock is not found in cache,
     * it is created using provided name, failover mode, and fairness mode parameters.
     *
     * @param name Name of the reentrant lock.
     * @param cfg Configuration.
     * @param failoverSafe Flag indicating behaviour in case of failure.
     * @param fair Flag indicating fairness policy of this lock.
     * @param create If {@code true} reentrant lock will be created in case it is not in cache.
     * @return ReentrantLock for the given name or {@code null} if it is not found and
     *      {@code create} is false.
     * @throws IgniteCheckedException If operation failed.
     */
    public IgniteLock reentrantLock(final String name, @Nullable AtomicConfiguration cfg, final boolean failoverSafe,
        final boolean fair, final boolean create) throws IgniteCheckedException {
        return getAtomic(new AtomicAccessor<GridCacheLockEx>() {
            @Override public T2<GridCacheLockEx, AtomicDataStructureValue> get(GridCacheInternalKey key, AtomicDataStructureValue val, IgniteInternalCache cache) throws IgniteCheckedException {
                // Check that reentrant lock hasn't been created in other thread yet.
                GridCacheLockEx reentrantLock = cast(dsMap.get(key), GridCacheLockEx.class);

                if (reentrantLock != null) {
                    assert val != null;

                    return new T2<>(reentrantLock, null);
                }

                if (val == null && !create)
                    return new T2<>(null, null);

                AtomicDataStructureValue retVal = (val == null ? new GridCacheLockState(0, ctx.localNodeId(),
                    0, failoverSafe, fair, ctx.discovery().gridStartTime()) : null);

                GridCacheLockEx reentrantLock0 = new GridCacheLockImpl(name, key, cache);

                return new T2<>(reentrantLock0, retVal);
            }
        }, cfg, name, REENTRANT_LOCK, create, GridCacheLockEx.class);
    }

    /**
     * Removes reentrant lock from cache.
     *
     * @param name Name of the reentrant lock.
     * @param grpName Group name.
     * @param broken Flag indicating the reentrant lock is broken and should be removed unconditionally.
     * @throws IgniteCheckedException If operation failed.
     */
    public void removeReentrantLock(final String name, final String grpName, final boolean broken) throws IgniteCheckedException {
        removeDataStructure(new IgnitePredicateX<AtomicDataStructureValue>() {
            @Override public boolean applyx(AtomicDataStructureValue val) throws IgniteCheckedException {
                assert val != null && val instanceof GridCacheLockState;

                GridCacheLockState lockVal = (GridCacheLockState) val;

                if (lockVal.get() > 0 && !broken)
                    throw new IgniteCheckedException("Failed to remove reentrant lock with blocked threads. ");

                return true;
            }
        }, name, grpName, REENTRANT_LOCK, null);
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
                        final GridCacheInternalKey key = evt.getKey();

                        // Notify latch on changes.
                        final GridCacheRemovable latch = dsMap.get(key);

                        GridCacheCountDownLatchValue val = (GridCacheCountDownLatchValue)val0;

                        if (latch instanceof GridCacheCountDownLatchEx) {
                            final GridCacheCountDownLatchEx latch0 = (GridCacheCountDownLatchEx)latch;

                            latch0.onUpdate(val.get());

                            if (val.get() == 0 && val.autoDelete()) {
                                dsMap.remove(key);

                                IgniteInternalFuture<?> rmvFut = ctx.closure().runLocalSafe(new GPR() {
                                    @Override public void run() {
                                        try {
                                            removeCountDownLatch(latch0.name(), key.groupName());
                                        }
                                        catch (IgniteCheckedException e) {
                                            U.error(log, "Failed to remove count down latch: " + latch0.name(), e);
                                        }
                                        finally {
                                            ctx.cache().context().txContextReset();
                                        }
                                    }
                                });

                                rmvFut.listen(new CI1<IgniteInternalFuture<?>>() {
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
     * @param grpName Group name. If present, will override groupName from configuration.
     * @param cfg Set configuration if new set should be created.
     * @return Set instance.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    @Nullable public <T> IgniteSet<T> set(final String name, @Nullable final String grpName, @Nullable final CollectionConfiguration cfg)
        throws IgniteCheckedException {
        A.notNull(name, "name");

        final boolean create = cfg != null;
        final boolean collocated = isCollocated(cfg);
        final boolean separated = !collocated &&
            U.isOldestNodeVersionAtLeast(SEPARATE_CACHE_PER_NON_COLLOCATED_SET_SINCE,  ctx.grid().cluster().nodes());

        return getCollection(new CX1<GridCacheContext, IgniteSet<T>>() {
            @Override public IgniteSet<T> applyx(GridCacheContext cctx) throws IgniteCheckedException {
                return cctx.dataStructures().set(name, collocated, create, separated);
            }
        }, cfg, name, grpName, SET, create, separated);
    }

    /**
     * @param name Set name.
     * @param cctx Set cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void removeSet(final String name, final GridCacheContext cctx) throws IgniteCheckedException {
        assert name != null;
        assert cctx != null;

        CIX1<GridCacheSetHeader> afterRmv = new CIX1<GridCacheSetHeader>() {
            @Override public void applyx(GridCacheSetHeader hdr) throws IgniteCheckedException {
                hdr = (GridCacheSetHeader) cctx.cache().withNoRetries().getAndRemove(new GridCacheSetHeaderKey(name));

                if (hdr != null)
                    cctx.dataStructures().removeSetData(hdr.id());
            }
        };

        removeDataStructure(null, name, cctx.group().name(), SET, afterRmv);
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
            return null;
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Data structure processor memory stats [igniteInstanceName=" + ctx.igniteInstanceName() + ']');
        X.println(">>>   dsMapSize: " + dsMap.size());
    }

    /**
     * @throws IgniteException If atomics configuration is not provided.
     */
    private void checkAtomicsConfiguration() throws IgniteException {
        if (dfltAtomicCfg == null)
            throw new IgniteException("Atomic data structure can not be created, " +
                "need to provide AtomicConfiguration.");
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
    private interface AtomicAccessor<T> {
        /**
         * @param key Key.
         * @param val Existing value.
         * @param cache Data structure cache.
         * @return Data structure instance and value to store in cache.
         * @throws IgniteCheckedException If failed.
         */
        T2<T, AtomicDataStructureValue> get(GridCacheInternalKey key,
            @Nullable AtomicDataStructureValue val,
            IgniteInternalCache cache) throws IgniteCheckedException;
    }
}

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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.affinity.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.dr.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.processors.datastreamer.*;
import org.apache.ignite.internal.processors.dr.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.transactions.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.mxbean.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import javax.cache.*;
import javax.cache.expiry.*;
import javax.cache.processor.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.GridClosureCallMode.*;
import static org.apache.ignite.internal.processors.dr.GridDrType.*;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 * Adapter for different cache implementations.
 */
@SuppressWarnings("unchecked")
public abstract class GridCacheAdapter<K, V> implements IgniteInternalCache<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** clearLocally() split threshold. */
    public static final int CLEAR_ALL_SPLIT_THRESHOLD = 10000;

    /** Maximum number of retries when topology changes. */
    public static final int MAX_RETRIES = IgniteSystemProperties.getInteger(IGNITE_CACHE_RETRIES_COUNT, 100);

    /** Deserialization stash. */
    private static final ThreadLocal<IgniteBiTuple<String, String>> stash = new ThreadLocal<IgniteBiTuple<String,
        String>>() {
        @Override protected IgniteBiTuple<String, String> initialValue() {
            return F.t2();
        }
    };

    /** {@link GridCacheReturn}-to-value conversion. */
    private static final IgniteClosure RET2VAL =
        new CX1<IgniteInternalFuture<GridCacheReturn>, Object>() {
            @Nullable @Override public Object applyx(IgniteInternalFuture<GridCacheReturn> fut)
                throws IgniteCheckedException {
                return fut.get().value();
            }

            @Override public String toString() {
                return "Cache return value to value converter.";
            }
        };

    /** {@link GridCacheReturn}-to-null conversion. */
    protected static final IgniteClosure RET2NULL =
        new CX1<IgniteInternalFuture<GridCacheReturn>, Object>() {
            @Nullable @Override public Object applyx(IgniteInternalFuture<GridCacheReturn> fut)
                throws IgniteCheckedException {
                fut.get();

                return null;
            }

            @Override public String toString() {
                return "Cache return value to null converter.";
            }
        };

    /** {@link GridCacheReturn}-to-success conversion. */
    private static final IgniteClosure RET2FLAG =
        new CX1<IgniteInternalFuture<GridCacheReturn>, Boolean>() {
            @Override public Boolean applyx(IgniteInternalFuture<GridCacheReturn> fut) throws IgniteCheckedException {
                return fut.get().success();
            }

            @Override public String toString() {
                return "Cache return value to boolean flag converter.";
            }
        };

    /** */
    protected boolean keyCheck = !Boolean.getBoolean(IGNITE_CACHE_KEY_VALIDATION_DISABLED);

    /** */
    private boolean valCheck = true;

    /** Last asynchronous future. */
    protected ThreadLocal<FutureHolder> lastFut = new ThreadLocal<FutureHolder>() {
        @Override protected FutureHolder initialValue() {
            return new FutureHolder();
        }
    };

    /** Cache configuration. */
    @GridToStringExclude
    protected GridCacheContext<K, V> ctx;

    /** Local map. */
    @GridToStringExclude
    protected GridCacheConcurrentMap map;

    /** Local node ID. */
    @GridToStringExclude
    protected UUID locNodeId;

    /** Cache configuration. */
    @GridToStringExclude
    protected CacheConfiguration cacheCfg;

    /** Grid configuration. */
    @GridToStringExclude
    protected IgniteConfiguration gridCfg;

    /** Cache metrics. */
    protected CacheMetricsImpl metrics;

    /** Cache mxBean. */
    protected CacheMetricsMXBean mxBean;

    /** Logger. */
    protected IgniteLogger log;

    /** Affinity impl. */
    private Affinity<K> aff;

    /** Whether this cache is IGFS data cache. */
    private boolean igfsDataCache;

    /** Whether this cache is Mongo data cache. */
    @SuppressWarnings("UnusedDeclaration")
    private boolean mongoDataCache;

    /** Whether this cache is Mongo meta cache. */
    @SuppressWarnings("UnusedDeclaration")
    private boolean mongoMetaCache;

    /** Current IGFS data cache size. */
    private LongAdder8 igfsDataCacheSize;

    /** Max space for IGFS. */
    private long igfsDataSpaceMax;

    /** Asynchronous operations limit semaphore. */
    private Semaphore asyncOpsSem;

    /** {@inheritDoc} */
    @Override public String name() {
        return cacheCfg.getName();
    }

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    protected GridCacheAdapter() {
        // No-op.
    }

    /**
     * @param ctx Cache context.
     * @param startSize Start size.
     */
    @SuppressWarnings("OverriddenMethodCallDuringObjectConstruction")
    protected GridCacheAdapter(GridCacheContext<K, V> ctx, int startSize) {
        this(ctx, new GridCacheConcurrentMap(ctx, startSize, 0.75F, null));
    }

    /**
     * @param ctx Cache context.
     * @param map Concurrent map.
     */
    @SuppressWarnings("OverriddenMethodCallDuringObjectConstruction")
    protected GridCacheAdapter(final GridCacheContext<K, V> ctx, GridCacheConcurrentMap map) {
        assert ctx != null;

        this.ctx = ctx;

        gridCfg = ctx.gridConfig();
        cacheCfg = ctx.config();

        locNodeId = ctx.gridConfig().getNodeId();

        this.map = map;

        log = ctx.gridConfig().getGridLogger().getLogger(getClass());

        metrics = new CacheMetricsImpl(ctx);

        mxBean = new CacheMetricsMXBeanImpl(this);

        FileSystemConfiguration[] igfsCfgs = gridCfg.getFileSystemConfiguration();

        if (igfsCfgs != null) {
            for (FileSystemConfiguration igfsCfg : igfsCfgs) {
                if (F.eq(ctx.name(), igfsCfg.getDataCacheName())) {
                    if (!ctx.isNear()) {
                        igfsDataCache = true;
                        igfsDataCacheSize = new LongAdder8();

                        igfsDataSpaceMax = igfsCfg.getMaxSpaceSize();

                        if (igfsDataSpaceMax == 0) {
                            long maxMem = Runtime.getRuntime().maxMemory();

                            // We leave JVM at least 500M of memory for correct operation.
                            long jvmFreeSize = (maxMem - 512 * 1024 * 1024);

                            if (jvmFreeSize <= 0)
                                jvmFreeSize = maxMem / 2;

                            long dfltMaxSize = (long)(0.8f * maxMem);

                            igfsDataSpaceMax = Math.min(dfltMaxSize, jvmFreeSize);
                        }
                    }

                    break;
                }
            }
        }

        if (ctx.config().getMaxConcurrentAsyncOperations() > 0)
            asyncOpsSem = new Semaphore(ctx.config().getMaxConcurrentAsyncOperations());

        init();

        aff = new GridCacheAffinityImpl<>(ctx);
    }

    /**
     * Prints memory stats.
     */
    public void printMemoryStats() {
        if (ctx.isNear()) {
            X.println(">>>  Near cache size: " + size());

            ctx.near().dht().printMemoryStats();
        }
        else if (ctx.isDht())
            X.println(">>>  DHT cache size: " + size());
        else
            X.println(">>>  Cache size: " + size());
    }

    /**
     * @return Base map.
     */
    public GridCacheConcurrentMap map() {
        return map;
    }

    /**
     * @return Context.
     */
    @Override public GridCacheContext<K, V> context() {
        return ctx;
    }

    /**
     * @return Logger.
     */
    protected IgniteLogger log() {
        return log;
    }

    /**
     * @return {@code True} if this is near cache.
     */
    public boolean isNear() {
        return false;
    }

    /**
     * @return {@code True} if cache is local.
     */
    public boolean isLocal() {
        return false;
    }

    /**
     * @return {@code True} if cache is colocated.
     */
    public boolean isColocated() {
        return false;
    }

    /**
     * @return {@code True} if cache is DHT Atomic.
     */
    public boolean isDhtAtomic() {
        return false;
    }

    /**
     * @return {@code True} if cache is DHT.
     */
    public boolean isDht() {
        return false;
    }

    /**
     * @return Preloader.
     */
    public abstract GridCachePreloader preloader();

    /** {@inheritDoc} */
    @Override public Affinity<K> affinity() {
        return aff;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "RedundantCast"})
    @Override public <K1, V1> IgniteInternalCache<K1, V1> cache() {
        return (IgniteInternalCache<K1, V1>)this;
    }

    /** {@inheritDoc} */
    @Override public GridCacheProxyImpl<K, V> forSubjectId(UUID subjId) {
        CacheOperationContext opCtx = new CacheOperationContext(false, subjId, false, null, false);

        return new GridCacheProxyImpl<>(ctx, this, opCtx);
    }

    /** {@inheritDoc} */
    @Override public boolean skipStore() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public GridCacheProxyImpl<K, V> setSkipStore(boolean skipStore) {
        CacheOperationContext opCtx = new CacheOperationContext(true, null, false, null, false);

        return new GridCacheProxyImpl<>(ctx, this, opCtx);
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> GridCacheProxyImpl<K1, V1> keepPortable() {
        CacheOperationContext opCtx = new CacheOperationContext(false, null, true, null, false);

        return new GridCacheProxyImpl<>((GridCacheContext<K1, V1>)ctx, (GridCacheAdapter<K1, V1>)this, opCtx);
    }


    /** {@inheritDoc} */
    @Nullable @Override public ExpiryPolicy expiry() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheProxyImpl<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        assert !CU.isUtilityCache(ctx.name());
        assert !CU.isAtomicsCache(ctx.name());
        assert !CU.isMarshallerCache(ctx.name());

        CacheOperationContext opCtx = new CacheOperationContext(false, null, false, plc, false);

        return new GridCacheProxyImpl<>(ctx, this, opCtx);
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration configuration() {
        return ctx.config();
    }

    /**
     * @param keys Keys to lock.
     * @param timeout Lock timeout.
     * @param tx Transaction.
     * @param isRead {@code True} for read operations.
     * @param retval Flag to return value.
     * @param isolation Transaction isolation.
     * @param invalidate Invalidate flag.
     * @param accessTtl TTL for read operation.
     * @return Locks future.
     */
    public abstract IgniteInternalFuture<Boolean> txLockAsync(
        Collection<KeyCacheObject> keys,
        long timeout,
        IgniteTxLocalEx tx,
        boolean isRead,
        boolean retval,
        TransactionIsolation isolation,
        boolean invalidate,
        long accessTtl);

    /**
     * Post constructor initialization for subclasses.
     */
    protected void init() {
        // No-op.
    }

    /**
     * Starts this cache. Child classes should override this method
     * to provide custom start-up behavior.
     *
     * @throws IgniteCheckedException If start failed.
     */
    public void start() throws IgniteCheckedException {
        // No-op.
    }

    /**
     * Startup info.
     *
     * @return Startup info.
     */
    protected final String startInfo() {
        return "Cache started: " + U.maskName(ctx.config().getName());
    }

    /**
     * Stops this cache. Child classes should override this method
     * to provide custom stop behavior.
     */
    public void stop() {
        // Nulling thread local reference to ensure values will be eventually GCed
        // no matter what references these futures are holding.
        lastFut = null;
    }

    /**
     * Stop info.
     *
     * @return Stop info.
     */
    protected final String stopInfo() {
        return "Cache stopped: " + U.maskName(ctx.config().getName());
    }

    /**
     * Kernal start callback.
     *
     * @throws IgniteCheckedException If callback failed.
     */
    protected void onKernalStart() throws IgniteCheckedException {
        // No-op.
    }

    /**
     * Kernal stop callback.
     */
    public void onKernalStop() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        try {
            return localSize(CachePeekModes.ONHEAP_ONLY) == 0;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        try {
            return containsKeyAsync(key).get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> containsKeyAsync(K key) {
        A.notNull(key, "key");

        return getAllAsync(
            Collections.singletonList(key),
            /*force primary*/false,
            /*skip tx*/false,
            /*entry*/null,
            /*subj id*/null,
            /*task name*/null,
            /*deserialize portable*/false,
            /*skip values*/true,
            /*can remap*/true
        ).chain(new CX1<IgniteInternalFuture<Map<K, V>>, Boolean>() {
            @Override public Boolean applyx(IgniteInternalFuture<Map<K, V>> fut) throws IgniteCheckedException {
                Map<K, V> map = fut.get();

                assert map.isEmpty() || map.size() == 1 : map.size();

                return map.isEmpty() ? false : map.values().iterator().next() != null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(Collection<? extends K> keys) {
        try {
            return containsKeysAsync(keys).get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> containsKeysAsync(final Collection<? extends K> keys) {
        A.notNull(keys, "keys");

        return getAllAsync(
            keys,
            /*force primary*/false,
            /*skip tx*/false,
            /*entry*/null,
            /*subj id*/null,
            /*task name*/null,
            /*deserialize portable*/false,
            /*skip values*/true,
            /*can remap*/true
        ).chain(new CX1<IgniteInternalFuture<Map<K, V>>, Boolean>() {
            @Override public Boolean applyx(IgniteInternalFuture<Map<K, V>> fut) throws IgniteCheckedException {
                Map<K, V> kvMap = fut.get();

                if (keys.size() != kvMap.size())
                    return false;

                for (Map.Entry<K, V> entry : kvMap.entrySet()) {
                    if (entry.getValue() == null)
                        return false;
                }

                return true;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Iterable<Cache.Entry<K, V>> localEntries(CachePeekMode[] peekModes) throws IgniteCheckedException {
        assert peekModes != null;

        ctx.checkSecurity(SecurityPermission.CACHE_READ);

        PeekModes modes = parsePeekModes(peekModes, false);

        Collection<Iterator<Cache.Entry<K, V>>> its = new ArrayList<>();

        if (ctx.isLocal()) {
            modes.primary = true;
            modes.backup = true;

            if (modes.heap)
                its.add(iterator(map.entries0().iterator(), !ctx.keepPortable()));
        }
        else if (modes.heap) {
            if (modes.near && ctx.isNear())
                its.add(ctx.near().nearEntries().iterator());

            if (modes.primary || modes.backup) {
                GridDhtCacheAdapter<K, V> cache = ctx.isNear() ? ctx.near().dht() : ctx.dht();

                its.add(cache.localEntriesIterator(modes.primary, modes.backup));
            }
        }

        // Swap and offheap are disabled for near cache.
        if (modes.primary || modes.backup) {
            AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

            GridCacheSwapManager swapMgr = ctx.isNear() ? ctx.near().dht().context().swap() : ctx.swap();

            if (modes.swap)
                its.add(swapMgr.<K, V>swapIterator(modes.primary, modes.backup, topVer));

            if (modes.offheap)
                its.add(swapMgr.<K, V>offheapIterator(modes.primary, modes.backup, topVer));
        }

        final Iterator<Cache.Entry<K, V>> it = F.flatIterators(its);

        return new Iterable<Cache.Entry<K, V>>() {
            @Override public Iterator<Cache.Entry<K, V>> iterator() {
                return it;
            }

            public String toString() {
                return "CacheLocalEntries []";
            }
        };
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Nullable @Override public V localPeek(K key,
        CachePeekMode[] peekModes,
        @Nullable IgniteCacheExpiryPolicy plc)
        throws IgniteCheckedException {
        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        ctx.checkSecurity(SecurityPermission.CACHE_READ);

        PeekModes modes = parsePeekModes(peekModes, false);

        try {
            KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

            CacheObject cacheVal = null;

            if (!ctx.isLocal()) {
                AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

                int part = ctx.affinity().partition(cacheKey);

                boolean nearKey;

                if (!(modes.near && modes.primary && modes.backup)) {
                    boolean keyPrimary = ctx.affinity().primary(ctx.localNode(), part, topVer);

                    if (keyPrimary) {
                        if (!modes.primary)
                            return null;

                        nearKey = false;
                    }
                    else {
                        boolean keyBackup = ctx.affinity().belongs(ctx.localNode(), part, topVer);

                        if (keyBackup) {
                            if (!modes.backup)
                                return null;

                            nearKey = false;
                        }
                        else {
                            if (!modes.near)
                                return null;

                            nearKey = true;

                            // Swap and offheap are disabled for near cache.
                            modes.offheap = false;
                            modes.swap = false;
                        }
                    }
                }
                else {
                    nearKey = !ctx.affinity().belongs(ctx.localNode(), part, topVer);

                    if (nearKey) {
                        // Swap and offheap are disabled for near cache.
                        modes.offheap = false;
                        modes.swap = false;
                    }
                }

                if (nearKey && !ctx.isNear())
                    return null;

                if (modes.heap) {
                    GridCacheEntryEx e = nearKey ? peekEx(cacheKey) :
                        (ctx.isNear() ? ctx.near().dht().peekEx(cacheKey) : peekEx(cacheKey));

                    if (e != null) {
                        cacheVal = e.peek(modes.heap, modes.offheap, modes.swap, topVer, plc);

                        modes.offheap = false;
                        modes.swap = false;
                    }
                }

                if (modes.offheap || modes.swap) {
                    GridCacheSwapManager swapMgr = ctx.isNear() ? ctx.near().dht().context().swap() : ctx.swap();

                    GridCacheSwapEntry swapEntry = swapMgr.read(cacheKey, modes.offheap, modes.swap);

                    cacheVal = swapEntry != null ? swapEntry.value() : null;
                }
            }
            else
                cacheVal = localCachePeek0(cacheKey, modes.heap, modes.offheap, modes.swap, plc);

            Object val = CU.value(cacheVal, ctx, true);

            val = ctx.unwrapPortableIfNeeded(val, ctx.keepPortable());

            return (V)val;
        }
        catch (GridCacheEntryRemovedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Got removed entry during 'peek': " + key);

            return null;
        }
    }

    /**
     * @param key Key.
     * @param heap Read heap flag.
     * @param offheap Read offheap flag.
     * @param swap Read swap flag.
     * @param plc Optional expiry policy.
     * @return Value.
     * @throws GridCacheEntryRemovedException If entry removed.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private CacheObject localCachePeek0(KeyCacheObject key,
        boolean heap,
        boolean offheap,
        boolean swap,
        IgniteCacheExpiryPolicy plc)
        throws GridCacheEntryRemovedException, IgniteCheckedException {
        assert ctx.isLocal();
        assert heap || offheap || swap;

        if (heap) {
            GridCacheEntryEx e = peekEx(key);

            if (e != null)
                return e.peek(heap, offheap, swap, AffinityTopologyVersion.NONE, plc);
        }

        if (offheap || swap) {
            GridCacheSwapManager swapMgr = ctx.isNear() ? ctx.near().dht().context().swap() : ctx.swap();

            GridCacheSwapEntry swapEntry = swapMgr.read(key, offheap, swap);

            return swapEntry != null ? swapEntry.value() : null;
        }

        return null;
    }

    /**
     * Undeploys and removes all entries for class loader.
     *
     * @param ldr Class loader to undeploy.
     */
    public void onUndeploy(ClassLoader ldr) {
        ctx.deploy().onUndeploy(ldr, context());
    }

    /**
     *
     * @param key Entry key.
     * @return Entry or <tt>null</tt>.
     */
    @Nullable public GridCacheEntryEx peekEx(KeyCacheObject key) {
        return entry0(key, ctx.affinity().affinityTopologyVersion(), false, false);
    }

    /**
     *
     * @param key Entry key.
     * @return Entry or <tt>null</tt>.
     */
    @Nullable public GridCacheEntryEx peekEx(Object key) {
        return entry0(ctx.toCacheKeyObject(key), ctx.affinity().affinityTopologyVersion(), false, false);
    }

    /**
     * @param key Entry key.
     * @return Entry (never {@code null}).
     */
    public GridCacheEntryEx entryEx(Object key) {
        return entryEx(ctx.toCacheKeyObject(key), false);
    }

    /**
     * @param key Entry key.
     * @return Entry (never {@code null}).
     */
    public GridCacheEntryEx entryEx(KeyCacheObject key) {
        return entryEx(key, false);
    }

    /**
     * @param key Entry key.
     * @param touch Whether created entry should be touched.
     * @return Entry (never {@code null}).
     */
    public GridCacheEntryEx entryEx(KeyCacheObject key, boolean touch) {
        GridCacheEntryEx e = entry0(key, ctx.affinity().affinityTopologyVersion(), true, touch);

        assert e != null;

        return e;
    }

    /**
     * @param topVer Topology version.
     * @param key Entry key.
     * @return Entry (never {@code null}).
     */
    public GridCacheEntryEx entryEx(KeyCacheObject key, AffinityTopologyVersion topVer) {
        GridCacheEntryEx e = entry0(key, topVer, true, false);

        assert e != null;

        return e;
    }

    /**
     * @param key Entry key.
     * @param topVer Topology version at the time of creation.
     * @param create Flag to create entry if it does not exist.
     * @param touch Flag to touch created entry (only if entry was actually created).
     * @return Entry or <tt>null</tt>.
     */
    @Nullable private GridCacheEntryEx entry0(KeyCacheObject key, AffinityTopologyVersion topVer, boolean create,
        boolean touch) {
        GridTriple<GridCacheMapEntry> t = map.putEntryIfObsoleteOrAbsent(topVer, key, null, create);

        GridCacheEntryEx cur = t.get1();
        GridCacheEntryEx created = t.get2();
        GridCacheEntryEx doomed = t.get3();

        if (doomed != null && ctx.events().isRecordable(EVT_CACHE_ENTRY_DESTROYED))
            // Event notification.
            ctx.events().addEvent(doomed.partition(), doomed.key(), locNodeId, (IgniteUuid)null, null,
                EVT_CACHE_ENTRY_DESTROYED, null, false, null, false, null, null, null);

        if (created != null) {
            // Event notification.
            if (ctx.events().isRecordable(EVT_CACHE_ENTRY_CREATED))
                ctx.events().addEvent(created.partition(), created.key(), locNodeId, (IgniteUuid)null, null,
                    EVT_CACHE_ENTRY_CREATED, null, false, null, false, null, null, null);

            if (touch)
                ctx.evicts().touch(cur, topVer);
        }

        return cur;
    }

    /**
     * @return Set of internal cached entry representations, excluding {@link GridCacheInternal} keys.
     */
    public Set<GridCacheEntryEx> entries() {
        return map.entries0();
    }

    /**
     * @return Set of internal cached entry representations, including {@link GridCacheInternal} keys.
     */
    public Set<GridCacheEntryEx> allEntries() {
        return map.allEntries0();
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<K, V>> entrySet() {
        return entrySet((CacheEntryPredicate[])null);
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<K, V>> entrySetx(CacheEntryPredicate... filter) {
        return map.entriesx(filter);
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<K, V>> entrySet(int part) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Set<K> keySet() {
        return keySet((CacheEntryPredicate[])null);
    }

    /** {@inheritDoc} */
    @Override public Set<K> keySetx() {
        return keySetx((CacheEntryPredicate[])null);
    }

    /** {@inheritDoc} */
    @Override public Set<K> primaryKeySet() {
        return primaryKeySet((CacheEntryPredicate[])null);
    }

    /** {@inheritDoc} */
    @Override public Collection<V> values() {
        return values((CacheEntryPredicate[])null);
    }

    /**
     * Collection of values cached on this node. You can remove
     * elements from this collection, but you cannot add elements to this collection.
     * All removal operation will be reflected on the cache itself.
     * <p>
     * Iterator over this collection will not fail if collection was
     * concurrently updated by another thread. This means that iterator may or
     * may not return latest values depending on whether they were added before
     * or after current iterator position.
     * <p>
     * NOTE: this operation is not distributed and returns only the values cached on this node.
     *
     * @param filter Filters.
     * @return Collection of cached values.
     */
    public Collection<V> values(CacheEntryPredicate... filter) {
        return map.values(filter);
    }

    /**
     *
     * @param key Entry key.
     */
    public void removeIfObsolete(KeyCacheObject key) {
        assert key != null;

        GridCacheEntryEx entry = map.removeEntryIfObsolete(key);

        if (entry != null) {
            assert entry.obsolete() : "Removed non-obsolete entry: " + entry;

            if (log.isDebugEnabled())
                log.debug("Removed entry from cache: " + entry);

            if (ctx.events().isRecordable(EVT_CACHE_ENTRY_DESTROYED))
                // Event notification.
                ctx.events().addEvent(entry.partition(), entry.key(), locNodeId, (IgniteUuid)null, null,
                    EVT_CACHE_ENTRY_DESTROYED, null, false, null, false, null, null, null);
        }
        else if (log.isDebugEnabled())
            log.debug("Remove will not be done for key (obsolete entry got replaced or removed): " + key);
    }

    /**
     * Split clearLocally all task into multiple runnables.
     *
     * @return Split runnables.
     */
    public List<GridCacheClearAllRunnable<K, V>> splitClearLocally() {
        assert CLEAR_ALL_SPLIT_THRESHOLD > 0;

        int keySize = size();

        int cnt = Math.min(keySize / CLEAR_ALL_SPLIT_THRESHOLD + (keySize % CLEAR_ALL_SPLIT_THRESHOLD != 0 ? 1 : 0),
            Runtime.getRuntime().availableProcessors());

        if (cnt == 0)
            cnt = 1; // Still perform cleanup since there could be entries in swap.

        GridCacheVersion obsoleteVer = ctx.versions().next();

        List<GridCacheClearAllRunnable<K, V>> res = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; i++)
            res.add(new GridCacheClearAllRunnable<>(this, obsoleteVer, i, cnt));

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean clearLocally(K key) {
        return clearLocally0(key);
    }

    /** {@inheritDoc} */
    @Override public void clearLocallyAll(Set<? extends K> keys) {
        clearLocally0(keys);
    }

    /** {@inheritDoc} */
    @Override public void clearLocally() {
        ctx.checkSecurity(SecurityPermission.CACHE_REMOVE);

        List<GridCacheClearAllRunnable<K, V>> jobs = splitClearLocally();

        if (!F.isEmpty(jobs)) {
            ExecutorService execSvc = null;

            if (jobs.size() > 1) {
                execSvc = Executors.newFixedThreadPool(jobs.size() - 1);

                for (int i = 1; i < jobs.size(); i++)
                    execSvc.submit(jobs.get(i));
            }

            try {
                jobs.get(0).run();
            }
            finally {
                if (execSvc != null) {
                    execSvc.shutdown();

                    try {
                        while (!execSvc.isTerminated() && !Thread.currentThread().isInterrupted())
                            execSvc.awaitTermination(1000, TimeUnit.MILLISECONDS);
                    }
                    catch (InterruptedException ignore) {
                        U.warn(log, "Got interrupted while waiting for Cache.clearLocally() executor service to " +
                            "finish.");

                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    /**
     * @param keys Keys.
     * @param readers Readers flag.
     */
    public void clearLocally(Collection<KeyCacheObject> keys, boolean readers) {
        if (F.isEmpty(keys))
            return;

        GridCacheVersion obsoleteVer = ctx.versions().next();

        for (KeyCacheObject key : keys) {
            GridCacheEntryEx e = peekEx(key);

            try {
                if (e != null)
                    e.clear(obsoleteVer, readers, null);
            }
            catch (IgniteCheckedException ex) {
                U.error(log, "Failed to clearLocally entry (will continue to clearLocally other entries): " + e,
                    ex);
            }
        }
    }

    /**
     * Clears entry from cache.
     *
     * @param obsoleteVer Obsolete version to set.
     * @param key Key to clearLocally.
     * @param filter Optional filter.
     * @return {@code True} if cleared.
     */
    private boolean clearLocally(GridCacheVersion obsoleteVer, K key, @Nullable CacheEntryPredicate[] filter) {
        try {
            KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

            GridCacheEntryEx entry = ctx.isSwapOrOffheapEnabled() ? entryEx(cacheKey) : peekEx(cacheKey);

            if (entry != null)
                return entry.clear(obsoleteVer, false, filter);
        }
        catch (GridDhtInvalidPartitionException ignored) {
            return false;
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Failed to clearLocally entry for key: " + key, ex);
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IgniteCheckedException {
        // Clear local cache synchronously.
        clearLocally();

        clearRemotes(0, null);
    }

    /** {@inheritDoc} */
    @Override public void clear(K key) throws IgniteCheckedException {
        // Clear local cache synchronously.
        clearLocally(key);

        clearRemotes(0, Collections.singleton(key));
    }

    /** {@inheritDoc} */
    @Override public void clearAll(Set<? extends K> keys) throws IgniteCheckedException {
        // Clear local cache synchronously.
        clearLocallyAll(keys);

        clearRemotes(0, keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAsync(K key) {
        return clearKeysAsync(Collections.singleton(key));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAsync(Set<? extends K> keys) {
        return clearKeysAsync(keys);
    }

    /**
     * @param timeout Timeout for clearLocally all task in milliseconds (0 for never).
     *      Set it to larger value for large caches.
     * @param keys Keys to clear or {@code null} if all cache should be cleared.
     * @throws IgniteCheckedException In case of cache could not be cleared on any of the nodes.
     */
    private void clearRemotes(long timeout, @Nullable final Set<? extends K> keys) throws IgniteCheckedException {
        // Send job to remote nodes only.
        Collection<ClusterNode> nodes =
            ctx.grid().cluster().forCacheNodes(name(), true, true, false).forRemotes().nodes();

        if (!nodes.isEmpty()) {
            ctx.kernalContext().task().setThreadContext(TC_TIMEOUT, timeout);

            ctx.kernalContext().task().setThreadContext(TC_SUBGRID, nodes);

            ctx.kernalContext().task().execute(
                new ClearTask(ctx.name(), ctx.affinity().affinityTopologyVersion(), keys), null).get();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAsync() {
        return clearKeysAsync(null);
    }

    /**
     * @param keys Keys to clear or {@code null} if all cache should be cleared.
     * @return Future.
     */
    private IgniteInternalFuture<?> clearKeysAsync(final Set<? extends K> keys) {
        Collection<ClusterNode> nodes = ctx.grid().cluster().forCacheNodes(name(), true, true, false).nodes();

        if (!nodes.isEmpty()) {
            ctx.kernalContext().task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.kernalContext().task().execute(
                new ClearTask(ctx.name(), ctx.affinity().affinityTopologyVersion(), keys), null);
        }
        else
            return new GridFinishedFuture<>();
    }

    /**
     * @param entry Removes entry from cache if currently mapped value is the same as passed.
     */
    public void removeEntry(GridCacheEntryEx entry) {
        map.removeEntry(entry);
    }

    /**
     * Evicts an entry from cache.
     *
     * @param key Key.
     * @param ver Version.
     * @param filter Filter.
     * @return {@code True} if entry was evicted.
     */
    private boolean evictx(K key, GridCacheVersion ver,
        @Nullable CacheEntryPredicate[] filter) {
        KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

        GridCacheEntryEx entry = peekEx(cacheKey);

        if (entry == null)
            return true;

        try {
            return ctx.evicts().evict(entry, ver, true, filter);
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Failed to evict entry from cache: " + entry, ex);

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public V getForcePrimary(K key) throws IgniteCheckedException {
        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAllAsync(
            F.asList(key),
            /*force primary*/true,
            /*skip tx*/false,
            /*cached entry*/null,
            /*subject id*/null,
            taskName,
            /*deserialize cache objects*/true,
            /*skip values*/false,
            /*can remap*/true
        ).get().get(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getForcePrimaryAsync(final K key) {
        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAllAsync(
            Collections.singletonList(key),
            /*force primary*/true,
            /*skip tx*/false,
            null,
            null,
            taskName,
            true,
            false,
            /*can remap*/true
        ).chain(new CX1<IgniteInternalFuture<Map<K, V>>, V>() {
            @Override public V applyx(IgniteInternalFuture<Map<K, V>> e) throws IgniteCheckedException {
                return e.get().get(key);
            }
        });
    }

    /**
     * Gets value without waiting for toplogy changes.
     *
     * @param key Key.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    public V getTopologySafe(K key) throws IgniteCheckedException {
        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAllAsync(
            F.asList(key),
            /*force primary*/false,
            /*skip tx*/false,
            /*cached entry*/null,
            /*subject id*/null,
            taskName,
            /*deserialize cache objects*/true,
            /*skip values*/false,
            /*can remap*/false
        ).get().get(key);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<K, V> getAllOutTx(Set<? extends K> keys) throws IgniteCheckedException {
        return getAllOutTxAsync(keys).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<K, V>> getAllOutTxAsync(Set<? extends K> keys) {
        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAllAsync(keys,
            !ctx.config().isReadFromBackup(),
            /*skip tx*/true,
            null,
            null,
            taskName,
            !ctx.keepPortable(),
            /*skip values*/false,
            /*can remap*/true);
    }

    /**
     * @param keys Keys.
     * @param reload Reload flag.
     * @param tx Transaction.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param vis Visitor.
     * @return Future.
     */
    public IgniteInternalFuture<Object> readThroughAllAsync(final Collection<KeyCacheObject> keys,
        boolean reload,
        boolean skipVals,
        @Nullable final IgniteInternalTx tx,
        @Nullable UUID subjId,
        String taskName,
        final IgniteBiInClosure<KeyCacheObject, Object> vis) {
        return ctx.closures().callLocalSafe(new GPC<Object>() {
            @Nullable @Override public Object call() {
                try {
                    ctx.store().loadAll(tx, keys, vis);
                }
                catch (IgniteCheckedException e) {
                    throw new GridClosureException(e);
                }

                return null;
            }
        }, true);
    }

    /**
     * @param keys Keys.
     * @param ret Return flag.
     * @param skipVals Skip values flag.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @return Future.
     */
    public IgniteInternalFuture<Map<KeyCacheObject, CacheObject>> reloadAllAsync0(
        Collection<KeyCacheObject> keys,
        boolean ret,
        boolean skipVals,
        @Nullable UUID subjId,
        String taskName)
    {
        final AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

        if (!F.isEmpty(keys)) {
            final Map<KeyCacheObject, GridCacheVersion> keyVers = new HashMap();

            for (KeyCacheObject key : keys) {
                if (key == null)
                    continue;

                // Skip primary or backup entries for near cache.
                if (ctx.isNear() && ctx.affinity().localNode(key, topVer))
                    continue;

                while (true) {
                    try {
                        GridCacheEntryEx entry = entryExSafe(key, topVer);

                        if (entry == null)
                            break;

                        GridCacheVersion ver = entry.version();

                        keyVers.put(key, ver);

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry for reload (will retry): " + key);
                    }
                    catch (GridDhtInvalidPartitionException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Got invalid partition for key (will skip): " + key);

                        break;
                    }
                }
            }

            final Map<KeyCacheObject, CacheObject> map =
                ret ? U.<KeyCacheObject, CacheObject>newHashMap(keys.size()) : null;

            final Collection<KeyCacheObject> absentKeys = F.view(keyVers.keySet());

            final Collection<KeyCacheObject> loadedKeys = new GridConcurrentHashSet<>();

            IgniteInternalFuture<Object> readFut = readThroughAllAsync(absentKeys, true, skipVals, null,
                subjId, taskName, new CI2<KeyCacheObject, Object>() {
                    /** Version for all loaded entries. */
                    private GridCacheVersion nextVer = ctx.versions().next();

                    /** {@inheritDoc} */
                    @Override public void apply(KeyCacheObject key, Object val) {
                        loadedKeys.add(key);

                        GridCacheEntryEx entry = peekEx(key);

                        if (entry != null) {
                            try {
                                GridCacheVersion curVer = keyVers.get(key);

                                if (curVer != null) {
                                    boolean wasNew = entry.isNewLocked();

                                    entry.unswap();

                                    CacheObject cacheVal = ctx.toCacheObject(val);

                                    boolean set = entry.versionedValue(cacheVal, curVer, nextVer);

                                    ctx.evicts().touch(entry, topVer);

                                    if (map != null) {
                                        if (set || wasNew)
                                            map.put(key, cacheVal);
                                        else {
                                            CacheObject v = entry.peek(true, false, false, null);

                                            if (v != null)
                                                map.put(key, v);
                                        }
                                    }

                                    if (log.isDebugEnabled()) {
                                        log.debug("Set value loaded from store into entry [set=" + set + ", " +
                                            "curVer=" +
                                            curVer + ", newVer=" + nextVer + ", entry=" + entry + ']');
                                    }
                                }
                                else {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Current version was not found (either entry was removed or " +
                                            "validation was not passed: " + entry);
                                    }
                                }
                            }
                            catch (GridCacheEntryRemovedException ignore) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Got removed entry for reload (will not store reloaded entry) " +
                                        "[entry=" + entry + ']');
                                }
                            }
                            catch (IgniteCheckedException e) {
                                throw new IgniteException(e);
                            }
                        }
                    }
                });

            return readFut.chain(new CX1<IgniteInternalFuture<Object>, Map<KeyCacheObject, CacheObject>>() {
                @Override public Map<KeyCacheObject, CacheObject> applyx(IgniteInternalFuture<Object> e)
                    throws IgniteCheckedException  {
                    // Touch all not loaded keys.
                    for (KeyCacheObject key : absentKeys) {
                        if (!loadedKeys.contains(key)) {
                            GridCacheEntryEx entry = peekEx(key);

                            if (entry != null)
                                ctx.evicts().touch(entry, topVer);
                        }
                    }

                    // Make sure there were no exceptions.
                    e.get();

                    return map;
                }
            });
        }

        return new GridFinishedFuture<>(Collections.<KeyCacheObject, CacheObject>emptyMap());
    }

    /**
     * @param key Key.
     * @param topVer Topology version.
     * @return Entry.
     */
    @Nullable protected GridCacheEntryEx entryExSafe(KeyCacheObject key, AffinityTopologyVersion topVer) {
        return entryEx(key);
    }

    /** {@inheritDoc} */
    @Nullable @Override public V get(K key) throws IgniteCheckedException {
        A.notNull(key, "key");

        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        V val = get(key, !ctx.keepPortable());

        if (ctx.config().getInterceptor() != null)
            val = (V)ctx.config().getInterceptor().onGet(key, val);

        if (statsEnabled)
            metrics0().addGetTimeNanos(System.nanoTime() - start);

        return val;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAsync(final K key) {
        A.notNull(key, "key");

        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        IgniteInternalFuture<V> fut = getAsync(key, !ctx.keepPortable());

        if (ctx.config().getInterceptor() != null)
            fut =  fut.chain(new CX1<IgniteInternalFuture<V>, V>() {
                @Override public V applyx(IgniteInternalFuture<V> f) throws IgniteCheckedException {
                    return (V)ctx.config().getInterceptor().onGet(key, f.get());
                }
            });

        if (statsEnabled)
            fut.listen(new UpdateGetTimeStatClosure<V>(metrics0(), start));

        return fut;
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        A.notNull(keys, "keys");

        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        Map<K, V> map = getAll(keys, !ctx.keepPortable());

        if (ctx.config().getInterceptor() != null)
            map = interceptGet(keys, map);

        if (statsEnabled)
            metrics0().addGetTimeNanos(System.nanoTime() - start);

        return map;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<K, V>> getAllAsync(@Nullable final Collection<? extends K> keys) {
        A.notNull(keys, "keys");

        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        IgniteInternalFuture<Map<K, V>> fut = getAllAsync(keys, !ctx.keepPortable());

        if (ctx.config().getInterceptor() != null)
            return fut.chain(new CX1<IgniteInternalFuture<Map<K, V>>, Map<K, V>>() {
                @Override public Map<K, V> applyx(IgniteInternalFuture<Map<K, V>> f) throws IgniteCheckedException {
                    return interceptGet(keys, f.get());
                }
            });

        if (statsEnabled)
            fut.listen(new UpdateGetTimeStatClosure<Map<K, V>>(metrics0(), start));

        return fut;
    }

    /**
     * Applies cache interceptor on result of 'get' operation.
     *
     * @param keys All requested keys.
     * @param map Result map.
     * @return Map with values returned by cache interceptor..
     */
    @SuppressWarnings("IfMayBeConditional")
    private Map<K, V> interceptGet(@Nullable Collection<? extends K> keys, Map<K, V> map) {
        if (F.isEmpty(keys))
            return map;

        CacheInterceptor<K, V> interceptor = cacheCfg.getInterceptor();

        assert interceptor != null;

        Map<K, V> res = U.newHashMap(keys.size());

        for (Map.Entry<K, V> e : map.entrySet()) {
            V val = interceptor.onGet(e.getKey(), e.getValue());

            if (val != null)
                res.put(e.getKey(), val);
        }

        if (map.size() != keys.size()) { // Not all requested keys were in cache.
            for (K key : keys) {
                if (key != null) {
                    if (!map.containsKey(key)) {
                        V val = interceptor.onGet(key, null);

                        if (val != null)
                            res.put(key, val);
                    }
                }
            }
        }

        return res;
    }

    /**
     * @param keys Keys.
     * @param forcePrimary Force primary.
     * @param skipTx Skip tx.
     * @param entry Entry.
     * @param subjId Subj Id.
     * @param taskName Task name.
     * @param deserializePortable Deserialize portable.
     * @param skipVals Skip values.
     * @return Future for the get operation.
     * @see GridCacheAdapter#getAllAsync(Collection)
     */
    protected IgniteInternalFuture<Map<K, V>> getAllAsync(
        @Nullable Collection<? extends K> keys,
        boolean forcePrimary,
        boolean skipTx,
        @Nullable GridCacheEntryEx entry,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializePortable,
        boolean skipVals,
        boolean canRemap
    ) {
        CacheOperationContext opCtx = ctx.operationContextPerCall();

        subjId = ctx.subjectIdPerCall(subjId, opCtx);

        return getAllAsync(keys,
            opCtx == null || !opCtx.skipStore(),
            entry,
            !skipTx,
            subjId,
            taskName,
            deserializePortable,
            forcePrimary,
            skipVals ? null : expiryPolicy(opCtx != null ? opCtx.expiry() : null),
            skipVals,
            canRemap);
    }

    /**
     * @param keys Keys.
     * @param readThrough Read through.
     * @param cached Cached.
     * @param checkTx Check tx.
     * @param subjId Subj Id.
     * @param taskName Task name.
     * @param deserializePortable Deserialize portable.
     * @param forcePrimary Froce primary.
     * @param expiry Expiry policy.
     * @param skipVals Skip values.
     * @return Future for the get operation.
     * @see GridCacheAdapter#getAllAsync(Collection)
     */
    public IgniteInternalFuture<Map<K, V>> getAllAsync(@Nullable final Collection<? extends K> keys,
        boolean readThrough,
        @Nullable GridCacheEntryEx cached,
        boolean checkTx,
        @Nullable final UUID subjId,
        final String taskName,
        final boolean deserializePortable,
        final boolean forcePrimary,
        @Nullable IgniteCacheExpiryPolicy expiry,
        final boolean skipVals,
        boolean canRemap
    ) {
        ctx.checkSecurity(SecurityPermission.CACHE_READ);

        if (keyCheck)
            validateCacheKeys(keys);

        return getAllAsync0(ctx.cacheKeysView(keys),
            readThrough,
            checkTx,
            subjId,
            taskName,
            deserializePortable,
            expiry,
            skipVals,
            false,
            canRemap);
    }

    /**
     * @param keys Keys.
     * @param readThrough Read-through flag.
     * @param checkTx Check local transaction flag.
     * @param subjId Subject ID.
     * @param taskName Task name/
     * @param deserializePortable Deserialize portable flag.
     * @param expiry Expiry policy.
     * @param skipVals Skip values flag.
     * @param keepCacheObjects Keep cache objects
     * @return Future.
     */
    public <K1, V1> IgniteInternalFuture<Map<K1, V1>> getAllAsync0(@Nullable final Collection<KeyCacheObject> keys,
        final boolean readThrough,
        boolean checkTx,
        @Nullable final UUID subjId,
        final String taskName,
        final boolean deserializePortable,
        @Nullable IgniteCacheExpiryPolicy expiry,
        final boolean skipVals,
        final boolean keepCacheObjects,
        boolean canRemap
        ) {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(Collections.<K1, V1>emptyMap());

        IgniteTxLocalAdapter tx = null;

        if (checkTx) {
            try {
                checkJta();
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(e);
            }

            tx = ctx.tm().threadLocalTx(ctx.systemTx() ? ctx : null);
        }

        if (tx == null || tx.implicit()) {
            try {
                assert keys != null;

                final AffinityTopologyVersion topVer = tx == null
                    ? (canRemap ? ctx.affinity().affinityTopologyVersion(): ctx.shared().exchange().readyAffinityVersion())
                    : tx.topologyVersion();

                final Map<K1, V1> map = new GridLeanMap<>(keys.size());

                Map<KeyCacheObject, GridCacheVersion> misses = null;

                for (KeyCacheObject key : keys) {
                    while (true) {
                        GridCacheEntryEx entry = entryEx(key);

                        try {
                            CacheObject val = entry.innerGet(null,
                                ctx.isSwapOrOffheapEnabled(),
                                /*don't read-through*/false,
                                /*fail-fast*/true,
                                /*unmarshal*/true,
                                /*update-metrics*/!skipVals,
                                /*event*/!skipVals,
                                /*temporary*/false,
                                subjId,
                                null,
                                taskName,
                                expiry);

                            if (val == null) {
                                GridCacheVersion ver = entry.version();

                                if (misses == null)
                                    misses = new GridLeanMap<>();

                                misses.put(key, ver);
                            }
                            else {
                                ctx.addResult(map, key, val, skipVals, keepCacheObjects, deserializePortable, true);

                                if (tx == null || (!tx.implicit() && tx.isolation() == READ_COMMITTED))
                                    ctx.evicts().touch(entry, topVer);

                                if (keys.size() == 1)
                                    // Safe to return because no locks are required in READ_COMMITTED mode.
                                    return new GridFinishedFuture<>(map);
                            }

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignored) {
                            if (log.isDebugEnabled())
                                log.debug("Got removed entry in getAllAsync(..) method (will retry): " + key);
                        }
                        catch (GridCacheFilterFailedException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Filter validation failed for entry: " + entry);

                            if (tx == null || (!tx.implicit() && tx.isolation() == READ_COMMITTED))
                                ctx.evicts().touch(entry, topVer);

                            break; // While loop.
                        }
                    }
                }

                if (!skipVals && misses != null && readThrough && ctx.readThrough()) {
                    final Map<KeyCacheObject, GridCacheVersion> loadKeys = misses;

                    final IgniteTxLocalAdapter tx0 = tx;

                    final Collection<KeyCacheObject> loaded = new HashSet<>();

                    return new GridEmbeddedFuture(
                        ctx.closures().callLocalSafe(ctx.projectSafe(new GPC<Map<K1, V1>>() {
                            @Override public Map<K1, V1> call() throws Exception {
                                ctx.store().loadAll(null/*tx*/, loadKeys.keySet(), new CI2<KeyCacheObject, Object>() {
                                    /** New version for all new entries. */
                                    private GridCacheVersion nextVer;

                                    @Override public void apply(KeyCacheObject key, Object val) {
                                        GridCacheVersion ver = loadKeys.get(key);

                                        if (ver == null) {
                                            if (log.isDebugEnabled())
                                                log.debug("Value from storage was never asked for [key=" + key +
                                                    ", val=" + val + ']');

                                            return;
                                        }

                                        // Initialize next version.
                                        if (nextVer == null)
                                            nextVer = ctx.versions().next();

                                        loaded.add(key);

                                        CacheObject cacheVal = ctx.toCacheObject(val);

                                        while (true) {
                                            GridCacheEntryEx entry = entryEx(key);

                                            try {
                                                boolean set = entry.versionedValue(cacheVal, ver, nextVer);

                                                if (log.isDebugEnabled())
                                                    log.debug("Set value loaded from store into entry [set=" + set +
                                                        ", curVer=" + ver + ", newVer=" + nextVer + ", " +
                                                        "entry=" + entry + ']');

                                                // Don't put key-value pair into result map if value is null.
                                                if (val != null) {
                                                    ctx.addResult(map,
                                                        key,
                                                        cacheVal,
                                                        skipVals,
                                                        keepCacheObjects,
                                                        deserializePortable,
                                                        false);
                                                }

                                                if (tx0 == null || (!tx0.implicit() &&
                                                    tx0.isolation() == READ_COMMITTED))
                                                    ctx.evicts().touch(entry, topVer);

                                                break;
                                            }
                                            catch (GridCacheEntryRemovedException ignore) {
                                                if (log.isDebugEnabled())
                                                    log.debug("Got removed entry during getAllAsync (will retry): " +
                                                        entry);
                                            }
                                            catch (IgniteCheckedException e) {
                                                // Wrap errors (will be unwrapped).
                                                throw new GridClosureException(e);
                                            }
                                        }
                                    }
                                });

                                if (loaded.size() != loadKeys.size()) {
                                    for (KeyCacheObject key : loadKeys.keySet()) {
                                        if (loaded.contains(key))
                                            continue;

                                        if (tx0 == null || (!tx0.implicit() &&
                                            tx0.isolation() == READ_COMMITTED)) {
                                            GridCacheEntryEx entry = peekEx(key);

                                            if (entry != null)
                                                ctx.evicts().touch(entry, topVer);
                                        }
                                    }
                                }

                                return map;
                            }
                        }), true),
                        new C2<Map<K, V>, Exception, IgniteInternalFuture<Map<K, V>>>() {
                            @Override public IgniteInternalFuture<Map<K, V>> apply(Map<K, V> map, Exception e) {
                                if (e != null)
                                    return new GridFinishedFuture<>(e);

                                if (tx0 == null || (!tx0.implicit() && tx0.isolation() == READ_COMMITTED)) {
                                    Collection<KeyCacheObject> notFound = new HashSet<>(loadKeys.keySet());

                                    notFound.removeAll(loaded);

                                    // Touch entries that were not found in store.
                                    for (KeyCacheObject key : notFound) {
                                        GridCacheEntryEx entry = peekEx(key);

                                        if (entry != null)
                                            ctx.evicts().touch(entry, topVer);
                                    }
                                }

                                // There were no misses.
                                return new GridFinishedFuture<>(Collections.<K,
                                    V>emptyMap());
                            }
                        },
                        new C2<Map<K1, V1>, Exception, Map<K1, V1>>() {
                            @Override public Map<K1, V1> apply(Map<K1, V1> loaded, Exception e) {
                                if (e == null)
                                    map.putAll(loaded);

                                return map;
                            }
                        }
                    );
                }
                else {
                    // If misses is not empty and store is disabled, we should touch missed entries.
                    if (misses != null) {
                        for (KeyCacheObject key : misses.keySet()) {
                            GridCacheEntryEx entry = peekEx(key);

                            if (entry != null)
                                ctx.evicts().touch(entry, topVer);
                        }
                    }
                }

                return new GridFinishedFuture<>(map);
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(e);
            }
        }
        else {
            return asyncOp(tx, new AsyncOp<Map<K1, V1>>(keys) {
                @Override public IgniteInternalFuture<Map<K1, V1>> op(IgniteTxLocalAdapter tx) {
                    return tx.getAllAsync(ctx, keys, null, deserializePortable, skipVals, false, !readThrough);
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val) throws IgniteCheckedException {
        return getAndPut(key, val, CU.empty0());
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param filter Optional filter.
     * @return Previous value.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public V getAndPut(final K key, final V val, @Nullable final CacheEntryPredicate[] filter)
        throws IgniteCheckedException
    {
        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        V prevVal = syncOp(new SyncOp<V>(true) {
            @Override public V op(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                return (V)tx.putAllAsync(ctx, F.t(key, val), true, null, -1, filter).get().value();
            }

            @Override public String toString() {
                return "put [key=" + key + ", val=" + val + ", filter=" + Arrays.toString(filter) + ']';
            }
        });

        if (statsEnabled)
            metrics0().addPutAndGetTimeNanos(System.nanoTime() - start);

        return prevVal;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAndPutAsync(K key, V val) {
        return getAndPutAsync(key, val, CU.empty0());
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param filter Filter.
     * @return Put operation future.
     */
    public IgniteInternalFuture<V> getAndPutAsync(K key, V val, @Nullable CacheEntryPredicate[] filter) {
        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        IgniteInternalFuture<V> fut = getAndPutAsync0(key, val, filter);

        if (statsEnabled)
            fut.listen(new UpdatePutAndGetTimeStatClosure<V>(metrics0(), start));

        return fut;
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param filter Optional filter.
     * @return Put operation future.
     */
    public IgniteInternalFuture<V> getAndPutAsync0(final K key, final V val,
        @Nullable final CacheEntryPredicate... filter) {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        return asyncOp(new AsyncOp<V>() {
            @Override public IgniteInternalFuture<V> op(IgniteTxLocalAdapter tx) {
                return tx.putAllAsync(ctx, F.t(key, val), true, null, -1, filter)
                    .chain((IgniteClosure<IgniteInternalFuture<GridCacheReturn>, V>)RET2VAL);
            }

            @Override public String toString() {
                return "putAsync [key=" + key + ", val=" + val + ", filter=" + Arrays.toString(filter) + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean put(final K key, final V val) throws IgniteCheckedException {
        return put(key, val, CU.empty0());
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param filter Filter.
     * @return {@code True} if optional filter passed and value was stored in cache,
     *      {@code false} otherwise. Note that this method will return {@code true} if filter is not
     *      specified.
     * @throws IgniteCheckedException If put operation failed.
     */
    public boolean put(final K key, final V val, final CacheEntryPredicate[] filter)
        throws IgniteCheckedException {
        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        Boolean stored = syncOp(new SyncOp<Boolean>(true) {
            @Override public Boolean op(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                return tx.putAllAsync(ctx, F.t(key, val), false, null, -1, filter).get().success();
            }

            @Override public String toString() {
                return "putx [key=" + key + ", val=" + val + ", filter=" + Arrays.toString(filter) + ']';
            }
        });

        if (statsEnabled)
            metrics0().addPutTimeNanos(System.nanoTime() - start);

        return stored;
    }

    /** {@inheritDoc} */
    @Override public void putAllConflict(final Map<KeyCacheObject, GridCacheDrInfo> drMap)
        throws IgniteCheckedException {
        if (F.isEmpty(drMap))
            return;

        ctx.dr().onReceiveCacheEntriesReceived(drMap.size());

        syncOp(new SyncInOp(drMap.size() == 1) {
            @Override public void inOp(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                tx.putAllDrAsync(ctx, drMap).get();
            }

            @Override public String toString() {
                return "putAllConflict [drMap=" + drMap + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllConflictAsync(final Map<KeyCacheObject, GridCacheDrInfo> drMap)
        throws IgniteCheckedException {
        if (F.isEmpty(drMap))
            return new GridFinishedFuture<Object>();

        ctx.dr().onReceiveCacheEntriesReceived(drMap.size());

        return asyncOp(new AsyncInOp(drMap.keySet()) {
            @Override public IgniteInternalFuture<?> inOp(IgniteTxLocalAdapter tx) {
                return tx.putAllDrAsync(ctx, drMap);
            }

            @Override public String toString() {
                return "putAllConflictAsync [drMap=" + drMap + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <T> EntryProcessorResult<T> invoke(final K key,
        final EntryProcessor<K, V, T> entryProcessor,
        final Object... args)
        throws IgniteCheckedException {
        A.notNull(key, "key", entryProcessor, "entryProcessor");

        if (keyCheck)
            validateCacheKey(key);

        return syncOp(new SyncOp<EntryProcessorResult<T>>(true) {
            @Nullable @Override public EntryProcessorResult<T> op(IgniteTxLocalAdapter tx)
                throws IgniteCheckedException {
                Map<? extends K, EntryProcessor<K, V, Object>> invokeMap =
                    Collections.singletonMap(key, (EntryProcessor<K, V, Object>) entryProcessor);

                IgniteInternalFuture<GridCacheReturn> fut = tx.invokeAsync(ctx, invokeMap, args);

                Map<K, EntryProcessorResult<T>> resMap = fut.get().value();

                EntryProcessorResult<T> res = null;

                if (resMap != null) {
                    assert resMap.isEmpty() || resMap.size() == 1 : resMap.size();

                    res = resMap.isEmpty() ? null : resMap.values().iterator().next();
                }

                return res != null ? res : new CacheInvokeResult();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(final Set<? extends K> keys,
        final EntryProcessor<K, V, T> entryProcessor,
        final Object... args) throws IgniteCheckedException {
        A.notNull(keys, "keys", entryProcessor, "entryProcessor");

        if (keyCheck)
            validateCacheKeys(keys);

        return syncOp(new SyncOp<Map<K, EntryProcessorResult<T>>>(keys.size() == 1) {
            @Nullable @Override public Map<K, EntryProcessorResult<T>> op(IgniteTxLocalAdapter tx)
                throws IgniteCheckedException {
                Map<? extends K, EntryProcessor<K, V, Object>> invokeMap = F.viewAsMap(keys,
                    new C1<K, EntryProcessor<K, V, Object>>() {
                            @Override public EntryProcessor apply(K k) {
                            return entryProcessor;
                        }
                    });

                IgniteInternalFuture<GridCacheReturn> fut = tx.invokeAsync(ctx, invokeMap, args);

                Map<K, EntryProcessorResult<T>> res = fut.get().value();

                return res != null ? res : Collections.<K, EntryProcessorResult<T>>emptyMap();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<EntryProcessorResult<T>> invokeAsync(
        final K key,
        final EntryProcessor<K, V, T> entryProcessor,
        final Object... args)
        throws EntryProcessorException {
        A.notNull(key, "key", entryProcessor, "entryProcessor");

        if (keyCheck)
            validateCacheKey(key);

        IgniteInternalFuture<?> fut = asyncOp(new AsyncInOp() {
            @Override public IgniteInternalFuture<GridCacheReturn> inOp(IgniteTxLocalAdapter tx) {
                Map<? extends K, EntryProcessor<K, V, Object>> invokeMap =
                    Collections.singletonMap(key, (EntryProcessor<K, V, Object>) entryProcessor);

                return tx.invokeAsync(ctx, invokeMap, args);
            }

            @Override public String toString() {
                return "invokeAsync [key=" + key + ", entryProcessor=" + entryProcessor + ']';
            }
        });

        IgniteInternalFuture<GridCacheReturn> fut0 = (IgniteInternalFuture<GridCacheReturn>)fut;

        return fut0.chain(new CX1<IgniteInternalFuture<GridCacheReturn>, EntryProcessorResult<T>>() {
            @Override public EntryProcessorResult<T> applyx(IgniteInternalFuture<GridCacheReturn> fut)
                throws IgniteCheckedException {
                GridCacheReturn ret = fut.get();

                Map<K, EntryProcessorResult<T>> resMap = ret.value();

                if (resMap != null) {
                    assert resMap.isEmpty() || resMap.size() == 1 : resMap.size();

                    return resMap.isEmpty() ? null : resMap.values().iterator().next();
                }

                return null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        final Set<? extends K> keys,
        final EntryProcessor<K, V, T> entryProcessor,
        final Object... args) {
        A.notNull(keys, "keys", entryProcessor, "entryProcessor");

        if (keyCheck)
            validateCacheKeys(keys);

        IgniteInternalFuture<?> fut = asyncOp(new AsyncInOp(keys) {
            @Override public IgniteInternalFuture<GridCacheReturn> inOp(IgniteTxLocalAdapter tx) {
                Map<? extends K, EntryProcessor<K, V, Object>> invokeMap = F.viewAsMap(keys, new C1<K, EntryProcessor<K, V, Object>>() {
                    @Override public EntryProcessor apply(K k) {
                        return entryProcessor;
                    }
                });

                return tx.invokeAsync(ctx, invokeMap, args);
            }

            @Override public String toString() {
                return "invokeAllAsync [keys=" + keys + ", entryProcessor=" + entryProcessor + ']';
            }
        });

        IgniteInternalFuture<GridCacheReturn> fut0 =
            (IgniteInternalFuture<GridCacheReturn>)fut;

        return fut0.chain(new CX1<IgniteInternalFuture<GridCacheReturn>, Map<K, EntryProcessorResult<T>>>() {
            @Override public Map<K, EntryProcessorResult<T>> applyx(IgniteInternalFuture<GridCacheReturn> fut)
                throws IgniteCheckedException {
                GridCacheReturn ret = fut.get();

                assert ret != null;

                return ret.value() != null ? ret.<Map<K, EntryProcessorResult<T>>>value() : Collections.<K, EntryProcessorResult<T>>emptyMap();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        final Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        final Object... args) {
        A.notNull(map, "map");

        if (keyCheck)
            validateCacheKeys(map.keySet());

        IgniteInternalFuture<?> fut = asyncOp(new AsyncInOp(map.keySet()) {
            @Override public IgniteInternalFuture<GridCacheReturn> inOp(IgniteTxLocalAdapter tx) {
                return tx.invokeAsync(ctx, (Map<? extends K, ? extends EntryProcessor<K, V, Object>>)map, args);
            }

            @Override public String toString() {
                return "invokeAllAsync [map=" + map + ']';
            }
        });

        IgniteInternalFuture<GridCacheReturn> fut0 = (IgniteInternalFuture<GridCacheReturn>)fut;

        return fut0.chain(new CX1<IgniteInternalFuture<GridCacheReturn>, Map<K, EntryProcessorResult<T>>>() {
            @Override public Map<K, EntryProcessorResult<T>> applyx(IgniteInternalFuture<GridCacheReturn> fut)
                throws IgniteCheckedException {
                GridCacheReturn ret = fut.get();

                assert ret != null;

                return ret.value() != null ? ret.<Map<K, EntryProcessorResult<T>>>value() : Collections.<K, EntryProcessorResult<T>>emptyMap();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        final Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        final Object... args) throws IgniteCheckedException {
        A.notNull(map, "map");

        if (keyCheck)
            validateCacheKeys(map.keySet());

        return syncOp(new SyncOp<Map<K, EntryProcessorResult<T>>>(map.size() == 1) {
            @Nullable @Override public Map<K, EntryProcessorResult<T>> op(IgniteTxLocalAdapter tx)
                throws IgniteCheckedException {
                IgniteInternalFuture<GridCacheReturn> fut =
                    tx.invokeAsync(ctx, (Map<? extends K, ? extends EntryProcessor<K, V, Object>>)map, args);

                return fut.get().value();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putAsync(K key, V val) {
        return putAsync(key, val, CU.empty0());
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param filter Filter.
     * @return Put future.
     */
    public IgniteInternalFuture<Boolean> putAsync(K key, V val, @Nullable CacheEntryPredicate... filter) {
        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        IgniteInternalFuture<Boolean> fut = putAsync0(key, val, filter);

        if (statsEnabled)
            fut.listen(new UpdatePutTimeStatClosure<Boolean>(metrics0(), start));

        return fut;
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param filter Optional filter.
     * @return Putx operation future.
     */
    public IgniteInternalFuture<Boolean> putAsync0(final K key, final V val,
        @Nullable final CacheEntryPredicate... filter) {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        return asyncOp(new AsyncOp<Boolean>() {
            @Override public IgniteInternalFuture<Boolean> op(IgniteTxLocalAdapter tx) {
                return tx.putAllAsync(ctx, F.t(key, val), false, null, -1, filter).chain(
                    (IgniteClosure<IgniteInternalFuture<GridCacheReturn>, Boolean>) RET2FLAG);
            }

            @Override public String toString() {
                return "putxAsync [key=" + key + ", val=" + val + ", filter=" + Arrays.toString(filter) + ']';
            }
        });
    }

    /**
     * Tries to put value in cache. Will fail with {@link GridCacheTryPutFailedException}
     * if topology exchange is in progress.
     *
     * @param key Key.
     * @param val value.
     * @return Old value.
     * @throws IgniteCheckedException In case of error.
     */
    @Nullable public V tryPutIfAbsent(K key, V val) throws IgniteCheckedException {
        // Supported only in ATOMIC cache.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndPutIfAbsent(final K key, final V val) throws IgniteCheckedException {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        return syncOp(new SyncOp<V>(true) {
            @Override public V op(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                return (V)tx.putAllAsync(ctx, F.t(key, val), true, null, -1, ctx.noValArray()).get().value();
            }

            @Override public String toString() {
                return "putIfAbsent [key=" + key + ", val=" + val + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAndPutIfAbsentAsync(final K key, final V val) {
        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        IgniteInternalFuture<V> fut = asyncOp(new AsyncOp<V>() {
            @Override public IgniteInternalFuture<V> op(IgniteTxLocalAdapter tx) {
                return tx.putAllAsync(ctx, F.t(key, val), true, null, -1, ctx.noValArray())
                    .chain((IgniteClosure<IgniteInternalFuture<GridCacheReturn>, V>) RET2VAL);
            }

            @Override public String toString() {
                return "putIfAbsentAsync [key=" + key + ", val=" + val + ']';
            }
        });

        if(statsEnabled)
            fut.listen(new UpdatePutTimeStatClosure<V>(metrics0(), start));

        return fut;
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(final K key, final V val) throws IgniteCheckedException {
        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        Boolean stored = syncOp(new SyncOp<Boolean>(true) {
            @Override public Boolean op(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                return tx.putAllAsync(ctx, F.t(key, val), false, null, -1, ctx.noValArray()).get().success();
            }

            @Override public String toString() {
                return "putxIfAbsent [key=" + key + ", val=" + val + ']';
            }
        });

        if (statsEnabled && stored)
            metrics0().addPutTimeNanos(System.nanoTime() - start);

        return stored;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> putIfAbsentAsync(final K key, final V val) {
        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        IgniteInternalFuture<Boolean> fut = asyncOp(new AsyncOp<Boolean>() {
            @Override public IgniteInternalFuture<Boolean> op(IgniteTxLocalAdapter tx) {
                return tx.putAllAsync(ctx, F.t(key, val), false, null, -1, ctx.noValArray()).chain(
                    (IgniteClosure<IgniteInternalFuture<GridCacheReturn>, Boolean>)RET2FLAG);
            }

            @Override public String toString() {
                return "putxIfAbsentAsync [key=" + key + ", val=" + val + ']';
            }
        });

        if (statsEnabled)
            fut.listen(new UpdatePutTimeStatClosure<Boolean>(metrics0(), start));

        return fut;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndReplace(final K key, final V val) throws IgniteCheckedException {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        return syncOp(new SyncOp<V>(true) {
            @Override public V op(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                return (V)tx.putAllAsync(ctx, F.t(key, val), true, null, -1, ctx.hasValArray()).get().value();
            }

            @Override public String toString() {
                return "replace [key=" + key + ", val=" + val + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAndReplaceAsync(final K key, final V val) {
        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        IgniteInternalFuture<V> fut = asyncOp(new AsyncOp<V>() {
            @Override public IgniteInternalFuture<V> op(IgniteTxLocalAdapter tx) {
                return tx.putAllAsync(ctx, F.t(key, val), true, null, -1, ctx.hasValArray()).chain(
                    (IgniteClosure<IgniteInternalFuture<GridCacheReturn>, V>)RET2VAL);
            }

            @Override public String toString() {
                return "replaceAsync [key=" + key + ", val=" + val + ']';
            }
        });

        if (statsEnabled)
            fut.listen(new UpdatePutAndGetTimeStatClosure<V>(metrics0(), start));

        return fut;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(final K key, final V val) throws IgniteCheckedException {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        return syncOp(new SyncOp<Boolean>(true) {
            @Override public Boolean op(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                return tx.putAllAsync(ctx, F.t(key, val), false, null, -1, ctx.hasValArray()).get().success();
            }

            @Override public String toString() {
                return "replacex [key=" + key + ", val=" + val + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(final K key, final V val) {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        return asyncOp(new AsyncOp<Boolean>() {
            @Override public IgniteInternalFuture<Boolean> op(IgniteTxLocalAdapter tx) {
                return tx.putAllAsync(ctx, F.t(key, val), false, null, -1, ctx.hasValArray()).chain(
                    (IgniteClosure<IgniteInternalFuture<GridCacheReturn>, Boolean>) RET2FLAG);
            }

            @Override public String toString() {
                return "replacexAsync [key=" + key + ", val=" + val + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean replace(final K key, final V oldVal, final V newVal) throws IgniteCheckedException {
        A.notNull(key, "key", oldVal, "oldVal", newVal, "newVal");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(oldVal);

        validateCacheValue(newVal);

        return syncOp(new SyncOp<Boolean>(true) {
            @Override public Boolean op(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                // Register before hiding in the filter.
                if (ctx.deploymentEnabled())
                    ctx.deploy().registerClass(oldVal);

                return tx.putAllAsync(ctx, F.t(key, newVal), false, null, -1, ctx.equalsValArray(oldVal)).get()
                    .success();
            }

            @Override public String toString() {
                return "replace [key=" + key + ", oldVal=" + oldVal + ", newVal=" + newVal + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(final K key, final V oldVal, final V newVal) {
        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key", oldVal, "oldVal", newVal, "newVal");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(oldVal);

        validateCacheValue(newVal);

        IgniteInternalFuture<Boolean> fut = asyncOp(new AsyncOp<Boolean>() {
            @Override public IgniteInternalFuture<Boolean> op(IgniteTxLocalAdapter tx) {
                // Register before hiding in the filter.
                if (ctx.deploymentEnabled()) {
                    try {
                        ctx.deploy().registerClass(oldVal);
                    }
                    catch (IgniteCheckedException e) {
                        return new GridFinishedFuture<>(e);
                    }
                }

                return tx.putAllAsync(ctx, F.t(key, newVal), false, null, -1, ctx.equalsValArray(oldVal)).chain(
                    (IgniteClosure<IgniteInternalFuture<GridCacheReturn>, Boolean>)RET2FLAG);
            }

            @Override public String toString() {
                return "replaceAsync [key=" + key + ", oldVal=" + oldVal + ", newVal=" + newVal + ']';
            }
        });

        if (statsEnabled)
            fut.listen(new UpdatePutAndGetTimeStatClosure<Boolean>(metrics0(), start));

        return fut;
    }

    /** {@inheritDoc} */
    @Override public void putAll(@Nullable final Map<? extends K, ? extends V> m) throws IgniteCheckedException {
        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        if (F.isEmpty(m))
            return;

        if (keyCheck)
            validateCacheKeys(m.keySet());

        validateCacheValues(m.values());

        syncOp(new SyncInOp(m.size() == 1) {
            @Override public void inOp(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                tx.putAllAsync(ctx, m, false, null, -1, CU.empty0()).get();
            }

            @Override public String toString() {
                return "putAll [map=" + m + ']';
            }
        });

        if (statsEnabled)
            metrics0().addPutTimeNanos(System.nanoTime() - start);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllAsync(final Map<? extends K, ? extends V> m) {
        if (F.isEmpty(m))
            return new GridFinishedFuture<Object>();

        if (keyCheck)
            validateCacheKeys(m.keySet());

        validateCacheValues(m.values());

        return asyncOp(new AsyncInOp(m.keySet()) {
            @Override public IgniteInternalFuture<?> inOp(IgniteTxLocalAdapter tx) {
                return tx.putAllAsync(ctx, m, false, null, -1, CU.empty0()).chain(RET2NULL);
            }

            @Override public String toString() {
                return "putAllAsync [map=" + m + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndRemove(final K key) throws IgniteCheckedException {
        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        V prevVal = syncOp(new SyncOp<V>(true) {
            @Override public V op(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                V ret = tx.removeAllAsync(ctx, Collections.singletonList(key), null, true, CU.empty0()).get().value();

                if (ctx.config().getInterceptor() != null)
                    return (V)ctx.config().getInterceptor().onBeforeRemove(new CacheEntryImpl(key, ret)).get2();

                return ret;
            }

            @Override public String toString() {
                return "remove [key=" + key + ']';
            }
        });

        if (statsEnabled)
            metrics0().addRemoveAndGetTimeNanos(System.nanoTime() - start);

        return prevVal;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAndRemoveAsync(final K key) {
        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        IgniteInternalFuture<V> fut = asyncOp(new AsyncOp<V>() {
            @Override public IgniteInternalFuture<V> op(IgniteTxLocalAdapter tx) {
                // TODO should we invoke interceptor here?
                return tx.removeAllAsync(ctx, Collections.singletonList(key), null, true, CU.empty0())
                    .chain((IgniteClosure<IgniteInternalFuture<GridCacheReturn>, V>) RET2VAL);
            }

            @Override public String toString() {
                return "removeAsync [key=" + key + ']';
            }
        });

        if (statsEnabled)
            fut.listen(new UpdateRemoveTimeStatClosure<V>(metrics0(), start));

        return fut;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void removeAll() throws IgniteCheckedException {
        assert ctx.isLocal();

        for (Iterator<KeyCacheObject> it = ctx.swap().offHeapKeyIterator(true, true, AffinityTopologyVersion.NONE);
             it.hasNext(); )
            remove((K)it.next());

        for (Iterator<KeyCacheObject> it = ctx.swap().swapKeyIterator(true, true, AffinityTopologyVersion.NONE);
             it.hasNext(); )
            remove((K)it.next());

        removeAll(keySet());
    }

    /** {@inheritDoc} */
    @Override public void removeAll(final Collection<? extends K> keys) throws IgniteCheckedException {
        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(keys, "keys");

        if (F.isEmpty(keys))
            return;

        if (keyCheck)
            validateCacheKeys(keys);

        syncOp(new SyncInOp(keys.size() == 1) {
            @Override public void inOp(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                tx.removeAllAsync(ctx, keys, null, false, CU.empty0()).get();
            }

            @Override public String toString() {
                return "removeAll [keys=" + keys + ']';
            }
        });

        if (statsEnabled)
            metrics0().addRemoveTimeNanos(System.nanoTime() - start);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync(@Nullable final Collection<? extends K> keys) {
        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        if (F.isEmpty(keys))
            return new GridFinishedFuture<Object>();

        if (keyCheck)
            validateCacheKeys(keys);

        IgniteInternalFuture<Object> fut = asyncOp(new AsyncInOp(keys) {
            @Override public IgniteInternalFuture<?> inOp(IgniteTxLocalAdapter tx) {
                return tx.removeAllAsync(ctx, keys, null, false, CU.empty0()).chain(RET2NULL);
            }

            @Override public String toString() {
                return "removeAllAsync [keys=" + keys + ']';
            }
        });

        if (statsEnabled)
            fut.listen(new UpdateRemoveTimeStatClosure<>(metrics0(), start));

        return fut;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(final K key) throws IgniteCheckedException {
        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        boolean rmv = syncOp(new SyncOp<Boolean>(true) {
            @Override public Boolean op(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                return tx.removeAllAsync(ctx, Collections.singletonList(key), null, false, CU.empty0()).get().success();
            }

            @Override public String toString() {
                return "removex [key=" + key + ']';
            }
        });

        if (statsEnabled && rmv)
            metrics0().addRemoveTimeNanos(System.nanoTime() - start);

        return rmv;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removeAsync(K key) {
        A.notNull(key, "key");

        return removeAsync(key, CU.empty0());
    }

    /**
     * @param key Key to remove.
     * @param filter Optional filter.
     * @return Putx operation future.
     */
    public IgniteInternalFuture<Boolean> removeAsync(final K key, @Nullable final CacheEntryPredicate... filter) {
        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        IgniteInternalFuture<Boolean> fut = asyncOp(new AsyncOp<Boolean>() {
            @Override public IgniteInternalFuture<Boolean> op(IgniteTxLocalAdapter tx) {
                return tx.removeAllAsync(ctx, Collections.singletonList(key), null, false, filter).chain(
                    (IgniteClosure<IgniteInternalFuture<GridCacheReturn>, Boolean>)RET2FLAG);
            }

            @Override public String toString() {
                return "removeAsync [key=" + key + ", filter=" + Arrays.toString(filter) + ']';
            }
        });

        if (statsEnabled)
            fut.listen(new UpdateRemoveTimeStatClosure<Boolean>(metrics0(), start));

        return fut;
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn removex(final K key, final V val) throws IgniteCheckedException {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        return syncOp(new SyncOp<GridCacheReturn>(true) {
            @Override public GridCacheReturn op(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                // Register before hiding in the filter.
                if (ctx.deploymentEnabled())
                    ctx.deploy().registerClass(val);

                return tx.removeAllAsync(ctx,
                    Collections.singletonList(key),
                    null,
                    true,
                    ctx.equalsValArray(val)).get();
            }

            @Override public String toString() {
                return "remove [key=" + key + ", val=" + val + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void removeAllConflict(final Map<KeyCacheObject, GridCacheVersion> drMap)
        throws IgniteCheckedException {
        if (F.isEmpty(drMap))
            return;

        ctx.dr().onReceiveCacheEntriesReceived(drMap.size());

        syncOp(new SyncInOp(false) {
            @Override public void inOp(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                tx.removeAllDrAsync(ctx, drMap).get();
            }

            @Override public String toString() {
                return "removeAllConflict [drMap=" + drMap + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllConflictAsync(final Map<KeyCacheObject, GridCacheVersion> drMap)
        throws IgniteCheckedException {
        if (F.isEmpty(drMap))
            return new GridFinishedFuture<Object>();

        ctx.dr().onReceiveCacheEntriesReceived(drMap.size());

        return asyncOp(new AsyncInOp(drMap.keySet()) {
            @Override public IgniteInternalFuture<?> inOp(IgniteTxLocalAdapter tx) {
                return tx.removeAllDrAsync(ctx, drMap);
            }

            @Override public String toString() {
                return "removeAllDrASync [drMap=" + drMap + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn replacex(final K key, final V oldVal, final V newVal)
        throws IgniteCheckedException
    {
        A.notNull(key, "key", oldVal, "oldVal", newVal, "newVal");

        if (keyCheck)
            validateCacheKey(key);

        return syncOp(new SyncOp<GridCacheReturn>(true) {
            @Override public GridCacheReturn op(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                // Register before hiding in the filter.
                if (ctx.deploymentEnabled())
                    ctx.deploy().registerClass(oldVal);

                return tx.putAllAsync(ctx,
                        F.t(key, newVal),
                        true,
                        null,
                        -1,
                        ctx.equalsValArray(oldVal)).get();
            }

            @Override public String toString() {
                return "replace [key=" + key + ", oldVal=" + oldVal + ", newVal=" + newVal + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridCacheReturn> removexAsync(final K key, final V val) {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        return asyncOp(new AsyncOp<GridCacheReturn>() {
            @Override public IgniteInternalFuture<GridCacheReturn> op(IgniteTxLocalAdapter tx) {
                // Register before hiding in the filter.
                try {
                    if (ctx.deploymentEnabled())
                        ctx.deploy().registerClass(val);
                }
                catch (IgniteCheckedException e) {
                    return new GridFinishedFuture<>(e);
                }

                IgniteInternalFuture<GridCacheReturn> fut = (IgniteInternalFuture)tx.removeAllAsync(ctx,
                    Collections.singletonList(key),
                    null,
                    true,
                    ctx.equalsValArray(val));

                return fut;
            }

            @Override public String toString() {
                return "removeAsync [key=" + key + ", val=" + val + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridCacheReturn> replacexAsync(final K key, final V oldVal, final V newVal)
    {
        A.notNull(key, "key", oldVal, "oldVal", newVal, "newVal");

        if (keyCheck)
            validateCacheKey(key);

        return asyncOp(new AsyncOp<GridCacheReturn>() {
            @Override public IgniteInternalFuture<GridCacheReturn> op(IgniteTxLocalAdapter tx) {
                // Register before hiding in the filter.
                try {
                    if (ctx.deploymentEnabled())
                        ctx.deploy().registerClass(oldVal);
                }
                catch (IgniteCheckedException e) {
                    return new GridFinishedFuture<>(e);
                }

                IgniteInternalFuture<GridCacheReturn> fut = (IgniteInternalFuture)tx.putAllAsync(ctx,
                    F.t(key, newVal),
                    true,
                    null,
                    -1,
                    ctx.equalsValArray(oldVal));

                return fut;
            }

            @Override public String toString() {
                return "replaceAsync [key=" + key + ", oldVal=" + oldVal + ", newVal=" + newVal + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean remove(final K key, final V val) throws IgniteCheckedException {
        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        boolean rmv = syncOp(new SyncOp<Boolean>(true) {
            @Override public Boolean op(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                // Register before hiding in the filter.
                if (ctx.deploymentEnabled())
                    ctx.deploy().registerClass(val);

                return tx.removeAllAsync(ctx, Collections.singletonList(key), null, false,
                    ctx.equalsValArray(val)).get().success();
            }

            @Override public String toString() {
                return "remove [key=" + key + ", val=" + val + ']';
            }
        });

        if (statsEnabled && rmv)
            metrics0().addRemoveTimeNanos(System.nanoTime() - start);

        return rmv;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removeAsync(final K key, final V val) {
        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        IgniteInternalFuture<Boolean> fut = asyncOp(new AsyncOp<Boolean>() {
            @Override public IgniteInternalFuture<Boolean> op(IgniteTxLocalAdapter tx) {
                // Register before hiding in the filter.
                if (ctx.deploymentEnabled()) {
                    try {
                        ctx.deploy().registerClass(val);
                    }
                    catch (IgniteCheckedException e) {
                        return new GridFinishedFuture<>(e);
                    }
                }

                return tx.removeAllAsync(ctx, Collections.singletonList(key), null, false,
                    ctx.equalsValArray(val)).chain(
                    (IgniteClosure<IgniteInternalFuture<GridCacheReturn>, Boolean>)RET2FLAG);
            }

            @Override public String toString() {
                return "removeAsync [key=" + key + ", val=" + val + ']';
            }
        });

        if (statsEnabled)
            fut.listen(new UpdateRemoveTimeStatClosure<Boolean>(metrics0(), start));

        return fut;
    }

    /**
     * @param filter Filter.
     * @return Future.
     */
    public IgniteInternalFuture<?> localRemoveAll(final CacheEntryPredicate filter) {
        final Set<? extends K> keys = filter != null ? keySet(filter) : keySet();

        return asyncOp(new AsyncInOp(keys) {
            @Override public IgniteInternalFuture<?> inOp(IgniteTxLocalAdapter tx) {
                return tx.removeAllAsync(ctx, keys, null, false, null);
            }

            @Override public String toString() {
                return "removeAllAsync [filter=" + filter + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics() {
        return new CacheMetricsSnapshot(metrics);
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean mxBean() {
        return mxBean;
    }

    /**
     * @return Metrics.
     */
    public CacheMetricsImpl metrics0() {
        return metrics;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Transaction tx() {
        IgniteTxAdapter tx = ctx.tm().threadLocalTx(ctx);

        return tx == null ? null : new TransactionProxyImpl<>(tx, ctx.shared(), false);
    }

    /** {@inheritDoc} */
    @Override public boolean lock(K key, long timeout) throws IgniteCheckedException {
        A.notNull(key, "key");

        return lockAll(Collections.singletonList(key), timeout);
    }

    /** {@inheritDoc} */
    @Override public boolean lockAll(@Nullable Collection<? extends K> keys, long timeout)
        throws IgniteCheckedException {
        if (F.isEmpty(keys))
            return true;

        if (keyCheck)
            validateCacheKeys(keys);

        IgniteInternalFuture<Boolean> fut = lockAllAsync(keys, timeout);

        boolean isInterrupted = false;

        try {
            while (true) {
                try {
                    return fut.get();
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    // Interrupted status of current thread was cleared, retry to get lock.
                    isInterrupted = true;
                }
            }
        }
        finally {
            if (isInterrupted)
                Thread.currentThread().interrupt();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAsync(K key, long timeout) {
        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        return lockAllAsync(Collections.singletonList(key), timeout);
    }

    /** {@inheritDoc} */
    @Override public void unlock(K key)
        throws IgniteCheckedException {
        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        unlockAll(Collections.singletonList(key));
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked(K key) {
        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

        while (true) {
            try {
                GridCacheEntryEx entry = peekEx(cacheKey);

                return entry != null && entry.lockedByAny();
            }
            catch (GridCacheEntryRemovedException ignore) {
                // No-op.
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread(K key) {
        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        try {
            KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

            GridCacheEntryEx e = entry0(cacheKey, new AffinityTopologyVersion(ctx.discovery().topologyVersion()),
                false, false);

            if (e == null)
                return false;

            // Delegate to near if dht.
            if (e.isDht() && CU.isNearEnabled(ctx)) {
                IgniteInternalCache<K, V> near = ctx.isDht() ? ctx.dht().near() : ctx.near();

                return near.isLockedByThread(key) || e.lockedByThread();
            }

            return e.lockedByThread();
        }
        catch (GridCacheEntryRemovedException ignore) {
            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        A.notNull(concurrency, "concurrency");
        A.notNull(isolation, "isolation");

        TransactionConfiguration cfg = ctx.gridConfig().getTransactionConfiguration();

        return txStart(
            concurrency,
            isolation,
            cfg.getDefaultTxTimeout(),
            0
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalTx txStartEx(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        IgniteTransactionsEx txs = ctx.kernalContext().cache().transactions();

        return txs.txStartEx(ctx, concurrency, isolation);
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart(TransactionConcurrency concurrency,
        TransactionIsolation isolation, long timeout, int txSize) throws IllegalStateException {
        IgniteTransactionsEx txs = ctx.kernalContext().cache().transactions();

        return txs.txStartEx(ctx, concurrency, isolation, timeout, txSize).proxy();
    }

    /** {@inheritDoc} */
    @Override public long overflowSize() throws IgniteCheckedException {
        GridCacheSwapManager swapMgr = ctx.swap();

        return swapMgr != null ? swapMgr.swapSize() : -1;
    }

    /**
     * Checks if cache is working in JTA transaction and enlist cache as XAResource if necessary.
     *
     * @throws IgniteCheckedException In case of error.
     */
    protected void checkJta() throws IgniteCheckedException {
        ctx.jta().checkJta();
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(final IgniteBiPredicate<K, V> p, Object[] args)
        throws IgniteCheckedException {
        final boolean replicate = ctx.isDrEnabled();
        final AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        ExpiryPolicy plc0 = opCtx != null ? opCtx.expiry() : null;

        final ExpiryPolicy plc = plc0 != null ? plc0 : ctx.expiry();

        if (p != null)
            ctx.kernalContext().resource().injectGeneric(p);

        try {
            if (ctx.store().isLocal()) {
                DataStreamerImpl ldr = ctx.kernalContext().dataStream().dataStreamer(ctx.namex());

                try {
                    ldr.skipStore(true);

                    ldr.receiver(new IgniteDrDataStreamerCacheUpdater());

                    LocalStoreLoadClosure c = new LocalStoreLoadClosure(p, ldr, plc);

                    ctx.store().loadCache(c, args);

                    c.onDone();
                }
                finally {
                    ldr.closeEx(false);
                }
            }
            else {
                // Version for all loaded entries.
                final GridCacheVersion ver0 = ctx.versions().nextForLoad();

                ctx.store().loadCache(new CIX3<KeyCacheObject, Object, GridCacheVersion>() {
                    @Override public void applyx(KeyCacheObject key, Object val, @Nullable GridCacheVersion ver)
                        throws IgniteException {
                        assert ver == null;

                        long ttl = CU.ttlForLoad(plc);

                        if (ttl == CU.TTL_ZERO)
                            return;

                        loadEntry(key, val, ver0, (IgniteBiPredicate<Object, Object>) p, topVer, replicate, ttl);
                    }
                }, args);
            }
        }
        finally {
            if (p instanceof GridLoadCacheCloseablePredicate)
                ((GridLoadCacheCloseablePredicate)p).onClose();
        }
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param ver Cache version.
     * @param p Optional predicate.
     * @param topVer Topology version.
     * @param replicate Replication flag.
     * @param ttl TTL.
     */
    private void loadEntry(KeyCacheObject key,
        Object val,
        GridCacheVersion ver,
        @Nullable IgniteBiPredicate<Object, Object> p,
        AffinityTopologyVersion topVer,
        boolean replicate,
        long ttl) {
        if (p != null && !p.apply(key.value(ctx.cacheObjectContext(), false), val))
            return;

        CacheObject cacheVal = ctx.toCacheObject(val);

        GridCacheEntryEx entry = entryEx(key, false);

        try {
            entry.initialValue(cacheVal, ver, ttl, CU.EXPIRE_TIME_CALCULATE, false, topVer,
                replicate ? DR_LOAD : DR_NONE);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to put cache value: " + entry, e);
        }
        catch (GridCacheEntryRemovedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Got removed entry during loadCache (will ignore): " + entry);
        }
        finally {
            ctx.evicts().touch(entry, topVer);
        }

        CU.unwindEvicts(ctx);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> localLoadCacheAsync(final IgniteBiPredicate<K, V> p,
        final Object[] args)
    {
        return ctx.closures().callLocalSafe(
            ctx.projectSafe(new Callable<Object>() {
                @Nullable @Override public Object call() throws IgniteCheckedException {
                    localLoadCache(p, args);

                    return null;
                }
            }), true);
    }

    /**
     * @param keys Keys.
     * @param replaceExisting Replace existing values flag.
     * @return Load future.
     */
    public IgniteInternalFuture<?> loadAll(
        final Set<? extends K> keys,
        boolean replaceExisting
    ) {
        A.notNull(keys, "keys");

        for (Object key : keys)
            A.notNull(key, "key");

        if (!ctx.store().configured())
            return new GridFinishedFuture<>();

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        ExpiryPolicy plc = opCtx != null ? opCtx.expiry() : null;

        if (replaceExisting) {
            if (ctx.store().isLocal()) {
                Collection<ClusterNode> nodes = ctx.grid().cluster().forDataNodes(name()).nodes();

                if (nodes.isEmpty())
                    return new GridFinishedFuture<>();

                return ctx.closures().callAsyncNoFailover(BROADCAST,
                    new LoadKeysCallable<>(ctx.name(), keys, true, plc),
                    nodes,
                    true);
            }
            else {
                return ctx.closures().callLocalSafe(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        localLoadAndUpdate(keys);

                        return null;
                    }
                });
            }
        }
        else {
            Collection<ClusterNode> nodes = ctx.grid().cluster().forDataNodes(name()).nodes();

            if (nodes.isEmpty())
                return new GridFinishedFuture<>();

            return ctx.closures().callAsyncNoFailover(BROADCAST,
                new LoadKeysCallable<>(ctx.name(), keys, false, plc),
                nodes,
                true);
        }
    }

    /**
     * @param keys Keys.
     * @throws IgniteCheckedException If failed.
     */
    private void localLoadAndUpdate(final Collection<? extends K> keys) throws IgniteCheckedException {
        try (final DataStreamerImpl<KeyCacheObject, CacheObject> ldr =
                 ctx.kernalContext().<KeyCacheObject, CacheObject>dataStream().dataStreamer(ctx.namex())) {
            ldr.allowOverwrite(true);
            ldr.skipStore(true);

            final Collection<DataStreamerEntry> col = new ArrayList<>(ldr.perNodeBufferSize());

            Collection<KeyCacheObject> keys0 = ctx.cacheKeysView(keys);

            ctx.store().loadAll(null, keys0, new CIX2<KeyCacheObject, Object>() {
                @Override public void applyx(KeyCacheObject key, Object val) {
                    col.add(new DataStreamerEntry(key, ctx.toCacheObject(val)));

                    if (col.size() == ldr.perNodeBufferSize()) {
                        ldr.addDataInternal(col);

                        col.clear();
                    }
                }
            });

            if (!col.isEmpty())
                ldr.addData(col);
        }
    }

    /**
     * @param keys Keys to load.
     * @param plc Optional expiry policy.
     * @throws IgniteCheckedException If failed.
     */
    public void localLoad(Collection<? extends K> keys,
        @Nullable ExpiryPolicy plc)
        throws IgniteCheckedException
    {
        final boolean replicate = ctx.isDrEnabled();
        final AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

        final ExpiryPolicy plc0 = plc != null ? plc : ctx.expiry();

        Collection<KeyCacheObject> keys0 = ctx.cacheKeysView(keys);

        if (ctx.store().isLocal()) {
            DataStreamerImpl ldr = ctx.kernalContext().dataStream().dataStreamer(ctx.namex());

            try {
                ldr.skipStore(true);

                ldr.receiver(new IgniteDrDataStreamerCacheUpdater());

                LocalStoreLoadClosure c = new LocalStoreLoadClosure(null, ldr, plc0);

                ctx.store().localStoreLoadAll(null, keys0, c);

                c.onDone();
            }
            finally {
                ldr.closeEx(false);
            }
        }
        else {
            // Version for all loaded entries.
            final GridCacheVersion ver0 = ctx.versions().nextForLoad();

            ctx.store().loadAll(null, keys0, new CI2<KeyCacheObject, Object>() {
                @Override public void apply(KeyCacheObject key, Object val) {
                    long ttl = CU.ttlForLoad(plc0);

                    if (ttl == CU.TTL_ZERO)
                        return;

                    loadEntry(key, val, ver0, null, topVer, replicate, ttl);
                }
            });
        }
    }

    /**
     * @param p Predicate.
     * @param args Arguments.
     * @throws IgniteCheckedException If failed.
     */
    void globalLoadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) throws IgniteCheckedException {
        globalLoadCacheAsync(p, args).get();
    }

    /**
     * @param p Predicate.
     * @param args Arguments.
     * @throws IgniteCheckedException If failed.
     * @return Load cache future.
     */
    IgniteInternalFuture<?> globalLoadCacheAsync(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args)
        throws IgniteCheckedException {
        ClusterGroup nodes = ctx.kernalContext().grid().cluster().forCacheNodes(ctx.name());

        ctx.kernalContext().task().setThreadContext(TC_NO_FAILOVER, true);

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        ExpiryPolicy plc = opCtx != null ? opCtx.expiry() : null;

        return ctx.kernalContext().closure().callAsync(BROADCAST,
            Arrays.asList(new LoadCacheClosure<>(ctx.name(), p, args, plc)),
            nodes.nodes());
    }

    /**
     * @return Random cache entry.
     */
    @Nullable public Cache.Entry<K, V> randomEntry() {
        GridCacheMapEntry e = map.randomEntry();

        return e == null || e.obsolete() ? null : e.<K, V>wrapLazyValue();
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode[] peekModes) throws IgniteCheckedException {
        if (isLocal())
            return localSize(peekModes);

        return sizeAsync(peekModes).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Integer> sizeAsync(final CachePeekMode[] peekModes) {
        assert peekModes != null;

        PeekModes modes = parsePeekModes(peekModes, true);

        IgniteClusterEx cluster = ctx.grid().cluster();

        ClusterGroup grp = modes.near ? cluster.forCacheNodes(name(), true, true, false) : cluster.forDataNodes(name());

        Collection<ClusterNode> nodes = grp.nodes();

        if (nodes.isEmpty())
            return new GridFinishedFuture<>(0);

        ctx.kernalContext().task().setThreadContext(TC_SUBGRID, nodes);

        return ctx.kernalContext().task().execute(
            new SizeTask(ctx.name(), ctx.affinity().affinityTopologyVersion(), peekModes), null);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public int localSize(CachePeekMode[] peekModes) throws IgniteCheckedException {
        PeekModes modes = parsePeekModes(peekModes, true);

        int size = 0;

        if (ctx.isLocal()) {
            modes.primary = true;
            modes.backup = true;

            if (modes.heap)
                size += size();
        }
        else {
            if (modes.heap) {
                if (modes.near)
                    size += nearSize();

                GridCacheAdapter cache = ctx.isNear() ? ctx.near().dht() : ctx.cache();

                if (!(modes.primary && modes.backup)) {
                    if (modes.primary)
                        size += cache.primarySize();

                    if (modes.backup)
                        size += (cache.size() - cache.primarySize());
                }
                else
                    size += cache.size();
            }
        }

        // Swap and offheap are disabled for near cache.
        if (modes.primary || modes.backup) {
            AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

            GridCacheSwapManager swapMgr = ctx.isNear() ? ctx.near().dht().context().swap() : ctx.swap();

            if (modes.swap)
                size += swapMgr.swapEntriesCount(modes.primary, modes.backup, topVer);

            if (modes.offheap)
                size += swapMgr.offheapEntriesCount(modes.primary, modes.backup, topVer);
        }

        return size;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return map.publicSize();
    }

    /** {@inheritDoc} */
    @Override public int nearSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int primarySize() {
        return map.publicSize();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAdapter.class, this, "name", name(), "size", size());
    }

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<K, V>> iterator() {
        return entrySet().iterator();
    }

    /**
     * @return JCache Iterator.
     */
    private Iterator<Cache.Entry<K, V>> localIteratorHonorExpirePolicy(final CacheOperationContext opCtx) {
        return F.iterator(iterator(),
            new IgniteClosure<Cache.Entry<K, V>, Cache.Entry<K, V>>() {
                private IgniteCacheExpiryPolicy expiryPlc =
                    ctx.cache().expiryPolicy(opCtx != null ? opCtx.expiry() : null);

                @Override public Cache.Entry<K, V> apply(Cache.Entry<K, V> lazyEntry) {
                    CacheOperationContext prev = ctx.gate().enter(opCtx);
                    try {
                        V val = localPeek(lazyEntry.getKey(), CachePeekModes.ONHEAP_ONLY, expiryPlc);

                        GridCacheVersion ver = null;

                        try {
                            ver = lazyEntry.unwrap(GridCacheVersion.class);
                        }
                        catch (IllegalArgumentException e) {
                            log.error("Failed to unwrap entry version information", e);
                        }

                        return new CacheEntryImpl<>(lazyEntry.getKey(), val, ver);
                    }
                    catch (IgniteCheckedException e) {
                        throw CU.convertToCacheException(e);
                    }
                    finally {
                        ctx.gate().leave(prev);
                    }
                }
            }, false
        );
    }

    /**
     * @return Distributed ignite cache iterator.
     */
    public Iterator<Cache.Entry<K, V>> igniteIterator() {
        GridCacheContext ctx0 = ctx.isNear() ? ctx.near().dht().context() : ctx;

        final CacheOperationContext opCtx = ctx.operationContextPerCall();

        if (!ctx0.isSwapOrOffheapEnabled() && ctx0.kernalContext().discovery().size() == 1)
            return localIteratorHonorExpirePolicy(opCtx);

        CacheQueryFuture<Map.Entry<K, V>> fut = ctx0.queries().createScanQuery(null, null, ctx.keepPortable())
            .keepAll(false)
            .execute();

        return ctx.itHolder().iterator(fut, new CacheIteratorConverter<Cache.Entry<K, V>, Map.Entry<K, V>>() {
            @Override protected Cache.Entry<K, V> convert(Map.Entry<K, V> e) {
                return new CacheEntryImpl<>(e.getKey(), e.getValue());
            }

            @Override protected void remove(Cache.Entry<K, V> item) {
                CacheOperationContext prev = ctx.gate().enter(opCtx);

                try {
                    GridCacheAdapter.this.remove(item.getKey());
                }
                catch (IgniteCheckedException e) {
                    throw CU.convertToCacheException(e);
                }
                finally {
                    ctx.gate().leave(prev);
                }
            }
        });
    }

    /**
     * @param key Key.
     * @param deserializePortable Deserialize portable flag.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("IfMayBeConditional")
    @Nullable public V promote(K key, boolean deserializePortable) throws IgniteCheckedException {
        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

        GridCacheSwapEntry unswapped = ctx.swap().readAndRemove(cacheKey);

        if (unswapped == null)
            return null;

        GridCacheEntryEx entry = entryEx(cacheKey);

        try {
            if (!entry.initialValue(cacheKey, unswapped))
                return null;
        }
        catch (GridCacheEntryRemovedException ignored) {
            if (log.isDebugEnabled())
                log.debug("Entry has been concurrently removed.");

            return null;
        }

        CacheObject val = unswapped.value();

        Object val0 = val != null ? val.value(ctx.cacheObjectContext(), true) : null;

        return (V)ctx.unwrapPortableIfNeeded(val0, !deserializePortable);
    }

    /** {@inheritDoc} */
    @Override public void promoteAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        if (F.isEmpty(keys))
            return;

        if (keyCheck)
            validateCacheKeys(keys);

        Collection<KeyCacheObject> unswap = new ArrayList<>(keys.size());

        for (K key : keys) {
            KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

            // Do not look up in swap for existing entries.
            GridCacheEntryEx entry = peekEx(cacheKey);

            try {
                if (entry == null || entry.obsolete() || entry.isNewLocked()) {
                    if (entry != null)
                        cacheKey = entry.key();

                    unswap.add(cacheKey);
                }
            }
            catch (GridCacheEntryRemovedException ignored) {
                // No-op.
            }
        }

        Collection<GridCacheBatchSwapEntry> swapped = ctx.swap().readAndRemove(unswap);

        for (GridCacheBatchSwapEntry swapEntry : swapped) {
            KeyCacheObject key = swapEntry.key();

            GridCacheEntryEx entry = entryEx(key);

            try {
                entry.initialValue(key, swapEntry);
            }
            catch (GridCacheEntryRemovedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Entry has been concurrently removed.");
            }
        }
    }

    /** {@inheritDoc} */
    @Override public long offHeapEntriesCount() {
        GridCacheSwapManager swapMgr = ctx.swap();

        return swapMgr != null ? swapMgr.offHeapEntriesCount() : -1;
    }

    /** {@inheritDoc} */
    @Override public long offHeapAllocatedSize() {
        GridCacheSwapManager swapMgr = ctx.swap();

        return swapMgr != null ? swapMgr.offHeapAllocatedSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public long swapSize() throws IgniteCheckedException {
        return ctx.swap().swapSize();
    }

    /** {@inheritDoc} */
    @Override public long swapKeys() throws IgniteCheckedException {
        return ctx.swap().swapKeys();
    }

    /**
     * Asynchronously commits transaction after all previous asynchronous operations are completed.
     *
     * @param tx Transaction to commit.
     * @return Transaction commit future.
     */
    @SuppressWarnings("unchecked")
    public IgniteInternalFuture<IgniteInternalTx> commitTxAsync(final IgniteInternalTx tx) {
        FutureHolder holder = lastFut.get();

        holder.lock();

        try {
            IgniteInternalFuture fut = holder.future();

            if (fut != null && !fut.isDone()) {
                IgniteInternalFuture<IgniteInternalTx> f = new GridEmbeddedFuture<>(fut,
                    new C2<Object, Exception, IgniteInternalFuture<IgniteInternalTx>>() {
                        @Override public IgniteInternalFuture<IgniteInternalTx> apply(Object o, Exception e) {
                            return tx.commitAsync();
                        }
                    });

                saveFuture(holder, f);

                return f;
            }

            IgniteInternalFuture<IgniteInternalTx> f = tx.commitAsync();

            saveFuture(holder, f);

            ctx.tm().resetContext();

            return f;
        }
        finally {
            holder.unlock();
        }
    }

    /**
     * Awaits for previous async operation to be completed.
     */
    @SuppressWarnings("unchecked")
    public void awaitLastFut() {
        FutureHolder holder = lastFut.get();

        IgniteInternalFuture fut = holder.future();

        if (fut != null && !fut.isDone()) {
            try {
                // Ignore any exception from previous async operation as it should be handled by user.
                fut.get();
            }
            catch (IgniteCheckedException ignored) {
                // No-op.
            }
        }
    }

    /**
     * @param op Cache operation.
     * @param <T> Return type.
     * @return Operation result.
     * @throws IgniteCheckedException If operation failed.
     */
    @SuppressWarnings({"TypeMayBeWeakened", "ErrorNotRethrown", "AssignmentToCatchBlockParameter"})
    @Nullable private <T> T syncOp(SyncOp<T> op) throws IgniteCheckedException {
        checkJta();

        awaitLastFut();

        IgniteTxLocalAdapter tx = ctx.tm().threadLocalTx(ctx);

        if (tx == null || tx.implicit()) {
            TransactionConfiguration tCfg = ctx.gridConfig().getTransactionConfiguration();

            CacheOperationContext opCtx = ctx.operationContextPerCall();

            int retries = opCtx != null && opCtx.noRetries() ? 1 : MAX_RETRIES;

            for (int i = 0; i < retries; i++) {
                tx = ctx.tm().newTx(
                    true,
                    op.single(),
                    ctx.systemTx() ? ctx : null,
                    OPTIMISTIC,
                    READ_COMMITTED,
                    tCfg.getDefaultTxTimeout(),
                    !ctx.skipStore(),
                    0
                );

                assert tx != null;

                try {
                    T t = op.op(tx);

                    assert tx.done() : "Transaction is not done: " + tx;

                    return t;
                }
                catch (IgniteInterruptedCheckedException | IgniteTxHeuristicCheckedException |
                    IgniteTxRollbackCheckedException e) {
                    throw e;
                }
                catch (IgniteCheckedException e) {
                    try {
                        tx.rollback();

                        e = new IgniteTxRollbackCheckedException("Transaction has been rolled back: " +
                            tx.xid(), e);
                    }
                    catch (IgniteCheckedException | AssertionError | RuntimeException e1) {
                        U.error(log, "Failed to rollback transaction (cache may contain stale locks): " + tx, e1);

                        U.addLastCause(e, e1, log);
                    }

                    if (X.hasCause(e, ClusterTopologyCheckedException.class) && i != retries - 1) {
                        ClusterTopologyCheckedException topErr = e.getCause(ClusterTopologyCheckedException.class);

                        if (!(topErr instanceof ClusterTopologyServerNotFoundException)) {
                            AffinityTopologyVersion topVer = tx.topologyVersion();

                            assert topVer != null && topVer.topologyVersion() > 0 : tx;

                            ctx.affinity().affinityReadyFuture(topVer.topologyVersion() + 1).get();

                            continue;
                        }
                    }

                    throw e;
                }
                finally {
                    ctx.tm().resetContext();

                    if (ctx.isNear())
                        ctx.near().dht().context().tm().resetContext();
                }
            }

            // Should not happen.
            throw new IgniteCheckedException("Failed to perform cache operation (maximum number of retries exceeded).");
        }
        else
            return op.op(tx);
    }

    /**
     * @param op Cache operation.
     * @param <T> Return type.
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    private <T> IgniteInternalFuture<T> asyncOp(final AsyncOp<T> op) {
        try {
            checkJta();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }

        if (log.isDebugEnabled())
            log.debug("Performing async op: " + op);

        IgniteTxLocalAdapter tx = ctx.tm().threadLocalTx(ctx);

        if (tx == null || tx.implicit()) {
            boolean skipStore = ctx.skipStore(); // Save value of thread-local flag.

            CacheOperationContext opCtx = ctx.operationContextPerCall();

            int retries = opCtx != null && opCtx.noRetries() ? 1 : MAX_RETRIES;

            if (retries == 1) {
                tx = ctx.tm().newTx(
                    true,
                    op.single(),
                    ctx.systemTx() ? ctx : null,
                    OPTIMISTIC,
                    READ_COMMITTED,
                    ctx.kernalContext().config().getTransactionConfiguration().getDefaultTxTimeout(),
                    !skipStore,
                    0);

                return asyncOp(tx, op);
            }
            else {
                AsyncOpRetryFuture<T> fut = new AsyncOpRetryFuture<>(op, skipStore, retries);

                fut.execute();

                return fut;
            }
        }
        else
            return asyncOp(tx, op);
    }

    /**
     * @param tx Transaction.
     * @param op Cache operation.
     * @param <T> Return type.
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    protected <T> IgniteInternalFuture<T> asyncOp(IgniteTxLocalAdapter tx, final AsyncOp<T> op) {
        IgniteInternalFuture<T> fail = asyncOpAcquire();

        if (fail != null)
            return fail;

        FutureHolder holder = lastFut.get();

        holder.lock();

        try {
            IgniteInternalFuture fut = holder.future();

            final IgniteTxLocalAdapter tx0 = tx;

            if (fut != null && !fut.isDone()) {
                IgniteInternalFuture<T> f = new GridEmbeddedFuture<>(fut,
                    new C2<T, Exception, IgniteInternalFuture<T>>() {
                        @Override public IgniteInternalFuture<T> apply(T t, Exception e) {
                            return op.op(tx0).chain(new CX1<IgniteInternalFuture<T>, T>() {
                                @Override public T applyx(IgniteInternalFuture<T> tFut) throws IgniteCheckedException {
                                    try {
                                        return tFut.get();
                                    }
                                    catch (IgniteCheckedException e1) {
                                        tx0.rollbackAsync();

                                        throw e1;
                                    }
                                }
                            });
                        }
                    });

                saveFuture(holder, f);

                return f;
            }

            IgniteInternalFuture<T> f = op.op(tx).chain(new CX1<IgniteInternalFuture<T>, T>() {
                @Override public T applyx(IgniteInternalFuture<T> tFut) throws IgniteCheckedException {
                    try {
                        return tFut.get();
                    }
                    catch (IgniteCheckedException e1) {
                        tx0.rollbackAsync();

                        throw e1;
                    }
                }
            });

            saveFuture(holder, f);

            if (tx.implicit())
                ctx.tm().resetContext();

            return f;
        }
        finally {
            holder.unlock();
        }
    }

    /**
     * Saves future in thread local holder and adds listener
     * that will clear holder when future is finished.
     *
     * @param holder Future holder.
     * @param fut Future to save.
     */
    protected void saveFuture(final FutureHolder holder, IgniteInternalFuture<?> fut) {
        assert holder != null;
        assert fut != null;
        assert holder.holdsLock();

        holder.future(fut);

        if (fut.isDone()) {
            holder.future(null);

            asyncOpRelease();
        }
        else {
            fut.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> f) {
                    asyncOpRelease();

                    if (!holder.tryLock())
                        return;

                    try {
                        if (holder.future() == f)
                            holder.future(null);
                    }
                    finally {
                        holder.unlock();
                    }
                }
            });
        }
    }

    /**
     * Tries to acquire asynchronous operations permit, if limited.
     *
     * @return Failed future if waiting was interrupted.
     */
    @Nullable protected <T> IgniteInternalFuture<T> asyncOpAcquire() {
        try {
            if (asyncOpsSem != null)
                asyncOpsSem.acquire();

            return null;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            return new GridFinishedFuture<>(new IgniteInterruptedCheckedException("Failed to wait for asynchronous " +
                "operation permit (thread got interrupted).", e));
        }
    }

    /**
     * Releases asynchronous operations permit, if limited.
     */
    protected void asyncOpRelease() {
        if (asyncOpsSem != null)
            asyncOpsSem.release();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, ctx.gridName());
        U.writeString(out, ctx.namex());
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        IgniteBiTuple<String, String> t = stash.get();

        t.set1(U.readString(in));
        t.set2(U.readString(in));
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            IgniteBiTuple<String, String> t = stash.get();

            return IgnitionEx.gridx(t.get1()).cachex(t.get2());
        }
        catch (IllegalStateException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> rebalance() {
        ctx.preloader().forcePreload();

        return ctx.preloader().syncFuture();
    }

    /** {@inheritDoc} */
    @Override public boolean isIgfsDataCache() {
        return igfsDataCache;
    }

    /** {@inheritDoc} */
    @Override public long igfsDataSpaceUsed() {
        assert igfsDataCache;

        return igfsDataCacheSize.longValue();
    }

    /** {@inheritDoc} */
    @Override public long igfsDataSpaceMax() {
        return igfsDataSpaceMax;
    }

    /** {@inheritDoc} */
    @Override public boolean isMongoDataCache() {
        return mongoDataCache;
    }

    /** {@inheritDoc} */
    @Override public boolean isMongoMetaCache() {
        return mongoMetaCache;
    }

    /**
     * Callback invoked when data is added to IGFS cache.
     *
     * @param delta Size delta.
     */
    public void onIgfsDataSizeChanged(long delta) {
        assert igfsDataCache;

        igfsDataCacheSize.add(delta);
    }

    /**
     * @param keys Keys.
     * @param filter Filters to evaluate.
     */
    public void clearLocally0(Collection<? extends K> keys,
        @Nullable CacheEntryPredicate... filter) {
        ctx.checkSecurity(SecurityPermission.CACHE_REMOVE);

        if (F.isEmpty(keys))
            return;

        if (keyCheck)
            validateCacheKeys(keys);

        GridCacheVersion obsoleteVer = ctx.versions().next();

        for (K k : keys)
            clearLocally(obsoleteVer, k, filter);
    }

    /**
     * @param key Key.
     * @param filter Filters to evaluate.
     * @return {@code True} if cleared.
     */
    public boolean clearLocally0(K key, @Nullable CacheEntryPredicate... filter) {
        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        ctx.checkSecurity(SecurityPermission.CACHE_REMOVE);

        return clearLocally(ctx.versions().next(), key, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean evict(K key) {
        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        return evictx(key, ctx.versions().next(), CU.empty0());
    }

    /** {@inheritDoc} */
    @Override public void evictAll(Collection<? extends K> keys) {
        A.notNull(keys, "keys");

        if (F.isEmpty(keys))
            return;

        if (keyCheck)
            validateCacheKey(keys);

        GridCacheVersion obsoleteVer = ctx.versions().next();

        if (!ctx.evicts().evictSyncOrNearSync() && ctx.isSwapOrOffheapEnabled()) {
            try {
                ctx.evicts().batchEvict(keys, obsoleteVer);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to perform batch evict for keys: " + keys, e);
            }
        }
        else {
            for (K k : keys)
                evictx(k, obsoleteVer, CU.empty0());
        }
    }

    /**
     * @param filter Filters to evaluate.
     * @return Entry set.
     */
    public Set<Cache.Entry<K, V>> entrySet(@Nullable CacheEntryPredicate... filter) {
        return map.entries(filter);
    }

    /**
     * @param filter Filters to evaluate.
     * @return Primary entry set.
     */
    public Set<Cache.Entry<K, V>> primaryEntrySet(
        @Nullable CacheEntryPredicate... filter) {
        return map.entries(
            F0.and0(
                filter,
                CU.cachePrimary(ctx.grid().affinity(ctx.name()), ctx.localNode())));
    }

    /**
     * @param filter Filters to evaluate.
     * @return Key set.
     */
    public Set<K> keySet(@Nullable CacheEntryPredicate... filter) {
        return map.keySet(filter);
    }

    /**
     * @param filter Filters to evaluate.
     * @return Key set including internal keys.
     */
    public Set<K> keySetx(@Nullable CacheEntryPredicate... filter) {
        return map.keySetx(filter);
    }

    /**
     * @param filter Primary key set.
     * @return Primary key set.
     */
    public Set<K> primaryKeySet(@Nullable CacheEntryPredicate... filter) {
        return map.keySet(
            F0.and0(
                filter,
                CU.cachePrimary(ctx.grid().affinity(ctx.name()), ctx.localNode())));
    }

    /**
     * @param key Key.
     * @param deserializePortable Deserialize portable flag.
     * @return Cached value.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public V get(K key, boolean deserializePortable)
        throws IgniteCheckedException {
        Map<K, V> map = getAllAsync(F.asList(key), deserializePortable).get();

        assert map.isEmpty() || map.size() == 1 : map.size();

        return map.get(key);
    }

    /**
     * @param key Key.
     * @param deserializePortable Deserialize portable flag.
     * @return Read operation future.
     */
    public final IgniteInternalFuture<V> getAsync(final K key, boolean deserializePortable) {
        try {
            checkJta();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }

        return getAllAsync(Collections.singletonList(key), deserializePortable).chain(
            new CX1<IgniteInternalFuture<Map<K, V>>, V>() {
                    @Override public V applyx(IgniteInternalFuture<Map<K, V>> e) throws IgniteCheckedException {
                    Map<K, V> map = e.get();

                    assert map.isEmpty() || map.size() == 1 : map.size();

                    return map.get(key);
                }
            });
    }

    /**
     * @param keys Keys.
     * @param deserializePortable Deserialize portable flag.
     * @return Map of cached values.
     * @throws IgniteCheckedException If read failed.
     */
    public Map<K, V> getAll(Collection<? extends K> keys, boolean deserializePortable) throws IgniteCheckedException {
        checkJta();

        return getAllAsync(keys, deserializePortable).get();
    }

    /**
     * @param keys Keys.
     * @param deserializePortable Deserialize portable flag.
     * @return Read future.
     */
    public IgniteInternalFuture<Map<K, V>> getAllAsync(@Nullable Collection<? extends K> keys,
        boolean deserializePortable) {
        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAllAsync(keys,
            !ctx.config().isReadFromBackup(),
            /*skip tx*/false,
            null,
            null,
            taskName,
            deserializePortable,
            false,
            /*can remap*/true);
    }

    /**
     * @param entry Entry.
     * @param ver Version.
     */
    public abstract void onDeferredDelete(GridCacheEntryEx entry, GridCacheVersion ver);

    /**
     *
     */
    public void onReconnected() {
        // No-op.
    }

    /**
     * Validates that given cache value implements {@link Externalizable}.
     *
     * @param val Cache value.
     */
    private void validateCacheValue(Object val) {
        if (valCheck) {
            CU.validateCacheValue(log, val);

            valCheck = false;
        }
    }

    /**
     * Validates that given cache values implement {@link Externalizable}.
     *
     * @param vals Cache values.
     */
    private void validateCacheValues(Iterable<?> vals) {
        if (valCheck) {
            for (Object val : vals) {
                if (val == null)
                    continue;

                CU.validateCacheValue(log, val);
            }

            valCheck = false;
        }
    }

    /**
     * Validates that given cache key has overridden equals and hashCode methods and
     * implements {@link Externalizable}.
     *
     * @param key Cache key.
     * @throws IllegalArgumentException If validation fails.
     */
    protected void validateCacheKey(Object key) {
        if (keyCheck) {
            CU.validateCacheKey(log, key);

            keyCheck = false;
        }
    }

    /**
     * Validates that given cache keys have overridden equals and hashCode methods and
     * implement {@link Externalizable}.
     *
     * @param keys Cache keys.
     * @throws IgniteException If validation fails.
     */
    protected void validateCacheKeys(Iterable<?> keys) {
        if (keys == null)
            return;

        if (keyCheck) {
            for (Object key : keys) {
                if (key == null || key instanceof GridCacheInternal)
                    continue;

                CU.validateCacheKey(log, key);

                keyCheck = false;
            }
        }
    }

    /**
     * @param it Internal entry iterator.
     * @param deserializePortable Deserialize portable flag.
     * @return Public API iterator.
     */
    protected Iterator<Cache.Entry<K, V>> iterator(final Iterator<GridCacheEntryEx> it,
        final boolean deserializePortable) {
        return new Iterator<Cache.Entry<K, V>>() {
            {
                advance();
            }

            /** */
            private Cache.Entry<K, V> next;

            @Override public boolean hasNext() {
                return next != null;
            }

            @Override public Cache.Entry<K, V> next() {
                if (next == null)
                    throw new NoSuchElementException();

                Cache.Entry<K, V> e = next;

                advance();

                return e;
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }

            /**
             * Switch to next entry.
             */
            private void advance() {
                next = null;

                while (it.hasNext()) {
                    GridCacheEntryEx entry = it.next();

                    try {
                        next = toCacheEntry(entry, deserializePortable);

                        if (next == null)
                            continue;

                        break;
                    }
                    catch (IgniteCheckedException e) {
                        throw CU.convertToCacheException(e);
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        // No-op.
                    }
                }
            }
        };
    }

    /**
     * @param entry Internal entry.
     * @param deserializePortable Deserialize portable flag.
     * @return Public API entry.
     * @throws IgniteCheckedException If failed.
     * @throws GridCacheEntryRemovedException If entry removed.
     */
    @Nullable private Cache.Entry<K, V> toCacheEntry(GridCacheEntryEx entry,
        boolean deserializePortable)
        throws IgniteCheckedException, GridCacheEntryRemovedException
    {
        try {
            CacheObject val = entry.innerGet(
                null,
                false,
                false,
                false,
                true,
                false,
                false,
                false,
                null,
                null,
                null,
                null);

            if (val == null)
                return null;

            KeyCacheObject key = entry.key();

            Object key0 = key.value(ctx.cacheObjectContext(), true);
            Object val0 = val.value(ctx.cacheObjectContext(), true);

            if (deserializePortable) {
                key0 = ctx.unwrapPortableIfNeeded(key0, true);
                val0 = ctx.unwrapPortableIfNeeded(val0, true);
            }

            return new CacheEntryImpl<>((K)key0, (V)val0, entry.version());
        }
        catch (GridCacheFilterFailedException ignore) {
            assert false;

            return null;
        }
    }

    /**
     *
     */
    private class AsyncOpRetryFuture<T> extends GridFutureAdapter<T> {
        /** */
        private AsyncOp<T> op;

        /** */
        private boolean skipStore;

        /** */
        private int retries;

        /** */
        private IgniteTxLocalAdapter tx;

        /**
         * @param op Operation.
         * @param skipStore Skip store flag.
         * @param retries Number of retries.
         */
        public AsyncOpRetryFuture(AsyncOp<T> op,
            boolean skipStore,
            int retries) {
            assert retries > 1 : retries;

            this.op = op;
            this.tx = null;
            this.skipStore = skipStore;
            this.retries = retries;
        }

        /**
         *
         */
        public void execute() {
            tx = ctx.tm().newTx(
                true,
                op.single(),
                ctx.systemTx() ? ctx : null,
                OPTIMISTIC,
                READ_COMMITTED,
                ctx.kernalContext().config().getTransactionConfiguration().getDefaultTxTimeout(),
                !skipStore,
                0);

            IgniteInternalFuture<T> fut = asyncOp(tx, op);

            fut.listen(new IgniteInClosure<IgniteInternalFuture<T>>() {
                @Override public void apply(IgniteInternalFuture<T> fut) {
                    try {
                        T res = fut.get();

                        onDone(res);
                    }
                    catch (IgniteCheckedException e) {
                        if (X.hasCause(e, ClusterTopologyCheckedException.class) && --retries > 0) {
                            ClusterTopologyCheckedException topErr = e.getCause(ClusterTopologyCheckedException.class);

                            if (!(topErr instanceof ClusterTopologyServerNotFoundException)) {
                                IgniteTxLocalAdapter tx = AsyncOpRetryFuture.this.tx;

                                assert tx != null;

                                AffinityTopologyVersion topVer = tx.topologyVersion();

                                assert topVer != null && topVer.topologyVersion() > 0 : tx;

                                IgniteInternalFuture<?> topFut =
                                    ctx.affinity().affinityReadyFuture(topVer.topologyVersion() + 1);

                                topFut.listen(new IgniteInClosure<IgniteInternalFuture<?>>() {
                                    @Override public void apply(IgniteInternalFuture<?> topFut) {
                                        try {
                                            topFut.get();

                                            execute();
                                        }
                                        catch (IgniteCheckedException e) {
                                            onDone(e);
                                        }
                                    }
                                });

                                return;
                            }
                        }

                        onDone(e);
                    }
                }
            });
        }
    }

    /**
     *
     */
    private static class PeekModes {
        /** */
        private boolean near;

        /** */
        private boolean primary;

        /** */
        private boolean backup;

        /** */
        private boolean heap;

        /** */
        private boolean offheap;

        /** */
        private boolean swap;

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PeekModes.class, this);
        }
    }

    /**
     * @param peekModes Cache peek modes array.
     * @param primary Defines the default behavior if affinity flags are not specified.
     * @return Peek modes flags.
     */
    private static PeekModes parsePeekModes(CachePeekMode[] peekModes, boolean primary) {
        PeekModes modes = new PeekModes();

        if (F.isEmpty(peekModes)) {
            modes.primary = true;

            if (!primary) {
                modes.backup = true;
                modes.near = true;
            }

            modes.heap = true;
            modes.offheap = true;
            modes.swap = true;
        }
        else {
            for (CachePeekMode peekMode : peekModes) {
                A.notNull(peekMode, "peekMode");

                switch (peekMode) {
                    case ALL:
                        modes.near = true;
                        modes.primary = true;
                        modes.backup = true;

                        modes.heap = true;
                        modes.offheap = true;
                        modes.swap = true;

                        break;

                    case BACKUP:
                        modes.backup = true;

                        break;

                    case PRIMARY:
                        modes.primary = true;

                        break;

                    case NEAR:
                        modes.near = true;

                        break;

                    case ONHEAP:
                        modes.heap = true;

                        break;

                    case OFFHEAP:
                        modes.offheap = true;

                        break;

                    case SWAP:
                        modes.swap = true;

                        break;

                    default:
                        assert false : peekMode;
                }
            }
        }

        if (!(modes.heap || modes.offheap || modes.swap)) {
            modes.heap = true;
            modes.offheap = true;
            modes.swap = true;
        }

        if (!(modes.primary || modes.backup || modes.near)) {
            modes.primary = true;

            if (!primary) {
                modes.backup = true;
                modes.near = true;
            }
        }

        assert modes.heap || modes.offheap || modes.swap;
        assert modes.primary || modes.backup || modes.near;

        return modes;
    }

    /**
     * @param plc Explicitly specified expiry policy for cache operation.
     * @return Expiry policy wrapper.
     */
    @Nullable public IgniteCacheExpiryPolicy expiryPolicy(@Nullable ExpiryPolicy plc) {
        if (plc == null)
            plc = ctx.expiry();

        return CacheExpiryPolicy.forPolicy(plc);
    }

    /**
     * Cache operation.
     */
    private abstract class SyncOp<T> {
        /** Flag to indicate only-one-key operation. */
        private final boolean single;

        /**
         * @param single Flag to indicate only-one-key operation.
         */
        SyncOp(boolean single) {
            this.single = single;
        }

        /**
         * @return Flag to indicate only-one-key operation.
         */
        final boolean single() {
            return single;
        }

        /**
         * @param tx Transaction.
         * @return Operation return value.
         * @throws IgniteCheckedException If failed.
         */
        @Nullable public abstract T op(IgniteTxLocalAdapter tx) throws IgniteCheckedException;
    }

    /**
     * Cache operation.
     */
    private abstract class SyncInOp extends SyncOp<Object> {
        /**
         * @param single Flag to indicate only-one-key operation.
         */
        SyncInOp(boolean single) {
            super(single);
        }

        /** {@inheritDoc} */
        @Nullable @Override public final Object op(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
            inOp(tx);

            return null;
        }

        /**
         * @param tx Transaction.
         * @throws IgniteCheckedException If failed.
         */
        public abstract void inOp(IgniteTxLocalAdapter tx) throws IgniteCheckedException;
    }

    /**
     * Cache operation.
     */
    protected abstract class AsyncOp<T> {
        /** Flag to indicate only-one-key operation. */
        private final boolean single;

        /**
         *
         */
        protected AsyncOp() {
            single = true;
        }

        /**
         * @param keys Keys involved.
         */
        protected AsyncOp(Collection<?> keys) {
            single = keys.size() == 1;
        }

        /**
         * @return Flag to indicate only-one-key operation.
         */
        final boolean single() {
            return single;
        }

        /**
         * @param tx Transaction.
         * @return Operation return value.
         */
        public abstract IgniteInternalFuture<T> op(IgniteTxLocalAdapter tx);
    }

    /**
     * Cache operation.
     */
    private abstract class AsyncInOp extends AsyncOp<Object> {
        /**
         *
         */
        protected AsyncInOp() {
            super();
        }

        /**
         * @param keys Keys involved.
         */
        protected AsyncInOp(Collection<?> keys) {
            super(keys);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public final IgniteInternalFuture<Object> op(IgniteTxLocalAdapter tx) {
            return (IgniteInternalFuture<Object>)inOp(tx);
        }

        /**
         * @param tx Transaction.
         * @return Operation return value.
         */
        public abstract IgniteInternalFuture<?> inOp(IgniteTxLocalAdapter tx);
    }

    /**
     * Global clear all.
     */
    @GridInternal
    private static class GlobalClearAllJob extends TopologyVersionAwareJob {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param cacheName Cache name.
         * @param topVer Affinity topology version.
         */
        private GlobalClearAllJob(String cacheName, AffinityTopologyVersion topVer) {
            super(cacheName, topVer);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object localExecute(@Nullable IgniteInternalCache cache) {
            if (cache != null)
                cache.clearLocally();

            return null;
        }
    }

    /**
     * Global clear keys.
     */
    @GridInternal
    private static class GlobalClearKeySetJob<K> extends TopologyVersionAwareJob {
        /** */
        private static final long serialVersionUID = 0L;

        /** Keys to remove. */
        private final Set<? extends K> keys;

        /**
         * @param cacheName Cache name.
         * @param topVer Affinity topology version.
         * @param keys Keys to clear.
         */
        private GlobalClearKeySetJob(String cacheName, AffinityTopologyVersion topVer, Set<? extends K> keys) {
            super(cacheName, topVer);

            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object localExecute(@Nullable IgniteInternalCache cache) {
            if (cache != null)
                cache.clearLocallyAll(keys);

            return null;
        }
    }

    /**
     * Internal callable for global size calculation.
     */
    @GridInternal
    private static class SizeJob extends TopologyVersionAwareJob {
        /** */
        private static final long serialVersionUID = 0L;

        /** Peek modes. */
        private final CachePeekMode[] peekModes;

        /**
         * @param cacheName Cache name.
         * @param topVer Affinity topology version.
         * @param peekModes Cache peek modes.
         */
        private SizeJob(String cacheName, AffinityTopologyVersion topVer, CachePeekMode[] peekModes) {
            super(cacheName, topVer);

            this.peekModes = peekModes;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object localExecute(@Nullable IgniteInternalCache cache) {
            if (cache == null)
                return 0;

            try {
                return cache.localSize(peekModes);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(SizeJob.class, this);
        }
    }

    /**
     * Holder for last async operation future.
     */
    protected static class FutureHolder {
        /** Lock. */
        private final ReentrantLock lock = new ReentrantLock();

        /** Future. */
        private IgniteInternalFuture fut;

        /**
         * Tries to acquire lock.
         *
         * @return Whether lock was actually acquired.
         */
        public boolean tryLock() {
            return lock.tryLock();
        }

        /**
         * Acquires lock.
         */
        @SuppressWarnings("LockAcquiredButNotSafelyReleased")
        public void lock() {
            lock.lock();
        }

        /**
         * Releases lock.
         */
        public void unlock() {
            lock.unlock();
        }

        /**
         * @return Whether lock is held by current thread.
         */
        public boolean holdsLock() {
            return lock.isHeldByCurrentThread();
        }

        /**
         * Gets future.
         *
         * @return Future.
         */
        public IgniteInternalFuture future() {
            return fut;
        }

        /**
         * Sets future.
         *
         * @param fut Future.
         */
        public void future(@Nullable IgniteInternalFuture fut) {
            this.fut = fut;
        }
    }

    /**
     *
     */
    protected abstract static class CacheExpiryPolicy implements IgniteCacheExpiryPolicy {
        /** */
        private Map<KeyCacheObject, GridCacheVersion> entries;

        /** */
        private Map<UUID, Collection<IgniteBiTuple<KeyCacheObject, GridCacheVersion>>> rdrsMap;

        /**
         * @param expiryPlc Expiry policy.
         * @return Access expire policy.
         */
        @Nullable private static CacheExpiryPolicy forPolicy(@Nullable final ExpiryPolicy expiryPlc) {
            if (expiryPlc == null)
                return null;

            return new CacheExpiryPolicy() {
                @Override public long forAccess() {
                    return CU.toTtl(expiryPlc.getExpiryForAccess());
                }

                @Override public long forCreate() {
                    return CU.toTtl(expiryPlc.getExpiryForCreation());
                }

                @Override public long forUpdate() {
                    return CU.toTtl(expiryPlc.getExpiryForUpdate());
                }
            };
        }

        /**
         * @param ttl Access TTL.
         * @return Access expire policy.
         */
        @Nullable public static CacheExpiryPolicy forAccess(final long ttl) {
            if (ttl == CU.TTL_NOT_CHANGED)
                return null;

            return new CacheExpiryPolicy() {
                @Override public long forAccess() {
                    return ttl;
                }
            };
        }

        /** {@inheritDoc} */
        @Override public long forCreate() {
            return CU.TTL_NOT_CHANGED;
        }

        /** {@inheritDoc} */
        @Override public long forUpdate() {
            return CU.TTL_NOT_CHANGED;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            if (entries != null)
                entries.clear();

            if (rdrsMap != null)
                rdrsMap.clear();
        }

        /**
         * @param key Entry key.
         * @param ver Entry version.
         */
        @SuppressWarnings("unchecked")
        @Override public void ttlUpdated(KeyCacheObject key,
            GridCacheVersion ver,
            @Nullable Collection<UUID> rdrs) {
            if (entries == null)
                entries = new HashMap<>();

            entries.put(key, ver);

            if (rdrs != null && !rdrs.isEmpty()) {
                if (rdrsMap == null)
                    rdrsMap = new HashMap<>();

                for (UUID nodeId : rdrs) {
                    Collection<IgniteBiTuple<KeyCacheObject, GridCacheVersion>> col = rdrsMap.get(nodeId);

                    if (col == null)
                        rdrsMap.put(nodeId, col = new ArrayList<>());

                    col.add(new T2<>(key, ver));
                }
            }
        }

        /**
         * @return TTL update request.
         */
        @Nullable @Override public Map<KeyCacheObject, GridCacheVersion> entries() {
            return entries;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Map<UUID, Collection<IgniteBiTuple<KeyCacheObject, GridCacheVersion>>> readers() {
            return rdrsMap;
        }

        /** {@inheritDoc} */
        @Override public boolean readyToFlush(int cnt) {
            return (entries != null && entries.size() > cnt) || (rdrsMap != null && rdrsMap.size() > cnt);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheExpiryPolicy.class, this);
        }
    }

    /**
     *
     */
    static class LoadKeysCallable<K, V> implements IgniteCallable<Void>, Externalizable{
        /** */
        private static final long serialVersionUID = 0L;

        /** Cache name. */
        private String cacheName;

        /** Injected grid instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Keys to load. */
        private Collection<? extends K> keys;

        /** Update flag. */
        private boolean update;

        /** */
        private ExpiryPolicy plc;

        /**
         * Required by {@link Externalizable}.
         */
        public LoadKeysCallable() {
            // No-op.
        }

        /**
         * @param cacheName Cache name.
         * @param keys Keys.
         * @param update If {@code true} calls {@link #localLoadAndUpdate(Collection)}
         *        otherwise {@link #localLoad(Collection, ExpiryPolicy)}.
         * @param plc Expiry policy.
         */
        LoadKeysCallable(String cacheName,
            Collection<? extends K> keys,
            boolean update,
            ExpiryPolicy plc) {
            this.cacheName = cacheName;
            this.keys = keys;
            this.update = update;
            this.plc = plc;
        }

        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            GridCacheAdapter<K, V> cache = ((IgniteKernal)ignite).context().cache().internalCache(cacheName);

            assert cache != null : cacheName;

            cache.context().gate().enter();

            try {
                if (update)
                    cache.localLoadAndUpdate(keys);
                else
                    cache.localLoad(keys, plc);
            }
            finally {
                cache.context().gate().leave();
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, cacheName);

            U.writeCollection(out, keys);

            out.writeBoolean(update);

            out.writeObject(plc != null ? new IgniteExternalizableExpiryPolicy(plc) : null);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            cacheName = U.readString(in);

            keys = U.readCollection(in);

            update = in.readBoolean();

            plc = (ExpiryPolicy)in.readObject();
        }
    }

    /**
     *
     */
    private class LocalStoreLoadClosure extends CIX3<KeyCacheObject, Object, GridCacheVersion> {
        /** */
        final IgniteBiPredicate<K, V> p;

        /** */
        final Collection<GridCacheRawVersionedEntry> col;

        /** */
        final DataStreamerImpl<K, V> ldr;

        /** */
        final ExpiryPolicy plc;

        /**
         * @param p Key/value predicate.
         * @param ldr Loader.
         * @param plc Optional expiry policy.
         */
        private LocalStoreLoadClosure(@Nullable IgniteBiPredicate<K, V> p,
            DataStreamerImpl<K, V> ldr,
            @Nullable ExpiryPolicy plc) {
            this.p = p;
            this.ldr = ldr;
            this.plc = plc;

            col = new ArrayList<>(ldr.perNodeBufferSize());
        }

        /** {@inheritDoc} */
        @Override public void applyx(KeyCacheObject key, Object val, GridCacheVersion ver)
            throws IgniteCheckedException
        {
            assert ver != null;

            if (p != null && !p.apply(key.<K>value(ctx.cacheObjectContext(), false), (V)val))
                return;

            long ttl = 0;

            if (plc != null) {
                ttl = CU.toTtl(plc.getExpiryForCreation());

                if (ttl == CU.TTL_ZERO)
                    return;
                else if (ttl == CU.TTL_NOT_CHANGED)
                    ttl = 0;
            }

            GridCacheRawVersionedEntry e = new GridCacheRawVersionedEntry(ctx.toCacheKeyObject(key),
                ctx.toCacheObject(val),
                ttl,
                0,
                ver);

            e.prepareDirectMarshal(ctx.cacheObjectContext());

            col.add(e);

            if (col.size() == ldr.perNodeBufferSize()) {
                ldr.addDataInternal(col);

                col.clear();
            }
        }

        /**
         * Adds remaining data to loader.
         */
        void onDone() {
            if (!col.isEmpty())
                ldr.addDataInternal(col);
        }
    }

    /**
     *
     */
    private static class LoadCacheClosure<K, V> implements Callable<Void>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private String cacheName;

        /** */
        private IgniteBiPredicate<K, V> p;

        /** */
        private Object[] args;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private ExpiryPolicy plc;

        /**
         * Required by {@link Externalizable}.
         */
        public LoadCacheClosure() {
            // No-op.
        }

        /**
         * @param cacheName Cache name.
         * @param p Predicate.
         * @param args Arguments.
         * @param plc Explicitly specified expiry policy.
         */
        private LoadCacheClosure(String cacheName,
            IgniteBiPredicate<K, V> p,
            Object[] args,
            @Nullable ExpiryPolicy plc)
        {
            this.cacheName = cacheName;
            this.p = p;
            this.args = args;
            this.plc = plc;
        }

        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            IgniteCache<K, V> cache = ignite.cache(cacheName);

            assert cache != null : cacheName;

            if (plc != null)
                cache = cache.withExpiryPolicy(plc);

            cache.localLoadCache(p, args);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(p);

            out.writeObject(args);

            U.writeString(out, cacheName);

            if (plc != null)
                out.writeObject(new IgniteExternalizableExpiryPolicy(plc));
            else
                out.writeObject(null);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            p = (IgniteBiPredicate<K, V>)in.readObject();

            args = (Object[])in.readObject();

            cacheName = U.readString(in);

            plc = (ExpiryPolicy)in.readObject();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LoadCacheClosure.class, this);
        }
    }

    /**
     *
     */
    protected abstract static class UpdateTimeStatClosure<T> implements CI1<IgniteInternalFuture<T>> {
        /** */
        protected final CacheMetricsImpl metrics;

        /** */
        protected final long start;

        /**
         * @param metrics Metrics.
         * @param start   Start time.
         */
        public UpdateTimeStatClosure(CacheMetricsImpl metrics, long start) {
            this.metrics = metrics;
            this.start = start;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteInternalFuture<T> fut) {
            try {
                if (!fut.isCancelled()) {
                    fut.get();

                    updateTimeStat();
                }
            }
            catch (IgniteCheckedException ignore) {
                //No-op.
            }
        }

        /**
         * Updates statistics.
         */
        protected abstract void updateTimeStat();
    }

    /**
     *
     */
    protected static class UpdateGetTimeStatClosure<T> extends UpdateTimeStatClosure<T> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param metrics Metrics.
         * @param start   Start time.
         */
        public UpdateGetTimeStatClosure(CacheMetricsImpl metrics, long start) {
            super(metrics, start);
        }

        /** {@inheritDoc} */
        @Override protected void updateTimeStat() {
            metrics.addGetTimeNanos(System.nanoTime() - start);
        }
    }

    /**
     *
     */
    protected static class UpdateRemoveTimeStatClosure<T> extends UpdateTimeStatClosure<T> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param metrics Metrics.
         * @param start   Start time.
         */
        public UpdateRemoveTimeStatClosure(CacheMetricsImpl metrics, long start) {
            super(metrics, start);
        }

        /** {@inheritDoc} */
        @Override protected void updateTimeStat() {
            metrics.addRemoveTimeNanos(System.nanoTime() - start);
        }
    }

    /**
     *
     */
    protected static class UpdatePutTimeStatClosure<T> extends UpdateTimeStatClosure {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param metrics Metrics.
         * @param start   Start time.
         */
        public UpdatePutTimeStatClosure(CacheMetricsImpl metrics, long start) {
            super(metrics, start);
        }

        /** {@inheritDoc} */
        @Override protected void updateTimeStat() {
            metrics.addPutTimeNanos(System.nanoTime() - start);
        }
    }

    /**
     *
     */
    protected static class UpdatePutAndGetTimeStatClosure<T> extends UpdateTimeStatClosure {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param metrics Metrics.
         * @param start   Start time.
         */
        public UpdatePutAndGetTimeStatClosure(CacheMetricsImpl metrics, long start) {
            super(metrics, start);
        }

        /** {@inheritDoc} */
        @Override protected void updateTimeStat() {
            metrics.addPutAndGetTimeNanos(System.nanoTime() - start);
        }
    }

    /**
     * Delayed callable class.
     */
    protected static abstract class TopologyVersionAwareJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** Injected job context. */
        @JobContextResource
        protected ComputeJobContext jobCtx;

        /** Injected grid instance. */
        @IgniteInstanceResource
        protected Ignite ignite;

        /** Affinity topology version. */
        protected final AffinityTopologyVersion topVer;

        /** Cache name. */
        protected final String cacheName;

        /**
         * @param cacheName Cache name.
         * @param topVer Affinity topology version.
         */
        public TopologyVersionAwareJob(String cacheName, AffinityTopologyVersion topVer) {
            assert topVer != null;

            this.cacheName = cacheName;
            this.topVer = topVer;
        }

        /** {@inheritDoc} */
        @Nullable @Override public final Object execute() {
            if (!waitAffinityReadyFuture())
                return null;

            IgniteInternalCache cache = ((IgniteKernal)ignite).context().cache().cache(cacheName);

            return localExecute(cache);
        }

        /**
         * @param cache Cache.
         * @return Local execution result.
         */
        @Nullable protected abstract Object localExecute(@Nullable IgniteInternalCache cache);

        /**
         * Holds (suspends) job execution until our cache version becomes equal to remote cache's version.
         *
         * @return {@code True} if topology check passed.
         */
        private boolean waitAffinityReadyFuture() {
            GridCacheProcessor cacheProc = ((IgniteKernal)ignite).context().cache();

            AffinityTopologyVersion locTopVer = cacheProc.context().exchange().readyAffinityVersion();

            if (locTopVer.compareTo(topVer) < 0) {
                IgniteInternalFuture<?> fut = cacheProc.context().exchange().affinityReadyFuture(topVer);

                if (fut != null && !fut.isDone()) {
                    jobCtx.holdcc();

                    fut.listen(new CI1<IgniteInternalFuture<?>>() {
                        @Override public void apply(IgniteInternalFuture<?> t) {
                            ((IgniteKernal)ignite).context().closure().runLocalSafe(new Runnable() {
                                @Override public void run() {
                                    jobCtx.callcc();
                                }
                            }, false);
                        }
                    });

                    return false;
                }
            }

            return true;
        }
    }

    /**
     * Size task.
     */
    private static class SizeTask extends ComputeTaskAdapter<Object, Integer> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Cache name. */
        private final String cacheName;

        /** Affinity topology version. */
        private final AffinityTopologyVersion topVer;

        /** Peek modes. */
        private final CachePeekMode[] peekModes;

        /**
         * @param cacheName Cache name.
         * @param topVer Affinity topology version.
         * @param peekModes Cache peek modes.
         */
        public SizeTask(String cacheName, AffinityTopologyVersion topVer, CachePeekMode[] peekModes) {
            this.cacheName = cacheName;
            this.topVer = topVer;
            this.peekModes = peekModes;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Object arg) throws IgniteException {
            Map<ComputeJob, ClusterNode> jobs = new HashMap();

            for (ClusterNode node : subgrid)
                jobs.put(new SizeJob(cacheName, topVer, peekModes), node);

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            IgniteException e = res.getException();

            if (e != null) {
                if (e instanceof ClusterTopologyException)
                    return ComputeJobResultPolicy.WAIT;

                throw new IgniteException("Remote job threw exception.", e);
            }

            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteException {
            int size = 0;

            for (ComputeJobResult res : results) {
                if (res.getException() == null && res != null)
                    size += res.<Integer>getData();
            }

            return size;
        }
    }

    /**
     * Clear task.
     */
    private static class ClearTask<K> extends ComputeTaskAdapter<Object, Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Cache name. */
        private final String cacheName;

        /** Affinity topology version. */
        private final AffinityTopologyVersion topVer;

        /** Keys to clear. */
        private final Set<? extends K> keys;

        /**
         * @param cacheName Cache name.
         * @param topVer Affinity topology version.
         * @param keys Keys to clear.
         */
        public ClearTask(String cacheName, AffinityTopologyVersion topVer, Set<? extends K> keys) {
            this.cacheName = cacheName;
            this.topVer = topVer;
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Object arg) throws IgniteException {
            Map<ComputeJob, ClusterNode> jobs = new HashMap();

            for (ClusterNode node : subgrid) {
                jobs.put(keys == null ? new GlobalClearAllJob(cacheName, topVer) :
                        new GlobalClearKeySetJob<K>(cacheName, topVer, keys),
                    node);
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            IgniteException e = res.getException();

            if (e != null) {
                if (e instanceof ClusterTopologyException)
                    return ComputeJobResultPolicy.WAIT;

                throw new IgniteException("Remote job threw exception.", e);
            }

            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }
}

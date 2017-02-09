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

import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.ComputeTaskInternalFuture;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteTransactionsEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.affinity.GridCacheAffinityImpl;
import org.apache.ignite.internal.processors.cache.distributed.IgniteExternalizableExpiryPolicy;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalEx;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheRawVersionedEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerEntry;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.processors.dr.IgniteDrDataStreamerCacheUpdater;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheEntryFilter;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridEmbeddedFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.C2;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.CIX2;
import org.apache.ignite.internal.util.typedef.CIX3;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.GPC;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.jsr166.LongAdder8;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_KEY_VALIDATION_DISABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_RETRIES_COUNT;
import static org.apache.ignite.internal.GridClosureCallMode.BROADCAST;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_LOAD;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_NO_FAILOVER;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_SUBGRID;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

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

    /** */
    public static final IgniteProductVersion LOAD_CACHE_JOB_SINCE = IgniteProductVersion.fromString("1.5.7");

    /** */
    public static final IgniteProductVersion LOAD_CACHE_JOB_V2_SINCE = IgniteProductVersion.fromString("1.5.19");

    /** Deserialization stash. */
    private static final ThreadLocal<IgniteBiTuple<String, String>> stash = new ThreadLocal<IgniteBiTuple<String,
        String>>() {
        @Override protected IgniteBiTuple<String, String> initialValue() {
            return new IgniteBiTuple<>();
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
    private IgniteConfiguration gridCfg;

    /** Cache metrics. */
    protected CacheMetricsImpl metrics;

    /** Cache localMxBean. */
    private CacheMetricsMXBean locMxBean;

    /** Cache mxBean. */
    private CacheMetricsMXBean clusterMxBean;

    /** Logger. */
    protected IgniteLogger log;

    /** Logger. */
    protected IgniteLogger txLockMsgLog;

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
        this(ctx, null);
    }

    /**
     * @param ctx Cache context.
     * @param map Concurrent map.
     */
    @SuppressWarnings({"OverriddenMethodCallDuringObjectConstruction", "deprecation"})
    protected GridCacheAdapter(final GridCacheContext<K, V> ctx, @Nullable GridCacheConcurrentMap map) {
        assert ctx != null;

        this.ctx = ctx;

        gridCfg = ctx.gridConfig();
        cacheCfg = ctx.config();

        locNodeId = ctx.gridConfig().getNodeId();

        this.map = map;

        log = ctx.logger(getClass());
        txLockMsgLog = ctx.shared().txLockMessageLogger();

        metrics = new CacheMetricsImpl(ctx);

        locMxBean = new CacheLocalMetricsMXBeanImpl(this);
        clusterMxBean = new CacheClusterMetricsMXBeanImpl(this);

        FileSystemConfiguration[] igfsCfgs = gridCfg.getFileSystemConfiguration();

        if (igfsCfgs != null) {
            for (FileSystemConfiguration igfsCfg : igfsCfgs) {
                if (F.eq(ctx.name(), igfsCfg.getDataCacheName())) {
                    if (!ctx.isNear()) {
                        igfsDataCache = true;
                        igfsDataCacheSize = new LongAdder8();

                        igfsDataSpaceMax = igfsCfg.getMaxSpaceSize();

                        // Do we have limits?
                        if (igfsDataSpaceMax <= 0)
                            igfsDataSpaceMax = Long.MAX_VALUE;
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
     * Increments map public size.
     * @param e Map entry.
     */
    public void incrementSize(GridCacheMapEntry e) {
        map.incrementPublicSize(e);
    }

    /**
     * Decrements map public size.
     * @param e Map entry.
     */
    public void decrementSize(GridCacheMapEntry e) {
        map.decrementPublicSize(e);
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
    @Override public final Affinity<K> affinity() {
        return aff;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "RedundantCast"})
    @Override public final <K1, V1> IgniteInternalCache<K1, V1> cache() {
        return (IgniteInternalCache<K1, V1>)this;
    }

    /** {@inheritDoc} */
    @Override public final GridCacheProxyImpl<K, V> forSubjectId(UUID subjId) {
        CacheOperationContext opCtx = new CacheOperationContext(false, subjId, false, null, false, null);

        return new GridCacheProxyImpl<>(ctx, this, opCtx);
    }

    /** {@inheritDoc} */
    @Override public final boolean skipStore() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public final GridCacheProxyImpl<K, V> setSkipStore(boolean skipStore) {
        CacheOperationContext opCtx = new CacheOperationContext(true, null, false, null, false, null);

        return new GridCacheProxyImpl<>(ctx, this, opCtx);
    }

    /** {@inheritDoc} */
    @Override public final <K1, V1> GridCacheProxyImpl<K1, V1> keepBinary() {
        CacheOperationContext opCtx = new CacheOperationContext(false, null, true, null, false, null);

        return new GridCacheProxyImpl<>((GridCacheContext<K1, V1>)ctx, (GridCacheAdapter<K1, V1>)this, opCtx);
    }

    /** {@inheritDoc} */
    @Nullable @Override public final ExpiryPolicy expiry() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public final GridCacheProxyImpl<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        assert !CU.isUtilityCache(ctx.name());
        assert !CU.isAtomicsCache(ctx.name());
        assert !CU.isMarshallerCache(ctx.name());

        CacheOperationContext opCtx = new CacheOperationContext(false, null, false, plc, false, null);

        return new GridCacheProxyImpl<>(ctx, this, opCtx);
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalCache<K, V> withNoRetries() {
        CacheOperationContext opCtx = new CacheOperationContext(false, null, false, null, true, null);

        return new GridCacheProxyImpl<>(ctx, this, opCtx);
    }

    /** {@inheritDoc} */
    @Override public final CacheConfiguration configuration() {
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
     * @param createTtl TTL for create operation.
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
        long createTtl,
        long accessTtl);

    /**
     * Post constructor initialization for subclasses.
     */
    protected void init() {
        // No-op.
    }

    /**
     * @return Entry factory.
     */
    protected abstract GridCacheMapEntryFactory entryFactory();

    /**
     * Starts this cache. Child classes should override this method
     * to provide custom start-up behavior.
     *
     * @throws IgniteCheckedException If start failed.
     */
    public void start() throws IgniteCheckedException {
        if (map == null) {
            int initSize = ctx.config().getStartSize();

            if (!isLocal())
                initSize /= ctx.affinity().partitions();

            map = new GridCacheConcurrentMapImpl(ctx, entryFactory(), initSize);
        }
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
    @Override public final boolean isEmpty() {
        try {
            return localSize(CachePeekModes.ONHEAP_ONLY) == 0;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean containsKey(K key) {
        try {
            return containsKeyAsync(key).get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalFuture<Boolean> containsKeyAsync(K key) {
        A.notNull(key, "key");

        return (IgniteInternalFuture)getAsync(
            key,
            /*force primary*/false,
            /*skip tx*/false,
            /*subj id*/null,
            /*task name*/null,
            /*deserialize binary*/false,
            /*skip values*/true,
            /*can remap*/true,
            false
        );
    }

    /** {@inheritDoc} */
    @Override public final boolean containsKeys(Collection<? extends K> keys) {
        try {
            return containsKeysAsync(keys).get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalFuture<Boolean> containsKeysAsync(final Collection<? extends K> keys) {
        A.notNull(keys, "keys");

        return getAllAsync(
            keys,
            /*force primary*/false,
            /*skip tx*/false,
            /*subj id*/null,
            /*task name*/null,
            /*deserialize binary*/false,
            /*skip values*/true,
            /*can remap*/true,
            false
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
    @Override public final Iterable<Cache.Entry<K, V>> localEntries(CachePeekMode[] peekModes) throws IgniteCheckedException {
        assert peekModes != null;

        ctx.checkSecurity(SecurityPermission.CACHE_READ);

        PeekModes modes = parsePeekModes(peekModes, false);

        Collection<Iterator<Cache.Entry<K, V>>> its = new ArrayList<>();

        final boolean keepBinary = ctx.keepBinary();

        if (ctx.isLocal()) {
            modes.primary = true;
            modes.backup = true;

            if (modes.heap)
                its.add(iterator(map.entries().iterator(), !keepBinary));
        }
        else if (modes.heap) {
            if (modes.near && ctx.isNear())
                its.add(ctx.near().nearEntries().iterator());

            if (modes.primary || modes.backup) {
                GridDhtCacheAdapter<K, V> cache = ctx.isNear() ? ctx.near().dht() : ctx.dht();

                its.add(cache.localEntriesIterator(modes.primary, modes.backup, keepBinary));
            }
        }

        // Swap and offheap are disabled for near cache.
        if (modes.primary || modes.backup) {
            AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

            GridCacheSwapManager swapMgr = ctx.isNear() ? ctx.near().dht().context().swap() : ctx.swap();

            if (modes.swap)
                its.add(swapMgr.<K, V>swapIterator(modes.primary, modes.backup, topVer, keepBinary));

            if (modes.offheap)
                its.add(swapMgr.<K, V>offheapIterator(modes.primary, modes.backup, topVer, keepBinary));
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
    @Nullable @Override public final V localPeek(K key,
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
                    boolean keyPrimary = ctx.affinity().primaryByPartition(ctx.localNode(), part, topVer);

                    if (keyPrimary) {
                        if (!modes.primary)
                            return null;

                        nearKey = false;
                    }
                    else {
                        boolean keyBackup = ctx.affinity().partitionBelongs(ctx.localNode(), part, topVer);

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
                    nearKey = !ctx.affinity().partitionBelongs(ctx.localNode(), part, topVer);

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

                    cacheVal = swapMgr.readValue(cacheKey, modes.offheap, modes.swap);
                }
            }
            else
                cacheVal = localCachePeek0(cacheKey, modes.heap, modes.offheap, modes.swap, plc);

            Object val = ctx.unwrapBinaryIfNeeded(cacheVal, ctx.keepBinary(), false);

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
    @SuppressWarnings("ConstantConditions")
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

            return swapMgr.readValue(key, offheap, swap);
        }

        return null;
    }

    /**
     * Undeploys and removes all entries for class loader.
     *
     * @param ldr Class loader to undeploy.
     */
    public final void onUndeploy(ClassLoader ldr) {
        ctx.deploy().onUndeploy(ldr, context());
    }

    /**
     *
     * @param key Entry key.
     * @return Entry or <tt>null</tt>.
     */
    @Nullable public final GridCacheEntryEx peekEx(KeyCacheObject key) {
        return entry0(key, ctx.affinity().affinityTopologyVersion(), false, false);
    }

    /**
     *
     * @param key Entry key.
     * @return Entry or <tt>null</tt>.
     */
    @Nullable public final GridCacheEntryEx peekEx(Object key) {
        return entry0(ctx.toCacheKeyObject(key), ctx.affinity().affinityTopologyVersion(), false, false);
    }

    /**
     * @param key Entry key.
     * @return Entry (never {@code null}).
     */
    public final GridCacheEntryEx entryEx(Object key) {
        return entryEx(ctx.toCacheKeyObject(key), false);
    }

    /**
     * @param key Entry key.
     * @return Entry (never {@code null}).
     */
    public final GridCacheEntryEx entryEx(KeyCacheObject key) {
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
        GridCacheMapEntry cur = map.getEntry(key);

        if (cur == null || cur.obsolete()) {
            cur = map.putEntryIfObsoleteOrAbsent(
                topVer,
                key,
                null,
                create, touch);
        }

        return cur;
    }

    /**
     * @return Set of internal cached entry representations.
     */
    public final Iterable<? extends GridCacheEntryEx> entries() {
        return allEntries();
    }

    /**
     * @return Set of internal cached entry representations.
     */
    public final Iterable<? extends GridCacheEntryEx> allEntries() {
        return map.entries();
    }

    /** {@inheritDoc} */
    @Override public final Set<Cache.Entry<K, V>> entrySet() {
        return entrySet((CacheEntryPredicate[])null);
    }

    /** {@inheritDoc} */
    @Override public final Set<Cache.Entry<K, V>> entrySetx(final CacheEntryPredicate... filter) {
        boolean keepBinary = ctx.keepBinary();

        return new EntrySet(map.entrySet(filter), keepBinary);
    }

    /** {@inheritDoc} */
    @Override public Set<Cache.Entry<K, V>> entrySet(int part) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public final Set<K> keySet() {
        return new KeySet(map.entrySet());
    }

    /** {@inheritDoc} */
    @Override public final Set<K> keySetx() {
        return keySet();
    }

    /** {@inheritDoc} */
    @Override public final Set<K> primaryKeySet() {
        return new KeySet(map.entrySet(CU.cachePrimary(ctx.grid().affinity(ctx.name()), ctx.localNode())));
    }

    /** {@inheritDoc} */
    @Override public Iterable<V> values() {
        return values((CacheEntryPredicate[])null);
    }

    /**
     * Collection of values cached on this node. You cannot modify this collection.
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
    public final Iterable<V> values(final CacheEntryPredicate... filter) {
        return new Iterable<V>() {
            @Override public Iterator<V> iterator() {
                return new Iterator<V>() {
                    private final Iterator<? extends GridCacheEntryEx> it = entries().iterator();

                    @Override public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override public V next() {
                        return (V) it.next().wrap().getValue();
                    }

                    @Override public void remove() {
                        throw new UnsupportedOperationException("remove");
                    }
                };
            }
        };
    }

    /**
     *
     * @param key Entry key.
     */
    public final void removeIfObsolete(KeyCacheObject key) {
        assert key != null;

        GridCacheMapEntry entry = map.getEntry(key);

        if (entry != null && entry.obsolete())
            removeEntry(entry);
    }

    /**
     * Split clearLocally all task into multiple runnables.
     *
     * @param srv Whether to clear server cache.
     * @param near Whether to clear near cache.
     * @param readers Whether to clear readers.
     * @return Split runnables.
     */
    public List<GridCacheClearAllRunnable<K, V>> splitClearLocally(boolean srv, boolean near, boolean readers) {
        if ((isNear() && near) || (!isNear() && srv)) {
            int keySize = size();

            int cnt = Math.min(keySize / CLEAR_ALL_SPLIT_THRESHOLD + (keySize % CLEAR_ALL_SPLIT_THRESHOLD != 0 ? 1 : 0),
                Runtime.getRuntime().availableProcessors());

            if (cnt == 0)
                cnt = 1; // Still perform cleanup since there could be entries in swap.

            GridCacheVersion obsoleteVer = ctx.versions().next();

            List<GridCacheClearAllRunnable<K, V>> res = new ArrayList<>(cnt);

            for (int i = 0; i < cnt; i++)
                res.add(new GridCacheClearAllRunnable<>(this, obsoleteVer, i, cnt, readers));

            return res;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Override public boolean clearLocally(K key) {
        return clearLocally0(key, false);
    }

    /** {@inheritDoc} */
    @Override public void clearLocallyAll(Set<? extends K> keys, boolean srv, boolean near, boolean readers) {
        if (keys != null && ((isNear() && near) || (!isNear() && srv))) {
            for (K key : keys)
                clearLocally0(key, readers);
        }
    }

    /** {@inheritDoc} */
    @Override public void clearLocally(boolean srv, boolean near, boolean readers) {
        ctx.checkSecurity(SecurityPermission.CACHE_REMOVE);

        List<GridCacheClearAllRunnable<K, V>> jobs = splitClearLocally(srv, near, readers);

        if (!F.isEmpty(jobs)) {
            ExecutorService execSvc = null;

            if (jobs.size() > 1) {
                execSvc = Executors.newFixedThreadPool(jobs.size() - 1);

                for (int i = 1; i < jobs.size(); i++)
                    execSvc.execute(jobs.get(i));
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

    /** {@inheritDoc} */
    @Override public void clear() throws IgniteCheckedException {
        clear((Set<? extends K>)null);
    }

    /** {@inheritDoc} */
    @Override public void clear(K key) throws IgniteCheckedException {
        clear(Collections.singleton(key));
    }

    /** {@inheritDoc} */
    @Override public void clearAll(Set<? extends K> keys) throws IgniteCheckedException {
        clear(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAsync() {
        return clearAsync((Set<? extends K>)null);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAsync(K key) {
        return clearAsync(Collections.singleton(key));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> clearAllAsync(Set<? extends K> keys) {
        return clearAsync(keys);
    }

    /**
     * @param keys Keys to clear.
     * @throws IgniteCheckedException In case of error.
     */
    private void clear(@Nullable Set<? extends K> keys) throws IgniteCheckedException {
        executeClearTask(keys, false).get();
        executeClearTask(keys, true).get();
    }

    /**
     * @param keys Keys to clear or {@code null} if all cache should be cleared.
     * @return Future.
     */
    private IgniteInternalFuture<?> clearAsync(@Nullable final Set<? extends K> keys) {
        return executeClearTask(keys, false).chain(new CX1<IgniteInternalFuture<?>, Object>() {
            @Override public Object applyx(IgniteInternalFuture<?> fut) throws IgniteCheckedException {
                executeClearTask(keys, true).get();

                return null;
            }
        });
    }

    /**
     * @param keys Keys to clear.
     * @param near Near cache flag.
     * @return Future.
     */
    private IgniteInternalFuture<?> executeClearTask(@Nullable Set<? extends K> keys, boolean near) {
        Collection<ClusterNode> srvNodes = ctx.grid().cluster().forCacheNodes(name(), !near, near, false).nodes();

        if (!srvNodes.isEmpty()) {
            ctx.kernalContext().task().setThreadContext(TC_SUBGRID, srvNodes);

            return ctx.kernalContext().task().execute(
                new ClearTask(ctx.name(), ctx.affinity().affinityTopologyVersion(), keys, near), null);
        }
        else
            return new GridFinishedFuture<>();
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
                    e.clear(obsoleteVer, readers);
            }
            catch (IgniteCheckedException ex) {
                U.error(log, "Failed to clearLocally entry (will continue to clearLocally other entries): " + e,
                    ex);
            }
        }
    }

    /**
     * @param entry Removes entry from cache if currently mapped value is the same as passed.
     */
    public final void removeEntry(GridCacheEntryEx entry) {
        boolean rmvd = map.removeEntry(entry);

        if (log.isDebugEnabled()) {
            if (rmvd)
                log.debug("Removed entry from cache: " + entry);
            else
                log.debug("Remove will not be done for key (entry got replaced or removed): " + entry.key());
        }
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
    @Override public final V getForcePrimary(K key) throws IgniteCheckedException {
        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAllAsync(
            F.asList(key),
            /*force primary*/true,
            /*skip tx*/false,
            /*subject id*/null,
            taskName,
            /*deserialize cache objects*/true,
            /*skip values*/false,
            /*can remap*/true,
            false
        ).get().get(key);
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalFuture<V> getForcePrimaryAsync(final K key) {
        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAllAsync(
            Collections.singletonList(key),
            /*force primary*/true,
            /*skip tx*/false,
            null,
            taskName,
            true,
            false,
            /*can remap*/true,
            false
        ).chain(new CX1<IgniteInternalFuture<Map<K, V>>, V>() {
            @Override public V applyx(IgniteInternalFuture<Map<K, V>> e) throws IgniteCheckedException {
                return e.get().get(key);
            }
        });
    }

    /** {@inheritDoc} */
    public final V getTopologySafe(K key) throws IgniteCheckedException {
        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAllAsync(
            F.asList(key),
            /*force primary*/false,
            /*skip tx*/false,
            /*subject id*/null,
            taskName,
            /*deserialize cache objects*/true,
            /*skip values*/false,
            /*can remap*/false,
            false
        ).get().get(key);
    }

    /** {@inheritDoc} */
    @Nullable @Override public final Map<K, V> getAllOutTx(Set<? extends K> keys) throws IgniteCheckedException {
        return getAllOutTxAsync(keys).get();
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalFuture<Map<K, V>> getAllOutTxAsync(Set<? extends K> keys) {
        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAllAsync(keys,
            !ctx.config().isReadFromBackup(),
            /*skip tx*/true,
            null,
            taskName,
            !ctx.keepBinary(),
            /*skip values*/false,
            /*can remap*/true,
            false);
    }

    /** {@inheritDoc} */
    @Nullable @Override public V get(K key) throws IgniteCheckedException {
        A.notNull(key, "key");

        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        boolean keepBinary = ctx.keepBinary();

        if (keepBinary)
            key = (K)ctx.toCacheKeyObject(key);

        V val = get(key, !keepBinary, false);

        if (ctx.config().getInterceptor() != null) {
            key = keepBinary ? (K) ctx.unwrapBinaryIfNeeded(key, true, false) : key;

            val = (V)ctx.config().getInterceptor().onGet(key, val);
        }

        if (statsEnabled)
            metrics0().addGetTimeNanos(System.nanoTime() - start);

        return val;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheEntry<K, V> getEntry(K key) throws IgniteCheckedException {
        A.notNull(key, "key");

        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        boolean keepBinary = ctx.keepBinary();

        if (keepBinary)
            key = (K)ctx.toCacheKeyObject(key);

        T2<V, GridCacheVersion> t = (T2<V, GridCacheVersion>)get(key, !keepBinary, true);

        CacheEntry<K, V> val = t != null ? new CacheEntryImplEx<>(
            keepBinary ? (K)ctx.unwrapBinaryIfNeeded(key, true, false) : key,
            t.get1(),
            t.get2())
            : null;

        if (ctx.config().getInterceptor() != null) {
            key = keepBinary ? (K) ctx.unwrapBinaryIfNeeded(key, true, false) : key;

            V val0 = (V)ctx.config().getInterceptor().onGet(key, t != null ? val.getValue() : null);

            val = (val0 != null) ? new CacheEntryImplEx<>(key, val0, t != null ? t.get2() : null) : null;
        }

        if (statsEnabled)
            metrics0().addGetTimeNanos(System.nanoTime() - start);

        return val;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAsync(final K key) {
        A.notNull(key, "key");

        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        final boolean keepBinary = ctx.keepBinary();

        final K key0 = keepBinary ? (K)ctx.toCacheKeyObject(key) : key;

        IgniteInternalFuture<V> fut = getAsync(key, !keepBinary, false);

        if (ctx.config().getInterceptor() != null)
            fut = fut.chain(new CX1<IgniteInternalFuture<V>, V>() {
                @Override public V applyx(IgniteInternalFuture<V> f) throws IgniteCheckedException {
                    K key = keepBinary ? (K)ctx.unwrapBinaryIfNeeded(key0, true, false) : key0;

                    return (V)ctx.config().getInterceptor().onGet(key, f.get());
                }
            });

        if (statsEnabled)
            fut.listen(new UpdateGetTimeStatClosure<V>(metrics0(), start));

        return fut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<CacheEntry<K, V>> getEntryAsync(final K key) {
        A.notNull(key, "key");

        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        final boolean keepBinary = ctx.keepBinary();

        final K key0 = keepBinary ? (K)ctx.toCacheKeyObject(key) : key;

        IgniteInternalFuture<T2<V, GridCacheVersion>> fut =
            (IgniteInternalFuture<T2<V, GridCacheVersion>>)getAsync(key0, !keepBinary, true);

        final boolean intercept = ctx.config().getInterceptor() != null;

        IgniteInternalFuture<CacheEntry<K, V>> fr = fut.chain(
            new CX1<IgniteInternalFuture<T2<V, GridCacheVersion>>, CacheEntry<K, V>>() {
                @Override public CacheEntry<K, V> applyx(IgniteInternalFuture<T2<V, GridCacheVersion>> f)
                    throws IgniteCheckedException {
                    T2<V, GridCacheVersion> t = f.get();

                K key = keepBinary ? (K)ctx.unwrapBinaryIfNeeded(key0, true, false) : key0;

                CacheEntry val = t != null ? new CacheEntryImplEx<>(
                    key,
                    t.get1(),
                    t.get2())
                    : null;

                if (intercept) {
                    V val0 = (V)ctx.config().getInterceptor().onGet(key, t != null ? val.getValue() : null);

                    return val0 != null ? new CacheEntryImplEx(key, val0, t != null ? t.get2() : null) : null;
                }
                else
                    return val;
            }
        });

        if (statsEnabled)
            fut.listen(new UpdateGetTimeStatClosure<T2<V, GridCacheVersion>>(metrics0(), start));

        return fr;
    }

    /** {@inheritDoc} */
    @Override public final Map<K, V> getAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        A.notNull(keys, "keys");

        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        Map<K, V> map = getAll0(keys, !ctx.keepBinary(), false);

        if (ctx.config().getInterceptor() != null)
            map = interceptGet(keys, map);

        if (statsEnabled)
            metrics0().addGetTimeNanos(System.nanoTime() - start);

        return map;
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheEntry<K, V>> getEntries(@Nullable Collection<? extends K> keys)
        throws IgniteCheckedException {
        A.notNull(keys, "keys");

        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        Map<K, T2<V, GridCacheVersion>> map = (Map<K, T2<V, GridCacheVersion>>)getAll0(keys, !ctx.keepBinary(), true);

        Collection<CacheEntry<K, V>> res = new HashSet<>();

        if (ctx.config().getInterceptor() != null)
            res = interceptGetEntries(keys, map);
        else
            for (Map.Entry<K, T2<V, GridCacheVersion>> e : map.entrySet())
                res.add(new CacheEntryImplEx<>(e.getKey(), e.getValue().get1(), e.getValue().get2()));

        if (statsEnabled)
            metrics0().addGetTimeNanos(System.nanoTime() - start);

        return res;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<K, V>> getAllAsync(@Nullable final Collection<? extends K> keys) {
        A.notNull(keys, "keys");

        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        IgniteInternalFuture<Map<K, V>> fut = getAllAsync(keys, !ctx.keepBinary(), false);

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

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Collection<CacheEntry<K, V>>> getEntriesAsync(
        @Nullable final Collection<? extends K> keys) {
        A.notNull(keys, "keys");

        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        IgniteInternalFuture<Map<K, T2<V, GridCacheVersion>>> fut =
            (IgniteInternalFuture<Map<K, T2<V, GridCacheVersion>>>)
                ((IgniteInternalFuture)getAllAsync(keys, !ctx.keepBinary(), true));

        final boolean intercept = ctx.config().getInterceptor() != null;

        IgniteInternalFuture<Collection<CacheEntry<K, V>>> rf =
            fut.chain(new CX1<IgniteInternalFuture<Map<K, T2<V, GridCacheVersion>>>, Collection<CacheEntry<K, V>>>() {
                @Override public Collection<CacheEntry<K, V>> applyx(
                    IgniteInternalFuture<Map<K, T2<V, GridCacheVersion>>> f) throws IgniteCheckedException {
                    if (intercept)
                        return interceptGetEntries(keys, f.get());
                    else {
                        Map<K, CacheEntry<K, V>> res = U.newHashMap(f.get().size());

                        for (Map.Entry<K, T2<V, GridCacheVersion>> e : f.get().entrySet())
                            res.put(e.getKey(),
                                new CacheEntryImplEx<>(e.getKey(), e.getValue().get1(), e.getValue().get2()));

                        return res.values();
                    }
                }
            });

        if (statsEnabled)
            fut.listen(new UpdateGetTimeStatClosure<Map<K, T2<V, GridCacheVersion>>>(metrics0(), start));

        return rf;
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
     * Applies cache interceptor on result of 'getEntries' operation.
     *
     * @param keys All requested keys.
     * @param map Result map.
     * @return Map with values returned by cache interceptor..
     */
    @SuppressWarnings("IfMayBeConditional")
    private Collection<CacheEntry<K, V>> interceptGetEntries(
        @Nullable Collection<? extends K> keys, Map<K, T2<V, GridCacheVersion>> map) {
        Map<K, CacheEntry<K, V>> res;

        if (F.isEmpty(keys)) {
            assert map.isEmpty();

            return Collections.emptySet();
        }

        res = U.newHashMap(keys.size());

        CacheInterceptor<K, V> interceptor = cacheCfg.getInterceptor();

        assert interceptor != null;

        for (Map.Entry<K, T2<V, GridCacheVersion>> e : map.entrySet()) {
            V val = interceptor.onGet(e.getKey(), e.getValue().get1());

            if (val != null)
                res.put(e.getKey(), new CacheEntryImplEx<>(e.getKey(), val, e.getValue().get2()));
        }

        if (map.size() != keys.size()) { // Not all requested keys were in cache.
            for (K key : keys) {
                if (key != null) {
                    if (!map.containsKey(key)) {
                        V val = interceptor.onGet(key, null);

                        if (val != null)
                            res.put(key, new CacheEntryImplEx<>(key, val, null));
                    }
                }
            }
        }

        return res.values();
    }

    /**
     * @param key Key.
     * @param forcePrimary Force primary.
     * @param skipTx Skip tx.
     * @param subjId Subj Id.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary.
     * @param skipVals Skip values.
     * @param canRemap Can remap flag.
     * @param needVer Need version.
     * @return Future for the get operation.
     */
    protected IgniteInternalFuture<V> getAsync(
        final K key,
        boolean forcePrimary,
        boolean skipTx,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializeBinary,
        final boolean skipVals,
        boolean canRemap,
        final boolean needVer
    ) {
        return getAllAsync(Collections.singletonList(key),
            forcePrimary,
            skipTx,
            subjId,
            taskName,
            deserializeBinary,
            skipVals,
            canRemap,
            needVer).chain(
            new CX1<IgniteInternalFuture<Map<K, V>>, V>() {
                @Override public V applyx(IgniteInternalFuture<Map<K, V>> e) throws IgniteCheckedException {
                    Map<K, V> map = e.get();

                    assert map.isEmpty() || map.size() == 1 : map.size();

                    if (skipVals) {
                        Boolean val = map.isEmpty() ? false : (Boolean)F.firstValue(map);

                        return (V)(val);
                    }

                    return F.firstValue(map);
                }
            });
    }

    /**
     * @param keys Keys.
     * @param forcePrimary Force primary.
     * @param skipTx Skip tx.
     * @param subjId Subj Id.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary.
     * @param skipVals Skip values.
     * @param canRemap Can remap flag.
     * @param needVer Need version.
     * @return Future for the get operation.
     * @see GridCacheAdapter#getAllAsync(Collection)
     */
    protected IgniteInternalFuture<Map<K, V>> getAllAsync(
        @Nullable Collection<? extends K> keys,
        boolean forcePrimary,
        boolean skipTx,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializeBinary,
        boolean skipVals,
        boolean canRemap,
        final boolean needVer
    ) {
        CacheOperationContext opCtx = ctx.operationContextPerCall();

        subjId = ctx.subjectIdPerCall(subjId, opCtx);

        return getAllAsync(keys,
            null,
            opCtx == null || !opCtx.skipStore(),
            !skipTx,
            subjId,
            taskName,
            deserializeBinary,
            forcePrimary,
            skipVals ? null : expiryPolicy(opCtx != null ? opCtx.expiry() : null),
            skipVals,
            canRemap,
            needVer);
    }

    /**
     * @param keys Keys.
     * @param readerArgs Near cache reader will be added if not null.
     * @param readThrough Read through.
     * @param checkTx Check tx.
     * @param subjId Subj Id.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary.
     * @param forcePrimary Froce primary.
     * @param expiry Expiry policy.
     * @param skipVals Skip values.
     * @param canRemap Can remap flag.
     * @param needVer Need version.
     * @return Future for the get operation.
     * @see GridCacheAdapter#getAllAsync(Collection)
     */
    public final IgniteInternalFuture<Map<K, V>> getAllAsync(@Nullable final Collection<? extends K> keys,
        @Nullable final ReaderArguments readerArgs,
        boolean readThrough,
        boolean checkTx,
        @Nullable final UUID subjId,
        final String taskName,
        final boolean deserializeBinary,
        final boolean forcePrimary,
        @Nullable IgniteCacheExpiryPolicy expiry,
        final boolean skipVals,
        boolean canRemap,
        final boolean needVer
    ) {
        ctx.checkSecurity(SecurityPermission.CACHE_READ);

        if (keyCheck)
            validateCacheKeys(keys);

        return getAllAsync0(ctx.cacheKeysView(keys),
            readerArgs,
            readThrough,
            checkTx,
            subjId,
            taskName,
            deserializeBinary,
            expiry,
            skipVals,
            false,
            canRemap,
            needVer);
    }

    /**
     * @param keys Keys.
     * @param readerArgs Near cache reader will be added if not null.
     * @param readThrough Read-through flag.
     * @param checkTx Check local transaction flag.
     * @param subjId Subject ID.
     * @param taskName Task name/
     * @param deserializeBinary Deserialize binary flag.
     * @param expiry Expiry policy.
     * @param skipVals Skip values flag.
     * @param keepCacheObjects Keep cache objects.
     * @param canRemap Can remap flag.
     * @param needVer If {@code true} returns values as tuples containing value and version.
     * @return Future.
     */
    protected final <K1, V1> IgniteInternalFuture<Map<K1, V1>> getAllAsync0(
        @Nullable final Collection<KeyCacheObject> keys,
        @Nullable final ReaderArguments readerArgs,
        final boolean readThrough,
        boolean checkTx,
        @Nullable final UUID subjId,
        final String taskName,
        final boolean deserializeBinary,
        @Nullable final IgniteCacheExpiryPolicy expiry,
        final boolean skipVals,
        final boolean keepCacheObjects,
        boolean canRemap,
        final boolean needVer
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
                final AffinityTopologyVersion topVer = tx == null ?
                    (canRemap ?
                        ctx.affinity().affinityTopologyVersion() : ctx.shared().exchange().readyAffinityVersion()) :
                    tx.topologyVersion();

                int keysSize = keys.size();

                final Map<K1, V1> map = keysSize == 1 ?
                    (Map<K1, V1>)new IgniteBiTuple<>() :
                    U.<K1, V1>newHashMap(keysSize);

                final boolean storeEnabled = !skipVals && readThrough && ctx.readThrough();

                final boolean needEntry = storeEnabled || ctx.isSwapOrOffheapEnabled();

                Map<KeyCacheObject, EntryGetResult> misses = null;

                for (KeyCacheObject key : keys) {
                    while (true) {
                        GridCacheEntryEx entry = needEntry ? entryEx(key) : peekEx(key);

                        if (entry == null) {
                            if (!skipVals && ctx.config().isStatisticsEnabled())
                                ctx.cache().metrics0().onRead(false);

                            break;
                        }

                        try {
                            EntryGetResult res;

                            boolean evt = !skipVals;
                            boolean updateMetrics = !skipVals;

                            if (storeEnabled) {
                                res = entry.innerGetAndReserveForLoad(ctx.isSwapOrOffheapEnabled(),
                                    updateMetrics,
                                    evt,
                                    subjId,
                                    taskName,
                                    expiry,
                                    !deserializeBinary,
                                    readerArgs);

                                assert res != null;

                                if (res.value() == null) {
                                    if (misses == null)
                                        misses = new HashMap<>();

                                    misses.put(key, res);

                                    res = null;
                                }
                            }
                            else {
                                res = entry.innerGetVersioned(
                                    null,
                                    null,
                                    ctx.isSwapOrOffheapEnabled(),
                                    /*unmarshal*/true,
                                    updateMetrics,
                                    evt,
                                    subjId,
                                    null,
                                    taskName,
                                    expiry,
                                    !deserializeBinary,
                                    readerArgs);

                                if (res == null)
                                    ctx.evicts().touch(entry, topVer);
                            }

                            if (res != null) {
                                ctx.addResult(map,
                                    key,
                                    res.value(),
                                    skipVals,
                                    keepCacheObjects,
                                    deserializeBinary,
                                    true,
                                    needVer ? res.version() : null);

                                if (tx == null || (!tx.implicit() && tx.isolation() == READ_COMMITTED))
                                    ctx.evicts().touch(entry, topVer);

                                if (keysSize == 1)
                                    // Safe to return because no locks are required in READ_COMMITTED mode.
                                    return new GridFinishedFuture<>(map);
                            }

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignored) {
                            if (log.isDebugEnabled())
                                log.debug("Got removed entry in getAllAsync(..) method (will retry): " + key);
                        }
                    }
                }

                if (storeEnabled && misses != null) {
                    final Map<KeyCacheObject, EntryGetResult> loadKeys = misses;

                    final IgniteTxLocalAdapter tx0 = tx;

                    final Collection<KeyCacheObject> loaded = new HashSet<>();

                    return new GridEmbeddedFuture(
                        ctx.closures().callLocalSafe(ctx.projectSafe(new GPC<Map<K1, V1>>() {
                            @Override public Map<K1, V1> call() throws Exception {
                                ctx.store().loadAll(null/*tx*/, loadKeys.keySet(), new CI2<KeyCacheObject, Object>() {
                                    @Override public void apply(KeyCacheObject key, Object val) {
                                        EntryGetResult res = loadKeys.get(key);

                                        if (res == null || val == null)
                                            return;

                                        loaded.add(key);

                                        CacheObject cacheVal = ctx.toCacheObject(val);

                                        while (true) {
                                            GridCacheEntryEx entry = entryEx(key);

                                            try {
                                                T2<CacheObject, GridCacheVersion> verVal = entry.versionedValue(
                                                    cacheVal,
                                                    res.version(),
                                                    null,
                                                    expiry,
                                                    readerArgs);

                                                if (log.isDebugEnabled())
                                                    log.debug("Set value loaded from store into entry [" +
                                                        "oldVer=" + res.version() +
                                                        ", newVer=" + verVal.get2() + ", " +
                                                        "entry=" + entry + ']');

                                                // Don't put key-value pair into result map if value is null.
                                                if (verVal.get1() != null) {
                                                    ctx.addResult(map,
                                                        key,
                                                        verVal.get1(),
                                                        skipVals,
                                                        keepCacheObjects,
                                                        deserializeBinary,
                                                        true,
                                                        needVer ? verVal.get2() : null);
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
                                    boolean needTouch =
                                        tx0 == null || (!tx0.implicit() && tx0.isolation() == READ_COMMITTED);

                                    for (Map.Entry<KeyCacheObject, EntryGetResult> e : loadKeys.entrySet()) {
                                        if (loaded.contains(e.getKey()))
                                            continue;

                                        if (needTouch || e.getValue().reserved()) {
                                            GridCacheEntryEx entry = peekEx(e.getKey());

                                            if (entry != null) {
                                                if (e.getValue().reserved())
                                                    entry.clearReserveForLoad(e.getValue().version());

                                                if (needTouch)
                                                    ctx.evicts().touch(entry, topVer);
                                            }
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
                else
                    // Misses can be non-zero only if store is enabled.
                    assert misses == null;

                return new GridFinishedFuture<>(map);
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(e);
            }
        }
        else {
            return asyncOp(tx, new AsyncOp<Map<K1, V1>>(keys) {
                @Override public IgniteInternalFuture<Map<K1, V1>> op(IgniteTxLocalAdapter tx, AffinityTopologyVersion readyTopVer) {
                    return tx.getAllAsync(ctx,
                        readyTopVer,
                        keys,
                        deserializeBinary,
                        skipVals,
                        false,
                        !readThrough,
                        needVer);
                }
            }, ctx.operationContextPerCall());
        }
    }

    /** {@inheritDoc} */
    @Override public final V getAndPut(K key, V val) throws IgniteCheckedException {
        return getAndPut(key, val, null);
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param filter Optional filter.
     * @return Previous value.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public V getAndPut(final K key, final V val, @Nullable final CacheEntryPredicate filter)
        throws IgniteCheckedException {
        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        V prevVal = getAndPut0(key, val, filter);

        if (statsEnabled)
            metrics0().addPutAndGetTimeNanos(System.nanoTime() - start);

        return prevVal;
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param filter Optional filter.
     * @return Previous value.
     * @throws IgniteCheckedException If failed.
     */
    protected V getAndPut0(final K key, final V val, @Nullable final CacheEntryPredicate filter)
        throws IgniteCheckedException {
        return syncOp(new SyncOp<V>(true) {
            @Override public V op(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                return (V)tx.putAsync(ctx, null, key, val, true, filter).get().value();
            }

            @Override public String toString() {
                return "put [key=" + key + ", val=" + val + ", filter=" + filter + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalFuture<V> getAndPutAsync(K key, V val) {
        return getAndPutAsync(key, val, null);
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param filter Filter.
     * @return Put operation future.
     */
    protected final IgniteInternalFuture<V> getAndPutAsync(K key, V val, @Nullable CacheEntryPredicate filter) {
        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

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
    public IgniteInternalFuture<V> getAndPutAsync0(final K key,
        final V val,
        @Nullable final CacheEntryPredicate filter)
    {
        return asyncOp(new AsyncOp<V>() {
            @Override public IgniteInternalFuture<V> op(IgniteTxLocalAdapter tx, AffinityTopologyVersion readyTopVer) {
                return tx.putAsync(ctx, readyTopVer, key, val, true, filter)
                    .chain((IgniteClosure<IgniteInternalFuture<GridCacheReturn>, V>)RET2VAL);
            }

            @Override public String toString() {
                return "putAsync [key=" + key + ", val=" + val + ", filter=" + filter + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public final boolean put(final K key, final V val) throws IgniteCheckedException {
        return put(key, val, null);
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
    public boolean put(final K key, final V val, final CacheEntryPredicate filter)
        throws IgniteCheckedException {
        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        boolean stored = put0(key, val, filter);

        if (statsEnabled && stored)
            metrics0().addPutTimeNanos(System.nanoTime() - start);

        return stored;
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
    protected boolean put0(final K key, final V val, final CacheEntryPredicate filter)
        throws IgniteCheckedException {
        Boolean res = syncOp(new SyncOp<Boolean>(true) {
            @Override public Boolean op(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                return tx.putAsync(ctx, null, key, val, false, filter).get().success();
            }

            @Override public String toString() {
                return "putx [key=" + key + ", val=" + val + ", filter=" + filter + ']';
            }
        });

        assert res != null;

        return res;
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

        return asyncOp(new AsyncOp(drMap.keySet()) {
            @Override public IgniteInternalFuture op(IgniteTxLocalAdapter tx, AffinityTopologyVersion readyTopVer) {
                return tx.putAllDrAsync(ctx, drMap);
            }

            @Override public String toString() {
                return "putAllConflictAsync [drMap=" + drMap + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Nullable @Override public final <T> EntryProcessorResult<T> invoke(@Nullable AffinityTopologyVersion topVer,
        K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException {
        return invoke0(topVer, key, entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> EntryProcessorResult<T> invoke(final K key,
        final EntryProcessor<K, V, T> entryProcessor,
        final Object... args) throws IgniteCheckedException {
        return invoke0(null, key, entryProcessor, args);
    }

    /**
     * @param topVer Locked topology version.
     * @param key Key.
     * @param entryProcessor Entry processor.
     * @param args Entry processor arguments.
     * @return Invoke result.
     * @throws IgniteCheckedException If failed.
     */
    private <T> EntryProcessorResult<T> invoke0(
        @Nullable final AffinityTopologyVersion topVer,
        final K key,
        final EntryProcessor<K, V, T> entryProcessor,
        final Object... args)
        throws IgniteCheckedException {
        A.notNull(key, "key", entryProcessor, "entryProcessor");

        if (keyCheck)
            validateCacheKey(key);

        return syncOp(new SyncOp<EntryProcessorResult<T>>(true) {
            @Nullable @Override public EntryProcessorResult<T> op(IgniteTxLocalAdapter tx)
                throws IgniteCheckedException {
                assert topVer == null || tx.implicit();

                if (topVer != null)
                    tx.topologyVersion(topVer);

                IgniteInternalFuture<GridCacheReturn> fut = tx.invokeAsync(ctx,
                    null,
                    key,
                    (EntryProcessor<K, V, Object>)entryProcessor,
                    args);

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

                IgniteInternalFuture<GridCacheReturn> fut = tx.invokeAsync(ctx, null, invokeMap, args);

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

        IgniteInternalFuture<?> fut = asyncOp(new AsyncOp() {
            @Override public IgniteInternalFuture op(IgniteTxLocalAdapter tx, AffinityTopologyVersion readyTopVer) {
                Map<? extends K, EntryProcessor<K, V, Object>> invokeMap =
                    Collections.singletonMap(key, (EntryProcessor<K, V, Object>)entryProcessor);

                return tx.invokeAsync(ctx, readyTopVer, invokeMap, args);
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

        IgniteInternalFuture<?> fut = asyncOp(new AsyncOp(keys) {
            @Override public IgniteInternalFuture<GridCacheReturn> op(IgniteTxLocalAdapter tx,
                AffinityTopologyVersion readyTopVer) {
                Map<? extends K, EntryProcessor<K, V, Object>> invokeMap = F.viewAsMap(keys, new C1<K, EntryProcessor<K, V, Object>>() {
                    @Override public EntryProcessor apply(K k) {
                        return entryProcessor;
                    }
                });

                return tx.invokeAsync(ctx, readyTopVer, invokeMap, args);
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

        IgniteInternalFuture<?> fut = asyncOp(new AsyncOp(map.keySet()) {
            @Override public IgniteInternalFuture<GridCacheReturn> op(IgniteTxLocalAdapter tx, AffinityTopologyVersion readyTopVer) {
                return tx.invokeAsync(ctx,
                    readyTopVer,
                    (Map<? extends K, ? extends EntryProcessor<K, V, Object>>)map,
                    args);
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
                    tx.invokeAsync(ctx, null, (Map<? extends K, ? extends EntryProcessor<K, V, Object>>)map, args);

                return fut.get().value();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalFuture<Boolean> putAsync(K key, V val) {
        return putAsync(key, val, null);
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param filter Filter.
     * @return Put future.
     */
    public final IgniteInternalFuture<Boolean> putAsync(K key, V val, @Nullable CacheEntryPredicate filter) {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

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
        @Nullable final CacheEntryPredicate filter) {
        return asyncOp(new AsyncOp<Boolean>() {
            @Override public IgniteInternalFuture<Boolean> op(IgniteTxLocalAdapter tx, AffinityTopologyVersion readyTopVer) {
                return tx.putAsync(ctx,
                    readyTopVer,
                    key,
                    val,
                    false,
                    filter).chain(
                    (IgniteClosure<IgniteInternalFuture<GridCacheReturn>, Boolean>)RET2FLAG);
            }

            @Override public String toString() {
                return S.toString("putxAsync",
                    "key", key, true,
                    "val", val, true,
                    "filter", filter, false);
            }
        });
    }

    /** {@inheritDoc} */
    @Nullable @Override public V tryGetAndPut(K key, V val) throws IgniteCheckedException {
        // Supported only in ATOMIC cache.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public final V getAndPutIfAbsent(final K key, final V val) throws IgniteCheckedException {
        return getAndPut(key, val, ctx.noVal());
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalFuture<V> getAndPutIfAbsentAsync(final K key, final V val) {
        return getAndPutAsync(key, val, ctx.noVal());
    }

    /** {@inheritDoc} */
    @Override public final boolean putIfAbsent(final K key, final V val) throws IgniteCheckedException {
        return put(key, val, ctx.noVal());
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalFuture<Boolean> putIfAbsentAsync(final K key, final V val) {
        return putAsync(key, val, ctx.noVal());
    }

    /** {@inheritDoc} */
    @Nullable @Override public final V getAndReplace(final K key, final V val) throws IgniteCheckedException {
        return getAndPut(key, val, ctx.hasVal());
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalFuture<V> getAndReplaceAsync(final K key, final V val) {
        return getAndPutAsync(key, val, ctx.hasVal());
    }

    /** {@inheritDoc} */
    @Override public final boolean replace(final K key, final V val) throws IgniteCheckedException {
        return put(key, val, ctx.hasVal());
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalFuture<Boolean> replaceAsync(final K key, final V val) {
        return putAsync(key, val, ctx.hasVal());
    }

    /** {@inheritDoc} */
    @Override public final boolean replace(final K key, final V oldVal, final V newVal) throws IgniteCheckedException {
        A.notNull(oldVal, "oldVal");

        return put(key, newVal, ctx.equalsVal(oldVal));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(final K key, final V oldVal, final V newVal) {
        A.notNull(oldVal, "oldVal");

        return putAsync(key, newVal, ctx.equalsVal(oldVal));
    }

    /** {@inheritDoc} */
    @Override public void putAll(@Nullable final Map<? extends K, ? extends V> m) throws IgniteCheckedException {
        A.notNull(m, "map");

        if (F.isEmpty(m))
            return;

        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        if (keyCheck)
            validateCacheKeys(m.keySet());

        putAll0(m);

        if (statsEnabled)
            metrics0().addPutTimeNanos(System.nanoTime() - start);
    }

    /**
     * @param m Map.
     * @throws IgniteCheckedException If failed.
     */
    protected void putAll0(final Map<? extends K, ? extends V> m) throws IgniteCheckedException {
        syncOp(new SyncInOp(m.size() == 1) {
            @Override public void inOp(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                tx.putAllAsync(ctx, null, m, false).get();
            }

            @Override public String toString() {
                return "putAll [map=" + m + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllAsync(final Map<? extends K, ? extends V> m) {
        if (F.isEmpty(m))
            return new GridFinishedFuture<Object>();

        if (keyCheck)
            validateCacheKeys(m.keySet());

        return putAllAsync0(m);
    }

    /**
     * @param m Map.
     * @return Future.
     */
    protected IgniteInternalFuture<?> putAllAsync0(final Map<? extends K, ? extends V> m) {
        return asyncOp(new AsyncOp(m.keySet()) {
            @Override public IgniteInternalFuture<?> op(IgniteTxLocalAdapter tx, AffinityTopologyVersion readyTopVer) {
                return tx.putAllAsync(ctx,
                    readyTopVer,
                    m,
                    false).chain(RET2NULL);
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

        V prevVal = getAndRemove0(key);

        if (statsEnabled)
            metrics0().addRemoveAndGetTimeNanos(System.nanoTime() - start);

        return prevVal;
    }

    /**
     * @param key Key.
     * @return Previous value.
     * @throws IgniteCheckedException If failed.
     */
    protected V getAndRemove0(final K key) throws IgniteCheckedException {
        final boolean keepBinary = ctx.keepBinary();

        return syncOp(new SyncOp<V>(true) {
            @Override public V op(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                K key0 = keepBinary ? (K) ctx.toCacheKeyObject(key) : key;

                V ret = tx.removeAllAsync(ctx,
                    null,
                    Collections.singletonList(key0),
                    /*retval*/true,
                    null,
                    /*singleRmv*/false).get().value();

                if (ctx.config().getInterceptor() != null) {
                    K key = keepBinary ? (K) ctx.unwrapBinaryIfNeeded(key0, true, false) : key0;

                    return (V) ctx.config().getInterceptor().onBeforeRemove(new CacheEntryImpl(key, ret)).get2();
                }

                return ret;
            }

            @Override public String toString() {
                return "remove [key=" + key + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAndRemoveAsync(final K key) {
        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        IgniteInternalFuture<V> fut = getAndRemoveAsync0(key);

        if (statsEnabled)
            fut.listen(new UpdateRemoveTimeStatClosure<V>(metrics0(), start));

        return fut;
    }

    /**
     * @param key Key.
     * @return Future.
     */
    protected IgniteInternalFuture<V> getAndRemoveAsync0(final K key) {
        return asyncOp(new AsyncOp<V>() {
            @Override public IgniteInternalFuture<V> op(IgniteTxLocalAdapter tx, AffinityTopologyVersion readyTopVer) {
                // TODO should we invoke interceptor here?
                return tx.removeAllAsync(ctx,
                    readyTopVer,
                    Collections.singletonList(key),
                    /*retval*/true,
                    null,
                    /*singleRmv*/false).chain((IgniteClosure<IgniteInternalFuture<GridCacheReturn>, V>)RET2VAL);
            }

            @Override public String toString() {
                return "removeAsync [key=" + key + ']';
            }
        });
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

        removeAll0(keys);

        if (statsEnabled)
            metrics0().addRemoveTimeNanos(System.nanoTime() - start);
    }

    /**
     * @param keys Keys.
     * @throws IgniteCheckedException If failed.
     */
    protected void removeAll0(final Collection<? extends K> keys) throws IgniteCheckedException {
        syncOp(new SyncInOp(keys.size() == 1) {
            @Override public void inOp(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                tx.removeAllAsync(ctx,
                    null,
                    keys,
                    /*retval*/false,
                    null,
                    /*singleRmv*/false).get();
            }

            @Override public String toString() {
                return "removeAll [keys=" + keys + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync(@Nullable final Collection<? extends K> keys) {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<Object>();

        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        if (keyCheck)
            validateCacheKeys(keys);

        IgniteInternalFuture<Object> fut = removeAllAsync0(keys);

        if (statsEnabled)
            fut.listen(new UpdateRemoveTimeStatClosure<>(metrics0(), start));

        return fut;
    }

    /**
     * @param keys Keys.
     * @return Future.
     */
    protected IgniteInternalFuture<Object> removeAllAsync0(final Collection<? extends K> keys) {
        return asyncOp(new AsyncOp(keys) {
            @Override public IgniteInternalFuture<?> op(IgniteTxLocalAdapter tx, AffinityTopologyVersion readyTopVer) {
                return tx.removeAllAsync(ctx,
                    readyTopVer,
                    keys,
                    /*retval*/false,
                    null,
                    /*singleRmv*/false).chain(RET2NULL);
            }

            @Override public String toString() {
                return "removeAllAsync [keys=" + keys + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean remove(final K key) throws IgniteCheckedException {
        return remove(key, (CacheEntryPredicate)null);
    }

    /**
     * @param key Key.
     * @param filter Filter.
     * @return {@code True} if entry was removed.
     * @throws IgniteCheckedException If failed.
     */
    public boolean remove(final K key, @Nullable CacheEntryPredicate filter) throws IgniteCheckedException {
        boolean statsEnabled = ctx.config().isStatisticsEnabled();

        long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        boolean rmv = remove0(key, filter);

        if (statsEnabled && rmv)
            metrics0().addRemoveTimeNanos(System.nanoTime() - start);

        return rmv;
    }

    /**
     * @param key Key.
     * @param filter Filter.
     * @return {@code True} if entry was removed.
     * @throws IgniteCheckedException If failed.
     */
    protected boolean remove0(final K key, final CacheEntryPredicate filter) throws IgniteCheckedException {
        Boolean res = syncOp(new SyncOp<Boolean>(true) {
            @Override public Boolean op(IgniteTxLocalAdapter tx) throws IgniteCheckedException {
                return tx.removeAllAsync(ctx,
                    null,
                    Collections.singletonList(key),
                    /*retval*/false,
                    filter,
                    /*singleRmv*/filter == null).get().success();
            }

            @Override public String toString() {
                return "removex [key=" + key + ']';
            }
        });

        assert res != null;

        return res;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removeAsync(K key) {
        A.notNull(key, "key");

        return removeAsync(key, (CacheEntryPredicate)null);
    }

    /**
     * @param key Key to remove.
     * @param filter Optional filter.
     * @return Putx operation future.
     */
    public IgniteInternalFuture<Boolean> removeAsync(final K key, @Nullable final CacheEntryPredicate filter) {
        final boolean statsEnabled = ctx.config().isStatisticsEnabled();

        final long start = statsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        IgniteInternalFuture<Boolean> fut = removeAsync0(key, filter);

        if (statsEnabled)
            fut.listen(new UpdateRemoveTimeStatClosure<Boolean>(metrics0(), start));

        return fut;
    }

    /**
     * @param key Key.
     * @param filter Filter.
     * @return Future.
     */
    protected IgniteInternalFuture<Boolean> removeAsync0(final K key, @Nullable final CacheEntryPredicate filter) {
        return asyncOp(new AsyncOp<Boolean>() {
            @Override public IgniteInternalFuture<Boolean> op(IgniteTxLocalAdapter tx, AffinityTopologyVersion readyTopVer) {
                return tx.removeAllAsync(ctx,
                    readyTopVer,
                    Collections.singletonList(key),
                    /*retval*/false,
                    filter,
                    /*singleRmv*/true).chain(
                    (IgniteClosure<IgniteInternalFuture<GridCacheReturn>, Boolean>)RET2FLAG);
            }

            @Override public String toString() {
                return "removeAsync [key=" + key + ", filter=" + filter + ']';
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
                tx.removeAllDrAsync(ctx, (Map)drMap).get();
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

        return asyncOp(new AsyncOp(drMap.keySet()) {
            @Override public IgniteInternalFuture<?> op(IgniteTxLocalAdapter tx, AffinityTopologyVersion readyTopVer) {
                return tx.removeAllDrAsync(ctx, (Map)drMap);
            }

            @Override public String toString() {
                return "removeAllDrASync [drMap=" + drMap + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public final boolean remove(final K key, final V val) throws IgniteCheckedException {
        A.notNull(val, "val");

        return remove(key, ctx.equalsVal(val));
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalFuture<Boolean> removeAsync(final K key, final V val) {
        A.notNull(key, "val");

        return removeAsync(key, ctx.equalsVal(val));
    }

    /** {@inheritDoc} */
    @Override public final CacheMetrics clusterMetrics() {
        return clusterMetrics(ctx.grid().cluster().forDataNodes(ctx.name()));
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics clusterMetrics(ClusterGroup grp) {
        List<CacheMetrics> metrics = new ArrayList<>(grp.nodes().size());

        for (ClusterNode node : grp.nodes()) {
            Map<Integer, CacheMetrics> nodeCacheMetrics = ((TcpDiscoveryNode)node).cacheMetrics();

            if (nodeCacheMetrics != null) {
                CacheMetrics e = nodeCacheMetrics.get(context().cacheId());

                if (e != null)
                    metrics.add(e);
            }
        }

        return new CacheMetricsSnapshot(ctx.cache().localMetrics(), metrics);
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics localMetrics() {
        return new CacheMetricsSnapshot(metrics);
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean localMxBean() {
        return locMxBean;
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean clusterMxBean() {
        return clusterMxBean;
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

            GridCacheEntryEx e = entry0(cacheKey, ctx.discovery().topologyVersionEx(),
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

        TransactionConfiguration cfg = CU.transactionConfiguration(ctx, ctx.kernalContext().config());

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

        final boolean keepBinary = opCtx != null && opCtx.isKeepBinary();

        if (p != null)
            ctx.kernalContext().resource().injectGeneric(p);

        try {
            if (ctx.store().isLocal()) {
                DataStreamerImpl ldr = ctx.kernalContext().dataStream().dataStreamer(ctx.namex());

                try {
                    ldr.skipStore(true);

                    ldr.receiver(new IgniteDrDataStreamerCacheUpdater());

                    ldr.keepBinary(keepBinary);

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

                        loadEntry(key, val, ver0, (IgniteBiPredicate<Object, Object>)p, topVer, replicate, ttl);
                    }
                }, args);
            }
        }
        finally {
            if (p instanceof PlatformCacheEntryFilter)
                ((PlatformCacheEntryFilter)p).onClose();
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
            entry.initialValue(cacheVal,
                ver,
                ttl,
                CU.EXPIRE_TIME_CALCULATE,
                false,
                topVer,
                replicate ? DR_LOAD : DR_NONE,
                true);
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
        final Object[] args) {
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

        final boolean keepBinary = opCtx != null && opCtx.isKeepBinary();

        if (replaceExisting) {
            if (ctx.store().isLocal())
                return runLoadKeysCallable(keys, plc, keepBinary, true);
            else {
                return ctx.closures().callLocalSafe(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        localLoadAndUpdate(keys);

                        return null;
                    }
                });
            }
        }
        else
            return runLoadKeysCallable(keys, plc, keepBinary, false);
    }

    /**
     * Run load keys callable on appropriate nodes.
     *
     * @param keys Keys.
     * @param plc Expiry policy.
     * @param keepBinary Keep binary flag. Will be ignored for releases older than {@link #LOAD_CACHE_JOB_V2_SINCE}.
     * @return Operation future.
     */
    private IgniteInternalFuture<?> runLoadKeysCallable(final Set<? extends K> keys, final ExpiryPolicy plc,
        final boolean keepBinary, final boolean update) {
        Collection<ClusterNode> nodes = ctx.grid().cluster().forDataNodes(name()).nodes();

        if (nodes.isEmpty())
            return new GridFinishedFuture<>();

        Collection<ClusterNode> oldNodes = ctx.grid().cluster().forDataNodes(name()).forPredicate(
            new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    return node.version().compareToIgnoreTimestamp(LOAD_CACHE_JOB_V2_SINCE) < 0;
                }
            }).nodes();

        if (oldNodes.isEmpty()) {
            return ctx.closures().callAsyncNoFailover(BROADCAST,
                new LoadKeysCallableV2<>(ctx.name(), keys, update, plc, keepBinary),
                nodes,
                true,
                0);
        }
        else {
            return ctx.closures().callAsyncNoFailover(BROADCAST,
                new LoadKeysCallable<>(ctx.name(), keys, update, plc),
                nodes,
                true,
                0);
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
    public void localLoad(Collection<? extends K> keys, @Nullable ExpiryPolicy plc, final boolean keepBinary)
        throws IgniteCheckedException {
        final boolean replicate = ctx.isDrEnabled();
        final AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

        final ExpiryPolicy plc0 = plc != null ? plc : ctx.expiry();

        Collection<KeyCacheObject> keys0 = ctx.cacheKeysView(keys);

        if (ctx.store().isLocal()) {
            DataStreamerImpl ldr = ctx.kernalContext().dataStream().dataStreamer(ctx.namex());

            try {
                ldr.skipStore(true);

                ldr.keepBinary(keepBinary);

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
        ClusterGroup oldNodes = ctx.kernalContext().grid().cluster().forDataNodes(ctx.name())
            .forPredicate(new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    return node.version().compareToIgnoreTimestamp(LOAD_CACHE_JOB_SINCE) < 0;
                }
            });

        ClusterGroup newNodes = ctx.kernalContext().grid().cluster().forDataNodes(ctx.name())
            .forPredicate(new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    return node.version().compareToIgnoreTimestamp(LOAD_CACHE_JOB_SINCE) >= 0 &&
                        node.version().compareToIgnoreTimestamp(LOAD_CACHE_JOB_V2_SINCE) < 0;
                }
            });

        ClusterGroup newNodesV2 = ctx.kernalContext().grid().cluster().forDataNodes(ctx.name())
            .forPredicate(new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    return node.version().compareToIgnoreTimestamp(LOAD_CACHE_JOB_V2_SINCE) >= 0;
                }
            });

        ctx.kernalContext().task().setThreadContext(TC_NO_FAILOVER, true);

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        ExpiryPolicy plc = opCtx != null ? opCtx.expiry() : null;

        GridCompoundFuture<Object, ?> fut = new GridCompoundFuture<>();

        if (!F.isEmpty(oldNodes.nodes())) {
            ComputeTaskInternalFuture oldNodesFut = ctx.kernalContext().closure().callAsync(BROADCAST,
                Collections.singletonList(new LoadCacheClosure<>(ctx.name(), p, args, plc)),
                oldNodes.nodes());

            fut.add(oldNodesFut);
        }

        if (!F.isEmpty(newNodes.nodes())) {
            ComputeTaskInternalFuture newNodesFut = ctx.kernalContext().closure().callAsync(BROADCAST,
                Collections.singletonList(
                    new LoadCacheJob<>(ctx.name(), ctx.affinity().affinityTopologyVersion(), p, args, plc)),
                newNodes.nodes());

            fut.add(newNodesFut);
        }

        if (!F.isEmpty(newNodesV2.nodes())) {
            final boolean keepBinary = opCtx != null && opCtx.isKeepBinary();

            ComputeTaskInternalFuture newNodesV2Fut = ctx.kernalContext().closure().callAsync(BROADCAST,
                Collections.singletonList(
                    new LoadCacheJobV2<>(ctx.name(), ctx.affinity().affinityTopologyVersion(), p, args, plc, keepBinary)),
                newNodesV2.nodes());

            fut.add(newNodesV2Fut);
        }

        fut.markInitialized();

        return fut;
    }

    /**
     * @return Random cache entry.
     */
    @Deprecated
    @Nullable public Cache.Entry<K, V> randomEntry() {
        GridCacheMapEntry entry;

        if (ctx.offheapTiered()) {
            Iterator<Cache.Entry<K, V>> it;

            try {
                it = ctx.swap().offheapIterator(true, true, ctx.affinity().affinityTopologyVersion(), ctx.keepBinary());
            }
            catch (IgniteCheckedException e) {
                throw CU.convertToCacheException(e);
            }

            return it.hasNext() ? it.next() : null;
        }
        else
            entry = map.randomEntry();

        return entry == null || entry.obsolete() ? null : entry.<K, V>wrapLazyValue(ctx.keepBinary());
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode[] peekModes) throws IgniteCheckedException {
        if (isLocal())
            return localSize(peekModes);

        return sizeAsync(peekModes).get();
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(CachePeekMode[] peekModes) throws IgniteCheckedException {
        if (isLocal())
            return localSizeLong(peekModes);

        return sizeLongAsync(peekModes).get();
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(int partition, CachePeekMode[] peekModes) throws IgniteCheckedException {
        if (isLocal())
            return localSizeLong(partition, peekModes);

        return sizeLongAsync(partition, peekModes).get();
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
    @Override public IgniteInternalFuture<Long> sizeLongAsync(final CachePeekMode[] peekModes) {
        assert peekModes != null;

        PeekModes modes = parsePeekModes(peekModes, true);

        IgniteClusterEx cluster = ctx.grid().cluster();

        ClusterGroup grp = modes.near ? cluster.forCacheNodes(name(), true, true, false) : cluster.forDataNodes(name());

        Collection<ClusterNode> nodes = grp.nodes();

        if (nodes.isEmpty())
            return new GridFinishedFuture<>(0L);

        ctx.kernalContext().task().setThreadContext(TC_SUBGRID, nodes);

        return ctx.kernalContext().task().execute(
            new SizeLongTask(ctx.name(), ctx.affinity().affinityTopologyVersion(), peekModes), null);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Long> sizeLongAsync(final int part, final CachePeekMode[] peekModes) {
        assert peekModes != null;

        final PeekModes modes = parsePeekModes(peekModes, true);

        IgniteClusterEx cluster = ctx.grid().cluster();
        final GridCacheAffinityManager aff = ctx.affinity();
        final AffinityTopologyVersion topVer = aff.affinityTopologyVersion();

        ClusterGroup grp = cluster.forDataNodes(name());

        Collection<ClusterNode> nodes = grp.forPredicate(new IgnitePredicate<ClusterNode>() {
            /** {@inheritDoc} */
            @Override public boolean apply(ClusterNode clusterNode) {
                return clusterNode.version().compareTo(PartitionSizeLongTask.SINCE_VER) >= 0 &&
                    ((modes.primary && aff.primaryByPartition(clusterNode, part, topVer)) ||
                        (modes.backup && aff.backupByPartition(clusterNode, part, topVer)));
            }
        }).nodes();

        if (nodes.isEmpty())
            return new GridFinishedFuture<>(0L);

        ctx.kernalContext().task().setThreadContext(TC_SUBGRID, nodes);

        return ctx.kernalContext().task().execute(
                new PartitionSizeLongTask(ctx.name(), ctx.affinity().affinityTopologyVersion(), peekModes, part), null);
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode[] peekModes) throws IgniteCheckedException {
        return (int)localSizeLong(peekModes);
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(CachePeekMode[] peekModes) throws IgniteCheckedException {
        PeekModes modes = parsePeekModes(peekModes, true);

        long size = 0;

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
    @Override public long localSizeLong(int part, CachePeekMode[] peekModes) throws IgniteCheckedException {
        PeekModes modes = parsePeekModes(peekModes, true);

        long size = 0;

        AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

        // Swap and offheap are disabled for near cache.
        GridCacheSwapManager swapMgr = ctx.isNear() ? ctx.near().dht().context().swap() : ctx.swap();

        if (ctx.isLocal()){
            modes.primary = true;
            modes.backup = true;

            if (modes.heap)
                size += size();

            if (modes.swap)
                size += swapMgr.swapEntriesCount(0);

            if (modes.offheap)
                size += swapMgr.offheapEntriesCount(0);
        }
        else {
            GridDhtLocalPartition dhtPart = ctx.topology().localPartition(part, topVer, false);

            if (dhtPart != null) {
                if (modes.primary && dhtPart.primary(topVer) || modes.backup && dhtPart.backup(topVer)) {
                    if (modes.heap)
                        size += dhtPart.publicSize();

                    if (modes.swap)
                        size += swapMgr.swapEntriesCount(part);

                    if (modes.offheap)
                        size += swapMgr.offheapEntriesCount(part);
                }
            }
        }

        return size;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return map.publicSize();
    }

    /** {@inheritDoc} */
    @Override public long sizeLong() {
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
    @Override public long primarySizeLong() {
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
     * @throws IgniteCheckedException If failed.
     */
    public Iterator<Cache.Entry<K, V>> igniteIterator() throws IgniteCheckedException {
        return igniteIterator(ctx.keepBinary());
    }

    /**
     * @param keepBinary
     * @return Distributed ignite cache iterator.
     * @throws IgniteCheckedException If failed.
     */
    public Iterator<Cache.Entry<K, V>> igniteIterator(boolean keepBinary) throws IgniteCheckedException {
        GridCacheContext ctx0 = ctx.isNear() ? ctx.near().dht().context() : ctx;

        final CacheOperationContext opCtx = ctx.operationContextPerCall();

        if (!ctx0.isSwapOrOffheapEnabled() && ctx0.kernalContext().discovery().size() == 1)
            return localIteratorHonorExpirePolicy(opCtx);

        final GridCloseableIterator<Map.Entry<K, V>> iter = ctx0.queries().createScanQuery(null, null, keepBinary)
            .keepAll(false)
            .executeScanQuery();

        return ctx.itHolder().iterator(iter, new CacheIteratorConverter<Cache.Entry<K, V>, Map.Entry<K, V>>() {
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
     * @param deserializeBinary Deserialize binary flag.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("IfMayBeConditional")
    @Nullable public V promote(K key, boolean deserializeBinary) throws IgniteCheckedException {
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

        return (V)ctx.unwrapBinaryIfNeeded(val0, !deserializeBinary);
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
        if (ctx.config().getMemoryMode() == CacheMemoryMode.OFFHEAP_VALUES) {
            assert ctx.unsafeMemory() != null;

            return ctx.unsafeMemory().allocatedSize();
        }

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
            TransactionConfiguration tCfg = CU.transactionConfiguration(ctx, ctx.kernalContext().config());

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
                catch (IgniteInterruptedCheckedException | IgniteTxHeuristicCheckedException e) {
                    throw e;
                }
                catch (IgniteCheckedException e) {
                    if (!(e instanceof IgniteTxRollbackCheckedException)) {
                        try {
                            tx.rollback();

                            e = new IgniteTxRollbackCheckedException("Transaction has been rolled back: " +
                                tx.xid(), e);
                        }
                        catch (IgniteCheckedException | AssertionError | RuntimeException e1) {
                            U.error(log, "Failed to rollback transaction (cache may contain stale locks): " + tx, e1);

                            U.addLastCause(e, e1, log);
                        }
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

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        final TransactionConfiguration txCfg = CU.transactionConfiguration(ctx, ctx.kernalContext().config());

        if (tx == null || tx.implicit()) {
            boolean skipStore = ctx.skipStore(); // Save value of thread-local flag.

            int retries = opCtx != null && opCtx.noRetries() ? 1 : MAX_RETRIES;

            if (retries == 1) {
                tx = ctx.tm().newTx(
                    true,
                    op.single(),
                    ctx.systemTx() ? ctx : null,
                    OPTIMISTIC,
                    READ_COMMITTED,
                    txCfg.getDefaultTxTimeout(),
                    !skipStore,
                    0);

                return asyncOp(tx, op, opCtx);
            }
            else {
                AsyncOpRetryFuture<T> fut = new AsyncOpRetryFuture<>(op, retries, opCtx);

                fut.execute();

                return fut;
            }
        }
        else
            return asyncOp(tx, op, opCtx);
    }

    /**
     * @param tx Transaction.
     * @param op Cache operation.
     * @param <T> Return type.
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    protected <T> IgniteInternalFuture<T> asyncOp(
        IgniteTxLocalAdapter tx,
        final AsyncOp<T> op,
        final CacheOperationContext opCtx
    ) {
        IgniteInternalFuture<T> fail = asyncOpAcquire();

        if (fail != null)
            return fail;

        FutureHolder holder = lastFut.get();

        holder.lock();

        try {
            IgniteInternalFuture fut = holder.future();

            final IgniteTxLocalAdapter tx0 = tx;

            if (fut != null && !fut.isDone()) {
                IgniteInternalFuture<T> f = new GridEmbeddedFuture(fut,
                    new IgniteOutClosure<IgniteInternalFuture>() {
                        @Override public IgniteInternalFuture<T> apply() {
                            if (ctx.kernalContext().isStopping())
                                return new GridFinishedFuture<>(
                                    new IgniteCheckedException("Operation has been cancelled (node is stopping)."));

                            return op.op(tx0, opCtx).chain(new CX1<IgniteInternalFuture<T>, T>() {
                                @Override public T applyx(IgniteInternalFuture<T> tFut) throws IgniteCheckedException {
                                    try {
                                        return tFut.get();
                                    }
                                    catch (IgniteTxRollbackCheckedException e) {
                                        throw e;
                                    }
                                    catch (IgniteCheckedException e1) {
                                        tx0.rollbackAsync();

                                        throw e1;
                                    }
                                    finally {
                                        ctx.shared().txContextReset();
                                    }
                                }
                            });
                        }
                    });

                saveFuture(holder, f);

                return f;
            }

            final IgniteInternalFuture<T> f = op.op(tx, opCtx).chain(new CX1<IgniteInternalFuture<T>, T>() {
                @Override public T applyx(IgniteInternalFuture<T> tFut) throws IgniteCheckedException {
                    try {
                        return tFut.get();
                    }
                    catch (IgniteTxRollbackCheckedException e) {
                        throw e;
                    }
                    catch (IgniteCheckedException e1) {
                        tx0.rollbackAsync();

                        throw e1;
                    }
                    finally {
                        ctx.shared().txContextReset();
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
    private void asyncOpRelease() {
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

            return IgnitionEx.localIgnite().cachex(t.get2());
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
        return ctx.preloader().forceRebalance();
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
     * @param key Key.
     * @param readers Whether to clear readers.
     */
    private boolean clearLocally0(K key, boolean readers) {
        ctx.checkSecurity(SecurityPermission.CACHE_REMOVE);

        if (keyCheck)
            validateCacheKey(key);

        GridCacheVersion obsoleteVer = ctx.versions().next();

        try {
            KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

            GridCacheEntryEx entry = ctx.isSwapOrOffheapEnabled() ? entryEx(cacheKey) : peekEx(cacheKey);

            if (entry != null)
                return entry.clear(obsoleteVer, readers);
        }
        catch (GridDhtInvalidPartitionException ignored) {
            // No-op.
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Failed to clearLocally entry for key: " + key, ex);
        }

        return false;
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
        return entrySetx(filter);
    }

    /**
     * @param key Key.
     * @param deserializeBinary Deserialize binary flag.
     * @param needVer Need version.
     * @return Cached value.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public final V get(K key, boolean deserializeBinary, final boolean needVer) throws IgniteCheckedException {
        String taskName = ctx.kernalContext().job().currentTaskName();

        return get0(key, taskName, deserializeBinary, needVer);
    }

    /**
     * @param key Key.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary flag.
     * @param needVer Need version.
     * @return Cached value.
     * @throws IgniteCheckedException If failed.
     */
    protected V get0(
        final K key,
        String taskName,
        boolean deserializeBinary,
        boolean needVer) throws IgniteCheckedException {
        checkJta();

        try {
            return getAsync(key,
                !ctx.config().isReadFromBackup(),
                /*skip tx*/false,
                null,
                taskName,
                deserializeBinary,
                false,
                /*can remap*/true,
                needVer).get();
        }
        catch (IgniteException e) {
            if (e.getCause(IgniteCheckedException.class) != null)
                throw e.getCause(IgniteCheckedException.class);
            else
                throw e;
        }
    }

    /**
     * @param key Key.
     * @param deserializeBinary Deserialize binary flag.
     * @param needVer Need version.
     * @return Read operation future.
     */
    public final IgniteInternalFuture<V> getAsync(final K key, boolean deserializeBinary, final boolean needVer) {
        try {
            checkJta();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }

        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAsync(key,
            !ctx.config().isReadFromBackup(),
            /*skip tx*/false,
            null,
            taskName,
            deserializeBinary,
            false,
            /*can remap*/true,
            needVer);
    }

    /**
     * @param keys Keys.
     * @param deserializeBinary Deserialize binary flag.
     * @param needVer Need version.
     * @return Map of cached values.
     * @throws IgniteCheckedException If read failed.
     */
    protected Map<K, V> getAll0(Collection<? extends K> keys, boolean deserializeBinary,
        boolean needVer) throws IgniteCheckedException {
        checkJta();

        return getAllAsync(keys, deserializeBinary, needVer).get();
    }

    /**
     * @param keys Keys.
     * @param deserializeBinary Deserialize binary flag.
     * @param needVer Need version.
     * @return Read future.
     */
    public IgniteInternalFuture<Map<K, V>> getAllAsync(@Nullable Collection<? extends K> keys,
        boolean deserializeBinary, boolean needVer) {
        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAllAsync(keys,
            !ctx.config().isReadFromBackup(),
            /*skip tx*/false,
            /*subject id*/null,
            taskName,
            deserializeBinary,
            /*skip vals*/false,
            /*can remap*/true,
            needVer);
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
     * For tests only.
     */
    public void forceKeyCheck() {
        keyCheck = true;
    }

    /**
     * Validates that given cache key has overridden equals and hashCode methods and
     * implements {@link Externalizable}.
     *
     * @param key Cache key.
     * @throws IllegalArgumentException If validation fails.
     */
    protected final void validateCacheKey(Object key) {
        if (keyCheck) {
            CU.validateCacheKey(key);

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
    protected final void validateCacheKeys(Iterable<?> keys) {
        if (keys == null)
            return;

        if (keyCheck) {
            for (Object key : keys) {
                if (key == null || key instanceof GridCacheInternal)
                    continue;

                CU.validateCacheKey(key);

                keyCheck = false;
            }
        }
    }

    /**
     * @param it Internal entry iterator.
     * @param deserializeBinary Deserialize binary flag.
     * @return Public API iterator.
     */
    protected final Iterator<Cache.Entry<K, V>> iterator(final Iterator<? extends GridCacheEntryEx> it,
        final boolean deserializeBinary) {
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
                        next = toCacheEntry(entry, deserializeBinary);

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
     * @param deserializeBinary Deserialize binary flag.
     * @return Public API entry.
     * @throws IgniteCheckedException If failed.
     * @throws GridCacheEntryRemovedException If entry removed.
     */
    @Nullable private Cache.Entry<K, V> toCacheEntry(GridCacheEntryEx entry,
        boolean deserializeBinary)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        CacheObject val = entry.innerGet(
            /*ver*/null,
            /*tx*/null,
            /*swap*/false,
            /*readThrough*/false,
            /*metrics*/false,
            /*evt*/false,
            /*tmp*/false,
            /*subjId*/null,
            /*transformClo*/null,
            /*taskName*/null,
            /*expiryPlc*/null,
            !deserializeBinary);

        if (val == null)
            return null;

        KeyCacheObject key = entry.key();

        Object key0 = ctx.unwrapBinaryIfNeeded(key, !deserializeBinary, true);
        Object val0 = ctx.unwrapBinaryIfNeeded(val, !deserializeBinary, true);

        return new CacheEntryImpl<>((K)key0, (V)val0, entry.version());
    }

    /**
     *
     */
    private class AsyncOpRetryFuture<T> extends GridFutureAdapter<T> {
        /** */
        private AsyncOp<T> op;

        /** */
        private int retries;

        /** */
        private IgniteTxLocalAdapter tx;

        /** */
        private CacheOperationContext opCtx;

        /**
         * @param op Operation.
         * @param retries Number of retries.
         * @param opCtx Operation context per call to save.
         */
        public AsyncOpRetryFuture(
            AsyncOp<T> op,
            int retries,
            CacheOperationContext opCtx
        ) {
            assert retries > 1 : retries;

            tx = null;

            this.op = op;
            this.retries = retries;
            this.opCtx = opCtx;
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
                CU.transactionConfiguration(ctx, ctx.kernalContext().config()).getDefaultTxTimeout(),
                opCtx == null || !opCtx.skipStore(),
                0);

            IgniteInternalFuture<T> fut = asyncOp(tx, op, opCtx);

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
                                        finally {
                                            ctx.shared().txContextReset();
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
    @Nullable public final IgniteCacheExpiryPolicy expiryPolicy(@Nullable ExpiryPolicy plc) {
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
         * @param readyTopVer Ready topology version.
         * @return Operation future.
         */
        public abstract IgniteInternalFuture<T> op(IgniteTxLocalAdapter tx, AffinityTopologyVersion readyTopVer);

        /**
         * @param tx Transaction.
         * @param opCtx Operation context.
         * @return Operation future.
         */
        public IgniteInternalFuture<T> op(final IgniteTxLocalAdapter tx, CacheOperationContext opCtx) {
            AffinityTopologyVersion txTopVer = tx.topologyVersionSnapshot();

            if (txTopVer != null)
                return op(tx, (AffinityTopologyVersion)null);

            // Tx needs affinity for entry creation, wait when affinity is ready to avoid blocking inside async operation.
            final AffinityTopologyVersion topVer = ctx.shared().exchange().topologyVersion();

            IgniteInternalFuture<?> topFut = ctx.shared().exchange().affinityReadyFuture(topVer);

            if (topFut == null || topFut.isDone())
                return op(tx, topVer);
            else
                return waitTopologyFuture(topFut, topVer, tx, opCtx);
        }

        /**
         * @param topFut Topology future.
         * @param topVer Topology version to use.
         * @param tx Transaction.
         * @param opCtx Operation context.
         * @return Operation future.
         */
        private IgniteInternalFuture<T> waitTopologyFuture(IgniteInternalFuture<?> topFut,
            final AffinityTopologyVersion topVer,
            final IgniteTxLocalAdapter tx,
            final CacheOperationContext opCtx) {
            final GridFutureAdapter fut0 = new GridFutureAdapter();

            topFut.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> topFut) {
                    try {
                        topFut.get();

                        IgniteInternalFuture<?> opFut = runOp(tx, topVer, opCtx);

                        opFut.listen(new CI1<IgniteInternalFuture<?>>() {
                            @Override public void apply(IgniteInternalFuture<?> opFut) {
                                try {
                                    fut0.onDone(opFut.get());
                                }
                                catch (IgniteCheckedException e) {
                                    fut0.onDone(e);
                                }
                            }
                        });
                    }
                    catch (IgniteCheckedException e) {
                        fut0.onDone(e);
                    }
                }
            });

            return fut0;
        }

        /**
         * @param tx Transaction.
         * @param topVer Ready topology version.
         * @param opCtx Operation context.
         * @return Future.
         */
        private IgniteInternalFuture<T> runOp(IgniteTxLocalAdapter tx,
            AffinityTopologyVersion topVer,
            CacheOperationContext opCtx) {
            ctx.operationContextPerCall(opCtx);

            ctx.shared().txContextReset();

            try {
                return op(tx, topVer);
            }
            finally {
                ctx.shared().txContextReset();

                ctx.operationContextPerCall(null);
            }
        }
    }

    /**
     * Global clear all.
     */
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
                cache.clearLocally(clearServerCache(), clearNearCache(), true);

            return null;
        }

        /**
         * @return Whether to clear server cache.
         */
        protected boolean clearServerCache() {
            return true;
        }

        /**
         * @return Whether to clear near cache.
         */
        protected boolean clearNearCache() {
            return false;
        }
    }

    /**
     * Global clear keys.
     */
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
                cache.clearLocallyAll(keys, clearServerCache(), clearNearCache(), true);

            return null;
        }

        /**
         * @return Whether to clear server cache.
         */
        protected boolean clearServerCache() {
            return true;
        }

        /**
         * @return Whether to clear near cache.
         */
        protected boolean clearNearCache() {
            return false;
        }
    }

    /**
     * Global clear all for near cache.
     */
    private static class GlobalClearAllNearJob extends GlobalClearAllJob {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param cacheName Cache name.
         * @param topVer Affinity topology version.
         */
        private GlobalClearAllNearJob(String cacheName, AffinityTopologyVersion topVer) {
            super(cacheName, topVer);
        }

        /**
         * @return Whether to clear server cache.
         */
        @Override protected boolean clearServerCache() {
            return false;
        }

        /**
         * @return Whether to clear near cache.
         */
        @Override protected boolean clearNearCache() {
            return true;
        }
    }

    /**
     * Global clear keys for near cache.
     */
    private static class GlobalClearKeySetNearJob<K> extends GlobalClearKeySetJob<K> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param cacheName Cache name.
         * @param topVer Affinity topology version.
         * @param keys Keys to clear.
         */
        private GlobalClearKeySetNearJob(String cacheName, AffinityTopologyVersion topVer, Set<? extends K> keys) {
            super(cacheName, topVer, keys);
        }

        /**
         * @return Whether to clear server cache.
         */
        protected boolean clearServerCache() {
            return false;
        }

        /**
         * @return Whether to clear near cache.
         */
        protected boolean clearNearCache() {
            return true;
        }
    }

    /**
     * Internal callable for partition size calculation.
     */
    private static class PartitionSizeLongJob extends TopologyVersionAwareJob {
        /** */
        private static final long serialVersionUID = 0L;

        /** Partition. */
        private final int partition;

        /** Peek modes. */
        private final CachePeekMode[] peekModes;

        /**
         * @param cacheName Cache name.
         * @param topVer Affinity topology version.
         * @param peekModes Cache peek modes.
         * @param partition partition.
         */
        private PartitionSizeLongJob(String cacheName, AffinityTopologyVersion topVer, CachePeekMode[] peekModes, int partition) {
            super(cacheName, topVer);

            this.peekModes = peekModes;
            this.partition = partition;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object localExecute(@Nullable IgniteInternalCache cache) {
            if (cache == null)
                return 0;

            try {
                return cache.localSizeLong(partition, peekModes);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(PartitionSizeLongJob.class, this);
        }
    }

    /**
     * Internal callable for global size calculation.
     */
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
     * Internal callable for global size calculation.
     */
    private static class SizeLongJob extends TopologyVersionAwareJob {
        /** */
        private static final long serialVersionUID = 0L;

        /** Peek modes. */
        private final CachePeekMode[] peekModes;

        /**
         * @param cacheName Cache name.
         * @param topVer Affinity topology version.
         * @param peekModes Cache peek modes.
         */
        private SizeLongJob(String cacheName, AffinityTopologyVersion topVer, CachePeekMode[] peekModes) {
            super(cacheName, topVer);

            this.peekModes = peekModes;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object localExecute(@Nullable IgniteInternalCache cache) {
            if (cache == null)
                return 0;

            try {
                return cache.localSizeLong(peekModes);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(SizeLongJob.class, this);
        }
    }

    /**
     * Internal callable for global size calculation.
     */
    @GridInternal
    private static class LoadCacheJob<K, V> extends TopologyVersionAwareJob {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteBiPredicate<K, V> p;

        /** */
        private final Object[] loadArgs;

        /** */
        private final ExpiryPolicy plc;

        /**
         * @param cacheName Cache name.
         * @param topVer Affinity topology version.
         * @param p Predicate.
         * @param loadArgs Arguments.
         * @param plc Policy.
         */
        private LoadCacheJob(String cacheName, AffinityTopologyVersion topVer, IgniteBiPredicate<K, V> p,
            Object[] loadArgs,
            ExpiryPolicy plc) {
            super(cacheName, topVer);

            this.p = p;
            this.loadArgs = loadArgs;
            this.plc = plc;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object localExecute(@Nullable IgniteInternalCache cache) {
            try {
                assert cache != null : "Failed to get a cache [cacheName=" + cacheName + ", topVer=" + topVer + "]";

                if (plc != null)
                    cache = cache.withExpiryPolicy(plc);

                cache.localLoadCache(p, loadArgs);

                return null;
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(LoadCacheJob.class, this);
        }
    }

    /**
     * Load cache job that with keepBinary flag.
     */
    private static class LoadCacheJobV2<K, V> extends LoadCacheJob<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final boolean keepBinary;

        /**
         * Constructor.
         *
         * @param cacheName Cache name.
         * @param topVer Affinity topology version.
         * @param p Predicate.
         * @param loadArgs Arguments.
         * @param keepBinary Keep binary flag.
         */
        public LoadCacheJobV2(final String cacheName, final AffinityTopologyVersion topVer,
            final IgniteBiPredicate<K, V> p, final Object[] loadArgs, final ExpiryPolicy plc,
            final boolean keepBinary) {
            super(cacheName, topVer, p, loadArgs, plc);

            this.keepBinary = keepBinary;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object localExecute(@Nullable IgniteInternalCache cache) {
            assert cache != null : "Failed to get a cache [cacheName=" + cacheName + ", topVer=" + topVer + "]";

            if (keepBinary)
                cache = cache.keepBinary();

            return super.localExecute(cache);
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(LoadCacheJobV2.class, this);
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
         * @param createTtl Create TTL.
         * @param accessTtl Access TTL.
         * @return Access expire policy.
         */
        @Nullable public static CacheExpiryPolicy fromRemote(final long createTtl, final long accessTtl) {
            if (createTtl == CU.TTL_NOT_CHANGED && accessTtl == CU.TTL_NOT_CHANGED)
                return null;

            return new CacheExpiryPolicy() {
                @Override public long forCreate() {
                    return createTtl;
                }

                @Override public long forAccess() {
                    return accessTtl;
                }

                /** {@inheritDoc} */
                @Override public long forUpdate() {
                    return CU.TTL_NOT_CHANGED;
                }
            };
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
    static class LoadKeysCallable<K, V> implements IgniteCallable<Void>, Externalizable {
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
         *        otherwise {@link #localLoad(Collection, ExpiryPolicy, boolean)}.
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
            return call0(false);
        }

        /**
         * Internal call routine.
         *
         * @param keepBinary Keep binary flag.
         * @return Result (always {@code null}).
         * @throws Exception If failed.
         */
        protected Void call0(boolean keepBinary) throws Exception {
            GridCacheAdapter<K, V> cache = ((IgniteKernal)ignite).context().cache().internalCache(cacheName);

            assert cache != null : cacheName;

            cache.context().gate().enter();

            try {
                if (update)
                    cache.localLoadAndUpdate(keys);
                else
                    cache.localLoad(keys, plc, keepBinary);
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
    static class LoadKeysCallableV2<K, V> extends LoadKeysCallable<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private boolean keepBinary;

        /**
         * Required by {@link Externalizable}.
         */
        public LoadKeysCallableV2() {
            // No-op.
        }

        /**
         * @param cacheName Cache name.
         * @param keys Keys.
         * @param update If {@code true} calls {@link #localLoadAndUpdate(Collection)}
         *        otherwise {@link #localLoad(Collection, ExpiryPolicy, boolean)}.
         * @param plc Expiry policy.
         * @param keepBinary Keep binary flag.
         */
        LoadKeysCallableV2(final String cacheName, final Collection<? extends K> keys, final boolean update,
            final ExpiryPolicy plc, final boolean keepBinary) {
            super(cacheName, keys, update, plc);

            this.keepBinary = keepBinary;
        }

        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            return call0(keepBinary);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(final ObjectOutput out) throws IOException {
            super.writeExternal(out);

            out.writeBoolean(keepBinary);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);

            keepBinary = in.readBoolean();
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
            throws IgniteCheckedException {
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
                ver.conflictVersion());

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
            @Nullable ExpiryPolicy plc) {
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
    @GridInternal
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
     * Size task.
     */
    @GridInternal
    private static class SizeLongTask extends ComputeTaskAdapter<Object, Long> {
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
        private SizeLongTask(String cacheName, AffinityTopologyVersion topVer, CachePeekMode[] peekModes) {
            this.cacheName = cacheName;
            this.topVer = topVer;
            this.peekModes = peekModes;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Object arg) throws IgniteException {
            Map<ComputeJob, ClusterNode> jobs = new HashMap();

            for (ClusterNode node : subgrid)
                jobs.put(new SizeLongJob(cacheName, topVer, peekModes), node);

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
        @Nullable @Override public Long reduce(List<ComputeJobResult> results) throws IgniteException {
            long size = 0;

            for (ComputeJobResult res : results) {
                if (res != null && res.getException() == null)
                    size += res.<Long>getData();
            }

            return size;
        }
    }

    /**
     * Partition Size Long task.
     */
    @GridInternal
    private static class PartitionSizeLongTask extends ComputeTaskAdapter<Object, Long> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private static final IgniteProductVersion SINCE_VER = IgniteProductVersion.fromString("1.5.30");

        /** Partition */
        private final int partition;

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
         * @param partition partition.
         */
        private PartitionSizeLongTask(
            String cacheName,
            AffinityTopologyVersion topVer,
            CachePeekMode[] peekModes,
            int partition
        ) {
            this.cacheName = cacheName;
            this.topVer = topVer;
            this.peekModes = peekModes;
            this.partition = partition;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            @Nullable Object arg
        ) throws IgniteException {
            Map<ComputeJob, ClusterNode> jobs = new HashMap();

            for (ClusterNode node : subgrid)
                jobs.put(new PartitionSizeLongJob(cacheName, topVer, peekModes, partition), node);

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
        @Nullable @Override public Long reduce(List<ComputeJobResult> results) throws IgniteException {
            long size = 0;

            for (ComputeJobResult res : results) {
                if (res != null) {
                    if (res.getException() == null)
                        size += res.<Long>getData();
                    else
                        throw res.getException();
                }
            }

            return size;
        }
    }

    /**
     * Clear task.
     */
    @GridInternal
    private static class ClearTask<K> extends ComputeTaskAdapter<Object, Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        public static final IgniteProductVersion NEAR_JOB_SINCE = IgniteProductVersion.fromString("1.5.0");

        /** Cache name. */
        private final String cacheName;

        /** Affinity topology version. */
        private final AffinityTopologyVersion topVer;

        /** Keys to clear. */
        private final Set<? extends K> keys;

        /** Near cache flag. */
        private final boolean near;

        /**
         * @param cacheName Cache name.
         * @param topVer Affinity topology version.
         * @param keys Keys to clear.
         * @param near Near cache flag.
         */
        public ClearTask(String cacheName, AffinityTopologyVersion topVer, Set<? extends K> keys, boolean near) {
            this.cacheName = cacheName;
            this.topVer = topVer;
            this.keys = keys;
            this.near = near;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Object arg) throws IgniteException {
            Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

            for (ClusterNode node : subgrid) {
                ComputeJob job;

                if (near && node.version().compareTo(NEAR_JOB_SINCE) >= 0) {
                    job = keys == null ? new GlobalClearAllNearJob(cacheName, topVer) :
                        new GlobalClearKeySetNearJob<>(cacheName, topVer, keys);
                }
                else {
                    job = keys == null ? new GlobalClearAllJob(cacheName, topVer) :
                        new GlobalClearKeySetJob<>(cacheName, topVer, keys);
                }

                jobs.put(job, node);
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

    /**
     * Iterator implementation for KeySet.
     */
    private final class KeySetIterator implements Iterator<K> {
        /** Internal map entry iterator. */
        private final Iterator<GridCacheMapEntry> internalIterator;

        /** Keep binary flag. */
        private final boolean keepBinary;

        /** Current entry. */
        private GridCacheMapEntry current;

        /**
         * Constructor.
         * @param internalIterator Internal iterator.
         * @param keepBinary Keep binary flag.
         */
        private KeySetIterator(Iterator<GridCacheMapEntry> internalIterator, boolean keepBinary) {
            this.internalIterator = internalIterator;
            this.keepBinary = keepBinary;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return internalIterator.hasNext();
        }

        /** {@inheritDoc} */
        @Override public K next() {
            current = internalIterator.next();

            return (K)ctx.unwrapBinaryIfNeeded(current.key(), keepBinary, true);
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            if (current == null)
                throw new IllegalStateException();

            try {
                GridCacheAdapter.this.getAndRemove((K)current.key());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            current = null;
        }
    }

    /**
     * A wrapper over internal map that provides set semantics and constant-time contains() check.
     */
    private final class KeySet extends AbstractSet<K> {
        /** Internal entry set. */
        private final Set<GridCacheMapEntry> internalSet;

        /** Keep binary flag. */
        private final boolean keepBinary;

        /**
         * Constructor
         * @param internalSet Internal set.
         */
        private KeySet(Set<GridCacheMapEntry> internalSet) {
            this.internalSet = internalSet;

            CacheOperationContext opCtx = ctx.operationContextPerCall();

            keepBinary = opCtx != null && opCtx.isKeepBinary();
        }

        /** {@inheritDoc} */
        @Override public Iterator<K> iterator() {
            return new KeySetIterator(internalSet.iterator(), keepBinary);
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return F.size(iterator());
        }

        /** {@inheritDoc} */
        @Override public boolean contains(Object o) {
            GridCacheMapEntry entry = map.getEntry(ctx.toCacheKeyObject(o));

            return entry != null && internalSet.contains(entry);
        }
    }

    /**
     * Iterator implementation for EntrySet.
     */
    private final class EntryIterator implements Iterator<Cache.Entry<K, V>> {

        /** Internal iterator. */
        private final Iterator<GridCacheMapEntry> internalIterator;

        /** Current entry. */
        private GridCacheMapEntry current;

        /** Keep binary flag. */
        private final boolean keepBinary;

        /**
         * Constructor.
         * @param internalIterator Internal iterator.
         */
        private EntryIterator(Iterator<GridCacheMapEntry> internalIterator, boolean keepBinary) {
            this.internalIterator = internalIterator;
            this.keepBinary = keepBinary;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return internalIterator.hasNext();
        }

        /** {@inheritDoc} */
        @Override public Cache.Entry<K, V> next() {
            current = internalIterator.next();

            return current.wrapLazyValue(keepBinary);
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            if (current == null)
                throw new IllegalStateException();

            try {
                GridCacheAdapter.this.getAndRemove((K)current.wrapLazyValue(keepBinary).getKey());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            current = null;
        }
    }

    /**
     * A wrapper over internal map that provides set semantics and constant-time contains() check.
     */
    private final class EntrySet extends AbstractSet<Cache.Entry<K, V>> {

        /** Internal set. */
        private final Set<GridCacheMapEntry> internalSet;

        /** Keep binary flag. */
        private final boolean keepBinary;

        /** Constructor. */
        private EntrySet(Set<GridCacheMapEntry> internalSet, boolean keepBinary) {
            this.internalSet = internalSet;
            this.keepBinary = keepBinary;
        }

        /** {@inheritDoc} */
        @Override public Iterator<Cache.Entry<K, V>> iterator() {
            return new EntryIterator(internalSet.iterator(), keepBinary);
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return F.size(iterator());
        }

        /** {@inheritDoc} */
        @Override public boolean contains(Object o) {
            GridCacheMapEntry entry = map.getEntry(ctx.toCacheKeyObject(o));

            return entry != null && internalSet.contains(entry);
        }
    }
}

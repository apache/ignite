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
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.ReadRepairStrategy;
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
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteTransactionsEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.managers.discovery.IgniteClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.affinity.GridCacheAffinityImpl;
import org.apache.ignite.internal.processors.cache.distributed.IgniteExternalizableExpiryPolicy;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.near.consistency.GridCompoundReadRepairFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.consistency.IgniteAtomicConsistencyViolationException;
import org.apache.ignite.internal.processors.cache.distributed.near.consistency.IgniteConsistencyViolationException;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheRawVersionedEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerEntry;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.processors.dr.IgniteDrDataStreamerCacheUpdater;
import org.apache.ignite.internal.processors.performancestatistics.OperationType;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheEntryFilter;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.transactions.TransactionCheckedException;
import org.apache.ignite.internal.util.GridSerializableMap;
import org.apache.ignite.internal.util.future.GridEmbeddedFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridPlainCallable;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.C1;
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
import org.apache.ignite.internal.util.typedef.internal.LT;
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
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_RETRIES_COUNT;
import static org.apache.ignite.internal.GridClosureCallMode.BROADCAST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_LOAD;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;
import static org.apache.ignite.internal.processors.task.TaskExecutionOptions.options;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Adapter for different cache implementations.
 */
@SuppressWarnings("unchecked")
public abstract class GridCacheAdapter<K, V> implements IgniteInternalCache<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** clearLocally() split threshold. */
    public static final int CLEAR_ALL_SPLIT_THRESHOLD = 10000;

    /** @see IgniteSystemProperties#IGNITE_CACHE_START_SIZE */
    public static final int DFLT_CACHE_START_SIZE = 4096;

    /** Default cache start size. */
    public static final int DFLT_START_CACHE_SIZE = IgniteSystemProperties.getInteger(
        IgniteSystemProperties.IGNITE_CACHE_START_SIZE, DFLT_CACHE_START_SIZE);

    /** Size of keys batch to removeAll. */
    private static final int REMOVE_ALL_KEYS_BATCH = 10000;

    /** @see IgniteSystemProperties#IGNITE_CACHE_RETRIES_COUNT */
    public static final int DFLT_CACHE_RETRIES_COUNT = 100;

    /** Maximum number of retries when topology changes. */
    public static final int MAX_RETRIES =
        IgniteSystemProperties.getInteger(IGNITE_CACHE_RETRIES_COUNT, DFLT_CACHE_RETRIES_COUNT);

    /** Minimum version supporting partition preloading. */
    private static final IgniteProductVersion PRELOAD_PARTITION_SINCE = IgniteProductVersion.fromString("2.7.0");

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

    /** Cache metrics. */
    protected CacheMetricsImpl metrics;

    /** Logger. */
    protected IgniteLogger log;

    /** Logger. */
    protected IgniteLogger txLockMsgLog;

    /** Affinity impl. */
    private Affinity<K> aff;

    /** Asynchronous operations limit semaphore. */
    private Semaphore asyncOpsSem;

    /** {@code True} if attempted to use partition preloading on outdated node. */
    private volatile boolean partPreloadBadVerWarned;

    /** Active. */
    private volatile boolean active;

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
     */
    protected GridCacheAdapter(GridCacheContext<K, V> ctx) {
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

        cacheCfg = ctx.config();

        locNodeId = ctx.gridConfig().getNodeId();

        this.map = map;

        log = ctx.logger(getClass());
        txLockMsgLog = ctx.shared().txLockMessageLogger();

        metrics = new CacheMetricsImpl(ctx, isNear());

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
     *
     * @param e Map entry.
     */
    public void incrementSize(GridCacheMapEntry e) {
        map.incrementPublicSize(null, e);
    }

    /**
     * Decrements map public size.
     *
     * @param e Map entry.
     */
    public void decrementSize(GridCacheMapEntry e) {
        map.decrementPublicSize(null, e);
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
     *
     */
    public boolean active() {
        return active;
    }

    /**
     * @param active Active.
     */
    public void active(boolean active) {
        this.active = active;
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
    @Override public final <K1, V1> IgniteInternalCache<K1, V1> cache() {
        return (IgniteInternalCache<K1, V1>)this;
    }

    /** {@inheritDoc} */
    @Override public final boolean skipStore() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public final GridCacheProxyImpl<K, V> setSkipStore(boolean skipStore) {
        CacheOperationContext opCtx = new CacheOperationContext(
            true,
            false,
            null,
            false,
            null,
            false,
            null);

        return new GridCacheProxyImpl<>(ctx, this, opCtx);
    }

    /** {@inheritDoc} */
    @Override public final <K1, V1> GridCacheProxyImpl<K1, V1> keepBinary() {
        CacheOperationContext opCtx = new CacheOperationContext(
            false,
            true,
            null,
            false,
            null,
            false,
            null);

        return new GridCacheProxyImpl<>((GridCacheContext<K1, V1>)ctx, (GridCacheAdapter<K1, V1>)this, opCtx);
    }

    /** {@inheritDoc} */
    @Nullable @Override public final ExpiryPolicy expiry() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public final GridCacheProxyImpl<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        assert !CU.isUtilityCache(ctx.name());

        CacheOperationContext opCtx = new CacheOperationContext(
            false,
            false,
            plc,
            false,
            null,
            false,
            null);

        return new GridCacheProxyImpl<>(ctx, this, opCtx);
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalCache<K, V> withNoRetries() {
        CacheOperationContext opCtx = new CacheOperationContext(
            false,
            false,
            null,
            true,
            null,
            false,
            null);

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
     * Starts this cache. Child classes should override this method
     * to provide custom start-up behavior.
     *
     * @throws IgniteCheckedException If start failed.
     */
    public abstract void start() throws IgniteCheckedException;

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
     * Remove cache metrics.
     *
     * @param destroy Group destroy flag.
     */
    public void removeMetrics(boolean destroy) {
        if (!ctx.kernalContext().isStopping())
            ctx.kernalContext().metric().remove(cacheMetricsRegistryName(ctx.name(), isNear()), destroy);
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
    public void onKernalStart() throws IgniteCheckedException {
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
            return localSize(null) == 0;
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

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        IgniteInternalFuture fut = getAsync(
            key,
            /*force primary*/ !ctx.config().isReadFromBackup(),
            /*skip tx*/false,
            /*task name*/null,
            !ctx.keepBinary(),
            /*skip values*/true,
            false);

        ReadRepairStrategy readRepairStrategy = opCtx != null ? opCtx.readRepairStrategy() : null;

        if (readRepairStrategy != null)
            return getWithRepairAsync(
                fut,
                (e) -> repairAsync(e, opCtx, true),
                () -> containsKeyAsync(key));

        return fut;
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

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        return repairableGetAllAsync(
            keys,
            /*force primary*/ !ctx.config().isReadFromBackup(),
            /*skip tx*/false,
            /*task name*/null,
            !ctx.keepBinary(),
            opCtx != null && opCtx.recovery(),
            opCtx != null ? opCtx.readRepairStrategy() : null,
            /*skip values*/true,
            /*need ver*/false).chain(new CX1<IgniteInternalFuture<Map<K, V>>, Boolean>() {
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
    @Override public final Iterable<Cache.Entry<K, V>> localEntries(
        CachePeekMode[] peekModes) throws IgniteCheckedException {
        assert peekModes != null;

        ctx.checkSecurity(SecurityPermission.CACHE_READ);

        PeekModes modes = parsePeekModes(peekModes, false);

        Collection<Iterator<Cache.Entry<K, V>>> its = new ArrayList<>();

        final boolean keepBinary = ctx.keepBinary();

        if (modes.offheap) {
            if (modes.heap && modes.near && ctx.isNear())
                its.add(ctx.near().nearEntries().iterator());

            if (modes.primary || modes.backup) {
                AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

                IgniteCacheOffheapManager offheapMgr = ctx.isNear() ? ctx.near().dht().context().offheap() : ctx.offheap();

                its.add(offheapMgr.cacheEntriesIterator(ctx, modes.primary, modes.backup, topVer, ctx.keepBinary(), null));
            }
        }
        else if (modes.heap) {
            if (modes.near && ctx.isNear())
                its.add(ctx.near().nearEntries().iterator());

            if (modes.primary || modes.backup) {
                GridDhtCacheAdapter<K, V> cache = ctx.isNear() ? ctx.near().dht() : ctx.dht();

                its.add(cache.localEntriesIterator(modes.primary, modes.backup, keepBinary));
            }
        }

        final Iterator<Cache.Entry<K, V>> it = F.flatIterators(its);

        return new Iterable<Cache.Entry<K, V>>() {
            @Override public Iterator<Cache.Entry<K, V>> iterator() {
                return it;
            }

            @Override public String toString() {
                return "CacheLocalEntries []";
            }
        };
    }

    /** {@inheritDoc} */
    @Override public final V localPeek(K key,
        CachePeekMode[] peekModes)
        throws IgniteCheckedException {
        A.notNull(key, "key");

        ctx.checkSecurity(SecurityPermission.CACHE_READ);

        PeekModes modes = parsePeekModes(peekModes, false);

        KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

        CacheObject cacheVal = null;

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
                }
            }
        }
        else {
            nearKey = !ctx.affinity().partitionBelongs(ctx.localNode(), part, topVer);

            if (nearKey) {
                // Swap and offheap are disabled for near cache.
                modes.offheap = false;
            }
        }

        if (nearKey && !ctx.isNear())
            return null;

        GridCacheEntryEx e;
        GridCacheContext ctx0;

        while (true) {
            if (nearKey)
                e = peekEx(key);
            else {
                ctx0 = ctx.isNear() ? ctx.near().dht().context() : ctx;
                e = modes.offheap ? ctx0.cache().entryEx(key) : ctx0.cache().peekEx(key);
            }

            if (e != null) {
                ctx.shared().database().checkpointReadLock();

                try {
                    cacheVal = e.peek(modes.heap, modes.offheap, topVer, null);
                }
                catch (GridCacheEntryRemovedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry during 'peek': " + key);

                    continue;
                }
                finally {
                    e.touch();

                    ctx.shared().database().checkpointReadUnlock();
                }
            }

            break;
        }

        Object val = ctx.unwrapBinaryIfNeeded(cacheVal, ctx.keepBinary(), false, null);

        return (V)val;
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
        return entryEx(ctx.toCacheKeyObject(key));
    }

    /**
     * @param key Entry key.
     * @return Entry (never {@code null}).
     */
    public final GridCacheEntryEx entryEx(KeyCacheObject key) {
        return entryEx(key, ctx.affinity().affinityTopologyVersion());
    }

    /**
     * @param topVer Topology version.
     * @param key Entry key.
     * @return Entry (never {@code null}).
     */
    public GridCacheEntryEx entryEx(KeyCacheObject key, AffinityTopologyVersion topVer) {
        GridCacheEntryEx e = map.putEntryIfObsoleteOrAbsent(ctx, topVer, key, true, false);

        assert e != null;

        return e;
    }

    /**
     * @param key Entry key.
     * @param topVer Topology version at the time of creation.
     * @param create Flag to create entry if it does not exist.
     * @param skipReserve Flag to skip partition reservation.
     * @return Entry or <tt>null</tt>.
     */
    @Nullable private GridCacheEntryEx entry0(KeyCacheObject key, AffinityTopologyVersion topVer, boolean create,
        boolean skipReserve) {
        GridCacheMapEntry cur = map.getEntry(ctx, key);

        if (cur == null || cur.obsolete()) {
            cur = map.putEntryIfObsoleteOrAbsent(
                ctx,
                topVer,
                key,
                create,
                skipReserve);
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
        return map.entries(ctx.cacheId());
    }

    /** {@inheritDoc} */
    @Override public final Set<Cache.Entry<K, V>> entrySet() {
        return entrySet((CacheEntryPredicate[])null);
    }

    /** {@inheritDoc} */
    @Override public final Set<K> keySet() {
        return new KeySet(map.entrySet(ctx.cacheId()));
    }

    /**
     *
     * @param key Entry key.
     */
    public final void removeIfObsolete(KeyCacheObject key) {
        assert key != null;

        GridCacheMapEntry entry = map.getEntry(ctx, key);

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

            GridCacheVersion obsoleteVer = nextVersion();

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

            try {
                if (jobs.size() > 1) {
                    execSvc = Executors.newFixedThreadPool(jobs.size() - 1,
                        new IgniteThreadFactory(ctx.igniteInstanceName(), "async-cache-cleaner"));

                    for (int i = 1; i < jobs.size(); i++)
                        execSvc.execute(jobs.get(i));
                }

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
        ctx.shared().cache().checkReadOnlyState("clear", ctx.config());

        executeClearTask(keys, false).get();
        executeClearTask(keys, true).get();
    }

    /**
     * @param keys Keys to clear or {@code null} if all cache should be cleared.
     * @return Future.
     */
    private IgniteInternalFuture<?> clearAsync(@Nullable final Set<? extends K> keys) {
        ctx.shared().cache().checkReadOnlyState("clear", ctx.config());

        return executeClearTask(keys, false).chainCompose(fut -> executeClearTask(keys, true));
    }

    /**
     * @param keys Keys to clear.
     * @param near Near cache flag.
     * @return Future.
     */
    private IgniteInternalFuture<?> executeClearTask(@Nullable Set<? extends K> keys, boolean near) {
        Collection<ClusterNode> srvNodes = ctx.grid().cluster().forCacheNodes(name(), !near, near, false).nodes();

        if (!srvNodes.isEmpty()) {
            return ctx.kernalContext().task().execute(
                new ClearTask(ctx.name(), ctx.affinity().affinityTopologyVersion(), keys, near),
                null,
                options(srvNodes)
            );
        }
        else
            return new GridFinishedFuture<>();
    }

    /**
     * @param part Partition id.
     * @return Future.
     */
    private IgniteInternalFuture<?> executePreloadTask(int part) throws IgniteCheckedException {
        ClusterGroup grp = ctx.grid().cluster().forDataNodes(ctx.name());

        @Nullable ClusterNode targetNode = ctx.affinity().primaryByPartition(part, ctx.topology().readyTopologyVersion());

        if (targetNode == null || targetNode.version().compareTo(PRELOAD_PARTITION_SINCE) < 0) {
            if (!partPreloadBadVerWarned) {
                U.warn(log(), "Attempting to execute partition preloading task on outdated or not mapped node " +
                    "[targetNodeVer=" + (targetNode == null ? "NA" : targetNode.version()) +
                    ", minSupportedNodeVer=" + PRELOAD_PARTITION_SINCE + ']');

                partPreloadBadVerWarned = true;
            }

            return new GridFinishedFuture<>();
        }

        return ctx.closures().affinityRun(
            Collections.singleton(name()),
            part,
            new PartitionPreloadJob(ctx.name(), part),
            options(grp.nodes())
        );
    }

    /**
     * @param keys Keys.
     * @param readers Readers flag.
     */
    public void clearLocally(Collection<KeyCacheObject> keys, boolean readers) {
        if (F.isEmpty(keys))
            return;

        GridCacheVersion obsoleteVer = nextVersion();

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
    @Override public Collection<Integer> lostPartitions() {
        return ctx.topology().lostPartitions();
    }

    /** {@inheritDoc} */
    @Override public final V getForcePrimary(K key) throws IgniteCheckedException {
        String taskName = ctx.kernalContext().job().currentTaskName();

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        return repairableGetAllAsync(
            F.asList(key),
            /*force primary*/true,
            /*skip tx*/false,
            taskName,
            /*deserialize cache objects*/true,
            opCtx != null && opCtx.recovery(),
            opCtx != null ? opCtx.readRepairStrategy() : null,
            /*skip values*/false,
            /*need ver*/false).get().get(key);
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalFuture<V> getForcePrimaryAsync(final K key) {
        String taskName = ctx.kernalContext().job().currentTaskName();

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        return repairableGetAllAsync(
            Collections.singletonList(key),
            /*force primary*/true,
            /*skip tx*/false,
            taskName,
            true,
            opCtx != null && opCtx.recovery(),
            opCtx != null ? opCtx.readRepairStrategy() : null,
            /*skip vals*/false,
            /*can remap*/false).chain(new CX1<IgniteInternalFuture<Map<K, V>>, V>() {
                @Override public V applyx(IgniteInternalFuture<Map<K, V>> e) throws IgniteCheckedException {
                    return e.get().get(key);
                }
            });
    }

    /** {@inheritDoc} */
    @Nullable @Override public final Map<K, V> getAllOutTx(Set<? extends K> keys) throws IgniteCheckedException {
        return getAllOutTxAsync(keys).get();
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalFuture<Map<K, V>> getAllOutTxAsync(Set<? extends K> keys) {
        final boolean statsEnabled = ctx.statisticsEnabled();
        final boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        final long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        String taskName = ctx.kernalContext().job().currentTaskName();

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        IgniteInternalFuture<Map<K, V>> fut = repairableGetAllAsync(keys,
            !ctx.config().isReadFromBackup(),
            /*skip tx*/true,
            taskName,
            !(opCtx != null && opCtx.isKeepBinary()),
            opCtx != null && opCtx.recovery(),
            opCtx != null ? opCtx.readRepairStrategy() : null,
            /*skip values*/false,
            /*need ver*/false);

        if (statsEnabled)
            fut.listen(new UpdateGetAllTimeStatClosure<>(metrics0(), start));

        if (performanceStatsEnabled)
            fut.listen(() -> writeStatistics(OperationType.CACHE_GET_ALL, start));

        return fut;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V get(K key) throws IgniteCheckedException {
        A.notNull(key, "key");

        boolean statsEnabled = ctx.statisticsEnabled();
        boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        boolean keepBinary = ctx.keepBinary();

        if (keepBinary)
            key = (K)ctx.toCacheKeyObject(key);

        V val = repairableGet(key, !keepBinary, false);

        if (ctx.config().getInterceptor() != null) {
            key = keepBinary ? (K)ctx.unwrapBinaryIfNeeded(key, true, false, null) : key;

            val = (V)ctx.config().getInterceptor().onGet(key, val);
        }

        if (statsEnabled)
            metrics0().addGetTimeNanos(System.nanoTime() - start);

        if (performanceStatsEnabled)
            writeStatistics(OperationType.CACHE_GET, start);

        return val;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheEntry<K, V> getEntry(K key) throws IgniteCheckedException {
        A.notNull(key, "key");

        boolean statsEnabled = ctx.statisticsEnabled();
        boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        boolean keepBinary = ctx.keepBinary();

        if (keepBinary)
            key = (K)ctx.toCacheKeyObject(key);

        EntryGetResult t
            = (EntryGetResult)repairableGet(key, !keepBinary, true);

        CacheEntry<K, V> val = t != null ? new CacheEntryImplEx<>(
            keepBinary ? (K)ctx.unwrapBinaryIfNeeded(key, true, false, null) : key,
            (V)t.value(),
            t.version())
            : null;

        if (ctx.config().getInterceptor() != null) {
            key = keepBinary ? (K)ctx.unwrapBinaryIfNeeded(key, true, false, null) : key;

            V val0 = (V)ctx.config().getInterceptor().onGet(key, t != null ? val.getValue() : null);

            val = (val0 != null) ? new CacheEntryImplEx<>(key, val0, t != null ? t.version() : null) : null;
        }

        if (statsEnabled)
            metrics0().addGetTimeNanos(System.nanoTime() - start);

        if (performanceStatsEnabled)
            writeStatistics(OperationType.CACHE_GET, start);

        return val;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAsync(final K key) {
        A.notNull(key, "key");

        final boolean statsEnabled = ctx.statisticsEnabled();
        final boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        final long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        final boolean keepBinary = ctx.keepBinary();

        final K key0 = keepBinary ? (K)ctx.toCacheKeyObject(key) : key;

        final CacheOperationContext opCtx = ctx.operationContextPerCall();

        IgniteInternalFuture<V> fut = repairableGetAsync(
            key,
            !keepBinary,
            false,
            opCtx != null ? opCtx.readRepairStrategy() : null);

        if (ctx.config().getInterceptor() != null)
            fut = fut.chain(new CX1<IgniteInternalFuture<V>, V>() {
                @Override public V applyx(IgniteInternalFuture<V> f) throws IgniteCheckedException {
                    K key = keepBinary ? (K)ctx.unwrapBinaryIfNeeded(key0, true, false, null) : key0;

                    return (V)ctx.config().getInterceptor().onGet(key, f.get());
                }
            });

        if (statsEnabled)
            fut.listen(new UpdateGetTimeStatClosure<V>(metrics0(), start));

        if (performanceStatsEnabled)
            fut.listen(() -> writeStatistics(OperationType.CACHE_GET, start));

        return fut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<CacheEntry<K, V>> getEntryAsync(final K key) {
        A.notNull(key, "key");

        final boolean statsEnabled = ctx.statisticsEnabled();
        final boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        final long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        final boolean keepBinary = ctx.keepBinary();

        final K key0 = keepBinary ? (K)ctx.toCacheKeyObject(key) : key;

        final CacheOperationContext opCtx = ctx.operationContextPerCall();

        IgniteInternalFuture<EntryGetResult> fut =
            (IgniteInternalFuture<EntryGetResult>)repairableGetAsync(
                key0,
                !keepBinary,
                true,
                opCtx != null ? opCtx.readRepairStrategy() : null);

        final boolean intercept = ctx.config().getInterceptor() != null;

        IgniteInternalFuture<CacheEntry<K, V>> fr = fut.chain(
            new CX1<IgniteInternalFuture<EntryGetResult>, CacheEntry<K, V>>() {
                @Override public CacheEntry<K, V> applyx(IgniteInternalFuture<EntryGetResult> f)
                    throws IgniteCheckedException {
                    EntryGetResult t = f.get();

                    K key = keepBinary ? (K)ctx.unwrapBinaryIfNeeded(key0, true, false, null) : key0;

                    CacheEntry val = t != null ? new CacheEntryImplEx<>(
                        key,
                        t.value(),
                        t.version())
                        : null;

                    if (intercept) {
                        V val0 = (V)ctx.config().getInterceptor().onGet(key, t != null ? val.getValue() : null);

                        return val0 != null ? new CacheEntryImplEx(key, val0, t != null ? t.version() : null) : null;
                    }
                    else
                        return val;
                }
            });

        if (statsEnabled)
            fut.listen(new UpdateGetTimeStatClosure<EntryGetResult>(metrics0(), start));

        if (performanceStatsEnabled)
            fut.listen(() -> writeStatistics(OperationType.CACHE_GET, start));

        return fr;
    }

    /** {@inheritDoc} */
    @Override public final Map<K, V> getAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        A.notNull(keys, "keys");

        boolean statsEnabled = ctx.statisticsEnabled();
        boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        final CacheOperationContext opCtx = ctx.operationContextPerCall();

        Map<K, V> map = repairableGetAll(
            keys,
            !ctx.keepBinary(),
            false,
            opCtx != null && opCtx.recovery(),
            opCtx != null ? opCtx.readRepairStrategy() : null);

        if (ctx.config().getInterceptor() != null)
            map = interceptGet(keys, map);

        if (statsEnabled)
            metrics0().addGetAllTimeNanos(System.nanoTime() - start);

        if (performanceStatsEnabled)
            writeStatistics(OperationType.CACHE_GET_ALL, start);

        return map;
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheEntry<K, V>> getEntries(@Nullable Collection<? extends K> keys)
        throws IgniteCheckedException {
        A.notNull(keys, "keys");

        boolean statsEnabled = ctx.statisticsEnabled();
        boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        final CacheOperationContext opCtx = ctx.operationContextPerCall();

        Map<K, EntryGetResult> map = (Map<K, EntryGetResult>)repairableGetAll(
            keys,
            !ctx.keepBinary(),
            true,
            opCtx != null && opCtx.recovery(),
            opCtx != null ? opCtx.readRepairStrategy() : null);

        Collection<CacheEntry<K, V>> res = new HashSet<>();

        if (ctx.config().getInterceptor() != null)
            res = interceptGetEntries(keys, map);
        else
            for (Map.Entry<K, EntryGetResult> e : map.entrySet())
                res.add(new CacheEntryImplEx<>(e.getKey(), (V)e.getValue().value(), e.getValue().version()));

        if (statsEnabled)
            metrics0().addGetAllTimeNanos(System.nanoTime() - start);

        if (performanceStatsEnabled)
            writeStatistics(OperationType.CACHE_GET_ALL, start);

        return res;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<K, V>> getAllAsync(@Nullable final Collection<? extends K> keys) {
        A.notNull(keys, "keys");

        final boolean statsEnabled = ctx.statisticsEnabled();
        final boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        final long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        String taskName = ctx.kernalContext().job().currentTaskName();

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        IgniteInternalFuture<Map<K, V>> fut = repairableGetAllAsync(
            keys,
            !ctx.config().isReadFromBackup(),
            /*skip tx*/false,
            taskName,
            !(opCtx != null && opCtx.isKeepBinary()),
            opCtx != null && opCtx.recovery(),
            opCtx != null ? opCtx.readRepairStrategy() : null,
            /*skip vals*/false,
            /*need ver*/false);

        if (ctx.config().getInterceptor() != null)
            return fut.chain(new CX1<IgniteInternalFuture<Map<K, V>>, Map<K, V>>() {
                @Override public Map<K, V> applyx(IgniteInternalFuture<Map<K, V>> f) throws IgniteCheckedException {
                    return interceptGet(keys, f.get());
                }
            });

        if (statsEnabled)
            fut.listen(new UpdateGetAllTimeStatClosure<Map<K, V>>(metrics0(), start));

        if (performanceStatsEnabled)
            fut.listen(() -> writeStatistics(OperationType.CACHE_GET_ALL, start));

        return fut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Collection<CacheEntry<K, V>>> getEntriesAsync(
        @Nullable final Collection<? extends K> keys) {
        A.notNull(keys, "keys");

        final boolean statsEnabled = ctx.statisticsEnabled();
        final boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        final long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        String taskName = ctx.kernalContext().job().currentTaskName();

        IgniteInternalFuture<Map<K, EntryGetResult>> fut =
            (IgniteInternalFuture<Map<K, EntryGetResult>>)((IgniteInternalFuture)repairableGetAllAsync(
                keys,
                !ctx.config().isReadFromBackup(),
                /*skip tx*/false,
                taskName,
                !(opCtx != null && opCtx.isKeepBinary()),
                opCtx != null && opCtx.recovery(),
                opCtx != null ? opCtx.readRepairStrategy() : null,
                /*skip vals*/false,
                /*need ver*/true));

        final boolean intercept = ctx.config().getInterceptor() != null;

        IgniteInternalFuture<Collection<CacheEntry<K, V>>> rf =
            fut.chain(new CX1<IgniteInternalFuture<Map<K, EntryGetResult>>, Collection<CacheEntry<K, V>>>() {
                @Override public Collection<CacheEntry<K, V>> applyx(
                    IgniteInternalFuture<Map<K, EntryGetResult>> f) throws IgniteCheckedException {
                    if (intercept)
                        return interceptGetEntries(keys, f.get());
                    else {
                        Map<K, CacheEntry<K, V>> res = U.newHashMap(f.get().size());

                        for (Map.Entry<K, EntryGetResult> e : f.get().entrySet())
                            res.put(e.getKey(),
                                new CacheEntryImplEx<>(e.getKey(), (V)e.getValue().value(), e.getValue().version()));

                        return res.values();
                    }
                }
            });

        if (statsEnabled)
            fut.listen(new UpdateGetAllTimeStatClosure<Map<K, EntryGetResult>>(metrics0(), start));

        if (performanceStatsEnabled)
            fut.listen(() -> writeStatistics(OperationType.CACHE_GET_ALL, start));

        return rf;
    }

    /**
     * Applies cache interceptor on result of 'get' operation.
     *
     * @param keys All requested keys.
     * @param map Result map.
     * @return Map with values returned by cache interceptor..
     */
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
    private Collection<CacheEntry<K, V>> interceptGetEntries(
        @Nullable Collection<? extends K> keys, Map<K, EntryGetResult> map) {
        if (F.isEmpty(keys)) {
            assert map.isEmpty();

            return Collections.emptySet();
        }

        Map<K, CacheEntry<K, V>> res = U.newHashMap(keys.size());

        CacheInterceptor<K, V> interceptor = cacheCfg.getInterceptor();

        assert interceptor != null;

        for (Map.Entry<K, EntryGetResult> e : map.entrySet()) {
            V val = interceptor.onGet(e.getKey(), (V)e.getValue().value());

            if (val != null)
                res.put(e.getKey(), new CacheEntryImplEx<>(e.getKey(), val, e.getValue().version()));
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
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary.
     * @param skipVals Skip values.
     * @param needVer Need version.
     * @return Future for the get operation.
     */
    protected IgniteInternalFuture<V> getAsync(
        final K key,
        boolean forcePrimary,
        boolean skipTx,
        String taskName,
        boolean deserializeBinary,
        final boolean skipVals,
        final boolean needVer
    ) {
        CacheOperationContext opCtx = ctx.operationContextPerCall();

        return getAllAsync(Collections.singletonList(key),
            forcePrimary,
            skipTx,
            taskName,
            deserializeBinary,
            opCtx != null && opCtx.recovery(),
            opCtx != null ? opCtx.readRepairStrategy() : null,
            skipVals,
            needVer).chain(new CX1<IgniteInternalFuture<Map<K, V>>, V>() {
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
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary.
     * @param recovery Recovery mode flag.
     * @param skipVals Skip values.
     * @param needVer Need version.
     * @return Future for the get operation.
     * @see GridCacheAdapter#getAllAsync(Collection)
     */
    protected abstract IgniteInternalFuture<Map<K, V>> getAllAsync(
        @Nullable Collection<? extends K> keys,
        boolean forcePrimary,
        boolean skipTx,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        ReadRepairStrategy readRepairStrategy,
        boolean skipVals,
        final boolean needVer
    );

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
        boolean statsEnabled = ctx.statisticsEnabled();
        boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key", val, "val");

        V prevVal = getAndPut0(key, val, filter);

        if (statsEnabled)
            metrics0().addPutAndGetTimeNanos(System.nanoTime() - start);

        if (performanceStatsEnabled)
            writeStatistics(OperationType.CACHE_GET_AND_PUT, start);

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
            @Override public V op(GridNearTxLocal tx) throws IgniteCheckedException {
                return (V)tx.putAsync(ctx, null, key, val, true, filter).get().value();
            }

            @Override public String toString() {
                return S.toString("put",
                    "key", key, true,
                    "val", val, true,
                    "filter", filter, false);
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
        final boolean statsEnabled = ctx.statisticsEnabled();
        final boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        final long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key", val, "val");

        IgniteInternalFuture<V> fut = getAndPutAsync0(key, val, filter);

        if (statsEnabled)
            fut.listen(new UpdatePutAndGetTimeStatClosure<V>(metrics0(), start));

        if (performanceStatsEnabled)
            fut.listen(() -> writeStatistics(OperationType.CACHE_GET_AND_PUT, start));

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
        @Nullable final CacheEntryPredicate filter) {
        return asyncOp(new AsyncOp<V>() {
            @Override public IgniteInternalFuture<V> op(GridNearTxLocal tx, AffinityTopologyVersion readyTopVer) {
                return tx.putAsync(ctx, readyTopVer, key, val, true, filter).chain(RET2VAL);
            }

            @Override public String toString() {
                return S.toString("putAsync",
                    "key", key, true,
                    "val", val, true,
                    "filter", filter, false);
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
        boolean statsEnabled = ctx.statisticsEnabled();
        boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key", val, "val");

        boolean stored = put0(key, val, filter);

        if (statsEnabled && stored)
            metrics0().addPutTimeNanos(System.nanoTime() - start);

        if (performanceStatsEnabled)
            writeStatistics(OperationType.CACHE_PUT, start);

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
            @Override public Boolean op(GridNearTxLocal tx) throws IgniteCheckedException {
                return tx.putAsync(ctx, null, key, val, false, filter).get().success();
            }

            @Override public String toString() {
                return S.toString("putx",
                    "key", key, true,
                    "val", val, true,
                    "filter", filter, false);
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
            @Override public void inOp(GridNearTxLocal tx) throws IgniteCheckedException {
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
            @Override public IgniteInternalFuture op(GridNearTxLocal tx, AffinityTopologyVersion readyTopVer) {
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

        return syncOp(new SyncOp<EntryProcessorResult<T>>(true) {
            @Override public EntryProcessorResult<T> op(GridNearTxLocal tx)
                throws IgniteCheckedException {
                assert topVer == null || tx.implicit();

                if (topVer != null)
                    tx.topologyVersion(topVer);

                final boolean statsEnabled = ctx.statisticsEnabled();
                final boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

                long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

                IgniteInternalFuture<GridCacheReturn> fut = tx.invokeAsync(ctx,
                    null,
                    key,
                    (EntryProcessor<K, V, Object>)entryProcessor,
                    args);

                Map<K, EntryProcessorResult<T>> resMap = fut.get().value();

                if (statsEnabled)
                    metrics0().addInvokeTimeNanos(System.nanoTime() - start);

                if (performanceStatsEnabled)
                    writeStatistics(OperationType.CACHE_INVOKE, start);

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

        warnIfUnordered(keys, BulkOperation.INVOKE);

        final boolean statsEnabled = ctx.statisticsEnabled();
        final boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        return syncOp(new SyncOp<Map<K, EntryProcessorResult<T>>>(keys.size() == 1) {
            @Override public Map<K, EntryProcessorResult<T>> op(GridNearTxLocal tx)
                throws IgniteCheckedException {
                Map<? extends K, EntryProcessor<K, V, Object>> invokeMap = F.viewAsMap(keys,
                    new C1<K, EntryProcessor<K, V, Object>>() {
                        @Override public EntryProcessor apply(K k) {
                            return entryProcessor;
                        }
                    });

                IgniteInternalFuture<GridCacheReturn> fut = tx.invokeAsync(ctx, null, invokeMap, args);

                Map<K, EntryProcessorResult<T>> res = fut.get().value();

                if (statsEnabled)
                    metrics0().addInvokeTimeNanos(System.nanoTime() - start);

                if (performanceStatsEnabled)
                    writeStatistics(OperationType.CACHE_INVOKE_ALL, start);

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

        final boolean statsEnabled = ctx.statisticsEnabled();
        final boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        IgniteInternalFuture<?> fut = asyncOp(new AsyncOp() {
            @Override public IgniteInternalFuture op(GridNearTxLocal tx, AffinityTopologyVersion readyTopVer) {
                Map<? extends K, EntryProcessor<K, V, Object>> invokeMap =
                    Collections.singletonMap(key, (EntryProcessor<K, V, Object>)entryProcessor);

                return tx.invokeAsync(ctx, readyTopVer, invokeMap, args);
            }

            @Override public String toString() {
                return S.toString("invokeAsync",
                    "key", key, true,
                    "entryProcessor", entryProcessor, false);
            }
        });

        IgniteInternalFuture<GridCacheReturn> fut0 = (IgniteInternalFuture<GridCacheReturn>)fut;

        return fut0.chain(new CX1<IgniteInternalFuture<GridCacheReturn>, EntryProcessorResult<T>>() {
            @Override public EntryProcessorResult<T> applyx(IgniteInternalFuture<GridCacheReturn> fut)
                throws IgniteCheckedException {
                GridCacheReturn ret = fut.get();

                if (statsEnabled)
                    metrics0().addInvokeTimeNanos(System.nanoTime() - start);

                if (performanceStatsEnabled)
                    writeStatistics(OperationType.CACHE_INVOKE, start);

                Map<K, EntryProcessorResult<T>> resMap = ret.value();

                if (resMap != null) {
                    assert resMap.isEmpty() || resMap.size() == 1 : resMap.size();

                    return resMap.isEmpty() ? new CacheInvokeResult<>() : resMap.values().iterator().next();
                }

                return new CacheInvokeResult<>();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        final Set<? extends K> keys,
        final EntryProcessor<K, V, T> entryProcessor,
        final Object... args) {
        A.notNull(keys, "keys", entryProcessor, "entryProcessor");

        warnIfUnordered(keys, BulkOperation.INVOKE);

        final boolean statsEnabled = ctx.statisticsEnabled();
        final boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        IgniteInternalFuture<?> fut = asyncOp(new AsyncOp(keys) {
            @Override public IgniteInternalFuture<GridCacheReturn> op(GridNearTxLocal tx,
                AffinityTopologyVersion readyTopVer) {
                Map<? extends K, EntryProcessor<K, V, Object>> invokeMap = F.viewAsMap(keys, new C1<K, EntryProcessor<K, V, Object>>() {
                    @Override public EntryProcessor apply(K k) {
                        return entryProcessor;
                    }
                });

                return tx.invokeAsync(ctx, readyTopVer, invokeMap, args);
            }

            @Override public String toString() {
                return S.toString("invokeAllAsync",
                    "keys", keys, true,
                    "entryProcessor", entryProcessor, false);
            }
        });

        IgniteInternalFuture<GridCacheReturn> fut0 =
            (IgniteInternalFuture<GridCacheReturn>)fut;

        return fut0.chain(new CX1<IgniteInternalFuture<GridCacheReturn>, Map<K, EntryProcessorResult<T>>>() {
            @Override public Map<K, EntryProcessorResult<T>> applyx(IgniteInternalFuture<GridCacheReturn> fut)
                throws IgniteCheckedException {
                GridCacheReturn ret = fut.get();

                if (statsEnabled)
                    metrics0().addInvokeTimeNanos(System.nanoTime() - start);

                if (performanceStatsEnabled)
                    writeStatistics(OperationType.CACHE_INVOKE_ALL, start);

                assert ret != null;

                return ret.value() != null ? ret.value() : Collections.emptyMap();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        final Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        final Object... args) {
        A.notNull(map, "map");

        warnIfUnordered(map, BulkOperation.INVOKE);

        final boolean statsEnabled = ctx.statisticsEnabled();
        final boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        IgniteInternalFuture<?> fut = asyncOp(new AsyncOp(map.keySet()) {
            @Override public IgniteInternalFuture<GridCacheReturn> op(GridNearTxLocal tx,
                AffinityTopologyVersion readyTopVer) {
                return tx.invokeAsync(ctx,
                    readyTopVer,
                    (Map<? extends K, ? extends EntryProcessor<K, V, Object>>)map,
                    args);
            }

            @Override public String toString() {
                return S.toString("invokeAllAsync",
                    "map", map, true);
            }
        });

        IgniteInternalFuture<GridCacheReturn> fut0 = (IgniteInternalFuture<GridCacheReturn>)fut;

        return fut0.chain(new CX1<IgniteInternalFuture<GridCacheReturn>, Map<K, EntryProcessorResult<T>>>() {
            @Override public Map<K, EntryProcessorResult<T>> applyx(IgniteInternalFuture<GridCacheReturn> fut)
                throws IgniteCheckedException {
                GridCacheReturn ret = fut.get();

                if (statsEnabled)
                    metrics0().addInvokeTimeNanos(System.nanoTime() - start);

                if (performanceStatsEnabled)
                    writeStatistics(OperationType.CACHE_INVOKE_ALL, start);

                assert ret != null;

                return ret.value() != null
                    ? ret.<Map<K, EntryProcessorResult<T>>>value()
                    : Collections.<K, EntryProcessorResult<T>>emptyMap();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        final Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        final Object... args) throws IgniteCheckedException {
        A.notNull(map, "map");

        warnIfUnordered(map, BulkOperation.INVOKE);

        final boolean statsEnabled = ctx.statisticsEnabled();
        final boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        return syncOp(new SyncOp<Map<K, EntryProcessorResult<T>>>(map.size() == 1) {
            @Nullable @Override public Map<K, EntryProcessorResult<T>> op(GridNearTxLocal tx)
                throws IgniteCheckedException {
                IgniteInternalFuture<GridCacheReturn> fut =
                    tx.invokeAsync(ctx, null, (Map<? extends K, ? extends EntryProcessor<K, V, Object>>)map, args);

                Map<K, EntryProcessorResult<T>> val = fut.get().value();

                if (statsEnabled)
                    metrics0().addInvokeTimeNanos(System.nanoTime() - start);

                if (performanceStatsEnabled)
                    writeStatistics(OperationType.CACHE_INVOKE_ALL, start);

                return val;
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

        final boolean statsEnabled = ctx.statisticsEnabled();
        final boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        final long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        IgniteInternalFuture<Boolean> fut = putAsync0(key, val, filter);

        if (statsEnabled)
            fut.listen(new UpdatePutTimeStatClosure<Boolean>(metrics0(), start));

        if (performanceStatsEnabled)
            fut.listen(() -> writeStatistics(OperationType.CACHE_PUT, start));

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
            @Override public IgniteInternalFuture<Boolean> op(GridNearTxLocal tx, AffinityTopologyVersion readyTopVer) {
                return tx.putAsync(ctx,
                    readyTopVer,
                    key,
                    val,
                    false,
                    filter).chain(RET2FLAG);
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

        boolean statsEnabled = ctx.statisticsEnabled();
        boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        warnIfUnordered(m, BulkOperation.PUT);

        putAll0(m);

        if (statsEnabled)
            metrics0().addPutAllTimeNanos(System.nanoTime() - start);

        if (performanceStatsEnabled)
            writeStatistics(OperationType.CACHE_PUT_ALL, start);
    }

    /**
     * @param m Map.
     * @throws IgniteCheckedException If failed.
     */
    protected void putAll0(final Map<? extends K, ? extends V> m) throws IgniteCheckedException {
        syncOp(new SyncInOp(m.size() == 1) {
            @Override public void inOp(GridNearTxLocal tx) throws IgniteCheckedException {
                tx.putAllAsync(ctx, null, m, false).get();
            }

            @Override public String toString() {
                return S.toString("putAll",
                    "map", m, true);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> putAllAsync(final Map<? extends K, ? extends V> m) {
        if (F.isEmpty(m))
            return new GridFinishedFuture<Object>();

        boolean statsEnabled = ctx.statisticsEnabled();
        boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        warnIfUnordered(m, BulkOperation.PUT);

        IgniteInternalFuture<?> fut = putAllAsync0(m);

        if (statsEnabled)
            fut.listen(new UpdatePutAllTimeStatClosure<>(metrics0(), start));

        if (performanceStatsEnabled)
            fut.listen(() -> writeStatistics(OperationType.CACHE_PUT_ALL, start));

        return fut;
    }

    /**
     * @param m Map.
     * @return Future.
     */
    protected IgniteInternalFuture<?> putAllAsync0(final Map<? extends K, ? extends V> m) {
        return asyncOp(new AsyncOp(m.keySet()) {
            @Override public IgniteInternalFuture<?> op(GridNearTxLocal tx, AffinityTopologyVersion readyTopVer) {
                return tx.putAllAsync(ctx,
                    readyTopVer,
                    m,
                    false).chain(RET2NULL);
            }

            @Override public String toString() {
                return S.toString("putAllAsync",
                    "map", m, true);
            }
        });
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndRemove(final K key) throws IgniteCheckedException {
        boolean statsEnabled = ctx.statisticsEnabled();
        boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key");

        V prevVal = getAndRemove0(key);

        if (statsEnabled)
            metrics0().addRemoveAndGetTimeNanos(System.nanoTime() - start);

        if (performanceStatsEnabled)
            writeStatistics(OperationType.CACHE_GET_AND_REMOVE, start);

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
            @Override public V op(GridNearTxLocal tx) throws IgniteCheckedException {
                K key0 = keepBinary ? (K)ctx.toCacheKeyObject(key) : key;

                IgniteInternalFuture<GridCacheReturn> fut = tx.removeAllAsync(ctx,
                        null,
                        Collections.singletonList(key0),
                        /*retval*/true,
                        null,
                        /*singleRmv*/false);

                V ret = fut.get().value();

                if (ctx.config().getInterceptor() != null) {
                    K key = keepBinary ? (K)ctx.unwrapBinaryIfNeeded(key0, true, false, null) : key0;

                    return (V)ctx.config().getInterceptor().onBeforeRemove(new CacheEntryImpl(key, ret)).get2();
                }

                return ret;
            }

            @Override public String toString() {
                return S.toString("remove",
                    "key", key, true);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAndRemoveAsync(final K key) {
        final boolean statsEnabled = ctx.statisticsEnabled();
        final boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        final long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key");

        IgniteInternalFuture<V> fut = getAndRemoveAsync0(key);

        if (statsEnabled)
            fut.listen(new UpdateGetAndRemoveTimeStatClosure<V>(metrics0(), start));

        if (performanceStatsEnabled)
            fut.listen(() -> writeStatistics(OperationType.CACHE_GET_AND_REMOVE, start));

        return fut;
    }

    /**
     * @param key Key.
     * @return Future.
     */
    protected IgniteInternalFuture<V> getAndRemoveAsync0(final K key) {
        return asyncOp(new AsyncOp<V>() {
            @Override public IgniteInternalFuture<V> op(GridNearTxLocal tx, AffinityTopologyVersion readyTopVer) {
                // TODO should we invoke interceptor here?
                return tx.removeAllAsync(ctx,
                    readyTopVer,
                    Collections.singletonList(key),
                    /*retval*/true,
                    null,
                    /*singleRmv*/false).chain(RET2VAL);
            }

            @Override public String toString() {
                return S.toString("removeAsync",
                    "key", key, true);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void removeAll() throws IgniteCheckedException {
        // We do batch and recreate cursor because removing using a single cursor
        // will cause it to reinitialize on each merged page.
        List<K> keys = new ArrayList<>(Math.min(REMOVE_ALL_KEYS_BATCH, size()));

        do {
            Iterator<CacheDataRow> it = ctx.offheap().cacheIterator(ctx.cacheId(),
                true, true, null, null);

            while (it.hasNext() && keys.size() < REMOVE_ALL_KEYS_BATCH)
                keys.add((K)it.next().key());

            removeAll(keys);

            keys.clear();
        }
        while (!isEmpty());
    }

    /** {@inheritDoc} */
    @Override public void removeAll(final Collection<? extends K> keys) throws IgniteCheckedException {
        boolean statsEnabled = ctx.statisticsEnabled();
        boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        A.notNull(keys, "keys");

        if (F.isEmpty(keys))
            return;

        warnIfUnordered(keys, BulkOperation.REMOVE);

        removeAll0(keys);

        if (statsEnabled)
            metrics0().addRemoveAllTimeNanos(System.nanoTime() - start);

        if (performanceStatsEnabled)
            writeStatistics(OperationType.CACHE_REMOVE_ALL, start);
    }

    /**
     * @param keys Keys.
     * @throws IgniteCheckedException If failed.
     */
    protected void removeAll0(final Collection<? extends K> keys) throws IgniteCheckedException {
        syncOp(new SyncInOp(keys.size() == 1) {
            @Override public void inOp(GridNearTxLocal tx) throws IgniteCheckedException {
                tx.removeAllAsync(ctx,
                    null,
                    keys,
                    /*retval*/false,
                    null,
                    /*singleRmv*/false).get();
            }

            @Override public String toString() {
                return S.toString("removeAll",
                    "keys", keys, true);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync(@Nullable final Collection<? extends K> keys) {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<Object>();

        final boolean statsEnabled = ctx.statisticsEnabled();
        final boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        final long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        warnIfUnordered(keys, BulkOperation.REMOVE);

        IgniteInternalFuture<Object> fut = removeAllAsync0(keys);

        if (statsEnabled)
            fut.listen(new UpdateRemoveAllTimeStatClosure<>(metrics0(), start));

        if (performanceStatsEnabled)
            fut.listen(() -> writeStatistics(OperationType.CACHE_REMOVE_ALL, start));

        return fut;
    }

    /**
     * @param keys Keys.
     * @return Future.
     */
    protected IgniteInternalFuture<Object> removeAllAsync0(final Collection<? extends K> keys) {
        return asyncOp(new AsyncOp(keys) {
            @Override public IgniteInternalFuture<?> op(GridNearTxLocal tx, AffinityTopologyVersion readyTopVer) {
                return tx.removeAllAsync(ctx,
                    readyTopVer,
                    keys,
                    /*retval*/false,
                    null,
                    /*singleRmv*/false).chain(RET2NULL);
            }

            @Override public String toString() {
                return S.toString("removeAllAsync",
                    "keys", keys, true);
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
        boolean statsEnabled = ctx.statisticsEnabled();
        boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key");

        boolean rmv = remove0(key, filter);

        if (statsEnabled && rmv)
            metrics0().addRemoveTimeNanos(System.nanoTime() - start);

        if (performanceStatsEnabled)
            writeStatistics(OperationType.CACHE_REMOVE, start);

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
            @Override public Boolean op(GridNearTxLocal tx) throws IgniteCheckedException {
                return tx.removeAllAsync(ctx,
                    null,
                    Collections.singletonList(key),
                    /*retval*/false,
                    filter,
                    /*singleRmv*/filter == null).get().success();
            }

            @Override public String toString() {
                return S.toString("removex",
                    "key", key, true);
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
        final boolean statsEnabled = ctx.statisticsEnabled();
        final boolean performanceStatsEnabled = ctx.kernalContext().performanceStatistics().enabled();

        final long start = statsEnabled || performanceStatsEnabled ? System.nanoTime() : 0L;

        A.notNull(key, "key");

        IgniteInternalFuture<Boolean> fut = removeAsync0(key, filter);

        if (statsEnabled)
            fut.listen(new UpdateRemoveTimeStatClosure(metrics0(), start));

        if (performanceStatsEnabled)
            fut.listen(() -> writeStatistics(OperationType.CACHE_REMOVE, start));

        return fut;
    }

    /**
     * @param key Key.
     * @param filter Filter.
     * @return Future.
     */
    protected IgniteInternalFuture<Boolean> removeAsync0(final K key, @Nullable final CacheEntryPredicate filter) {
        return asyncOp(new AsyncOp<Boolean>() {
            @Override public IgniteInternalFuture<Boolean> op(GridNearTxLocal tx, AffinityTopologyVersion readyTopVer) {
                return tx.removeAllAsync(ctx,
                    readyTopVer,
                    Collections.singletonList(key),
                    /*retval*/false,
                    filter,
                    /*singleRmv*/filter == null).chain(RET2FLAG);
            }

            @Override public String toString() {
                return S.toString("removeAsync",
                    "key", key, true,
                    "filter", filter, false);
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
            @Override public void inOp(GridNearTxLocal tx) throws IgniteCheckedException {
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

        return asyncOp(new AsyncOp(drMap.keySet()) {
            @Override public IgniteInternalFuture<?> op(GridNearTxLocal tx, AffinityTopologyVersion readyTopVer) {
                return tx.removeAllDrAsync(ctx, drMap);
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
            Map<Integer, CacheMetrics> nodeCacheMetrics = ((IgniteClusterNode)node).cacheMetrics();

            if (nodeCacheMetrics != null) {
                CacheMetrics e = nodeCacheMetrics.get(context().cacheId());

                if (e != null)
                    metrics.add(e);
            }
        }

        return isCacheMetricsV2Supported() ? new CacheMetricsSnapshotV2(ctx.cache().localMetrics(), metrics) :
            new CacheMetricsSnapshot(ctx.cache().localMetrics(), metrics);
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics localMetrics() {
        return isCacheMetricsV2Supported() ? new CacheMetricsSnapshotV2(metrics) :
            new CacheMetricsSnapshot(metrics);
    }

    /**
     * @return checks cluster server nodes version is compatible with Cache Metrics V2
     */
    private boolean isCacheMetricsV2Supported() {
        Collection<ClusterNode> nodes = ctx.discovery().allNodes();

        return IgniteFeatures.allNodesSupports(nodes, IgniteFeatures.CACHE_METRICS_V2);
    }

    /**
     * @return Metrics.
     */
    public CacheMetricsImpl metrics0() {
        return metrics;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridNearTxLocal tx() {
        return ctx.tm().threadLocalTx(ctx);
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

        IgniteInternalFuture<Boolean> fut = lockAllAsync(keys, timeout);

        boolean isInterrupted = false;

        try {
            IgniteCheckedException e = null;

            while (!ctx.shared().kernalContext().isStopping()) {
                try {
                    return fut.get();
                }
                catch (IgniteInterruptedCheckedException ex) {
                    // Interrupted status of current thread was cleared, retry to get lock.
                    isInterrupted = true;

                    e = ex;
                }
            }

            try {
                fut.cancel();
            }
            catch (IgniteCheckedException ex) {
                if (e != null)
                    ex.addSuppressed(e);

                e = ex;
            }

            throw new NodeStoppingException(e);
        }
        finally {
            if (isInterrupted)
                Thread.currentThread().interrupt();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAsync(K key, long timeout) {
        A.notNull(key, "key");

        return lockAllAsync(Collections.singletonList(key), timeout);
    }

    /** {@inheritDoc} */
    @Override public void unlock(K key)
        throws IgniteCheckedException {
        A.notNull(key, "key");

        unlockAll(Collections.singletonList(key));
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked(K key) {
        A.notNull(key, "key");

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

        try {
            KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

            GridCacheEntryEx e = entry0(cacheKey,
                ctx.shared().exchange().readyAffinityVersion(),
                false,
                false);

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
    @Override public GridNearTxLocal txStartEx(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        IgniteTransactionsEx txs = ctx.kernalContext().cache().transactions();

        return txs.txStartEx(ctx, concurrency, isolation);
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart(TransactionConcurrency concurrency,
        TransactionIsolation isolation, long timeout, int txSize) throws IllegalStateException {
        IgniteTransactionsEx txs = ctx.kernalContext().cache().transactions();

        return txs.txStartEx(ctx, concurrency, isolation, timeout, txSize).proxy();
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
                DataStreamerImpl ldr = ctx.kernalContext().dataStream().dataStreamer(ctx.name());

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

        GridCacheEntryEx entry = entryEx(key);

        try {
            entry.initialValue(cacheVal,
                ver,
                ttl,
                CU.EXPIRE_TIME_CALCULATE,
                false,
                topVer,
                replicate ? DR_LOAD : DR_NONE,
                true,
                false);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to put cache value: " + entry, e);
        }
        catch (GridCacheEntryRemovedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Got removed entry during loadCache (will ignore): " + entry);
        }
        finally {
            entry.touch();
        }

        CU.unwindEvicts(ctx);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> localLoadCacheAsync(final IgniteBiPredicate<K, V> p,
        final Object[] args) {
        return ctx.closures().callLocalSafe(
            ctx.projectSafe(new GridPlainCallable<Object>() {
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

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        ExpiryPolicy plc = opCtx != null ? opCtx.expiry() : null;

        final boolean keepBinary = opCtx != null && opCtx.isKeepBinary();

        return runLoadKeysCallable(keys, plc, keepBinary, replaceExisting);
    }

    /**
     * Run load keys callable on appropriate nodes.
     *
     * @param keys Keys.
     * @param plc Expiry policy.
     * @param keepBinary Keep binary flag.
     * @return Operation future.
     */
    private IgniteInternalFuture<?> runLoadKeysCallable(final Set<? extends K> keys, final ExpiryPolicy plc,
        final boolean keepBinary, final boolean update) {
        Collection<ClusterNode> nodes = ctx.grid().cluster().forDataNodes(name()).nodes();

        if (nodes.isEmpty())
            return new GridFinishedFuture<>();

        return ctx.closures().callAsync(
            BROADCAST,
            new LoadKeysCallable<>(ctx.name(), keys, update, plc, keepBinary),
            options(nodes)
                .withFailoverDisabled()
                .asSystemTask()
        );
    }

    /**
     * @param keys Keys.
     * @throws IgniteCheckedException If failed.
     */
    private void localLoadAndUpdate(final Collection<? extends K> keys) throws IgniteCheckedException {
        try (final DataStreamerImpl<KeyCacheObject, CacheObject> ldr =
                 ctx.kernalContext().<KeyCacheObject, CacheObject>dataStream().dataStreamer(ctx.name())) {
            ldr.allowOverwrite(true);
            ldr.skipStore(true);

            final Collection<DataStreamerEntry> col = new ArrayList<>(ldr.perNodeBufferSize());

            Collection<KeyCacheObject> keys0 = ctx.cacheKeysView(keys);

            ctx.store().loadAll(null, keys0, new CIX2<KeyCacheObject, Object>() {
                @Override public void applyx(KeyCacheObject key, Object val) {
                    col.add(new DataStreamerEntry(key, ctx.toCacheObject(val)));

                    if (col.size() == ldr.perNodeBufferSize()) {
                        ldr.addDataInternal(col, false);

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
            DataStreamerImpl ldr = ctx.kernalContext().dataStream().dataStreamer(ctx.name());

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
     * @return Load cache future.
     * @throws IgniteCheckedException If failed.
     */
    IgniteInternalFuture<?> globalLoadCacheAsync(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args)
        throws IgniteCheckedException {

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        ExpiryPolicy plc = opCtx != null ? opCtx.expiry() : null;

        Collection<ClusterNode> nodes = ctx.kernalContext().grid().cluster().forDataNodes(ctx.name()).nodes();

        assert !F.isEmpty(nodes) : "There are not datanodes fo cache: " + ctx.name();

        final boolean keepBinary = opCtx != null && opCtx.isKeepBinary();

        IgniteInternalFuture<?> fut = ctx.kernalContext().closure().callAsync(
            BROADCAST,
            Collections.singletonList(new LoadCacheJobV2<>(ctx.name(), ctx.affinity().affinityTopologyVersion(), p, args, plc, keepBinary)),
            options(nodes));

        return fut;
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode[] peekModes) throws IgniteCheckedException {
        return sizeAsync(peekModes).get();
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(CachePeekMode[] peekModes) throws IgniteCheckedException {
        return sizeLongAsync(peekModes).get();
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(int partition, CachePeekMode[] peekModes) throws IgniteCheckedException {
        return sizeLongAsync(partition, peekModes).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Integer> sizeAsync(final CachePeekMode[] peekModes) {
        assert peekModes != null;

        PeekModes modes = parsePeekModes(peekModes, true);

        IgniteClusterEx cluster = ctx.grid().cluster();

        ClusterGroup grp = modes.near ? cluster.forCacheNodes(name(), true, true, false) : cluster.forDataNodes(name());

        Collection<ClusterNode> nodes = new ArrayList<>(grp.nodes());

        if (nodes.isEmpty())
            return new GridFinishedFuture<>(0);

        return ctx.kernalContext().task().execute(
            new SizeTask(ctx.name(), ctx.affinity().affinityTopologyVersion(), peekModes),
            null,
            options(nodes)
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Long> sizeLongAsync(final CachePeekMode[] peekModes) {
        assert peekModes != null;

        PeekModes modes = parsePeekModes(peekModes, true);

        IgniteClusterEx cluster = ctx.grid().cluster();

        ClusterGroup grp = modes.near ? cluster.forCacheNodes(name(), true, true, false) : cluster.forDataNodes(name());

        Collection<ClusterNode> nodes = new ArrayList<>(grp.nodes());

        if (nodes.isEmpty())
            return new GridFinishedFuture<>(0L);

        return ctx.kernalContext().task().execute(
            new SizeLongTask(ctx.name(), ctx.affinity().affinityTopologyVersion(), peekModes),
            null,
            options(nodes)
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Long> sizeLongAsync(final int part, final CachePeekMode[] peekModes) {
        assert peekModes != null;

        final PeekModes modes = parsePeekModes(peekModes, true);

        IgniteClusterEx cluster = ctx.grid().cluster();
        final GridCacheAffinityManager aff = ctx.affinity();
        final AffinityTopologyVersion topVer = aff.affinityTopologyVersion();

        ClusterGroup grp = cluster.forDataNodes(name());

        Collection<ClusterNode> nodes = new ArrayList<>(grp.forPredicate(new IgnitePredicate<ClusterNode>() {
            /** {@inheritDoc} */
            @Override public boolean apply(ClusterNode clusterNode) {
                return ((modes.primary && aff.primaryByPartition(clusterNode, part, topVer)) ||
                        (modes.backup && aff.backupByPartition(clusterNode, part, topVer)));
            }
        }).nodes());

        if (nodes.isEmpty())
            return new GridFinishedFuture<>(0L);

        return ctx.kernalContext().task().execute(
            new PartitionSizeLongTask(ctx.name(), ctx.affinity().affinityTopologyVersion(), peekModes, part),
            null,
            options(nodes)
        );
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode[] peekModes) throws IgniteCheckedException {
        return (int)localSizeLong(peekModes);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return map.publicSize(ctx.cacheId());
    }

    /** {@inheritDoc} */
    @Override public long sizeLong() {
        return map.publicSize(ctx.cacheId());
    }

    /** {@inheritDoc} */
    @Override public int nearSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int primarySize() {
        return map.publicSize(ctx.cacheId());
    }

    /** {@inheritDoc} */
    @Override public long primarySizeLong() {
        return map.publicSize(ctx.cacheId());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAdapter.class, this, "name", name(), "size", size());
    }

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<K, V>> iterator() {
        return entrySet().iterator();
    }

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<K, V>> scanIterator(boolean keepBinary,
        @Nullable IgniteBiPredicate<Object, Object> p)
        throws IgniteCheckedException {
        return igniteIterator(keepBinary, p);
    }

    /**
     * @return Distributed ignite cache iterator.
     * @throws IgniteCheckedException If failed.
     */
    public Iterator<Cache.Entry<K, V>> igniteIterator() throws IgniteCheckedException {
        return igniteIterator(ctx.keepBinary(), null);
    }

    /**
     * @param keepBinary Keep binary flag.
     * @return Distributed ignite cache iterator.
     * @throws IgniteCheckedException If failed.
     */
    public Iterator<Cache.Entry<K, V>> igniteIterator(boolean keepBinary) throws IgniteCheckedException {
        return igniteIterator(keepBinary, null);
    }

    /**
     * Gets next grid cache version.
     *
     * @return Next version based on given topology version.
     */
    public GridCacheVersion nextVersion() {
        return ctx.versions().next(ctx.topology().readyTopologyVersion().topologyVersion());
    }

    /**
     * Gets next grid cache version.
     *
     * @param dataCenterId Data center id.
     * @return Next version based on given topology version.
     */
    public GridCacheVersion nextVersion(byte dataCenterId) {
        return ctx.versions().next(ctx.topology().readyTopologyVersion().topologyVersion(), dataCenterId);
    }

    /**
     * @param keepBinary Keep binary flag.
     * @param p Optional predicate.
     * @return Distributed ignite cache iterator.
     * @throws IgniteCheckedException If failed.
     */
    private Iterator<Cache.Entry<K, V>> igniteIterator(boolean keepBinary,
        @Nullable IgniteBiPredicate<Object, Object> p)
        throws IgniteCheckedException {
        GridCacheContext ctx0 = ctx.isNear() ? ctx.near().dht().context() : ctx;

        final CacheOperationContext opCtx = ctx.operationContextPerCall();

        final GridCloseableIterator<Map.Entry<K, V>> iter = ctx0.queries().createScanQuery(p, null, null, keepBinary, false, null, null)
            .executeScanQuery();

        return ctx.itHolder().iterator(iter, new CacheIteratorConverter<Cache.Entry<K, V>, Map.Entry<K, V>>() {
            @Override protected Cache.Entry<K, V> convert(Map.Entry<K, V> e) {
                // Actually Scan Query returns Iterator<CacheQueryEntry> by default,
                // CacheQueryEntry implements both Map.Entry and Cache.Entry interfaces.
                return (Cache.Entry<K, V>)e;
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

    /** {@inheritDoc} */
    @Override public long offHeapEntriesCount() {
        IgniteCacheOffheapManager mgr = ctx.offheap();

        return mgr != null ? mgr.cacheEntriesCount(ctx.cacheId(),
            true,
            true,
            ctx.affinity().affinityTopologyVersion()) : -1;
    }

    /** {@inheritDoc} */
    @Override public long offHeapAllocatedSize() {
        IgniteCacheOffheapManager mgr = ctx.offheap();

        return mgr != null ? mgr.offHeapAllocatedSize() : -1;
    }

    /**
     * Last async operation future for implicit transactions. For explicit transactions future is
     * bound to the transaction and stored inside the transaction.
     * These futures are required to linearize async operations made by the same thread.
     */
    public FutureHolder lastAsyncFuture() {
        return lastFut.get();
    }

    /**
     * @param op Cache operation.
     * @param <T> Return type.
     * @return Operation result.
     * @throws IgniteCheckedException If operation failed.
     */
    @SuppressWarnings({"ErrorNotRethrown", "AssignmentToCatchBlockParameter"})
    @Nullable private <T> T syncOp(SyncOp<T> op) throws IgniteCheckedException {
        checkJta();

        GridNearTxLocal tx = ctx.tm().threadLocalTx(ctx);

        if (tx == null || tx.implicit()) {
            lastAsyncFuture().await();

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
                    0,
                    null
                );

                assert tx != null;

                try {
                    T t = op.op(tx);

                    assert tx.done() : "Transaction is not done: " + tx;

                    return t;
                }
                catch (IgniteInterruptedCheckedException |
                    IgniteTxHeuristicCheckedException |
                    NodeStoppingException |
                    IgniteConsistencyViolationException e) {
                    throw e;
                }
                catch (IgniteCheckedException e) {
                    if (!(e instanceof IgniteTxRollbackCheckedException)) {
                        try {
                            tx.rollback();

                            if (!(e instanceof TransactionCheckedException))
                                e = new IgniteTxRollbackCheckedException("Transaction has been rolled back: " +
                                    tx.xid(), e);
                        }
                        catch (IgniteCheckedException | AssertionError | RuntimeException e1) {
                            U.error(log, "Failed to rollback transaction (cache may contain stale locks): " +
                                CU.txString(tx), e1);

                            if (e != e1)
                                e.addSuppressed(e1);
                        }
                    }

                    if (X.hasCause(e, ClusterTopologyCheckedException.class) && i != retries - 1) {
                        ClusterTopologyCheckedException topErr = e.getCause(ClusterTopologyCheckedException.class);

                        if (!(topErr instanceof ClusterTopologyServerNotFoundException)) {
                            AffinityTopologyVersion topVer = tx.topologyVersion();

                            assert topVer != null && topVer.topologyVersion() > 0 : tx;

                            AffinityTopologyVersion awaitVer = new AffinityTopologyVersion(
                                topVer.topologyVersion() + 1, 0);

                            ctx.shared().exchange().affinityReadyFuture(awaitVer).get();

                            continue;
                        }
                    }

                    throw e;
                }
                catch (RuntimeException e) {
                    try {
                        tx.rollback();
                    }
                    catch (IgniteCheckedException | AssertionError | RuntimeException e1) {
                        U.error(log, "Failed to rollback transaction " + CU.txString(tx), e1);
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
        else {
            tx.txState().awaitLastFuture();

            return op.op(tx);
        }
    }

    /**
     * @param op Cache operation.
     * @param <T> Return type.
     * @return Future.
     */
    private <T> IgniteInternalFuture<T> asyncOp(final AsyncOp<T> op) {
        try {
            checkJta();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }

        if (log.isDebugEnabled())
            log.debug("Performing async op: " + op);

        GridNearTxLocal tx = ctx.tm().threadLocalTx(ctx);

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
                    0,
                    null);

                return asyncOp(tx, op, opCtx, /*retry*/false);
            }
            else {
                AsyncOpRetryFuture<T> fut = new AsyncOpRetryFuture<>(op, retries, opCtx);

                fut.execute(/*retry*/false);

                return fut;
            }
        }
        else
            return asyncOp(tx, op, opCtx, /*retry*/false);
    }

    /**
     * @param tx Transaction.
     * @param op Cache operation.
     * @param opCtx Cache operation context.
     * @param <T> Return type.
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    protected <T> IgniteInternalFuture<T> asyncOp(
        GridNearTxLocal tx,
        final AsyncOp<T> op,
        final CacheOperationContext opCtx,
        final boolean retry
    ) {
        IgniteInternalFuture<T> fail = asyncOpAcquire(retry);

        if (fail != null)
            return fail;

        FutureHolder holder = tx.implicit() ? lastAsyncFuture() : tx.txState().lastAsyncFuture();

        holder.lock();

        try {
            IgniteInternalFuture<?> fut = holder.future();

            final GridNearTxLocal tx0 = tx;

            final CX1 clo = new CX1<IgniteInternalFuture<T>, T>() {
                @Override public T applyx(IgniteInternalFuture<T> tFut) throws IgniteCheckedException {
                    try {
                        return tFut.get();
                    }
                    catch (IgniteTxTimeoutCheckedException |
                        IgniteTxRollbackCheckedException |
                        NodeStoppingException |
                        IgniteConsistencyViolationException e) {
                        throw e;
                    }
                    catch (IgniteCheckedException e1) {
                        try {
                            tx0.rollbackNearTxLocalAsync();
                        }
                        catch (Throwable e2) {
                            if (e1 != e2)
                                e1.addSuppressed(e2);
                        }

                        throw e1;
                    }
                    finally {
                        ctx.shared().txContextReset();
                    }
                }
            };

            if (fut != null && !fut.isDone()) {
                IgniteInternalFuture<T> f = new GridEmbeddedFuture(fut,
                    (IgniteOutClosure<IgniteInternalFuture>)() -> {
                        GridFutureAdapter resFut = new GridFutureAdapter();

                        ctx.kernalContext().closure().runLocalSafe((GridPlainRunnable)() -> {
                            IgniteInternalFuture fut0;

                            if (ctx.kernalContext().isStopping())
                                fut0 = new GridFinishedFuture<>(
                                    new IgniteCheckedException("Operation has been cancelled (node or cache is stopping)."));
                            else if (ctx.gate().isStopped())
                                fut0 = new GridFinishedFuture<>(new CacheStoppedException(ctx.name()));
                            else {
                                ctx.operationContextPerCall(opCtx);
                                ctx.shared().txContextReset();

                                try {
                                    fut0 = op.op(tx0).chain(clo);
                                }
                                finally {
                                    // It is necessary to clear tx context in this thread as well.
                                    ctx.shared().txContextReset();
                                    ctx.operationContextPerCall(null);
                                }
                            }

                            fut0.listen(() -> {
                                try {
                                    resFut.onDone(fut0.get());
                                }
                                catch (Throwable ex) {
                                    resFut.onDone(ex);
                                }
                            });
                        }, true);

                        return resFut;
                    });

                holder.saveFuture(f);

                f.listen(f0 -> asyncOpRelease(retry));

                return f;
            }

            /**
             * Wait for concurrent tx operation to finish.
             * See {@link GridDhtTxLocalAdapter#updateLockFuture(IgniteInternalFuture, IgniteInternalFuture)}
             */
            if (!tx0.txState().implicitSingle())
                tx0.txState().awaitLastFuture();

            IgniteInternalFuture<T> f;

            try {
                f = op.op(tx).chain(clo);
            }
            finally {
                // It is necessary to clear tx context in this thread as well.
                ctx.shared().txContextReset();
            }

            holder.saveFuture(f);

            f.listen(f0 -> asyncOpRelease(retry));

            if (tx.implicit())
                ctx.tm().resetContext();

            return f;
        }
        finally {
            holder.unlock();
        }
    }

    /**
     * Tries to acquire asynchronous operations permit, if limited.
     *
     * @param retry Retry flag.
     * @return Failed future if waiting was interrupted.
     */
    @Nullable protected <T> IgniteInternalFuture<T> asyncOpAcquire(boolean retry) {
        try {
            if (!retry && asyncOpsSem != null)
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
     *
     * @param retry Retry flag.
     */
    protected final void asyncOpRelease(boolean retry) {
        if (!retry && asyncOpsSem != null)
            asyncOpsSem.release();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, ctx.igniteInstanceName());
        U.writeString(out, ctx.name());
    }

    /** {@inheritDoc} */
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

    /**
     * @param key Key.
     * @param readers Whether to clear readers.
     */
    private boolean clearLocally0(K key, boolean readers) {
        ctx.shared().cache().checkReadOnlyState("clear", ctx.config());

        ctx.checkSecurity(SecurityPermission.CACHE_REMOVE);

        GridCacheVersion obsoleteVer = nextVersion();

        ctx.shared().database().checkpointReadLock();

        try {
            KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

            GridCacheEntryEx entry = ctx.isNear() ? peekEx(cacheKey) : entryEx(cacheKey);

            if (entry != null)
                return entry.clear(obsoleteVer, readers);
        }
        catch (GridDhtInvalidPartitionException ignored) {
            // No-op.
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Failed to clearLocally entry for key: " + key, ex);
        }
        finally {
            ctx.shared().database().checkpointReadUnlock();
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean evict(K key) {
        A.notNull(key, "key");

        return evictx(key, nextVersion(), CU.empty0());
    }

    /** {@inheritDoc} */
    @Override public void evictAll(Collection<? extends K> keys) {
        A.notNull(keys, "keys");

        if (F.isEmpty(keys))
            return;

        GridCacheVersion obsoleteVer = nextVersion();

        try {
            ctx.evicts().batchEvict(keys, obsoleteVer);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to perform batch evict for keys: " + keys, e);
        }
    }

    /**
     * @param filter Filters to evaluate.
     * @return Entry set.
     */
    public Set<Cache.Entry<K, V>> entrySet(@Nullable CacheEntryPredicate... filter) {
        boolean keepBinary = ctx.keepBinary();

        return new EntrySet(map.entrySet(ctx.cacheId(), filter), keepBinary);
    }

    /**
     * @param key Key.
     * @param deserializeBinary Deserialize binary flag.
     * @param needVer Need version.
     * @return Cached value.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public final V repairableGet(
        K key,
        boolean deserializeBinary,
        boolean needVer) throws IgniteCheckedException {
        try {
            return get(
                key,
                ctx.kernalContext().job().currentTaskName(),
                deserializeBinary,
                needVer);
        }
        catch (IgniteConsistencyViolationException e) {
            repairAsync(e, ctx.operationContextPerCall(), false).get();

            return repairableGet(key, deserializeBinary, needVer);
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
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary flag.
     * @param needVer Need version.
     * @return Cached value.
     * @throws IgniteCheckedException If failed.
     */
    protected V get(
        final K key,
        String taskName,
        boolean deserializeBinary,
        boolean needVer) throws IgniteCheckedException {
        checkJta();

        return getAsync(key,
            !ctx.config().isReadFromBackup(),
            /*skip tx*/false,
            taskName,
            deserializeBinary,
            /*skip vals*/false,
            needVer).get();
    }

    /**
     * @param key Key.
     * @param deserializeBinary Deserialize binary flag.
     * @param needVer Need version.
     * @return Read operation future.
     */
    public final IgniteInternalFuture<V> repairableGetAsync(
        final K key,
        boolean deserializeBinary,
        final boolean needVer,
        ReadRepairStrategy readRepairStrategy) {
        try {
            checkJta();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }

        IgniteInternalFuture<V> fut = getAsync(key,
            !ctx.config().isReadFromBackup(),
            /*skip tx*/false,
            ctx.kernalContext().job().currentTaskName(),
            deserializeBinary,
            /*skip vals*/false,
            needVer);

        if (readRepairStrategy != null) {
            CacheOperationContext opCtx = ctx.operationContextPerCall();

            return getWithRepairAsync(
                fut,
                (e) -> repairAsync(e, opCtx, false),
                () -> repairableGetAsync(key, deserializeBinary, needVer, readRepairStrategy));
        }

        return fut;
    }

    /**
     * @param keys Keys.
     * @param deserializeBinary Deserialize binary flag.
     * @param needVer Need version.
     * @return Map of cached values.
     * @throws IgniteCheckedException If read failed.
     */
    protected Map<K, V> repairableGetAll(
        Collection<? extends K> keys,
        boolean deserializeBinary,
        boolean needVer,
        boolean recovery,
        ReadRepairStrategy readRepairStrategy) throws IgniteCheckedException {
        try {
            return getAll(keys, deserializeBinary, needVer, recovery, readRepairStrategy);
        }
        catch (IgniteConsistencyViolationException e) {
            repairAsync(e, ctx.operationContextPerCall(), false).get();

            return repairableGetAll(keys, deserializeBinary, needVer, recovery, readRepairStrategy);
        }
    }

    /**
     * @param keys Keys.
     * @param deserializeBinary Deserialize binary flag.
     * @param needVer Need version.
     * @param recovery Recovery flag.
     * @param readRepairStrategy Read repair strategy.
     * @return Map of cached values.
     * @throws IgniteCheckedException If read failed.
     */
    protected Map<K, V> getAll(
        Collection<? extends K> keys,
        boolean deserializeBinary,
        boolean needVer,
        boolean recovery,
        ReadRepairStrategy readRepairStrategy) throws IgniteCheckedException {
        checkJta();

        return getAllAsync(keys,
            !ctx.config().isReadFromBackup(),
            /*skip tx*/false,
            ctx.kernalContext().job().currentTaskName(),
            deserializeBinary,
            recovery,
            readRepairStrategy,
            /*skip vals*/false,
            needVer).get();
    }

    /**
     * @param keys Keys.
     * @param forcePrimary Force primary.
     * @param skipTx Skip tx.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary.
     * @param recovery Recovery mode flag.
     * @param skipVals Skip values.
     * @param needVer Need version.
     * @return Future for the get operation.
     * @see GridCacheAdapter#getAllAsync(Collection)
     */
    public IgniteInternalFuture<Map<K, V>> repairableGetAllAsync(
        @Nullable Collection<? extends K> keys,
        boolean forcePrimary,
        boolean skipTx,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        ReadRepairStrategy readRepairStrategy,
        boolean skipVals,
        final boolean needVer
    ) {
        IgniteInternalFuture<Map<K, V>> fut = getAllAsync(
            keys,
            forcePrimary,
            skipTx,
            taskName,
            deserializeBinary,
            recovery,
            readRepairStrategy,
            skipVals,
            needVer);

        if (readRepairStrategy != null) {
            CacheOperationContext opCtx = ctx.operationContextPerCall();

            return getWithRepairAsync(
                fut,
                (e) -> repairAsync(e, opCtx, skipVals),
                () -> repairableGetAllAsync(
                    keys,
                    forcePrimary,
                    skipTx,
                    taskName,
                    deserializeBinary,
                    recovery,
                    readRepairStrategy,
                    skipVals,
                    needVer));
        }

        return fut;
    }

    /**
     * Performs repair and retries get if necessary.
     * @param orig Original get future.
     * @param repair Repair callback, used in case of inconcstency.
     * @param retry Callback to original method to retry operation after repair.
     */
    private <R> IgniteInternalFuture<R> getWithRepairAsync(
        IgniteInternalFuture<R> orig,
        Function<IgniteConsistencyViolationException, IgniteInternalFuture<Void>> repair,
        Supplier<IgniteInternalFuture<R>> retry) {
        final GridNearTxLocal tx = ctx.tm().threadLocalTx(ctx);
        final CacheOperationContext opCtx = ctx.operationContextPerCall();

        GridFutureAdapter<R> fut = new GridFutureAdapter<>();

        orig.listen(() -> {
            try {
                fut.onDone(orig.get());
            }
            catch (IgniteConsistencyViolationException e1) {
                repair.apply(e1).listen((repFut) -> {
                    if (repFut.error() != null)
                        fut.onDone(repFut.error());
                    else {
                        CacheOperationContext prevOpCtx = ctx.operationContextPerCall();

                        IgniteInternalTx prevTx = ctx.tm().tx(tx); // Within the original tx.
                        ctx.operationContextPerCall(opCtx); // With the same operation context.

                        try {
                            fut.onDone(retry.get().get());
                        }
                        catch (IgniteCheckedException e2) {
                            fut.onDone(e2);
                        }
                        finally {
                            ctx.tm().tx(prevTx);
                            ctx.operationContextPerCall(prevOpCtx);
                        }
                    }
                });
            }
            catch (IgniteCheckedException e1) {
                fut.onDone(e1);
            }
        });

        return fut;
    }

    /**
     * Checks the given {@code keys} and repairs entries across the topology if needed.
     *
     * @param ex Consistency violation exception.
     * @param opCtx Operation context.
     * @param skipVals Skip values flag.
     * @return Compound future that represents a result of repair action.
     */
    private IgniteInternalFuture<Void> repairAsync(
        IgniteConsistencyViolationException ex,
        final CacheOperationContext opCtx,
        boolean skipVals) {
        GridCompoundReadRepairFuture fut = new GridCompoundReadRepairFuture();

        for (KeyCacheObject key : ex.keys()) {
            fut.add(ctx.transactional() ?
                repairTxAsync(key, opCtx, skipVals) :
                repairAtomicAsync(key, (IgniteAtomicConsistencyViolationException)ex, opCtx));
        }

        fut.markInitialized();

        return fut;
    }

    /**
     * Checks the given {@code key} and repairs entry across the topology if needed.
     *
     * @param key Key.
     * @param opCtx Operation context.
     * @param skipVals Skip values flag.
     * @return Recovery future.
     */
    private IgniteInternalFuture<Void> repairTxAsync(
        final KeyCacheObject key,
        final CacheOperationContext opCtx,
        boolean skipVals) {
        assert ctx.transactional();

        final GridNearTxLocal orig = ctx.tm().threadLocalTx(ctx);

        assert orig == null || orig.optimistic() || orig.readCommitted() || /*contains*/ skipVals :
            "Pessimistic non-read-committed 'get' should be fixed inside its own tx, the only exception is 'contains' " +
                "[tx=" + orig + ", skipVals=" + skipVals + "]";

        // Async check and recover if necessary.
        return ctx.kernalContext().closure().callLocalSafe(new GridPlainCallable<Void>() {
            @Override public Void call() throws IgniteCheckedException {
                CacheOperationContext prevOpCtx = ctx.operationContextPerCall();

                ctx.operationContextPerCall(opCtx);

                try (Transaction tx = ctx.grid().transactions().txStart(PESSIMISTIC, SERIALIZABLE)) {
                    get((K)key, null, !ctx.keepBinary(), false); // Repair.

                    final GridNearTxLocal tx0 = ctx.tm().threadLocalTx(ctx);

                    final IgniteTxKey txKey = ctx.txKey(ctx.toCacheKeyObject(key));

                    // Value was broken, fixed locally and should be spread across the topology.
                    if (tx0.txState().hasWriteKey(txKey))
                        tx.commit();

                    return null;
                }
                finally {
                    ctx.operationContextPerCall(prevOpCtx);
                }
            }
        });
    }

    /**
     * Checks the given {@code key} and repairs entry across the topology if needed.
     *
     * @param key Key.
     * @param ex Ignite atomic consistency violation exception
     * @param opCtx Operation context.
     * @return Recovery future.
     */
    private IgniteInternalFuture<Void> repairAtomicAsync(
        final KeyCacheObject key,
        final IgniteAtomicConsistencyViolationException ex,
        final CacheOperationContext opCtx) {
        assert ctx.atomic();

        EntryGetResult correctedRes = ex.correctedMap().get(key);
        EntryGetResult primRes = ex.primaryMap().get(key);

        CacheObject correctedObj = correctedRes != null ? correctedRes.value() : null;

        V correctedVal = correctedObj != null ? (V)ctx.unwrapBinaryIfNeeded(correctedObj, true, false, null) : null;

        GridCacheVersion primVer = primRes != null ? primRes.version() : null;

        return ctx.kernalContext().closure().callLocalSafe(new GridPlainCallable<Boolean>() {
            @Override public Boolean call() throws IgniteCheckedException {
                CacheOperationContext prevOpCtx = ctx.operationContextPerCall();

                ctx.operationContextPerCall(opCtx.keepBinary());

                try {
                    return invoke((K)key, new AtomicReadRepairEntryProcessor<>(correctedVal, primVer)).get();
                }
                finally {
                    ctx.operationContextPerCall(prevOpCtx);
                }
            }
        }).chain(fut -> {
            if (fut.result())
                ex.onRepaired(key); // Event recording after fixed.

            return null;
        });
    }

    /**
     *
     */
    protected static final class AtomicReadRepairEntryProcessor<K, V> implements CacheEntryProcessor<K, V, Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Corrected value. */
        private final V correctedVal;

        /** Primary version. */
        private final GridCacheVersion primVer;

        /**
         * @param correctedVal Corrected value.
         * @param primVer Primary version.
         */
        public AtomicReadRepairEntryProcessor(V correctedVal, GridCacheVersion primVer) {
            this.correctedVal = correctedVal;
            this.primVer = primVer;
        }

        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry<K, V> entry, Object... arguments) throws EntryProcessorException {
            try {
                if ((primVer == null && entry.getValue() == null) || // Still null at primary.
                    // No updates since consistency violation has been found.
                    primVer.equals(((CacheInvokeEntry<Object, Object>)entry).entry().version())) {

                    if (correctedVal != null)
                        entry.setValue(correctedVal);
                    else
                        entry.remove();

                    return true;
                }
                else
                    return false;
            }
            catch (GridCacheEntryRemovedException e) {
                throw new EntryProcessorException(e);
            }
        }
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
     * Checks that given map is sorted or otherwise constant order, or processed inside deadlock-detecting transaction.
     *
     * Issues developer warning otherwise.
     *
     * @param m Map to examine.
     */
    protected void warnIfUnordered(Map<?, ?> m, BulkOperation op) {
        if (ctx.atomic())
            return;

        if (m == null || m.size() <= 1)
            return;

        if (m instanceof SortedMap || m instanceof GridSerializableMap)
            return;

        Transaction tx = ctx.kernalContext().cache().transactions().tx();

        if (tx != null && !op.canBlockTx(tx.concurrency(), tx.isolation()))
            return;

        LT.warn(log, "Unordered map " + m.getClass().getName() +
            " is used for " + op.title() + " operation on cache " + name() + ". " +
            "This can lead to a distributed deadlock. Switch to a sorted map like TreeMap instead.");
    }

    /**
     * Checks that given collection is sorted set, or processed inside deadlock-detecting transaction.
     *
     * Issues developer warning otherwise.
     *
     * @param coll Collection to examine.
     */
    protected void warnIfUnordered(Collection<?> coll, BulkOperation op) {
        if (ctx.atomic())
            return;

        if (coll == null || coll.size() <= 1)
            return;

        if (coll instanceof SortedSet || coll instanceof GridCacheAdapter.KeySet)
            return;

        // To avoid false positives, once removeAll() is called, cache will never issue Remove All warnings.
        if (ctx.lastRemoveAllJobFut().get() != null && op == BulkOperation.REMOVE)
            return;

        Transaction tx = ctx.kernalContext().cache().transactions().tx();

        if (op == BulkOperation.GET && tx == null)
            return;

        if (tx != null && !op.canBlockTx(tx.concurrency(), tx.isolation()))
            return;

        LT.warn(log, "Unordered collection " + coll.getClass().getName() +
            " is used for " + op.title() + " operation on cache " + name() + ". " +
            "This can lead to a distributed deadlock. Switch to a sorted set like TreeSet instead.");
    }

    /** */
    protected enum BulkOperation {
        /** */
        GET,

        /** */
        PUT,

        /** */
        INVOKE,

        /** */
        REMOVE;

        /** */
        public String title() {
            return name().toLowerCase() + "All";
        }

        /** */
        public boolean canBlockTx(TransactionConcurrency concurrency, TransactionIsolation isolation) {
            if (concurrency == OPTIMISTIC && isolation == SERIALIZABLE)
                return false;

            if (this == GET && concurrency == PESSIMISTIC && isolation == READ_COMMITTED)
                return false;

            return true;
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
            /*readThrough*/false,
            /*metrics*/false,
            /*evt*/false,
            /*transformClo*/null,
            /*taskName*/null,
            /*expiryPlc*/null,
            !deserializeBinary);

        if (val == null)
            return null;

        KeyCacheObject key = entry.key();

        Object key0 = ctx.unwrapBinaryIfNeeded(key, !deserializeBinary, true, null);
        Object val0 = ctx.unwrapBinaryIfNeeded(val, !deserializeBinary, true, null);

        return new CacheEntryImpl<>((K)key0, (V)val0, entry.version());
    }

    /** {@inheritDoc} */
    @Override public void preloadPartition(int part) throws IgniteCheckedException {
        executePreloadTask(part).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> preloadPartitionAsync(int part) throws IgniteCheckedException {
        return executePreloadTask(part);
    }

    /** {@inheritDoc} */
    @Override public boolean localPreloadPartition(int part) throws IgniteCheckedException {
        if (!ctx.affinityNode())
            return false;

        GridDhtPartitionTopology top = ctx.group().topology();

        @Nullable GridDhtLocalPartition p = top.localPartition(part, top.readyTopologyVersion(), false);

        if (p == null)
            return false;

        try {
            if (!p.reserve() || p.state() != OWNING)
                return false;

            p.dataStore().preload();
        }
        finally {
            p.release();
        }

        return true;
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
        private GridNearTxLocal tx;

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
        public void execute(boolean retry) {
            tx = ctx.tm().newTx(
                true,
                op.single(),
                ctx.systemTx() ? ctx : null,
                OPTIMISTIC,
                READ_COMMITTED,
                CU.transactionConfiguration(ctx, ctx.kernalContext().config()).getDefaultTxTimeout(),
                opCtx == null || !opCtx.skipStore(),
                0,
                null);

            IgniteInternalFuture<T> fut = asyncOp(tx, op, opCtx, retry);

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

                                AffinityTopologyVersion awaitVer = new AffinityTopologyVersion(topVer.topologyVersion() + 1, 0);

                                IgniteInternalFuture<?> topFut =
                                    ctx.shared().exchange().affinityReadyFuture(awaitVer);

                                topFut.listen(new IgniteInClosure<IgniteInternalFuture<?>>() {
                                    @Override public void apply(IgniteInternalFuture<?> topFut) {
                                        try {
                                            topFut.get();

                                            execute(/*retry*/true);
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
    protected static final class PeekModes {
        /** */
        public boolean near;

        /** */
        public boolean primary;

        /** */
        public boolean backup;

        /** */
        public boolean heap;

        /** */
        public boolean offheap;

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
    protected static PeekModes parsePeekModes(CachePeekMode[] peekModes, boolean primary) {
        PeekModes modes = new PeekModes();

        if (F.isEmpty(peekModes)) {
            modes.primary = true;

            if (!primary) {
                modes.backup = true;
                modes.near = true;
            }

            modes.heap = true;
            modes.offheap = true;
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

                    default:
                        assert false : peekMode;
                }
            }
        }

        if (!(modes.heap || modes.offheap)) {
            modes.heap = true;
            modes.offheap = true;
        }

        if (!(modes.primary || modes.backup || modes.near)) {
            modes.primary = true;

            if (!primary) {
                modes.backup = true;
                modes.near = true;
            }
        }

        assert modes.heap || modes.offheap;
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
        @Nullable public abstract T op(GridNearTxLocal tx) throws IgniteCheckedException;
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
        @Nullable @Override public final Object op(GridNearTxLocal tx) throws IgniteCheckedException {
            inOp(tx);

            return null;
        }

        /**
         * @param tx Transaction.
         * @throws IgniteCheckedException If failed.
         */
        public abstract void inOp(GridNearTxLocal tx) throws IgniteCheckedException;
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
        public abstract IgniteInternalFuture<T> op(GridNearTxLocal tx, AffinityTopologyVersion readyTopVer);

        /**
         * @param tx Transaction.
         * @return Operation future.
         */
        public IgniteInternalFuture<T> op(final GridNearTxLocal tx) {
            AffinityTopologyVersion txTopVer = tx.topologyVersionSnapshot();

            if (txTopVer != null)
                return op(tx, (AffinityTopologyVersion)null);

            // Tx needs affinity for entry creation, wait when affinity is ready to avoid blocking inside async operation.
            final AffinityTopologyVersion topVer = ctx.shared().exchange().readyAffinityVersion();

            IgniteInternalFuture<?> topFut = ctx.shared().exchange().affinityReadyFuture(topVer);

            if (topFut == null || topFut.isDone())
                return op(tx, topVer);
            else
                return waitTopologyFuture(topFut, topVer, tx, ctx.operationContextPerCall());
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
            final GridNearTxLocal tx,
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
        private IgniteInternalFuture<T> runOp(GridNearTxLocal tx,
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
        private PartitionSizeLongJob(String cacheName, AffinityTopologyVersion topVer, CachePeekMode[] peekModes,
            int partition) {
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
        @Override public String toString() {
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
        @Override public String toString() {
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
                return 0L;

            try {
                return cache.localSizeLong(peekModes);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
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
        @Override public String toString() {
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
        @Override public String toString() {
            return S.toString(LoadCacheJobV2.class, this);
        }
    }

    /**
     * Holder for last async operation future.
     */
    public static class FutureHolder {
        /** Lock. */
        private final ReentrantLock lock = new ReentrantLock();

        /** Future. */
        private IgniteInternalFuture<?> fut;

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
        public IgniteInternalFuture<?> future() {
            return fut;
        }

        /**
         * Sets future.
         *
         * @param fut Future.
         */
        public void future(@Nullable IgniteInternalFuture<?> fut) {
            this.fut = fut;
        }

        /**
         * Awaits for previous async operation to be completed.
         */
        public void await() {
            IgniteInternalFuture<?> fut = this.fut;

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
         * Saves future in the holder and adds listener that will clear holder when future is finished.
         *
         * @param fut Future to save.
         */
        public void saveFuture(IgniteInternalFuture<?> fut) {
            assert fut != null;
            assert holdsLock();

            if (fut.isDone())
                future(null);
            else {
                future(fut);

                fut.listen(f -> {
                    if (!tryLock())
                        return;

                    try {
                        if (future() == f)
                            future(null);
                    }
                    finally {
                        unlock();
                    }
                });
            }
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

        /** */
        private boolean keepBinary;

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
         * @param keepBinary Keep binary flag.
         */
        LoadKeysCallable(final String cacheName, final Collection<? extends K> keys, final boolean update,
            final ExpiryPolicy plc, final boolean keepBinary) {
            this.cacheName = cacheName;
            this.keys = keys;
            this.update = update;
            this.plc = plc;
            this.keepBinary = keepBinary;
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
                    cache.localLoad(keys, plc, keepBinary);
            }
            finally {
                cache.context().gate().leave();
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(final ObjectOutput out) throws IOException {
            U.writeString(out, cacheName);

            U.writeCollection(out, keys);

            out.writeBoolean(update);

            out.writeObject(plc != null ? new IgniteExternalizableExpiryPolicy(plc) : null);

            out.writeBoolean(keepBinary);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
            cacheName = U.readString(in);

            keys = U.readCollection(in);

            update = in.readBoolean();

            plc = (ExpiryPolicy)in.readObject();

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
                ldr.addDataInternal(col, false);

                col.clear();
            }
        }

        /**
         * Adds remaining data to loader.
         */
        void onDone() {
            if (!col.isEmpty())
                ldr.addDataInternal(col, false);
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
         * @param start Start time.
         */
        public UpdateTimeStatClosure(CacheMetricsImpl metrics, long start) {
            this.metrics = metrics;
            this.start = start;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteInternalFuture<T> fut) {
            try {
                if (!fut.isCancelled()) {
                    T res = fut.get();

                    updateTimeStat(res);
                }
            }
            catch (IgniteCheckedException ignore) {
                //No-op.
            }
        }

        /**
         * Updates statistics.
         *
         * @param res Result of operation.
         */
        protected abstract void updateTimeStat(T res);
    }

    /**
     *
     */
    protected static class UpdateGetTimeStatClosure<T> extends UpdateTimeStatClosure<T> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param metrics Metrics.
         * @param start Start time.
         */
        public UpdateGetTimeStatClosure(CacheMetricsImpl metrics, long start) {
            super(metrics, start);
        }

        /** {@inheritDoc} */
        @Override protected void updateTimeStat(T res) {
            metrics.addGetTimeNanos(System.nanoTime() - start);
        }
    }

    /**
     *
     */
    protected static class UpdateGetAllTimeStatClosure<T> extends UpdateTimeStatClosure<T> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param metrics Metrics.
         * @param start Start time.
         */
        public UpdateGetAllTimeStatClosure(CacheMetricsImpl metrics, long start) {
            super(metrics, start);
        }

        /** {@inheritDoc} */
        @Override protected void updateTimeStat(T res) {
            metrics.addGetAllTimeNanos(System.nanoTime() - start);
        }
    }

    /**
     *
     */
    protected static class UpdateGetAndRemoveTimeStatClosure<T> extends UpdateTimeStatClosure<T> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param metrics Metrics.
         * @param start Start time.
         */
        public UpdateGetAndRemoveTimeStatClosure(CacheMetricsImpl metrics, long start) {
            super(metrics, start);
        }

        /** {@inheritDoc} */
        @Override protected void updateTimeStat(T res) {
            metrics.addRemoveAndGetTimeNanos(System.nanoTime() - start);
        }
    }

    /**
     *
     */
    protected static class UpdateRemoveTimeStatClosure extends UpdateTimeStatClosure<Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param metrics Metrics.
         * @param start Start time.
         */
        public UpdateRemoveTimeStatClosure(CacheMetricsImpl metrics, long start) {
            super(metrics, start);
        }

        /** {@inheritDoc} */
        @Override protected void updateTimeStat(Boolean res) {
            if (res)
                metrics.addRemoveTimeNanos(System.nanoTime() - start);
        }
    }

    /**
     *
     */
    protected static class UpdateRemoveAllTimeStatClosure<T> extends UpdateTimeStatClosure<T> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param metrics Metrics.
         * @param start Start time.
         */
        public UpdateRemoveAllTimeStatClosure(CacheMetricsImpl metrics, long start) {
            super(metrics, start);
        }

        /** {@inheritDoc} */
        @Override protected void updateTimeStat(T res) {
            metrics.addRemoveAllTimeNanos(System.nanoTime() - start);
        }
    }

    /**
     *
     */
    protected static class UpdatePutTimeStatClosure<T> extends UpdateTimeStatClosure<T> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param metrics Metrics.
         * @param start Start time.
         */
        public UpdatePutTimeStatClosure(CacheMetricsImpl metrics, long start) {
            super(metrics, start);
        }

        /** {@inheritDoc} */
        @Override protected void updateTimeStat(T res) {
            metrics.addPutTimeNanos(System.nanoTime() - start);
        }
    }

    /**
     *
     */
    protected static class UpdatePutAllTimeStatClosure<T> extends UpdateTimeStatClosure<T> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param metrics Metrics.
         * @param start Start time.
         */
        public UpdatePutAllTimeStatClosure(CacheMetricsImpl metrics, long start) {
            super(metrics, start);
        }

        /** {@inheritDoc} */
        @Override protected void updateTimeStat(T res) {
            metrics.addPutAllTimeNanos(System.nanoTime() - start);
        }
    }

    /**
     *
     */
    protected static class UpdatePutAndGetTimeStatClosure<T> extends UpdateTimeStatClosure<T> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param metrics Metrics.
         * @param start Start time.
         */
        public UpdatePutAndGetTimeStatClosure(CacheMetricsImpl metrics, long start) {
            super(metrics, start);
        }

        /** {@inheritDoc} */
        @Override protected void updateTimeStat(T res) {
            metrics.addPutAndGetTimeNanos(System.nanoTime() - start);
        }
    }

    /**
     *
     */
    protected static class InvokeAllTimeStatClosure<T> extends UpdateTimeStatClosure<T> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param metrics Metrics.
         * @param start Start time.
         */
        public InvokeAllTimeStatClosure(CacheMetricsImpl metrics, final long start) {
            super(metrics, start);
        }

        /** {@inheritDoc} */
        @Override protected void updateTimeStat(T res) {
            metrics.addInvokeTimeNanos(System.nanoTime() - start);
        }
    }

    /**
     * Writes cache operation performance statistics.
     *
     * @param op Operation type.
     * @param start Start time in nanoseconds.
     */
    private void writeStatistics(OperationType op, long start) {
        ctx.kernalContext().performanceStatistics().cacheOperation(
            op,
            ctx.cacheId(),
            U.currentTimeMillis(),
            System.nanoTime() - start);
    }

    /**
     * Delayed callable class.
     */
    public abstract static class TopologyVersionAwareJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** Injected job context. */
        @JobContextResource
        protected ComputeJobContext jobCtx;

        /** Injected grid instance. */
        @IgniteInstanceResource
        protected IgniteEx ignite;

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

            IgniteInternalCache cache = ignite.context().cache().cache(cacheName);

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
            GridCacheProcessor cacheProc = ignite.context().cache();

            AffinityTopologyVersion locTopVer = cacheProc.context().exchange().readyAffinityVersion();

            if (locTopVer.compareTo(topVer) < 0) {
                IgniteInternalFuture<?> fut = cacheProc.context().exchange().affinityReadyFuture(topVer);

                if (fut != null && !fut.isDone()) {
                    jobCtx.holdcc();

                    fut.listen(new CI1<IgniteInternalFuture<?>>() {
                        @Override public void apply(IgniteInternalFuture<?> t) {
                            ignite.context().closure().runLocalSafe(new GridPlainRunnable() {
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
        @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
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
        @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
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
        @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(
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
        @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Object arg) throws IgniteException {
            Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

            for (ClusterNode node : subgrid) {
                ComputeJob job;

                if (near) {
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
     * Partition preload job.
     */
    @GridInternal
    private static class PartitionPreloadJob implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private final String name;

        /** Cache name. */
        private final int part;

        /**
         * @param name Name.
         * @param part Partition.
         */
        public PartitionPreloadJob(String name, int part) {
            this.name = name;
            this.part = part;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteInternalCache cache = ignite.context().cache().cache(name);

            try {
                cache.context().offheap().preloadPartition(part);
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to preload the partition [cache=" + name + ", partition=" + part + ']', e);

                throw new IgniteException(e);
            }
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
         *
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

            return (K)ctx.unwrapBinaryIfNeeded(current.key(), keepBinary, true, null);
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            if (current == null)
                throw new IllegalStateException();

            try {
                getAndRemove((K)current.key());
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
         *
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
            GridCacheMapEntry entry = map.getEntry(ctx, ctx.toCacheKeyObject(o));

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
         *
         * @param internalIterator Internal iterator.
         * @param keepBinary Keep binary.
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
                getAndRemove((K)current.wrapLazyValue(keepBinary).getKey());
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

        /**
         * Constructor.
         *
         * @param internalSet Internal set.
         * @param keepBinary Keep binary flag.
         */
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
            GridCacheMapEntry entry = map.getEntry(ctx, ctx.toCacheKeyObject(o));

            return entry != null && internalSet.contains(entry);
        }
    }
}

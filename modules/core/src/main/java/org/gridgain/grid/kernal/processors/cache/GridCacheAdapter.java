/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.fs.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.portables.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.affinity.*;
import org.gridgain.grid.kernal.processors.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.dr.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.kernal.processors.dr.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import static java.util.Collections.*;
import static org.gridgain.grid.GridSystemProperties.*;
import static org.gridgain.grid.cache.GridCacheFlag.*;
import static org.gridgain.grid.cache.GridCachePeekMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheTxState.*;
import static org.apache.ignite.events.IgniteEventType.*;
import static org.gridgain.grid.kernal.GridClosureCallMode.*;
import static org.gridgain.grid.kernal.processors.dr.GridDrType.*;
import static org.gridgain.grid.kernal.processors.task.GridTaskThreadContextKey.*;

/**
 * Adapter for different cache implementations.
 */
public abstract class GridCacheAdapter<K, V> extends GridMetadataAwareAdapter implements GridCache<K, V>,
    GridCacheProjectionEx<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** clearAll() split threshold. */
    public static final int CLEAR_ALL_SPLIT_THRESHOLD = 10000;

    /** Deserialization stash. */
    private static final ThreadLocal<IgniteBiTuple<String, String>> stash = new ThreadLocal<IgniteBiTuple<String,
                String>>() {
        @Override protected IgniteBiTuple<String, String> initialValue() {
            return F.t2();
        }
    };

    /** {@link GridCacheReturn}-to-value conversion. */
    private static final IgniteClosure RET2VAL =
        new CX1<IgniteFuture<GridCacheReturn<Object>>, Object>() {
            @Nullable @Override public Object applyx(IgniteFuture<GridCacheReturn<Object>> fut) throws GridException {
                return fut.get().value();
            }

            @Override public String toString() {
                return "Cache return value to value converter.";
            }
        };

    /** {@link GridCacheReturn}-to-success conversion. */
    private static final IgniteClosure RET2FLAG =
        new CX1<IgniteFuture<GridCacheReturn<Object>>, Boolean>() {
            @Override public Boolean applyx(IgniteFuture<GridCacheReturn<Object>> fut) throws GridException {
                return fut.get().success();
            }

            @Override public String toString() {
                return "Cache return value to boolean flag converter.";
            }
        };

    /** */
    protected boolean keyCheck = !Boolean.getBoolean(GG_CACHE_KEY_VALIDATION_DISABLED);

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
    protected GridCacheConcurrentMap<K, V> map;

    /** Local node ID. */
    @GridToStringExclude
    protected UUID locNodeId;

    /** Cache configuration. */
    @GridToStringExclude
    protected GridCacheConfiguration cacheCfg;

    /** Grid configuration. */
    @GridToStringExclude
    protected IgniteConfiguration gridCfg;

    /** Cache metrics. */
    protected volatile GridCacheMetricsAdapter metrics;

    /** Logger. */
    protected IgniteLogger log;

    /** Queries impl. */
    private GridCacheQueries<K, V> qry;

    /** Data structures impl. */
    private GridCacheDataStructures dataStructures;

    /** Affinity impl. */
    private GridCacheAffinity<K> aff;

    /** Whether this cache is GGFS data cache. */
    private boolean ggfsDataCache;

    /** Whether this cache is Mongo data cache. */
    @SuppressWarnings("UnusedDeclaration")
    private boolean mongoDataCache;

    /** Whether this cache is Mongo meta cache. */
    @SuppressWarnings("UnusedDeclaration")
    private boolean mongoMetaCache;

    /** Current GGFS data cache size. */
    private LongAdder ggfsDataCacheSize;

    /** Max space for GGFS. */
    private long ggfsDataSpaceMax;

    /** Asynchronous operations limit semaphore. */
    private Semaphore asyncOpsSem;

    /** {@inheritDoc} */
    @Override public String name() {
        return ctx.config().getName();
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup gridProjection() {
        return ctx.grid().forCache(name());
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
        this(ctx, new GridCacheConcurrentMap<>(ctx, startSize, 0.75F));
    }

    /**
     * @param ctx Cache context.
     * @param map Concurrent map.
     */
    @SuppressWarnings("OverriddenMethodCallDuringObjectConstruction")
    protected GridCacheAdapter(GridCacheContext<K, V> ctx, GridCacheConcurrentMap<K, V> map) {
        assert ctx != null;

        this.ctx = ctx;

        gridCfg = ctx.gridConfig();
        cacheCfg = ctx.config();

        locNodeId = ctx.gridConfig().getNodeId();

        this.map = map;

        log = ctx.gridConfig().getGridLogger().getLogger(getClass());

        metrics = new GridCacheMetricsAdapter();

        IgniteFsConfiguration[] ggfsCfgs = gridCfg.getGgfsConfiguration();

        if (ggfsCfgs != null) {
            for (IgniteFsConfiguration ggfsCfg : ggfsCfgs) {
                if (F.eq(ctx.name(), ggfsCfg.getDataCacheName())) {
                    if (!ctx.isNear()) {
                        ggfsDataCache = true;
                        ggfsDataCacheSize = new LongAdder();

                        ggfsDataSpaceMax = ggfsCfg.getMaxSpaceSize();

                        if (ggfsDataSpaceMax == 0) {
                            long maxMem = Runtime.getRuntime().maxMemory();

                            // We leave JVM at least 500M of memory for correct operation.
                            long jvmFreeSize = (maxMem - 512 * 1024 * 1024);

                            if (jvmFreeSize <= 0)
                                jvmFreeSize = maxMem / 2;

                            long dfltMaxSize = (long)(0.8f * maxMem);

                            ggfsDataSpaceMax = Math.min(dfltMaxSize, jvmFreeSize);
                        }
                    }

                    break;
                }
            }
        }

        if (ctx.config().getMaxConcurrentAsyncOperations() > 0)
            asyncOpsSem = new Semaphore(ctx.config().getMaxConcurrentAsyncOperations());

        init();

        qry = new GridCacheQueriesImpl<>(ctx, null);
        dataStructures = new GridCacheDataStructuresImpl<>(ctx);
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
    public GridCacheConcurrentMap<K, V> map() {
        return map;
    }

    /**
     * @return Context.
     */
    public GridCacheContext<K, V> context() {
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
    public abstract GridCachePreloader<K, V> preloader();

    /** {@inheritDoc} */
    @Override public GridCacheQueries<K, V> queries() {
        return qry;
    }

    /** {@inheritDoc} */
    @Override public GridCacheAffinity<K> affinity() {
        return aff;
    }

    /** {@inheritDoc} */
    @Override public GridCacheDataStructures dataStructures() {
        return dataStructures;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "RedundantCast"})
    @Override public <K1, V1> GridCache<K1, V1> cache() {
        return (GridCache<K1, V1>)this;
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheFlag> flags() {
        return F.asSet(ctx.forcedFlags());
    }

    /** {@inheritDoc} */
    @Override public IgnitePredicate<GridCacheEntry<K, V>> predicate() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheProjectionEx<K, V> forSubjectId(UUID subjId) {
        GridCacheProjectionImpl<K, V> prj = new GridCacheProjectionImpl<>(this, ctx, null, null,
            null, subjId, false);

        return new GridCacheProxyImpl<>(ctx, prj, prj);
    }

    /** {@inheritDoc} */
    @Override public GridCacheProjection<K, V> flagsOn(@Nullable GridCacheFlag[] flags) {
        if (F.isEmpty(flags))
            return this;

        GridCacheProjectionImpl<K, V> prj = new GridCacheProjectionImpl<>(this, ctx, null, null,
            EnumSet.copyOf(F.asList(flags)), null, false);

        return new GridCacheProxyImpl<>(ctx, prj, prj);
    }

    /** {@inheritDoc} */
    @Override public GridCacheProjection<K, V> flagsOff(@Nullable GridCacheFlag[] flags) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> GridCacheProjection<K1, V1> keepPortable() {
        GridCacheProjectionImpl<K1, V1> prj = new GridCacheProjectionImpl<>(
            (GridCacheProjection<K1, V1>)this,
            (GridCacheContext<K1, V1>)ctx,
            null,
            null,
            null,
            null,
            ctx.portableEnabled());

        return new GridCacheProxyImpl<>((GridCacheContext<K1, V1>)ctx, prj, prj);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "RedundantCast"})
    @Override public <K1, V1> GridCacheProjection<K1, V1> projection(
        Class<? super K1> keyType,
        Class<? super V1> valType
    ) {
        if (PortableObject.class.isAssignableFrom(keyType) || PortableObject.class.isAssignableFrom(valType))
            throw new IllegalStateException("Failed to create cache projection for portable objects. " +
                "Use keepPortable() method instead.");

        if (ctx.deploymentEnabled()) {
            try {
                ctx.deploy().registerClasses(keyType, valType);
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }

        GridCacheProjectionImpl<K1, V1> prj = new GridCacheProjectionImpl<>((GridCacheProjection<K1, V1>)this,
            (GridCacheContext<K1, V1>)ctx, CU.<K1, V1>typeFilter(keyType, valType), /*filter*/null, /*flags*/null,
            /*clientId*/null, false);

        return new GridCacheProxyImpl<>((GridCacheContext<K1, V1>)ctx, prj, prj);
    }

    /** {@inheritDoc} */
    @Override public GridCacheProjection<K, V> projection(IgniteBiPredicate<K, V> p) {
        if (p == null)
            return this;

        if (ctx.deploymentEnabled()) {
            try {
                ctx.deploy().registerClasses(p);
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }

        GridCacheProjectionImpl<K, V> prj = new GridCacheProjectionImpl<>(this, ctx, p, null, null, null, false);

        return new GridCacheProxyImpl<>(ctx, prj, prj);
    }

    /** {@inheritDoc} */
    @Override public GridCacheProjection<K, V> projection(IgnitePredicate<GridCacheEntry<K, V>> filter) {
        if (filter == null)
            return this;

        if (ctx.deploymentEnabled()) {
            try {
                ctx.deploy().registerClasses(filter);
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }

        GridCacheProjectionImpl<K, V> prj = new GridCacheProjectionImpl<>(
            this, ctx, null, filter, null, null, false);

        return new GridCacheProxyImpl<>(ctx, prj, prj);
    }

    /** {@inheritDoc} */
    @Override public GridCacheConfiguration configuration() {
        return ctx.config();
    }

    /**
     *
     * @param keys Keys to lock.
     * @param timeout Lock timeout.
     * @param tx Transaction.
     * @param isRead {@code True} for read operations.
     * @param retval Flag to return value.
     * @param isolation Transaction isolation.
     * @param invalidate Invalidate flag.
     * @param filter Optional filter.
     * @return Locks future.
     */
    public abstract IgniteFuture<Boolean> txLockAsync(
        Collection<? extends K> keys,
        long timeout,
        GridCacheTxLocalEx<K, V> tx,
        boolean isRead,
        boolean retval,
        GridCacheTxIsolation isolation,
        boolean invalidate,
        IgnitePredicate<GridCacheEntry<K, V>>[] filter);

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
     * @throws GridException If start failed.
     */
    public void start() throws GridException {
        if (!ctx.isNear()) {
            ctx.io().addHandler(ctx.cacheId(), GridCacheOptimisticCheckPreparedTxRequest.class,
                new CI2<UUID, GridCacheOptimisticCheckPreparedTxRequest<K, V>>() {
                    @Override public void apply(UUID nodeId, GridCacheOptimisticCheckPreparedTxRequest<K, V> req) {
                        processCheckPreparedTxRequest(nodeId, req);
                    }
                });

            ctx.io().addHandler(ctx.cacheId(), GridCacheOptimisticCheckPreparedTxResponse.class,
                new CI2<UUID, GridCacheOptimisticCheckPreparedTxResponse<K, V>>() {
                    @Override public void apply(UUID nodeId, GridCacheOptimisticCheckPreparedTxResponse<K, V> res) {
                        processCheckPreparedTxResponse(nodeId, res);
                    }
                });

            ctx.io().addHandler(ctx.cacheId(), GridCachePessimisticCheckCommittedTxRequest.class,
                new CI2<UUID, GridCachePessimisticCheckCommittedTxRequest<K, V>>() {
                    @Override public void apply(UUID nodeId, GridCachePessimisticCheckCommittedTxRequest<K, V> req) {
                        processCheckCommittedTxRequest(nodeId, req);
                    }
                });

            ctx.io().addHandler(ctx.cacheId(), GridCachePessimisticCheckCommittedTxResponse.class,
                new CI2<UUID, GridCachePessimisticCheckCommittedTxResponse<K, V>>() {
                    @Override public void apply(UUID nodeId, GridCachePessimisticCheckCommittedTxResponse<K, V> res) {
                        processCheckCommittedTxResponse(nodeId, res);
                    }
                });
        }
    }

    /**
     * Startup info.
     *
     * @return Startup info.
     */
    protected final String startInfo() {
        return "Cache started: " + ctx.config().getName();
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
        return "Cache stopped: " + ctx.config().getName();
    }

    /**
     * Kernal start callback.
     *
     * @throws GridException If callback failed.
     */
    protected void onKernalStart() throws GridException {
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
        return values().isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        return containsKey(key, null);
    }

    /** {@inheritDoc} */
    @Override public boolean containsValue(V val) {
        return containsValue(val, null);
    }

    /** {@inheritDoc} */
    @Override public V peek(K key) {
        return peek(key, (IgnitePredicate<GridCacheEntry<K, V>>)null);
    }

    /** {@inheritDoc} */
    @Override public V peek(K key, @Nullable Collection<GridCachePeekMode> modes) throws GridException {
        return peek0(key, modes, ctx.tm().localTxx());
    }

    /** {@inheritDoc} */
    public Map<K, V> peekAll(@Nullable Collection<? extends K> keys,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        return peekAll0(keys, filter, null);
    }

    /**
     * @param failFast Fail fast flag.
     * @param key Key.
     * @param mode Peek mode.
     * @param filter Filter.
     * @return Peeked value.
     * @throws GridCacheFilterFailedException If filter failed.
     */
    @Nullable protected GridTuple<V> peek0(boolean failFast, K key, GridCachePeekMode mode,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws GridCacheFilterFailedException {
        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        ctx.checkSecurity(GridSecurityPermission.CACHE_READ);

        GridCacheEntryEx<K, V> e = null;

        try {
            if (ctx.portableEnabled())
                key = (K)ctx.marshalToPortable(key);

            e = peekEx(key);

            if (e != null) {
                GridTuple<V> peek = e.peek0(failFast, mode, filter, ctx.tm().localTxx());

                if (peek != null) {
                    V v = peek.get();

                    if (ctx.portableEnabled() && !ctx.keepPortable() && v instanceof PortableObject)
                        v = ((PortableObject)v).deserialize();

                    return F.t(ctx.cloneOnFlag(v));
                }
            }

            GridCacheTxEx<K, V> tx = ctx.tm().localTx();

            if (tx != null) {
                GridTuple<V> peek = tx.peek(ctx, failFast, key, filter);

                if (peek != null) {
                    V v = peek.get();

                    if (ctx.portableEnabled() && !ctx.keepPortable() && v instanceof PortableObject)
                        v = ((PortableObject)v).deserialize();

                    return F.t(ctx.cloneOnFlag(v));
                }
            }

            return null;
        }
        catch (GridCacheEntryRemovedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Got removed entry during 'peek': " + e);

            return null;
        }
        catch (GridException ex) {
            throw new GridRuntimeException(ex);
        }
    }

    /**
     * @param keys Keys.
     * @param filter Filter.
     * @param skipped Skipped keys, possibly {@code null}.
     * @return Peeked map.
     */
    protected Map<K, V> peekAll0(@Nullable Collection<? extends K> keys,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter, @Nullable Collection<K> skipped) {
        if (F.isEmpty(keys))
            return Collections.emptyMap();

        if (keyCheck)
            validateCacheKeys(keys);

        ctx.checkSecurity(GridSecurityPermission.CACHE_READ);

        Map<K, V> ret = new HashMap<>(keys.size(), 1.0f);

        for (K k : keys) {
            GridCacheEntryEx<K, V> e = peekEx(k);

            if (e != null)
                try {
                    ret.put(k, ctx.cloneOnFlag(e.peekFailFast(SMART, filter)));
                }
                catch (GridCacheEntryRemovedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry during 'peek' (will skip): " + e);
                }
                catch (GridCacheFilterFailedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Filter failed during peek (will skip): " + e);

                    if (skipped != null)
                        skipped.add(k);
                }
                catch (GridException ex) {
                    throw new GridRuntimeException(ex);
                }
        }

        return ret;
    }

    /**
     * @param key Key.
     * @param modes Peek modes.
     * @param tx Transaction to peek at (if modes contains TX value).
     * @return Peeked value.
     * @throws GridException In case of error.
     */
    @Nullable protected V peek0(K key, @Nullable Collection<GridCachePeekMode> modes, GridCacheTxEx<K, V> tx)
        throws GridException {
        try {
            GridTuple<V> peek = peek0(false, key, modes, tx);

            return peek != null ? peek.get() : null;
        }
        catch (GridCacheFilterFailedException ex) {
            ex.printStackTrace();

            assert false; // Should never happen.

            return null;
        }
    }

    /**
     * @param failFast If {@code true}, then filter failure will result in exception.
     * @param key Key.
     * @param modes Peek modes.
     * @param tx Transaction to peek at (if modes contains TX value).
     * @return Peeked value.
     * @throws GridException In case of error.
     * @throws GridCacheFilterFailedException If filer validation failed.
     */
    @Nullable protected GridTuple<V> peek0(boolean failFast, K key, @Nullable Collection<GridCachePeekMode> modes,
        GridCacheTxEx<K, V> tx) throws GridException, GridCacheFilterFailedException {
        if (F.isEmpty(modes))
            return F.t(peek(key, (IgnitePredicate<GridCacheEntry<K, V>>)null));

        assert modes != null;

        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        ctx.checkSecurity(GridSecurityPermission.CACHE_READ);

        GridCacheEntryEx<K, V> e = peekEx(key);

        try {
            for (GridCachePeekMode m : modes) {
                GridTuple<V> val = null;

                if (e != null)
                    val = e.peek0(failFast, m, null, tx);
                else if (m == TX || m == SMART)
                    val = tx != null ? tx.peek(ctx, failFast, key, null) : null;
                else if (m == SWAP)
                    val = peekSwap(key);
                else if (m == DB)
                    val = peekDb(key);

                if (val != null)
                    return F.t(ctx.cloneOnFlag(val.get()));
            }
        }
        catch (GridCacheEntryRemovedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Got removed entry during 'peek': " + e);
        }

        return null;
    }

    /**
     * @param key Key to read from swap storage.
     * @return Value from swap storage.
     * @throws GridException In case of any errors.
     */
    @Nullable private GridTuple<V> peekSwap(K key) throws GridException {
        GridCacheSwapEntry<V> e = ctx.swap().read(key);

        return e != null ? F.t(e.value()) : null;
    }

    /**
     * @param key Key to read from persistent store.
     * @return Value from persistent store.
     * @throws GridException In case of any errors.
     */
    @Nullable private GridTuple<V> peekDb(K key) throws GridException {
        V val = ctx.store().loadFromStore(ctx.tm().localTxx(), key);

        return val != null ? F.t(val) : null;
    }

    /**
     * @param keys Keys.
     * @param modes Modes.
     * @param tx Transaction.
     * @param skipped Keys skipped during filter validation.
     * @return Peeked values.
     * @throws GridException If failed.
     */
    protected Map<K, V> peekAll0(@Nullable Collection<? extends K> keys, @Nullable Collection<GridCachePeekMode> modes,
        GridCacheTxEx<K, V> tx, @Nullable Collection<K> skipped) throws GridException {
        if (F.isEmpty(keys))
            return emptyMap();

        if (keyCheck)
            validateCacheKeys(keys);

        Map<K, V> ret = new HashMap<>(keys.size(), 1.0f);

        for (K k : keys) {
            try {
                GridTuple<V> val = peek0(skipped != null, k, modes, tx);

                if (val != null)
                    ret.put(k, val.get());
            }
            catch (GridCacheFilterFailedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Filter validation failed for key: " + k);

                if (skipped != null)
                    skipped.add(k);
            }
        }

        return ret;
    }

    /**
     * Pokes an entry.
     *
     * @param key Key.
     * @param newVal New values.
     * @return {@code True} if entry was poked.
     * @throws GridException If failed.
     */
    private boolean poke0(K key, @Nullable V newVal) throws GridException {
        GridCacheEntryEx<K, V> entryEx = peekEx(key);

        if (entryEx == null || entryEx.deleted())
            return newVal == null;

        if (newVal == null)
            return entryEx.markObsolete(ctx.versions().next());

        try {
            entryEx.poke(newVal);
        }
        catch (GridCacheEntryRemovedException ignore) {
            return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void forEach(IgniteInClosure<GridCacheEntry<K, V>> vis) {
        A.notNull(vis, "vis");

        for (GridCacheEntry<K, V> e : entrySet())
            vis.apply(e);
    }

    /** {@inheritDoc} */
    @Override public boolean forAll(IgnitePredicate<GridCacheEntry<K, V>> vis) {
        A.notNull(vis, "vis");

        for (GridCacheEntry<K, V> e : entrySet())
            if (!vis.apply(e))
                return false;

        return true;
    }

    /**
     * Undeploys and removes all entries for class loader.
     *
     * @param leftNodeId Left node ID.
     * @param ldr Class loader to undeploy.
     */
    public void onUndeploy(@Nullable UUID leftNodeId, ClassLoader ldr) {
        ctx.deploy().onUndeploy(leftNodeId, ldr);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheEntry<K, V> entry(K key) {
        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        return entryEx(key, true).wrap(true);
    }

    /**
     *
     * @param key Entry key.
     * @return Entry or <tt>null</tt>.
     */
    @Nullable public GridCacheEntryEx<K, V> peekEx(K key) {
        return entry0(key, ctx.affinity().affinityTopologyVersion(), false, false);
    }

    /**
     * @param key Entry key.
     * @return Entry (never {@code null}).
     */
    public GridCacheEntryEx<K, V> entryEx(K key) {
        return entryEx(key, false);
    }

    /**
     * @param key Entry key.
     * @param touch Whether created entry should be touched.
     * @return Entry (never {@code null}).
     */
    public GridCacheEntryEx<K, V> entryEx(K key, boolean touch) {
        GridCacheEntryEx<K, V> e = entry0(key, ctx.affinity().affinityTopologyVersion(), true, touch);

        assert e != null;

        return e;
    }

    /**
     * @param topVer Topology version.
     * @param key Entry key.
     * @return Entry (never {@code null}).
     */
    public GridCacheEntryEx<K, V> entryEx(K key, long topVer) {
        GridCacheEntryEx<K, V> e = entry0(key, topVer, true, false);

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
    @Nullable private GridCacheEntryEx<K, V> entry0(K key, long topVer, boolean create, boolean touch) {
        GridTriple<GridCacheMapEntry<K, V>> t = map.putEntryIfObsoleteOrAbsent(topVer, key, null,
            ctx.config().getDefaultTimeToLive(), create);

        GridCacheEntryEx<K, V> cur = t.get1();
        GridCacheEntryEx<K, V> created = t.get2();
        GridCacheEntryEx<K, V> doomed = t.get3();

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
     * Same as {@link #entrySet()} but for internal use only to
     * avoid casting.
     *
     * @return Set of entry wrappers.
     */
    public Set<GridCacheEntryImpl<K, V>> wrappers() {
        return map.wrappers(CU.<K, V>empty());
    }

    /**
     * @return Set of internal cached entry representations, excluding {@link GridCacheInternal} keys.
     */
    public Set<GridCacheEntryEx<K, V>> entries() {
        return map.entries0();
    }

    /**
     * @return Set of internal cached entry representations, including {@link GridCacheInternal} keys.
     */
    public Set<GridCacheEntryEx<K, V>> allEntries() {
        return map.allEntries0();
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheEntry<K, V>> entrySet() {
        return entrySet((IgnitePredicate<GridCacheEntry<K, V>>[])null);
    }


    /** {@inheritDoc} */
    @Override public Set<GridCacheEntry<K, V>> entrySetx(IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        return map.entriesx(filter);
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheEntry<K, V>> primaryEntrySetx(IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        return map.entriesx(F.and(filter, F.<K, V>cachePrimary()));
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheEntry<K, V>> entrySet(int part) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheEntry<K, V>> primaryEntrySet() {
        return primaryEntrySet((IgnitePredicate<GridCacheEntry<K, V>>[])null);
    }

    /** {@inheritDoc} */
    @Override public Set<K> keySet() {
        return keySet((IgnitePredicate<GridCacheEntry<K, V>>[])null);
    }

    /** {@inheritDoc} */
    @Override public Set<K> primaryKeySet() {
        return primaryKeySet((IgnitePredicate<GridCacheEntry<K, V>>[])null);
    }

    /** {@inheritDoc} */
    @Override public Collection<V> values() {
        return values((IgnitePredicate<GridCacheEntry<K, V>>[])null);
    }

    /** {@inheritDoc} */
    public Collection<V> values(IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        return map.values(filter);
    }

    /** {@inheritDoc} */
    @Override public Collection<V> primaryValues() {
        return primaryValues((IgnitePredicate<GridCacheEntry<K, V>>[])null);
    }

    /**
     *
     * @param key Entry key.
     */
    public void removeIfObsolete(K key) {
        assert key != null;

        GridCacheEntryEx<K, V> entry = map.removeEntryIfObsolete(key);

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
     * Split clear all task into multiple runnables.
     *
     * @return Split runnables.
     */
    public List<GridCacheClearAllRunnable<K, V>> splitClearAll() {
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
    @Override public boolean clear(K key) {
        return clear0(key);
    }

    /** {@inheritDoc} */
    @Override public void clearAll() {
        ctx.denyOnFlag(READ);
        ctx.checkSecurity(GridSecurityPermission.CACHE_REMOVE);

        List<GridCacheClearAllRunnable<K, V>> jobs = splitClearAll();

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
                        U.warn(log, "Got interrupted while waiting for GridCache.clearAll() executor service to " +
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
    public void clearAll(Collection<? extends K> keys, boolean readers) {
        if (F.isEmpty(keys))
            return;

        if (keyCheck)
            validateCacheKeys(keys);

        GridCacheVersion obsoleteVer = ctx.versions().next();

        for (K key : keys) {
            GridCacheEntryEx<K, V> e = peekEx(key);

            try {
                if (e != null)
                    e.clear(obsoleteVer, readers, null);
            }
            catch (GridException ex) {
                U.error(log, "Failed to clear entry (will continue to clear other entries): " + e,
                    ex);
            }
        }
    }

    /**
     * Clears entry from cache.
     *
     * @param obsoleteVer Obsolete version to set.
     * @param key Key to clear.
     * @param filter Optional filter.
     * @return {@code True} if cleared.
     */
    private boolean clear(GridCacheVersion obsoleteVer, K key,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        try {
            if (ctx.portableEnabled())
                key = (K)ctx.marshalToPortable(key);

            GridCacheEntryEx<K, V> e = peekEx(key);

            return e != null && e.clear(obsoleteVer, false, filter);
        }
        catch (GridException ex) {
            U.error(log, "Failed to clear entry for key: " + key, ex);
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void globalClearAll() throws GridException {
        globalClearAll(0);
    }

    /** {@inheritDoc} */
    @Override public void globalClearAll(long timeout) throws GridException {
        try {
            // Send job to remote nodes only.
            Collection<ClusterNode> nodes = ctx.grid().forCache(name()).forRemotes().nodes();

            IgniteFuture<Object> fut = null;

            if (!nodes.isEmpty()) {
                ctx.kernalContext().task().setThreadContext(TC_TIMEOUT, timeout);

                fut = ctx.closures().callAsyncNoFailover(BROADCAST, new GlobalClearAllCallable(name()), nodes, true);
            }

            // Clear local cache synchronously.
            clearAll();

            if (fut != null)
                fut.get();
        }
        catch (GridEmptyProjectionException ignore) {
            if (log.isDebugEnabled())
                log.debug("All remote nodes left while cache clear [cacheName=" + name() + "]");
        }
        catch (ComputeTaskTimeoutException e) {
            U.warn(log, "Timed out waiting for remote nodes to finish cache clear (consider increasing " +
                "'networkTimeout' configuration property) [cacheName=" + name() + "]");

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean compact(K key) throws GridException {
        return compact(key, (IgnitePredicate<GridCacheEntry<K, V>>[])null);
    }

    /** {@inheritDoc} */
    @Override public void compactAll() throws GridException {
        compactAll(keySet());
    }

    /**
     * @param entry Removes entry from cache if currently mapped value is the same as passed.
     */
    protected void removeEntry(GridCacheEntryEx<K, V> entry) {
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
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        if (ctx.portableEnabled()) {
            try {
                key = (K)ctx.marshalToPortable(key);
            }
            catch (PortableException e) {
                throw new GridRuntimeException(e);
            }
        }

        GridCacheEntryEx<K, V> entry = peekEx(key);

        if (entry == null)
            return true;

        try {
            return ctx.evicts().evict(entry, ver, true, filter);
        }
        catch (GridException ex) {
            U.error(log, "Failed to evict entry from cache: " + entry, ex);

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public V get(K key, @Nullable GridCacheEntryEx<K, V> entry, boolean deserializePortable,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAllAsync(F.asList(key), ctx.hasFlag(GET_PRIMARY), /*skip tx*/false, entry, null, taskName,
            deserializePortable, filter).get().get(key);
    }

    /** {@inheritDoc} */
    @Override public V getForcePrimary(K key) throws GridException {
        ctx.denyOnFlag(LOCAL);

        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAllAsync(F.asList(key), /*force primary*/true, /*skip tx*/false, null, null, taskName, true)
            .get().get(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getForcePrimaryAsync(final K key) {
        ctx.denyOnFlag(LOCAL);

        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAllAsync(Collections.singletonList(key), /*force primary*/true, /*skip tx*/false, null, null,
            taskName, true).chain(new CX1<IgniteFuture<Map<K, V>>, V>() {
                @Override
                public V applyx(IgniteFuture<Map<K, V>> e) throws GridException {
                    return e.get().get(key);
                }
            });
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<K, V> getAllOutTx(List<K> keys) throws GridException {
        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAllAsync(keys, ctx.hasFlag(GET_PRIMARY), /*skip tx*/true, null, null, taskName, true).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Map<K, V>> getAllOutTxAsync(List<K> keys) {
        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAllAsync(keys, ctx.hasFlag(GET_PRIMARY), /*skip tx*/true, null, null, taskName, true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public V reload(K key) throws GridException {
        return reload(key, (IgnitePredicate<GridCacheEntry<K, V>>[])null);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> reloadAsync(K key) {
        return reloadAsync(key, (IgnitePredicate<GridCacheEntry<K, V>>[])null);
    }

    /** {@inheritDoc} */
    @Override public void reloadAll(@Nullable Collection<? extends K> keys) throws GridException {
        reloadAll(keys, (IgnitePredicate<GridCacheEntry<K, V>>[])null);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> reloadAllAsync(@Nullable Collection<? extends K> keys) {
        return reloadAllAsync(keys, (IgnitePredicate<GridCacheEntry<K, V>>[])null);
    }

    /** {@inheritDoc} */
    @Override public void reloadAll() throws GridException {
        ctx.denyOnFlags(F.asList(LOCAL, READ));

        reloadAll(keySet(), (IgnitePredicate<GridCacheEntry<K, V>>[])null);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> reloadAllAsync() {
        ctx.denyOnFlags(F.asList(LOCAL, READ));

        return reloadAllAsync(keySet(), (IgnitePredicate<GridCacheEntry<K, V>>[])null);
    }

    /**
     * @param keys Keys.
     * @param reload Reload flag.
     * @param tx Transaction.
     * @param filter Filter.
     * @param vis Visitor.
     * @return Future.
     */
    public IgniteFuture<Object> readThroughAllAsync(final Collection<? extends K> keys, boolean reload,
        @Nullable final GridCacheTxEx<K, V> tx, IgnitePredicate<GridCacheEntry<K, V>>[] filter, @Nullable UUID subjId,
        String taskName, final IgniteBiInClosure<K, V> vis) {
        return ctx.closures().callLocalSafe(new GPC<Object>() {
            @Nullable @Override public Object call() {
                try {
                    ctx.store().loadAllFromStore(tx, keys, vis);
                }
                catch (GridException e) {
                    throw new GridClosureException(e);
                }

                return null;
            }
        }, true);
    }

    /**
     * @param keys Keys.
     * @param ret Return flag.
     * @param filter Optional filter.
     * @return Non-{@code null} map if return flag is {@code true}.
     * @throws GridException If failed.
     */
    @Nullable public Map<K, V> reloadAll(@Nullable Collection<? extends K> keys, boolean ret,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        UUID subjId = ctx.subjectIdPerCall(null);

        String taskName = ctx.kernalContext().job().currentTaskName();

        return reloadAllAsync(keys, ret, subjId, taskName, filter).get();
    }

    /**
     * @param keys Keys.
     * @param ret Return flag.
     * @param filter Filter.
     * @return Future.
     */
    public IgniteFuture<Map<K, V>> reloadAllAsync(@Nullable Collection<? extends K> keys, boolean ret,
        @Nullable UUID subjId, String taskName, @Nullable final IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        ctx.denyOnFlag(READ);

        final long topVer = ctx.affinity().affinityTopologyVersion();

        if (!F.isEmpty(keys)) {
            try {
                final String uid = CU.uuid(); // Get meta UUID for this thread.

                assert keys != null;

                if (keyCheck)
                    validateCacheKeys(keys);

                for (K key : keys) {
                    if (key == null)
                        continue;

                    // Skip primary or backup entries for near cache.
                    if (ctx.isNear() && ctx.affinity().localNode(key, topVer))
                        continue;

                    while (true) {
                        try {
                            GridCacheEntryEx<K, V> entry = entryExSafe(key, topVer);

                            if (entry == null)
                                break;

                            // Get version before checking filer.
                            GridCacheVersion ver = entry.version();

                            if (ctx.isAll(entry, filter))
                                // Tag entry with current version.
                                entry.addMeta(uid, ver);
                            else
                                ctx.evicts().touch(entry, topVer);

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

                final Map<K, V> map = ret ? new HashMap<K, V>(keys.size(), 1.0f) : null;

                final Collection<? extends K> absentKeys = F.view(keys, CU.keyHasMeta(ctx, uid));

                final Collection<K> loadedKeys = new GridConcurrentHashSet<>();

                IgniteFuture<Object> readFut =
                    readThroughAllAsync(absentKeys, true, null, filter, subjId, taskName, new CI2<K, V>() {
                        /** Version for all loaded entries. */
                        private GridCacheVersion nextVer = ctx.versions().next();

                        /** {@inheritDoc} */
                        @Override public void apply(K key, V val) {
                            loadedKeys.add(key);

                            GridCacheEntryEx<K, V> entry = peekEx(key);

                            if (entry != null) {
                                try {
                                    GridCacheVersion curVer = entry.removeMeta(uid);

                                    // If entry passed the filter.
                                    if (curVer != null) {
                                        boolean wasNew = entry.isNewLocked();

                                        entry.unswap();

                                        boolean set = entry.versionedValue(val, curVer, nextVer);

                                        ctx.evicts().touch(entry, topVer);

                                        if (map != null) {
                                            if (set || wasNew)
                                                map.put(key, val);
                                            else {
                                                try {
                                                    GridTuple<V> v = peek0(false, key, GLOBAL, filter);

                                                    if (v != null)
                                                        map.put(key, val);
                                                }
                                                catch (GridCacheFilterFailedException ex) {
                                                    ex.printStackTrace();

                                                    assert false;
                                                }
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
                                catch (GridException e) {
                                    throw new GridRuntimeException(e);
                                }
                            }
                        }
                    });

                return readFut.chain(new CX1<IgniteFuture<Object>, Map<K, V>>() {
                    @Override public Map<K, V> applyx(IgniteFuture<Object> e) throws GridException {
                        // Touch all not loaded keys.
                        for (K key : absentKeys) {
                            if (!loadedKeys.contains(key)) {
                                GridCacheEntryEx<K, V> entry = peekEx(key);

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
            catch (GridException e) {
                return new GridFinishedFuture<>(ctx.kernalContext(), e);
            }
        }

        return new GridFinishedFuture<>(ctx.kernalContext(), Collections.<K, V>emptyMap());
    }

    /**
     * @param key Key.
     * @return Entry.
     */
    @Nullable protected GridCacheEntryEx<K, V> entryExSafe(K key, long topVer) {
        return entryEx(key);
    }

    /** {@inheritDoc} */
    @Override public boolean evict(K key) {
        return evict(key, (IgnitePredicate<GridCacheEntry<K, V>>[])null);
    }

    /** {@inheritDoc} */
    @Override public void evictAll() {
        evictAll(keySet());
    }

    /** {@inheritDoc} */
    @Override public void evictAll(@Nullable Collection<? extends K> keys) {
        evictAll(keys, (IgnitePredicate<GridCacheEntry<K, V>>[])null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public V get(K key) throws GridException {
        V val = get(key, true, null);

        if (ctx.config().getInterceptor() != null)
            val = (V)ctx.config().getInterceptor().onGet(key, val);

        return val;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAsync(final K key) {
        IgniteFuture<V> fut = getAsync(key, true, null);

        if (ctx.config().getInterceptor() != null)
            return fut.chain(new CX1<IgniteFuture<V>, V>() {
                @Override public V applyx(IgniteFuture<V> f) throws GridException {
                    return (V)ctx.config().getInterceptor().onGet(key, f.get());
                }
            });

        return fut;
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(@Nullable Collection<? extends K> keys) throws GridException {
        Map<K, V> map = getAll(keys, true, null);

        if (ctx.config().getInterceptor() != null)
            map = interceptGet(keys, map);

        return map;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Map<K, V>> getAllAsync(@Nullable final Collection<? extends K> keys) {
        IgniteFuture<Map<K, V>> fut = getAllAsync(keys, true, null);

        if (ctx.config().getInterceptor() != null)
            return fut.chain(new CX1<IgniteFuture<Map<K, V>>, Map<K, V>>() {
                @Override public Map<K, V> applyx(IgniteFuture<Map<K, V>> f) throws GridException {
                    return interceptGet(keys, f.get());
                }
            });

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

        GridCacheInterceptor<K, V> interceptor = cacheCfg.getInterceptor();

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

    /** {@inheritDoc} */
    protected IgniteFuture<Map<K, V>> getAllAsync(
        @Nullable Collection<? extends K> keys,
        boolean forcePrimary,
        boolean skipTx,
        @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializePortable,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter
    ) {
        subjId = ctx.subjectIdPerCall(subjId);

        return getAllAsync(keys, entry, !skipTx, subjId, taskName, deserializePortable, forcePrimary, filter);
    }

    /** {@inheritDoc} */
    public IgniteFuture<Map<K, V>> getAllAsync(@Nullable final Collection<? extends K> keys,
        @Nullable GridCacheEntryEx<K, V> cached, boolean checkTx, @Nullable final UUID subjId, final String taskName,
        final boolean deserializePortable, final boolean forcePrimary,
        @Nullable final IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        ctx.checkSecurity(GridSecurityPermission.CACHE_READ);

        ctx.denyOnFlag(LOCAL);

        // Entry must be passed for one key only.
        assert cached == null || keys.size() == 1;
        assert ctx.portableEnabled() || cached == null || F.first(keys).equals(cached.key());

        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(ctx.kernalContext(), Collections.<K, V>emptyMap());

        if (keyCheck)
            validateCacheKeys(keys);

        GridCacheTxLocalAdapter<K, V> tx = null;

        if (checkTx) {
            try {
                checkJta();
            }
            catch (GridException e) {
                return new GridFinishedFuture<>(ctx.kernalContext(), e);
            }

            tx = ctx.tm().threadLocalTx();
        }

        if (tx == null || tx.implicit()) {
            try {
                assert keys != null;

                final long topVer = tx == null ? ctx.affinity().affinityTopologyVersion() : tx.topologyVersion();

                final Map<K, V> map = new GridLeanMap<>(keys.size());

                Map<K, GridCacheVersion> misses = null;

                for (K key : keys) {
                    // Ignore null keys.
                    if (key == null)
                        continue;

                    K key0 = null;

                    while (true) {
                        GridCacheEntryEx<K, V> entry;

                        if (cached != null) {
                            entry = cached;

                            cached = null;
                        }
                        else {
                            if (key0 == null)
                                key0 = ctx.portableEnabled() ? (K)ctx.marshalToPortable(key) : key;

                            entry = entryEx(key0);
                        }

                        try {
                            V val = entry.innerGet(null,
                                ctx.isSwapOrOffheapEnabled(),
                                /*don't read-through*/false,
                                /*fail-fast*/true,
                                /*unmarshal*/true,
                                /*update-metrics*/true,
                                /*event*/true,
                                /*temporary*/false,
                                subjId,
                                null,
                                taskName,
                                filter);

                            GridCacheVersion ver = entry.version();

                            if (val == null) {
                                if (misses == null)
                                    misses = new GridLeanMap<>();

                                misses.put(key, ver);
                            }
                            else {
                                val = ctx.cloneOnFlag(val);

                                if (ctx.portableEnabled() && deserializePortable && val instanceof PortableObject)
                                    val = ((PortableObject)val).deserialize();

                                map.put(key, val);

                                if (tx == null || (!tx.implicit() && tx.isolation() == READ_COMMITTED))
                                    ctx.evicts().touch(entry, topVer);

                                if (keys.size() == 1)
                                    // Safe to return because no locks are required in READ_COMMITTED mode.
                                    return new GridFinishedFuture<>(ctx.kernalContext(), map);
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

                if (misses != null && ctx.isStoreEnabled()) {
                    final Map<K, GridCacheVersion> loadKeys = misses;

                    final Collection<K> redos = new LinkedList<>();

                    final GridCacheTxLocalAdapter<K, V> tx0 = tx;

                    final Collection<K> loaded = new HashSet<>();

                    return new GridEmbeddedFuture<>(
                        ctx.kernalContext(),
                        ctx.closures().callLocalSafe(ctx.projectSafe(new GPC<Map<K, V>>() {
                            @Override public Map<K, V> call() throws Exception {
                                ctx.store().loadAllFromStore(null/*tx*/, loadKeys.keySet(), new CI2<K, V>() {
                                    /** New version for all new entries. */
                                    private GridCacheVersion nextVer;

                                    @Override public void apply(K key, V val) {
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

                                        while (true) {
                                            GridCacheEntryEx<K, V> entry = entryEx(key);

                                            try {
                                                boolean set = entry.versionedValue(val, ver, nextVer);

                                                if (log.isDebugEnabled())
                                                    log.debug("Set value loaded from store into entry [set=" + set +
                                                        ", curVer=" + ver + ", newVer=" + nextVer + ", " +
                                                        "entry=" + entry + ']');

                                                boolean touch = true;

                                                // Don't put key-value pair into result map if value is null.
                                                if (val != null) {
                                                    if (set || F.isEmptyOrNulls(filter))
                                                        map.put(key, ctx.cloneOnFlag(val));
                                                    else {
                                                        touch = false;

                                                        // Try again, so we can return consistent values.
                                                        redos.add(key);
                                                    }
                                                }

                                                if (touch && (tx0 == null || (!tx0.implicit() &&
                                                    tx0.isolation() == READ_COMMITTED)))
                                                    ctx.evicts().touch(entry, topVer);

                                                break;
                                            }
                                            catch (GridCacheEntryRemovedException ignore) {
                                                if (log.isDebugEnabled())
                                                    log.debug("Got removed entry during getAllAsync (will retry): " +
                                                        entry);
                                            }
                                            catch (GridException e) {
                                                // Wrap errors (will be unwrapped).
                                                throw new GridClosureException(e);
                                            }
                                        }
                                    }
                                });

                                if (loaded.size() != loadKeys.size()) {
                                    for (K key : loadKeys.keySet()) {
                                        if (loaded.contains(key))
                                            continue;

                                        if (tx0 == null || (!tx0.implicit() &&
                                            tx0.isolation() == READ_COMMITTED)) {
                                            GridCacheEntryEx<K, V> entry = peekEx(key);

                                            if (entry != null)
                                                ctx.evicts().touch(entry, topVer);
                                        }
                                    }
                                }

                                return map;
                            }
                        }), true),
                        new C2<Map<K, V>, Exception, IgniteFuture<Map<K, V>>>() {
                            @Override public IgniteFuture<Map<K, V>> apply(Map<K, V> map, Exception e) {
                                if (e != null)
                                    return new GridFinishedFuture<>(ctx.kernalContext(), e);

                                if (tx0 == null || (!tx0.implicit() && tx0.isolation() == READ_COMMITTED)) {
                                    Collection<K> notFound = new HashSet<>(loadKeys.keySet());

                                    notFound.removeAll(loaded);

                                    // Touch entries that were not found in store.
                                    for (K key : notFound) {
                                        GridCacheEntryEx<K, V> entry = peekEx(key);

                                        if (entry != null)
                                            ctx.evicts().touch(entry, topVer);
                                    }
                                }

                                if (!redos.isEmpty())
                                    // Future recursion.
                                    return getAllAsync(redos, forcePrimary, /*skip tx*/false,
                                        /*entry*/null, subjId, taskName, deserializePortable, filter);

                                // There were no misses.
                                return new GridFinishedFuture<>(ctx.kernalContext(), Collections.<K,
                                    V>emptyMap());
                            }
                        },
                        new C2<Map<K, V>, Exception, Map<K, V>>() {
                            @Override public Map<K, V> apply(Map<K, V> loaded, Exception e) {
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
                        for (K key : misses.keySet()) {
                            GridCacheEntryEx<K, V> entry = peekEx(key);

                            if (entry != null)
                                ctx.evicts().touch(entry, topVer);
                        }
                    }
                }

                return new GridFinishedFuture<>(ctx.kernalContext(), map);
            }
            catch (GridException e) {
                return new GridFinishedFuture<>(ctx.kernalContext(), e);
            }
        }
        else {
            final GridCacheEntryEx<K, V> cached0 = cached;

            return asyncOp(tx, new AsyncOp<Map<K, V>>(keys) {
                @Override public IgniteFuture<Map<K, V>> op(GridCacheTxLocalAdapter<K, V> tx) {
                    return ctx.wrapCloneMap(tx.getAllAsync(ctx, keys, cached0, deserializePortable, filter));
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override public V put(K key, V val, @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter)
        throws GridException {
        return put(key, val, null, -1, filter);
    }

    /** {@inheritDoc} */
    @Nullable @Override public V put(final K key, final V val, @Nullable final GridCacheEntryEx<K, V> cached,
        final long ttl, @Nullable final IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        ctx.denyOnLocalRead();

        return ctx.cloneOnFlag(syncOp(new SyncOp<V>(true) {
            @Override public V op(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                return tx.putAllAsync(ctx, F.t(key, val), true, cached, ttl, filter).get().value();
            }

            @Override
            public String toString() {
                return "put [key=" + key + ", val=" + val + ", filter=" + Arrays.toString(filter) + ']';
            }
        }));
    }

    /** {@inheritDoc} */
    @Override public boolean putx(final K key, final V val, @Nullable final GridCacheEntryEx<K, V> cached,
        final long ttl, @Nullable final IgnitePredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        ctx.denyOnLocalRead();

        return syncOp(new SyncOp<Boolean>(true) {
            @Override
            public Boolean op(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                return tx.putAllAsync(ctx, F.t(key, val), false, cached, ttl, filter).get().success();
            }

            @Override
            public String toString() {
                return "put [key=" + key + ", val=" + val + ", filter=" + Arrays.toString(filter) + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> putAsync(K key, V val,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        return putAsync(key, val, null, -1, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> putAsync(final K key, final V val, @Nullable final GridCacheEntryEx<K, V> entry,
        final long ttl, @Nullable final IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        ctx.denyOnLocalRead();

        return ctx.wrapClone(asyncOp(new AsyncOp<V>(key) {
            @Override
            public IgniteFuture<V> op(GridCacheTxLocalAdapter<K, V> tx) {
                return tx.putAllAsync(ctx, F.t(key, val), true, entry, ttl, filter)
                    .chain((IgniteClosure<IgniteFuture<GridCacheReturn<V>>, V>)RET2VAL);
            }

            @Override
            public String toString() {
                return "putAsync [key=" + key + ", val=" + val + ", filter=" + Arrays.toString(filter) + ']';
            }
        }));
    }

    /** {@inheritDoc} */
    @Override public boolean putx(final K key, final V val,
        final IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        ctx.denyOnLocalRead();

        return syncOp(new SyncOp<Boolean>(true) {
            @Override
            public Boolean op(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                return tx.putAllAsync(ctx, F.t(key, val), false, null, -1, filter).get().success();
            }

            @Override
            public String toString() {
                return "putx [key=" + key + ", val=" + val + ", filter=" + Arrays.toString(filter) + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void putAllDr(final Map<? extends K, GridCacheDrInfo<V>> drMap) throws GridException {
        if (F.isEmpty(drMap))
            return;

        ctx.dr().onReceiveCacheEntriesReceived(drMap.size());

        ctx.denyOnLocalRead();

        syncOp(new SyncInOp(drMap.size() == 1) {
            @Override public void inOp(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                tx.putAllDrAsync(ctx, drMap).get();
            }

            @Override public String toString() {
                return "putAllDr [drMap=" + drMap + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> putAllDrAsync(final Map<? extends K, GridCacheDrInfo<V>> drMap)
        throws GridException {
        if (F.isEmpty(drMap))
            return new GridFinishedFuture<Object>(ctx.kernalContext());

        ctx.dr().onReceiveCacheEntriesReceived(drMap.size());

        ctx.denyOnLocalRead();

        return asyncOp(new AsyncInOp(drMap.keySet()) {
            @Override public IgniteFuture<?> inOp(GridCacheTxLocalAdapter<K, V> tx) {
                return tx.putAllDrAsync(ctx, drMap);
            }

            @Override public String toString() {
                return "putAllDrAsync [drMap=" + drMap + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void transform(final K key, final IgniteClosure<V, V> transformer) throws GridException {
        A.notNull(key, "key", transformer, "valTransform");

        if (keyCheck)
            validateCacheKey(key);

        ctx.denyOnLocalRead();

        syncOp(new SyncInOp(true) {
            @Override public void inOp(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                tx.transformAllAsync(ctx, Collections.singletonMap(key, transformer), false, null, -1).get();
            }

            @Override public String toString() {
                return "transform [key=" + key + ", valTransform=" + transformer + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <R> R transformAndCompute(final K key, final IgniteClosure<V, IgniteBiTuple<V, R>> transformer)
        throws GridException {
        A.notNull(key, "key", transformer, "transformer");

        if (keyCheck)
            validateCacheKey(key);

        ctx.denyOnLocalRead();

        return syncOp(new SyncOp<R>(true) {
            @Override public R op(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                IgniteFuture<GridCacheReturn<V>> ret = tx.transformAllAsync(ctx,
                    F.t(key, new GridCacheTransformComputeClosure<>(transformer)), true, null, -1);

                return transformer.apply(ret.get().value()).get2();
            }

            @Override public String toString() {
                return "transformAndCompute [key=" + key + ", valTransform=" + transformer + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> putxAsync(K key, V val,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        return putxAsync(key, val, null, -1, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> putxAsync(final K key, final V val,
        @Nullable final GridCacheEntryEx<K, V> entry, final long ttl,
        @Nullable final IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        ctx.denyOnLocalRead();

        return asyncOp(new AsyncOp<Boolean>(key) {
            @Override public IgniteFuture<Boolean> op(GridCacheTxLocalAdapter<K, V> tx) {
                return tx.putAllAsync(ctx, F.t(key, val), false, entry, ttl, filter).chain(
                    (IgniteClosure<IgniteFuture<GridCacheReturn<V>>, Boolean>)RET2FLAG);
            }

            @Override public String toString() {
                return "putxAsync [key=" + key + ", val=" + val + ", filter=" + Arrays.toString(filter) + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> transformAsync(final K key, final IgniteClosure<V, V> transformer) {
        return transformAsync(key, transformer, null, -1);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> transformAsync(final K key, final IgniteClosure<V, V> transformer,
        @Nullable final GridCacheEntryEx<K, V> entry, final long ttl) {
        A.notNull(key, "key", transformer, "transformer");

        if (keyCheck)
            validateCacheKey(key);

        ctx.denyOnLocalRead();

        return asyncOp(new AsyncInOp(key) {
            @Override public IgniteFuture<?> inOp(GridCacheTxLocalAdapter<K, V> tx) {
                return tx.transformAllAsync(ctx, F.t(key, transformer), false, entry, ttl);
            }

            @Override public String toString() {
                return "transformAsync [key=" + key + ", valTransform=" + transformer + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Nullable @Override public V putIfAbsent(final K key, final V val) throws GridException {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        ctx.denyOnLocalRead();

        return ctx.cloneOnFlag(syncOp(new SyncOp<V>(true) {
            @Override public V op(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                return tx.putAllAsync(ctx, F.t(key, val), true, null, -1, ctx.noPeekArray()).get().value();
            }

            @Override public String toString() {
                return "putIfAbsent [key=" + key + ", val=" + val + ']';
            }
        }));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> putIfAbsentAsync(final K key, final V val) {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        ctx.denyOnLocalRead();

        return ctx.wrapClone(asyncOp(new AsyncOp<V>(key) {
            @Override public IgniteFuture<V> op(GridCacheTxLocalAdapter<K, V> tx) {
                return tx.putAllAsync(ctx, F.t(key, val), true, null, -1, ctx.noPeekArray())
                    .chain((IgniteClosure<IgniteFuture<GridCacheReturn<V>>, V>)RET2VAL);
            }

            @Override public String toString() {
                return "putIfAbsentAsync [key=" + key + ", val=" + val + ']';
            }
        }));
    }

    /** {@inheritDoc} */
    @Override public boolean putxIfAbsent(final K key, final V val) throws GridException {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        ctx.denyOnLocalRead();

        return syncOp(new SyncOp<Boolean>(true) {
            @Override public Boolean op(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                return tx.putAllAsync(ctx, F.t(key, val), false, null, -1, ctx.noPeekArray()).get().success();
            }

            @Override public String toString() {
                return "putxIfAbsent [key=" + key + ", val=" + val + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> putxIfAbsentAsync(final K key, final V val) {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        ctx.denyOnLocalRead();

        return asyncOp(new AsyncOp<Boolean>(key) {
            @Override public IgniteFuture<Boolean> op(GridCacheTxLocalAdapter<K, V> tx) {
                return tx.putAllAsync(ctx, F.t(key, val), false, null, -1, ctx.noPeekArray()).chain(
                    (IgniteClosure<IgniteFuture<GridCacheReturn<V>>, Boolean>)RET2FLAG);
            }

            @Override public String toString() {
                return "putxIfAbsentAsync [key=" + key + ", val=" + val + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public V replace(final K key, final V val) throws GridException {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        ctx.denyOnLocalRead();

        return ctx.cloneOnFlag(syncOp(new SyncOp<V>(true) {
            @Override public V op(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                return tx.putAllAsync(ctx, F.t(key, val), true, null, -1, ctx.hasPeekArray()).get().value();
            }

            @Override public String toString() {
                return "replace [key=" + key + ", val=" + val + ']';
            }
        }));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> replaceAsync(final K key, final V val) {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        ctx.denyOnLocalRead();

        return ctx.wrapClone(asyncOp(new AsyncOp<V>(key) {
            @Override public IgniteFuture<V> op(GridCacheTxLocalAdapter<K, V> tx) {
                return tx.putAllAsync(ctx, F.t(key, val), true, null, -1, ctx.hasPeekArray()).chain(
                    (IgniteClosure<IgniteFuture<GridCacheReturn<V>>, V>)RET2VAL);
            }

            @Override public String toString() {
                return "replaceAsync [key=" + key + ", val=" + val + ']';
            }
        }));
    }

    /** {@inheritDoc} */
    @Override public boolean replacex(final K key, final V val) throws GridException {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        ctx.denyOnLocalRead();

        return syncOp(new SyncOp<Boolean>(true) {
            @Override public Boolean op(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                return tx.putAllAsync(ctx, F.t(key, val), false, null, -1, ctx.hasPeekArray()).get().success();
            }

            @Override public String toString() {
                return "replacex [key=" + key + ", val=" + val + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replacexAsync(final K key, final V val) {
        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        ctx.denyOnLocalRead();

        return asyncOp(new AsyncOp<Boolean>(key) {
            @Override public IgniteFuture<Boolean> op(GridCacheTxLocalAdapter<K, V> tx) {
                return tx.putAllAsync(ctx, F.t(key, val), false, null, -1, ctx.hasPeekArray()).chain(
                    (IgniteClosure<IgniteFuture<GridCacheReturn<V>>, Boolean>)RET2FLAG);
            }

            @Override public String toString() {
                return "replacexAsync [key=" + key + ", val=" + val + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean replace(final K key, final V oldVal, final V newVal) throws GridException {
        A.notNull(key, "key", oldVal, "oldVal", newVal, "newVal");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(oldVal);

        validateCacheValue(newVal);

        ctx.denyOnLocalRead();

        return syncOp(new SyncOp<Boolean>(true) {
            @Override public Boolean op(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                // Register before hiding in the filter.
                if (ctx.deploymentEnabled())
                    ctx.deploy().registerClass(oldVal);

                return tx.putAllAsync(ctx, F.t(key, newVal), false, null, -1, ctx.equalsPeekArray(oldVal)).get()
                    .success();
            }

            @Override public String toString() {
                return "replace [key=" + key + ", oldVal=" + oldVal + ", newVal=" + newVal + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replaceAsync(final K key, final V oldVal, final V newVal) {
        A.notNull(key, "key", oldVal, "oldVal", newVal, "newVal");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(oldVal);

        validateCacheValue(newVal);

        ctx.denyOnLocalRead();

        return asyncOp(new AsyncOp<Boolean>(key) {
            @Override public IgniteFuture<Boolean> op(GridCacheTxLocalAdapter<K, V> tx) {
                // Register before hiding in the filter.
                if (ctx.deploymentEnabled()) {
                    try {
                        ctx.deploy().registerClass(oldVal);
                    }
                    catch (GridException e) {
                        return new GridFinishedFuture<>(ctx.kernalContext(), e);
                    }
                }

                return tx.putAllAsync(ctx, F.t(key, newVal), false, null, -1, ctx.equalsPeekArray(oldVal)).chain(
                    (IgniteClosure<IgniteFuture<GridCacheReturn<V>>, Boolean>)RET2FLAG);
            }

            @Override public String toString() {
                return "replaceAsync [key=" + key + ", oldVal=" + oldVal + ", newVal=" + newVal + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void putAll(@Nullable final Map<? extends K, ? extends V> m,
        final IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        if (F.isEmpty(m))
            return;

        if (keyCheck)
            validateCacheKeys(m.keySet());

        validateCacheValues(m.values());

        ctx.denyOnLocalRead();

        syncOp(new SyncInOp(m.size() == 1) {
            @Override public void inOp(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                tx.putAllAsync(ctx, m, false, null, -1, filter).get();
            }

            @Override public String toString() {
                return "putAll [map=" + m + ", filter=" + Arrays.toString(filter) + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void transformAll(@Nullable final Map<? extends K, ? extends IgniteClosure<V, V>> m)
        throws GridException {
        if (F.isEmpty(m))
            return;

        if (keyCheck)
            validateCacheKeys(m.keySet());

        ctx.denyOnLocalRead();

        syncOp(new SyncInOp(m.size() == 1) {
            @Override public void inOp(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                tx.transformAllAsync(ctx, m, false, null, -1).get();
            }

            @Override public String toString() {
                return "transformAll [map=" + m + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void transformAll(@Nullable Set<? extends K> keys, final IgniteClosure<V, V> transformer)
        throws GridException {
        if (F.isEmpty(keys))
            return;

        // Reuse transformAll(Map), mapping all keys to a transformer closure.
        transformAll(F.viewAsMap(keys, new C1<K, IgniteClosure<V, V>>() {
            @Override public IgniteClosure<V, V> apply(K k) {
                return transformer;
            }
        }));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> putAllAsync(final Map<? extends K, ? extends V> m,
        @Nullable final IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        if (F.isEmpty(m))
            return new GridFinishedFuture<Object>(ctx.kernalContext());

        if (keyCheck)
            validateCacheKeys(m.keySet());

        validateCacheValues(m.values());

        ctx.denyOnLocalRead();

        return asyncOp(new AsyncInOp(m.keySet()) {
            @Override public IgniteFuture<?> inOp(GridCacheTxLocalAdapter<K, V> tx) {
                return tx.putAllAsync(ctx, m, false, null, -1, filter);
            }

            @Override public String toString() {
                return "putAllAsync [map=" + m + ", filter=" + Arrays.toString(filter) + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> transformAllAsync(@Nullable final Map<? extends K, ? extends IgniteClosure<V, V>> m) {
        if (F.isEmpty(m))
            return new GridFinishedFuture<>(ctx.kernalContext());

        if (keyCheck)
            validateCacheKeys(m.keySet());

        ctx.denyOnLocalRead();

        return asyncOp(new AsyncInOp(m.keySet()) {
            @Override public IgniteFuture<?> inOp(GridCacheTxLocalAdapter<K, V> tx) {
                return tx.transformAllAsync(ctx, m, false, null, -1);
            }

            @Override public String toString() {
                return "transformAllAsync [map=" + m + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> transformAllAsync(@Nullable Set<? extends K> keys,
        final IgniteClosure<V, V> transformer) throws GridException {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(ctx.kernalContext());

        // Reuse transformAllAsync(Map), mapping all keys to a transformer closure.
        return transformAllAsync(F.viewAsMap(keys, new C1<K, IgniteClosure<V, V>>() {
            @Override public IgniteClosure<V, V> apply(K k) {
                return transformer;
            }
        }));
    }

    /** {@inheritDoc} */
    @Nullable @Override public V remove(K key, IgnitePredicate<GridCacheEntry<K, V>>[] filter)
        throws GridException {
        return remove(key, null, filter);
    }

    /** {@inheritDoc} */
    @Override public V remove(final K key, @Nullable final GridCacheEntryEx<K, V> entry,
        @Nullable final IgnitePredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        ctx.denyOnLocalRead();

        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        return ctx.cloneOnFlag(syncOp(new SyncOp<V>(true) {
            @Override public V op(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                V ret = tx.removeAllAsync(ctx, Collections.singletonList(key), entry, true, filter).get().value();

                if (ctx.config().getInterceptor() != null)
                    return (V)ctx.config().getInterceptor().onBeforeRemove(key, ret).get2();

                return ret;
            }

            @Override public String toString() {
                return "remove [key=" + key + ", filter=" + Arrays.toString(filter) + ']';
            }
        }));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> removeAsync(K key, IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        return removeAsync(key, null, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> removeAsync(final K key, @Nullable final GridCacheEntryEx<K, V> entry,
        @Nullable final IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        ctx.denyOnLocalRead();

        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        return ctx.wrapClone(asyncOp(new AsyncOp<V>(key) {
            @Override public IgniteFuture<V> op(GridCacheTxLocalAdapter<K, V> tx) {
                // TODO should we invoke interceptor here?
                return tx.removeAllAsync(ctx, Collections.singletonList(key), null, true, filter)
                    .chain((IgniteClosure<IgniteFuture<GridCacheReturn<V>>, V>) RET2VAL);
            }

            @Override public String toString() {
                return "removeAsync [key=" + key + ", filter=" + Arrays.toString(filter) + ']';
            }
        }));
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable final Collection<? extends K> keys,
        final IgnitePredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        ctx.denyOnLocalRead();

        if (F.isEmpty(keys))
            return;

        if (keyCheck)
            validateCacheKeys(keys);

        Collection<K> pKeys = null;

        if (ctx.portableEnabled()) {
            pKeys = new ArrayList<>(keys.size());

            for (K key : keys)
                pKeys.add((K)ctx.marshalToPortable(key));
        }

        final Collection<K> pKeys0 = pKeys;

        syncOp(new SyncInOp(keys.size() == 1) {
            @Override public void inOp(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                tx.removeAllAsync(ctx, pKeys0 != null ? pKeys0 : keys, null, false, filter).get();
            }

            @Override public String toString() {
                return "removeAll [keys=" + keys + ", filter=" + Arrays.toString(filter) + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> removeAllAsync(@Nullable final Collection<? extends K> keys,
        final IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<Object>(ctx.kernalContext());

        if (keyCheck)
            validateCacheKeys(keys);

        ctx.denyOnLocalRead();

        return asyncOp(new AsyncInOp(keys) {
            @Override public IgniteFuture<?> inOp(GridCacheTxLocalAdapter<K, V> tx) {
                return tx.removeAllAsync(ctx, keys, null, false, filter);
            }

            @Override public String toString() {
                return "removeAllAsync [keys=" + keys + ", filter=" + Arrays.toString(filter) + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean removex(final K key, final IgnitePredicate<GridCacheEntry<K, V>>... filter)
        throws GridException {
        return removex(key, null, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean removex(final K key, @Nullable final GridCacheEntryEx<K, V> entry,
        @Nullable final IgnitePredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        ctx.denyOnLocalRead();

        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        return syncOp(new SyncOp<Boolean>(true) {
            @Override public Boolean op(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                return tx.removeAllAsync(ctx, Collections.singletonList(key), entry, false, filter).get().success();
            }

            @Override public String toString() {
                return "removex [key=" + key + ", filter=" + Arrays.toString(filter) + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removexAsync(K key, IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        return removexAsync(key, null, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removexAsync(final K key, @Nullable final GridCacheEntryEx<K, V> entry,
        @Nullable final IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        ctx.denyOnLocalRead();

        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        return asyncOp(new AsyncOp<Boolean>(key) {
            @Override public IgniteFuture<Boolean> op(GridCacheTxLocalAdapter<K, V> tx) {
                return tx.removeAllAsync(ctx, Collections.singletonList(key), entry, false, filter).chain(
                    (IgniteClosure<IgniteFuture<GridCacheReturn<V>>, Boolean>)RET2FLAG);
            }

            @Override public String toString() {
                return "removeAsync [key=" + key + ", filter=" + Arrays.toString(filter) + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn<V> removex(final K key, final V val) throws GridException {
        ctx.denyOnLocalRead();

        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        return syncOp(new SyncOp<GridCacheReturn<V>>(true) {
            @Override public GridCacheReturn<V> op(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                // Register before hiding in the filter.
                if (ctx.deploymentEnabled())
                    ctx.deploy().registerClass(val);

                return tx.removeAllAsync(ctx, Collections.singletonList(key), null, true,
                    ctx.vararg(F.<K, V>cacheContainsPeek(val))).get();
            }

            @Override public String toString() {
                return "remove [key=" + key + ", val=" + val + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void removeAllDr(final Map<? extends K, GridCacheVersion> drMap) throws GridException {
        ctx.denyOnLocalRead();

        if (F.isEmpty(drMap))
            return;

        ctx.dr().onReceiveCacheEntriesReceived(drMap.size());

        syncOp(new SyncInOp(false) {
            @Override public void inOp(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                tx.removeAllDrAsync(ctx, drMap).get();
            }

            @Override public String toString() {
                return "removeAllDr [drMap=" + drMap + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> removeAllDrAsync(final Map<? extends K, GridCacheVersion> drMap)
        throws GridException {
        ctx.denyOnLocalRead();

        if (F.isEmpty(drMap))
            return new GridFinishedFuture<Object>(ctx.kernalContext());

        ctx.dr().onReceiveCacheEntriesReceived(drMap.size());

        return asyncOp(new AsyncInOp(drMap.keySet()) {
            @Override public IgniteFuture<?> inOp(GridCacheTxLocalAdapter<K, V> tx) {
                return tx.removeAllDrAsync(ctx, drMap);
            }

            @Override public String toString() {
                return "removeAllDrASync [drMap=" + drMap + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridCacheReturn<V> replacex(final K key, final V oldVal, final V newVal) throws GridException {
        A.notNull(key, "key", oldVal, "oldVal", newVal, "newVal");

        if (keyCheck)
            validateCacheKey(key);

        ctx.denyOnLocalRead();

        return syncOp(new SyncOp<GridCacheReturn<V>>(true) {
            @Override public GridCacheReturn<V> op(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                // Register before hiding in the filter.
                if (ctx.deploymentEnabled())
                    ctx.deploy().registerClass(oldVal);

                return tx.putAllAsync(ctx, F.t(key, newVal), true, null, -1, ctx.equalsPeekArray(oldVal)).get();
            }

            @Override public String toString() {
                return "replace [key=" + key + ", oldVal=" + oldVal + ", newVal=" + newVal + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<GridCacheReturn<V>> removexAsync(final K key, final V val) {
        ctx.denyOnLocalRead();

        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        return asyncOp(new AsyncOp<GridCacheReturn<V>>(key) {
            @Override public IgniteFuture<GridCacheReturn<V>> op(GridCacheTxLocalAdapter<K, V> tx) {
                // Register before hiding in the filter.
                try {
                    if (ctx.deploymentEnabled())
                        ctx.deploy().registerClass(val);
                }
                catch (GridException e) {
                    return new GridFinishedFuture<>(ctx.kernalContext(), e);
                }

                return tx.removeAllAsync(ctx, Collections.singletonList(key), null, true,
                    ctx.vararg(F.<K, V>cacheContainsPeek(val)));
            }

            @Override public String toString() {
                return "removeAsync [key=" + key + ", val=" + val + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<GridCacheReturn<V>> replacexAsync(final K key, final V oldVal, final V newVal) {
        A.notNull(key, "key", oldVal, "oldVal", newVal, "newVal");

        if (keyCheck)
            validateCacheKey(key);

        ctx.denyOnLocalRead();

        return asyncOp(new AsyncOp<GridCacheReturn<V>>(key) {
            @Override public IgniteFuture<GridCacheReturn<V>> op(GridCacheTxLocalAdapter<K, V> tx) {
                // Register before hiding in the filter.
                try {
                    if (ctx.deploymentEnabled())
                        ctx.deploy().registerClass(oldVal);
                }
                catch (GridException e) {
                    return new GridFinishedFuture<>(ctx.kernalContext(), e);
                }

                return tx.putAllAsync(ctx, F.t(key, newVal), true, null, -1, ctx.equalsPeekArray(oldVal));
            }

            @Override public String toString() {
                return "replaceAsync [key=" + key + ", oldVal=" + oldVal + ", newVal=" + newVal + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean remove(final K key, final V val) throws GridException {
        ctx.denyOnLocalRead();

        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        return syncOp(new SyncOp<Boolean>(true) {
            @Override public Boolean op(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                // Register before hiding in the filter.
                if (ctx.deploymentEnabled())
                    ctx.deploy().registerClass(val);

                K key0 = key;

                if (ctx.portableEnabled())
                    key0 = (K)ctx.marshalToPortable(key);

                return tx.removeAllAsync(ctx, Collections.singletonList(key0), null, false,
                    ctx.vararg(F.<K, V>cacheContainsPeek(val))).get().success();
            }

            @Override public String toString() {
                return "remove [key=" + key + ", val=" + val + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removeAsync(final K key, final V val) {
        ctx.denyOnLocalRead();

        A.notNull(key, "key", val, "val");

        if (keyCheck)
            validateCacheKey(key);

        validateCacheValue(val);

        return asyncOp(new AsyncOp<Boolean>(key) {
            @Override public IgniteFuture<Boolean> op(GridCacheTxLocalAdapter<K, V> tx) {
                // Register before hiding in the filter.
                if (ctx.deploymentEnabled()) {
                    try {
                        ctx.deploy().registerClass(val);
                    }
                    catch (GridException e) {
                        return new GridFinishedFuture<>(ctx.kernalContext(), e);
                    }
                }

                K key0 = key;

                if (ctx.portableEnabled()) {
                    try {
                        key0 = (K)ctx.marshalToPortable(key);
                    }
                    catch (PortableException e) {
                        return new GridFinishedFuture<>(ctx.kernalContext(), e);
                    }
                }

                return tx.removeAllAsync(ctx, Collections.singletonList(key0), null, false,
                    ctx.vararg(F.<K, V>cacheContainsPeek(val))).chain(
                    (IgniteClosure<IgniteFuture<GridCacheReturn<V>>, Boolean>)RET2FLAG);
            }

            @Override public String toString() {
                return "removeAsync [key=" + key + ", val=" + val + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void removeAll(IgnitePredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        ctx.denyOnLocalRead();

        if (F.isEmptyOrNulls(filter))
            filter = ctx.trueArray();

        final IgnitePredicate<GridCacheEntry<K, V>>[] p = filter;

        syncOp(new SyncInOp(false) {
            @Override public void inOp(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
                tx.removeAllAsync(ctx, keySet(p), null, false, null).get();
            }

            @Override public String toString() {
                return "removeAll [filter=" + Arrays.toString(p) + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> removeAllAsync(final IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        ctx.denyOnLocalRead();

        final Set<? extends K> keys = keySet(filter);

        return asyncOp(new AsyncInOp(keys) {
            @Override public IgniteFuture<?> inOp(GridCacheTxLocalAdapter<K, V> tx) {
                return tx.removeAllAsync(ctx, keys, null, false, null);
            }

            @Override public String toString() {
                return "removeAllAsync [filter=" + Arrays.toString(filter) + ']';
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridCacheMetrics metrics() {
        return GridCacheMetricsAdapter.copyOf(metrics);
    }

    /**
     * @return Metrics.
     */
    public GridCacheMetricsAdapter metrics0() {
        return metrics;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheTx tx() {
        GridCacheTxAdapter<K, V> tx = ctx.tm().threadLocalTx();

        return tx == null ? null : new GridCacheTxProxyImpl<>(tx, ctx.shared());
    }

    /** {@inheritDoc} */
    @Override public boolean lock(K key, long timeout,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        A.notNull(key, "key");

        return lockAll(Collections.singletonList(key), timeout, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean lockAll(@Nullable Collection<? extends K> keys, long timeout,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        if (F.isEmpty(keys))
            return true;

        if (keyCheck)
            validateCacheKeys(keys);

        return lockAllAsync(keys, timeout, filter).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> lockAsync(K key, long timeout,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        return lockAllAsync(Collections.singletonList(key), timeout, filter);
    }

    /** {@inheritDoc} */
    @Override public void unlock(K key, IgnitePredicate<GridCacheEntry<K, V>>... filter)
        throws GridException {
        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        unlockAll(Collections.singletonList(key), filter);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked(K key) {
        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        while (true) {
            try {
                GridCacheEntryEx<K, V> entry = peekEx(key);

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

        GridCacheEntryEx<K, V> entry = peekEx(key);

        return entry != null && entry.wrap(false).isLockedByThread();
    }

    /** {@inheritDoc} */
    @Override public GridCacheTx txStart() throws IllegalStateException {
        GridTransactionsConfiguration cfg = ctx.gridConfig().getTransactionsConfiguration();

        return txStart(cfg.getDefaultTxConcurrency(), cfg.getDefaultTxIsolation());
    }

    /** {@inheritDoc} */
    @Override public GridCacheTx txStart(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation) {
        A.notNull(concurrency, "concurrency");
        A.notNull(isolation, "isolation");

        GridTransactionsConfiguration cfg = ctx.gridConfig().getTransactionsConfiguration();

        return txStart(
            concurrency,
            isolation,
            cfg.getDefaultTxTimeout(),
            0
        );
    }

    /** {@inheritDoc} */
    @Override public GridCacheTx txStart(GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation, long timeout, int txSize) throws IllegalStateException {
        A.notNull(concurrency, "concurrency");
        A.notNull(isolation, "isolation");
        A.ensure(timeout >= 0, "timeout cannot be negative");
        A.ensure(txSize >= 0, "transaction size cannot be negative");

        GridTransactionsConfiguration cfg = ctx.gridConfig().getTransactionsConfiguration();

        if (!cfg.isTxSerializableEnabled() && isolation == SERIALIZABLE)
            throw new IllegalArgumentException("SERIALIZABLE isolation level is disabled (to enable change " +
                "'txSerializableEnabled' configuration property)");

        GridCacheTxEx<K, V> tx = (GridCacheTxEx<K, V>)ctx.tm().userTx();

        if (tx != null)
            throw new IllegalStateException("Failed to start new transaction " +
                "(current thread already has a transaction): " + tx);

        tx = ctx.tm().newTx(
            false,
            false,
            concurrency,
            isolation,
            timeout,
            txSize,
            /** group lock keys */null,
            /** partition lock */false
        );

        assert tx != null;

        // Wrap into proxy.
        return new GridCacheTxProxyImpl<>(tx, ctx.shared());
    }

    /** {@inheritDoc} */
    @Override public GridCacheTx txStartAffinity(Object affinityKey, GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation, long timeout, int txSize) throws IllegalStateException, GridException {
        return txStartGroupLock(ctx.txKey((K)affinityKey), concurrency, isolation, false, timeout, txSize);
    }

    /** {@inheritDoc} */
    @Override public GridCacheTx txStartPartition(int partId, GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation, long timeout, int txSize) throws IllegalStateException, GridException {
        Object grpLockKey = ctx.affinity().partitionAffinityKey(partId);

        return txStartGroupLock(ctx.txKey((K)grpLockKey), concurrency, isolation, true, timeout, txSize);
    }

    /**
     * Internal method to start group-lock transaction.
     *
     * @param grpLockKey Group lock key.
     * @param concurrency Transaction concurrency control.
     * @param isolation Transaction isolation level.
     * @param partLock {@code True} if this is a partition-lock transaction. In this case {@code grpLockKey}
     *      should be a unique partition-specific key.
     * @param timeout Tx timeout.
     * @param txSize Expected transaction size.
     * @return Started transaction.
     * @throws IllegalStateException If other transaction was already started.
     * @throws GridException In case of error.
     */
    @SuppressWarnings("unchecked")
    private GridCacheTx txStartGroupLock(GridCacheTxKey grpLockKey, GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation, boolean partLock, long timeout, int txSize)
        throws IllegalStateException, GridException {
        GridCacheTx tx = ctx.tm().userTx();

        if (tx != null)
            throw new IllegalStateException("Failed to start new transaction " +
                "(current thread already has a transaction): " + tx);

        GridCacheTxLocalEx<K, V> tx0 = ctx.tm().newTx(
            false,
            false,
            concurrency,
            isolation,
            timeout,
            txSize,
            grpLockKey,
            partLock
        );

        assert tx0 != null;

        IgniteFuture<?> lockFut = tx0.groupLockAsync(ctx, (Collection)F.asList(grpLockKey));

        try {
            lockFut.get();
        }
        catch (GridException e) {
            tx0.rollback();

            throw e;
        }

        // Wrap into proxy.
        return new GridCacheTxProxyImpl<>(tx0, ctx.shared());
    }

    /** {@inheritDoc} */
    @Override public long overflowSize() throws GridException {
        return ctx.swap().swapSize();
    }

    /** {@inheritDoc} */
    @Override public ConcurrentMap<K, V> toMap() {
        return new GridCacheMapAdapter<>(this);
    }

    /**
     * Checks if cache is working in JTA transaction and enlist cache as XAResource if necessary.
     *
     * @throws GridException In case of error.
     */
    protected void checkJta() throws GridException {
        ctx.jta().checkJta();
    }

    /** {@inheritDoc} */
    @Override public void txSynchronize(GridCacheTxSynchronization syncs) {
        ctx.tm().addSynchronizations(syncs);
    }

    /** {@inheritDoc} */
    @Override public void txUnsynchronize(GridCacheTxSynchronization syncs) {
        ctx.tm().removeSynchronizations(syncs);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheTxSynchronization> txSynchronizations() {
        return ctx.tm().synchronizations();
    }

    /** {@inheritDoc} */
    @Override public void loadCache(final IgniteBiPredicate<K, V> p, final long ttl, Object[] args) throws GridException {
        final boolean replicate = ctx.isDrEnabled();
        final long topVer = ctx.affinity().affinityTopologyVersion();

        if (ctx.store().isLocalStore()) {
            try (final IgniteDataLoader<K, V> ldr = ctx.kernalContext().<K, V>dataLoad().dataLoader(ctx.namex(), false)) {
                ldr.updater(new GridDrDataLoadCacheUpdater<K, V>());

                final Collection<Map.Entry<K, V>> col = new ArrayList<>(ldr.perNodeBufferSize());

                ctx.store().loadCache(new CIX3<K, V, GridCacheVersion>() {
                    @Override public void applyx(K key, V val, GridCacheVersion ver) throws GridException {
                        assert ver != null;

                        if (p != null && !p.apply(key, val))
                            return;

                        if (ctx.portableEnabled()) {
                            key = (K)ctx.marshalToPortable(key);
                            val = (V)ctx.marshalToPortable(val);
                        }

                        GridRawVersionedEntry<K, V> e = new GridRawVersionedEntry<>(key, null, val, null, ttl, 0, ver);

                        e.marshal(ctx.marshaller());

                        col.add(e);

                        if (col.size() == ldr.perNodeBufferSize()) {
                            ldr.addData(col);

                            col.clear();
                        }
                    }
                }, args);

                if (!col.isEmpty())
                    ldr.addData(col);
            }
        }
        else {
            // Version for all loaded entries.
            final GridCacheVersion ver0 = ctx.versions().nextForLoad();

            ctx.store().loadCache(new CIX3<K, V, GridCacheVersion>() {
                @Override public void applyx(K key, V val, @Nullable GridCacheVersion ver)
                    throws PortableException {
                    assert ver == null;

                    if (p != null && !p.apply(key, val))
                        return;

                    if (ctx.portableEnabled()) {
                        key = (K)ctx.marshalToPortable(key);
                        val = (V)ctx.marshalToPortable(val);
                    }

                    GridCacheEntryEx<K, V> entry = entryEx(key, false);

                    try {
                        entry.initialValue(val, null, ver0, ttl, -1, false, topVer, replicate ? DR_LOAD : DR_NONE);
                    }
                    catch (GridException e) {
                        throw new GridRuntimeException("Failed to put cache value: " + entry, e);
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
            }, args);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> loadCacheAsync(final IgniteBiPredicate<K, V> p, final long ttl, final Object[] args) {
        return ctx.closures().callLocalSafe(
            ctx.projectSafe(new Callable<Object>() {
                @Nullable
                @Override public Object call() throws GridException {
                    loadCache(p, ttl, args);

                    return null;
                }
            }), true);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheEntry<K, V> randomEntry() {
        GridCacheMapEntry<K, V> e = map.randomEntry();

        return e == null || e.obsolete() ? null : e.wrap(true);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return map.publicSize();
    }

    /** {@inheritDoc} */
    @Override public int globalSize() throws GridException {
        return globalSize(false);
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
    @Override public int globalPrimarySize() throws GridException {
        return globalSize(true);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAdapter.class, this, "name", name(), "size", size());
    }

    /** {@inheritDoc} */
    @Override public Iterator<GridCacheEntry<K, V>> iterator() {
        return entrySet().iterator();
    }

    /** {@inheritDoc} */
    @Nullable @Override public V promote(K key) throws GridException {
        return promote(key, true);
    }

    /**
     * @param key Key.
     * @param deserializePortable Deserialize portable flag.
     * @return Value.
     * @throws GridException If failed.
     */
    @SuppressWarnings("IfMayBeConditional")
    @Nullable public V promote(K key, boolean deserializePortable) throws GridException {
        ctx.denyOnFlags(F.asList(READ, SKIP_SWAP));

        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        GridCacheSwapEntry<V> unswapped = ctx.swap().readAndRemove(key);

        if (unswapped == null)
            return null;

        GridCacheEntryEx<K, V> entry = entryEx(key);

        try {
            if (!entry.initialValue(key, unswapped))
                return null;
        }
        catch (GridCacheEntryRemovedException ignored) {
            if (log.isDebugEnabled())
                log.debug("Entry has been concurrently removed.");

            return null;
        }

        V val = unswapped.value();

        if (ctx.portableEnabled() && deserializePortable && val instanceof PortableObject)
            return (V)((PortableObject)val).deserialize();
        else
            return ctx.cloneOnFlag(val);
    }

    /** {@inheritDoc} */
    @Override public void promoteAll(@Nullable Collection<? extends K> keys) throws GridException {
        ctx.denyOnFlags(F.asList(READ, SKIP_SWAP));

        if (F.isEmpty(keys))
            return;

        if (keyCheck)
            validateCacheKeys(keys);

        Collection<K> unswap = new ArrayList<>(keys.size());

        for (K key : keys) {
            // Do not look up in swap for existing entries.
            GridCacheEntryEx<K, V> entry = peekEx(key);

            try {
                if (entry == null || entry.obsolete() || entry.isNewLocked())
                    unswap.add(key);
            }
            catch (GridCacheEntryRemovedException ignored) {
                // No-op.
            }
        }

        Collection<GridCacheBatchSwapEntry<K, V>> swapped = ctx.swap().readAndRemove(unswap);

        for (GridCacheBatchSwapEntry<K, V> swapEntry : swapped) {
            K key = swapEntry.key();

            GridCacheEntryEx<K, V> entry = entryEx(key);

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
    @Override public Iterator<Map.Entry<K, V>> swapIterator() throws GridException {
        ctx.denyOnFlags(F.asList(SKIP_SWAP));

        return ctx.swap().lazySwapIterator();
    }

    /** {@inheritDoc} */
    @Override public Iterator<Map.Entry<K, V>> offHeapIterator() throws GridException {
        return ctx.swap().lazyOffHeapIterator();
    }

    /** {@inheritDoc} */
    @Override public long offHeapEntriesCount() {
        return ctx.swap().offHeapEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long offHeapAllocatedSize() {
        return ctx.swap().offHeapAllocatedSize();
    }

    /** {@inheritDoc} */
    @Override public long swapSize() throws GridException {
        return ctx.swap().swapSize();
    }

    /** {@inheritDoc} */
    @Override public long swapKeys() throws GridException {
        return ctx.swap().swapKeys();
    }

    /**
     * Asynchronously commits transaction after all previous asynchronous operations are completed.
     *
     * @param tx Transaction to commit.
     * @return Transaction commit future.
     */
    @SuppressWarnings("unchecked")
    public IgniteFuture<GridCacheTx> commitTxAsync(final GridCacheTx tx) {
        FutureHolder holder = lastFut.get();

        holder.lock();

        try {
            IgniteFuture fut = holder.future();

            if (fut != null && !fut.isDone()) {
                IgniteFuture<GridCacheTx> f = new GridEmbeddedFuture<>(fut,
                    new C2<Object, Exception, IgniteFuture<GridCacheTx>>() {
                        @Override public IgniteFuture<GridCacheTx> apply(Object o, Exception e) {
                            return tx.commitAsync();
                        }
                    }, ctx.kernalContext());

                saveFuture(holder, f);

                return f;
            }

            IgniteFuture<GridCacheTx> f = tx.commitAsync();

            saveFuture(holder, f);

            ctx.tm().txContextReset();

            return f;
        }
        finally {
            holder.unlock();
        }
    }

    /**
     * Synchronously commits transaction after all previous asynchronous operations are completed.
     *
     * @param tx Transaction to commit.
     * @throws GridException If commit failed.
     */
    void commitTx(GridCacheTx tx) throws GridException {
        awaitLastFut();

        tx.commit();
    }

    /**
     * Synchronously rolls back transaction after all previous asynchronous operations are completed.
     *
     * @param tx Transaction to commit.
     * @throws GridException If commit failed.
     */
    void rollbackTx(GridCacheTx tx) throws GridException {
        awaitLastFut();

        tx.rollback();
    }

    /**
     * Synchronously ends transaction after all previous asynchronous operations are completed.
     *
     * @param tx Transaction to commit.
     * @throws GridException If commit failed.
     */
    void endTx(GridCacheTx tx) throws GridException {
        awaitLastFut();

        tx.close();
    }

    /**
     * Awaits for previous async operation to be completed.
     */
    @SuppressWarnings("unchecked")
    public void awaitLastFut() {
        FutureHolder holder = lastFut.get();

        IgniteFuture fut = holder.future();

        if (fut != null && !fut.isDone()) {
            try {
                // Ignore any exception from previous async operation as it should be handled by user.
                fut.get();
            }
            catch (GridException ignored) {
                // No-op.
            }
        }
    }

    /**
     * Gets cache global size (with or without backups).
     *
     * @param primaryOnly {@code True} if only primary sizes should be included.
     * @return Global size.
     * @throws GridException If internal task execution failed.
     */
    private int globalSize(boolean primaryOnly) throws GridException {
        try {
            // Send job to remote nodes only.
            Collection<ClusterNode> nodes = ctx.grid().forCache(name()).forRemotes().nodes();

            IgniteFuture<Collection<Integer>> fut = null;

            if (!nodes.isEmpty()) {
                ctx.kernalContext().task().setThreadContext(TC_TIMEOUT, gridCfg.getNetworkTimeout());

                fut = ctx.closures().broadcastNoFailover(new GlobalSizeCallable(name(), primaryOnly), null, nodes);
            }

            // Get local value.
            int globalSize = primaryOnly ? primarySize() : size();

            if (fut != null) {
                for (Integer i : fut.get())
                    globalSize += i;
            }

            return globalSize;
        }
        catch (GridEmptyProjectionException ignore) {
            if (log.isDebugEnabled())
                log.debug("All remote nodes left while cache clear [cacheName=" + name() + "]");

            return primaryOnly ? primarySize() : size();
        }
        catch (ComputeTaskTimeoutException e) {
            U.warn(log, "Timed out waiting for remote nodes to finish cache clear (consider increasing " +
                "'networkTimeout' configuration property) [cacheName=" + name() + "]");

            throw e;
        }
    }

    /**
     * @param op Cache operation.
     * @param <T> Return type.
     * @return Operation result.
     * @throws GridException If operation failed.
     */
    @SuppressWarnings({"TypeMayBeWeakened", "ErrorNotRethrown", "AssignmentToCatchBlockParameter"})
    @Nullable private <T> T syncOp(SyncOp<T> op) throws GridException {
        checkJta();

        awaitLastFut();

        GridCacheTxLocalAdapter<K, V> tx = ctx.tm().threadLocalTx();

        if (tx == null || tx.implicit()) {
            GridTransactionsConfiguration tCfg = ctx.gridConfig().getTransactionsConfiguration();

            tx = ctx.tm().newTx(
                true,
                op.single(),
                PESSIMISTIC,
                READ_COMMITTED,
                tCfg.getDefaultTxTimeout(),
                0,
                /** group lock keys */null,
                /** partition lock */false
            );

            assert tx != null;

            try {
                T t = op.op(tx);

                assert tx.done() : "Transaction is not done: " + tx;

                return t;
            }
            catch (GridInterruptedException | GridCacheTxHeuristicException | GridCacheTxRollbackException e) {
                throw e;
            }
            catch (GridException e) {
                try {
                    tx.rollback();

                    e = new GridCacheTxRollbackException("Transaction has been rolled back: " +
                        tx.xid(), e);
                }
                catch (GridException | AssertionError | RuntimeException e1) {
                    U.error(log, "Failed to rollback transaction (cache may contain stale locks): " + tx, e1);

                    U.addLastCause(e, e1, log);
                }

                throw e;
            }
            finally {
                ctx.tm().txContextReset();

                if (ctx.isNear())
                    ctx.near().dht().context().tm().txContextReset();
            }
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
    private <T> IgniteFuture<T> asyncOp(final AsyncOp<T> op) {
        try {
            checkJta();
        }
        catch (GridException e) {
            return new GridFinishedFuture<>(ctx.kernalContext(), e);
        }

        if (log.isDebugEnabled())
            log.debug("Performing async op: " + op);

        GridCacheTxLocalAdapter<K, V> tx = ctx.tm().threadLocalTx();

        if (tx == null || tx.implicit())
            tx = ctx.tm().newTx(
                true,
                op.single(),
                PESSIMISTIC,
                READ_COMMITTED,
                ctx.kernalContext().config().getTransactionsConfiguration().getDefaultTxTimeout(),
                0,
                null,
                false);

        return asyncOp(tx, op);
    }

    /**
     * @param tx Transaction.
     * @param op Cache operation.
     * @param <T> Return type.
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    protected <T> IgniteFuture<T> asyncOp(GridCacheTxLocalAdapter<K, V> tx, final AsyncOp<T> op) {
        IgniteFuture<T> fail = asyncOpAcquire();

        if (fail != null)
            return fail;

        FutureHolder holder = lastFut.get();

        holder.lock();

        try {
            IgniteFuture fut = holder.future();

            if (fut != null && !fut.isDone()) {
                final GridCacheTxLocalAdapter<K, V> tx0 = tx;

                IgniteFuture<T> f = new GridEmbeddedFuture<>(fut,
                    new C2<T, Exception, IgniteFuture<T>>() {
                        @Override public IgniteFuture<T> apply(T t, Exception e) {
                            return op.op(tx0);
                        }
                    }, ctx.kernalContext());

                saveFuture(holder, f);

                return f;
            }

            IgniteFuture<T> f = op.op(tx);

            saveFuture(holder, f);

            if (tx.implicit())
                ctx.tm().txContextReset();

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
    protected void saveFuture(final FutureHolder holder, IgniteFuture<?> fut) {
        assert holder != null;
        assert fut != null;
        assert holder.holdsLock();

        holder.future(fut);

        if (fut.isDone()) {
            holder.future(null);

            asyncOpRelease();
        }
        else {
            fut.listenAsync(new CI1<IgniteFuture<?>>() {
                @Override public void apply(IgniteFuture<?> f) {
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
    @Nullable protected <T> IgniteFuture<T> asyncOpAcquire() {
        try {
            if (asyncOpsSem != null)
                asyncOpsSem.acquire();

            return null;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            return new GridFinishedFutureEx<>(new GridInterruptedException("Failed to wait for asynchronous " +
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

            return GridGainEx.gridx(t.get1()).cachex(t.get2());
        }
        catch (IllegalStateException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        metrics = new GridCacheMetricsAdapter();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> forceRepartition() {
        ctx.preloader().forcePreload();

        return ctx.preloader().syncFuture();
    }

    /** {@inheritDoc} */
    @Override public boolean isGgfsDataCache() {
        return ggfsDataCache;
    }

    /** {@inheritDoc} */
    @Override public long ggfsDataSpaceUsed() {
        assert ggfsDataCache;

        return ggfsDataCacheSize.longValue();
    }

    /** {@inheritDoc} */
    @Override public long ggfsDataSpaceMax() {
        return ggfsDataSpaceMax;
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
     * Callback invoked when data is added to GGFS cache.
     *
     * @param delta Size delta.
     */
    public void onGgfsDataSizeChanged(long delta) {
        assert ggfsDataCache;

        ggfsDataCacheSize.add(delta);
    }

    /**
     * @param keys Keys.
     * @param filter Filters to evaluate.
     */
    public void clearAll0(Collection<? extends K> keys,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        ctx.denyOnFlag(READ);
        ctx.checkSecurity(GridSecurityPermission.CACHE_REMOVE);

        if (F.isEmpty(keys))
            return;

        if (keyCheck)
            validateCacheKeys(keys);

        GridCacheVersion obsoleteVer = ctx.versions().next();

        for (K k : keys)
            clear(obsoleteVer, k, filter);
    }

    /**
     * @param key Key.
     * @param filter Filters to evaluate.
     * @return {@code True} if cleared.
     */
    public boolean clear0(K key, @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        ctx.denyOnFlag(READ);
        ctx.checkSecurity(GridSecurityPermission.CACHE_REMOVE);

        return clear(ctx.versions().next(), key, filter);
    }

    /**
     * @param key Key.
     * @param filter Filters to evaluate.
     * @return {@code True} if compacted.
     * @throws GridException If failed.
     */
    public boolean compact(K key, @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter)
        throws GridException {
        ctx.denyOnFlag(READ);

        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        GridCacheEntryEx<K, V> entry = peekEx(key);

        try {
            if (entry != null && entry.compact(filter)) {
                removeIfObsolete(key);

                return true;
            }
        }
        catch (GridCacheEntryRemovedException ignored) {
            if (log().isDebugEnabled())
                log().debug("Got removed entry in invalidate(...): " + key);
        }

        return false;
    }

    /**
     * @param key Key.
     * @param filter Filters to evaluate.
     * @return {@code True} if evicted.
     */
    public boolean evict(K key, @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        ctx.denyOnFlag(READ);

        return evictx(key, ctx.versions().next(), filter);
    }

    /**
     * @param keys Keys.
     * @param filter Filters to evaluate.
     */
    public void evictAll(Collection<? extends K> keys,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        ctx.denyOnFlag(READ);

        if (F.isEmpty(keys))
            return;

        if (keyCheck)
            validateCacheKey(keys);

        GridCacheVersion obsoleteVer = ctx.versions().next();

        if (!ctx.evicts().evictSyncOrNearSync() && F.isEmptyOrNulls(filter) && ctx.isSwapOrOffheapEnabled()) {
            try {
                ctx.evicts().batchEvict(keys, obsoleteVer);
            }
            catch (GridException e) {
                U.error(log, "Failed to perform batch evict for keys: " + keys, e);
            }
        }
        else {
            for (K k : keys)
                evictx(k, obsoleteVer, filter);
        }
    }

    /**
     * @param key Key.
     * @param filter Filters to evaluate.
     * @return {@code True} if contains key.
     */
    public boolean containsKey(K key, @Nullable IgnitePredicate<GridCacheEntry<K, V>> filter) {
        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        if (ctx.portableEnabled()) {
            try {
                key = (K)ctx.marshalToPortable(key);
            }
            catch (PortableException e) {
                throw new GridRuntimeException(e);
            }
        }

        GridCacheEntryEx<K, V> e = peekEx(key);

        try {
            return e != null && e.peek(SMART, filter) != null;
        }
        catch (GridCacheEntryRemovedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Got removed entry during peek (will ignore): " + e);

            return false;
        }
    }

    /**
     * @param keys Keys.
     * @param filter Filter to evaluate.
     * @return {@code True} if contains all keys.
     */
    public boolean containsAllKeys(@Nullable Collection<? extends K> keys,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>> filter) {
        if (F.isEmpty(keys))
            return true;

        if (keyCheck)
            validateCacheKeys(keys);

        for (K k : keys)
            if (!containsKey(k, filter))
                return false;

        return true;
    }

    /**
     * @param keys Keys.
     * @param filter Filter to evaluate.
     * @return {@code True} if cache contains any of given keys.
     */
    public boolean containsAnyKeys(@Nullable Collection<? extends K> keys,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>> filter) {
        if (F.isEmpty(keys))
            return true;

        if (keyCheck)
            validateCacheKeys(keys);

        for (K k : keys) {
            if (containsKey(k, filter))
                return true;
        }

        return false;
    }

    /**
     * @param val Value.
     * @param filter Filter to evaluate.
     * @return {@code True} if contains value.
     */
    public boolean containsValue(V val, @Nullable IgnitePredicate<GridCacheEntry<K, V>> filter) {
        A.notNull(val, "val");

        validateCacheValue(val);

        return values(filter).contains(val);
    }

    /**
     * @param vals Values.
     * @param filter Filter to evaluate.
     * @return {@code True} if contains all given values.
     */
    public boolean containsAllValues(@Nullable Collection<? extends V> vals,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>> filter) {
        if (F.isEmpty(vals))
            return true;

        validateCacheValues(vals);

        return values(filter).containsAll(vals);
    }

    /**
     * @param vals Values.
     * @param filter Filter to evaluate.
     * @return {@code True} if contains any of given values.
     */
    public boolean containsAnyValues(@Nullable Collection<? extends V> vals,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>> filter) {
        if (F.isEmpty(vals))
            return true;

        validateCacheValues(vals);

        return !values(F.and(filter, F.<K, V>cacheContainsPeek(vals))).isEmpty();
    }

    /**
     * @param key Key.
     * @param filter Filter to evaluate.
     * @return Peeked value.
     */
    public V peek(K key, @Nullable IgnitePredicate<GridCacheEntry<K, V>> filter) {
        try {
            GridTuple<V> peek = peek0(false, key, SMART, filter);

            return peek != null ? peek.get() : null;
        }
        catch (GridCacheFilterFailedException e) {
            e.printStackTrace();

            assert false; // Should never happen.

            return null;
        }
    }

    /**
     * @param filter Filters to evaluate.
     * @return Entry set.
     */
    public Set<GridCacheEntry<K, V>> entrySet(@Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        return map.entries(filter);
    }

    /**
     * @param keys Keys.
     * @param keyFilter Key filter.
     * @param filter Entry filter.
     * @return Entry set.
     */
    public Set<GridCacheEntry<K, V>> entrySet(@Nullable Collection<? extends K> keys,
        @Nullable IgnitePredicate<K> keyFilter, @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        if (F.isEmpty(keys))
            return emptySet();

        if (keyCheck)
            validateCacheKeys(keys);

        return new GridCacheEntrySet<>(ctx, F.viewReadOnly(keys, CU.cacheKey2Entry(ctx), keyFilter), filter);
    }

    /**
     * @param filter Filters to evaluate.
     * @return Primary entry set.
     */
    public Set<GridCacheEntry<K, V>> primaryEntrySet(
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        return map.entries(F.and(filter, F.<K, V>cachePrimary()));
    }

    /**
     * @param filter Filters to evaluate.
     * @return Key set.
     */
    public Set<K> keySet(@Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        return map.keySet(filter);
    }

    /**
     * @param filter Primary key set.
     * @return Primary key set.
     */
    public Set<K> primaryKeySet(@Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        return map.keySet(F.and(filter, F.<K, V>cachePrimary()));
    }

    /**
     * @param filter Filters to evaluate.
     * @return Primary values.
     */
    public Collection<V> primaryValues(@Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        return map.values(F.and(filter, F.<K, V>cachePrimary()));
    }

    /**
     * @param keys Keys.
     * @param filter Filters to evaluate.
     * @throws GridException If failed.
     */
    public void compactAll(@Nullable Iterable<K> keys,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        ctx.denyOnFlag(READ);

        if (keys != null) {
            for (K key : keys)
                compact(key, filter);
        }
    }

    /**
     * @param key Key.
     * @param filter Filter to evaluate.
     * @return Cached value.
     * @throws GridException If failed.
     */
    @Nullable public V get(K key, boolean deserializePortable, @Nullable IgnitePredicate<GridCacheEntry<K, V>> filter)
        throws GridException {
        return getAllAsync(F.asList(key), deserializePortable, filter).get().get(key);
    }

    /**
     * @param key Key.
     * @param filter Filter to evaluate.
     * @return Read operation future.
     */
    public final IgniteFuture<V> getAsync(final K key, boolean deserializePortable,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>> filter) {
        ctx.denyOnFlag(LOCAL);

        try {
            checkJta();
        }
        catch (GridException e) {
            return new GridFinishedFuture<>(ctx.kernalContext(), e);
        }

        return getAllAsync(Collections.singletonList(key), deserializePortable, filter).chain(new CX1<IgniteFuture<Map<K, V>>, V>() {
            @Override
            public V applyx(IgniteFuture<Map<K, V>> e) throws GridException {
                return e.get().get(key);
            }
        });
    }

    /**
     * @param keys Keys.
     * @param filter Filter to evaluate.
     * @return Map of cached values.
     * @throws GridException If read failed.
     */
    public Map<K, V> getAll(Collection<? extends K> keys, boolean deserializePortable,
        IgnitePredicate<GridCacheEntry<K, V>> filter) throws GridException {
        ctx.denyOnFlag(LOCAL);

        checkJta();

        return getAllAsync(keys, deserializePortable, filter).get();
    }

    /**
     * @param key Key.
     * @param filter Filter to evaluate.
     * @return Reloaded value.
     * @throws GridException If failed.
     */
    @Nullable public V reload(K key, @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter)
        throws GridException {
        ctx.denyOnFlags(F.asList(LOCAL, READ));

        A.notNull(key, "key");

        if (keyCheck)
            validateCacheKey(key);

        long topVer = ctx.affinity().affinityTopologyVersion();

        if (ctx.portableEnabled())
            key = (K)ctx.marshalToPortable(key);

        while (true) {
            try {
                // Do not reload near entries, they will be reloaded in DHT cache.
                if (ctx.isNear() && ctx.affinity().localNode(key, topVer))
                    return null;

                return ctx.cloneOnFlag(entryEx(key).innerReload(filter));
            }
            catch (GridCacheEntryRemovedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Attempted to reload a removed entry for key (will retry): " + key);
            }
        }
    }

    /**
     * @param keys Keys.
     * @param filter Filter to evaluate.
     * @throws GridException If failed.
     */
    public void reloadAll(@Nullable Collection<? extends K> keys,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        reloadAll(keys, false, filter);
    }

    /**
     * @param keys Keys.
     * @param filter Filter to evaluate.
     * @return Reload future.
     */
    public IgniteFuture<?> reloadAllAsync(@Nullable Collection<? extends K> keys,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        UUID subjId = ctx.subjectIdPerCall(null);

        String taskName = ctx.kernalContext().job().currentTaskName();

        return reloadAllAsync(keys, false, subjId, taskName, filter);
    }

    /**
     * @param key Key.
     * @param filter Filter to evaluate.
     * @return Reload future.
     */
    public IgniteFuture<V> reloadAsync(final K key,
        @Nullable final IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        ctx.denyOnFlags(F.asList(LOCAL, READ));

        return ctx.closures().callLocalSafe(ctx.projectSafe(new Callable<V>() {
            @Nullable @Override public V call() throws GridException {
                return reload(key, filter);
            }
        }), true);
    }

    /**
     * @param filter Filter to evaluate.
     * @throws GridException If reload failed.
     */
    public final void reloadAll(@Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws GridException {
        ctx.denyOnFlag(READ);

        Set<K> keys = keySet();

        // Don't reload empty cache.
        if (!keys.isEmpty())
            reloadAll(keys, filter);
    }

    /**
     * @param filter Filter to evaluate.
     * @return Reload future.
     */
    public IgniteFuture<?> reloadAllAsync(@Nullable final IgnitePredicate<GridCacheEntry<K, V>> filter) {
        ctx.denyOnFlag(READ);

        return ctx.closures().callLocalSafe(ctx.projectSafe(new GPC() {
            @Nullable @Override public Object call() throws GridException {
                reloadAll(filter);

                return null;
            }
        }), true);
    }

    /**
     * @param keys Keys.
     * @param filter Filter to evaluate.
     * @return Read future.
     */
    public IgniteFuture<Map<K, V>> getAllAsync(@Nullable Collection<? extends K> keys,
        boolean deserializePortable, @Nullable IgnitePredicate<GridCacheEntry<K, V>> filter) {
        String taskName = ctx.kernalContext().job().currentTaskName();

        return getAllAsync(keys, ctx.hasFlag(GET_PRIMARY), /*skip tx*/false, null, null, taskName,
            deserializePortable, filter);
    }

    /**
     * @param entry Entry.
     * @param ver Version.
     */
    public abstract void onDeferredDelete(GridCacheEntryEx<K, V> entry, GridCacheVersion ver);

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
    private void validateCacheKey(Object key) {
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
     * @throws GridRuntimeException If validation fails.
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
     * @param nodeId Node ID.
     * @param req Request.
     */
    protected void processCheckPreparedTxRequest(UUID nodeId, GridCacheOptimisticCheckPreparedTxRequest<K, V> req) {
        if (log.isDebugEnabled())
            log.debug("Processing check prepared transaction requests [nodeId=" + nodeId + ", req=" + req + ']');

        boolean prepared = ctx.tm().txsPreparedOrCommitted(req.nearXidVersion(), req.transactions());

        GridCacheOptimisticCheckPreparedTxResponse<K, V> res =
            new GridCacheOptimisticCheckPreparedTxResponse<>(req.version(), req.futureId(), req.miniId(), prepared);

        try {
            if (log.isDebugEnabled())
                log.debug("Sending check prepared transaction response [nodeId=" + nodeId + ", res=" + res + ']');

            ctx.io().send(nodeId, res);
        }
        catch (GridTopologyException ignored) {
            if (log.isDebugEnabled())
                log.debug("Failed to send check prepared transaction response (did node leave grid?) [nodeId=" +
                    nodeId + ", res=" + res + ']');
        }
        catch (GridException e) {
            U.error(log, "Failed to send response to node [nodeId=" + nodeId + ", res=" + res + ']', e);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    protected void processCheckPreparedTxResponse(UUID nodeId, GridCacheOptimisticCheckPreparedTxResponse<K, V> res) {
        if (log.isDebugEnabled())
            log.debug("Processing check prepared transaction response [nodeId=" + nodeId + ", res=" + res + ']');

        GridCacheOptimisticCheckPreparedTxFuture<K, V> fut = (GridCacheOptimisticCheckPreparedTxFuture<K, V>)ctx.mvcc().
            <Boolean>future(res.version(), res.futureId());

        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Received response for unknown future (will ignore): " + res);

            return;
        }

        fut.onResult(nodeId, res);
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    protected void processCheckCommittedTxRequest(final UUID nodeId,
        final GridCachePessimisticCheckCommittedTxRequest<K, V> req) {
        if (log.isDebugEnabled())
            log.debug("Processing check committed transaction request [nodeId=" + nodeId + ", req=" + req + ']');

        // First check if we have near transaction with this ID.
        GridCacheTxEx<K, V> tx = ctx.tm().localTxForRecovery(req.nearXidVersion(), !req.nearOnlyCheck());

        // Either we found near transaction or one of transactions is being committed by user.
        // Wait for it and send reply.
        if (tx != null) {
            assert tx.local();

            if (log.isDebugEnabled())
                log.debug("Found active near transaction, will wait for completion [req=" + req + ", tx=" + tx + ']');

            final GridCacheTxEx<K, V> tx0 = tx;

            tx.finishFuture().listenAsync(new CI1<IgniteFuture<GridCacheTx>>() {
                @Override public void apply(IgniteFuture<GridCacheTx> f) {
                    GridCacheCommittedTxInfo<K, V> info = null;

                    if (tx0.state() == COMMITTED)
                        info = new GridCacheCommittedTxInfo<>(tx0);

                    GridCachePessimisticCheckCommittedTxResponse<K, V>
                        res = new GridCachePessimisticCheckCommittedTxResponse<>(
                        req.version(), req.futureId(), req.miniId(), info);

                    if (log.isDebugEnabled())
                        log.debug("Finished near transaction, sending response [req=" + req + ", res=" + res + ']');

                    sendCheckCommittedResponse(nodeId, res);
                }
            });

            return;
        }

        GridCacheCommittedTxInfo<K, V> info = ctx.tm().txCommitted(req.nearXidVersion(), req.originatingNodeId(),
            req.originatingThreadId());

        if (info == null && CU.isNearEnabled(ctx))
            info = ctx.dht().near().context().tm().txCommitted(req.nearXidVersion(), req.originatingNodeId(),
                req.originatingThreadId());

        GridCachePessimisticCheckCommittedTxResponse<K, V> res = new GridCachePessimisticCheckCommittedTxResponse<>(
            req.version(), req.futureId(), req.miniId(), info);

        sendCheckCommittedResponse(nodeId, res);
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    protected void processCheckCommittedTxResponse(UUID nodeId,
        GridCachePessimisticCheckCommittedTxResponse<K, V> res) {
        if (log.isDebugEnabled())
            log.debug("Processing check committed transaction response [nodeId=" + nodeId + ", res=" + res + ']');

        GridCachePessimisticCheckCommittedTxFuture<K, V> fut =
            (GridCachePessimisticCheckCommittedTxFuture<K, V>)ctx.mvcc().<GridCacheCommittedTxInfo<K, V>>future(
                res.version(), res.futureId());

        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Received response for unknown future (will ignore): " + res);

            return;
        }

        fut.onResult(nodeId, res);
    }

    /**
     * Sends check committed response to remote node.
     *
     * @param nodeId Node ID to send to.
     * @param res Reponse to send.
     */
    private void sendCheckCommittedResponse(UUID nodeId, GridCachePessimisticCheckCommittedTxResponse<K, V> res) {
        try {
            if (log.isDebugEnabled())
                log.debug("Sending check committed transaction response [nodeId=" + nodeId + ", res=" + res + ']');

            ctx.io().send(nodeId, res);
        }
        catch (GridTopologyException ignored) {
            if (log.isDebugEnabled())
                log.debug("Failed to send check committed transaction response (did node leave grid?) [nodeId=" +
                    nodeId + ", res=" + res + ']');
        }
        catch (GridException e) {
            U.error(log, "Failed to send response to node [nodeId=" + nodeId + ", res=" + res + ']', e);
        }
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
         * @throws GridException If failed.
         */
        @Nullable public abstract T op(GridCacheTxLocalAdapter<K, V> tx) throws GridException;
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
        @Nullable @Override public final Object op(GridCacheTxLocalAdapter<K, V> tx) throws GridException {
            inOp(tx);

            return null;
        }

        /**
         * @param tx Transaction.
         * @throws GridException If failed.
         */
        public abstract void inOp(GridCacheTxLocalAdapter<K, V> tx) throws GridException;
    }

    /**
     * Cache operation.
     */
    protected abstract class AsyncOp<T> {
        /** Flag to indicate only-one-key operation. */
        private final boolean single;

        /** Keys. */
        private final Collection<? extends K> keys;

        /**
         * @param key Key.
         */
        protected AsyncOp(K key) {
            keys = Arrays.asList(key);

            single = true;
        }

        /**
         * @param keys Keys involved.
         */
        protected AsyncOp(Collection<? extends K> keys) {
            this.keys = keys;

            single = keys.size() == 1;
        }

        /**
         * @return Flag to indicate only-one-key operation.
         */
        final boolean single() {
            return single;
        }

        /**
         * @return Keys.
         */
        Collection<? extends K> keys() {
            return keys;
        }

        /**
         * @param tx Transaction.
         * @return Operation return value.
         */
        public abstract IgniteFuture<T> op(GridCacheTxLocalAdapter<K, V> tx);
    }

    /**
     * Cache operation.
     */
    private abstract class AsyncInOp extends AsyncOp<Object> {
        /**
         * @param key Key.
         */
        protected AsyncInOp(K key) {
            super(key);
        }

        /**
         * @param keys Keys involved.
         */
        protected AsyncInOp(Collection<? extends K> keys) {
            super(keys);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public final IgniteFuture<Object> op(GridCacheTxLocalAdapter<K, V> tx) {
            return (IgniteFuture<Object>)inOp(tx);
        }

        /**
         * @param tx Transaction.
         * @return Operation return value.
         */
        public abstract IgniteFuture<?> inOp(GridCacheTxLocalAdapter<K, V> tx);
    }

    /**
     * Internal callable which performs {@link GridCacheProjection#clearAll()}
     * operation on a cache with the given name.
     */
    @GridInternal
    private static class GlobalClearAllCallable implements Callable<Object>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Cache name. */
        private String cacheName;

        /** Injected grid instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * Empty constructor for serialization.
         */
        public GlobalClearAllCallable() {
            // No-op.
        }

        /**
         * @param cacheName Cache name.
         */
        private GlobalClearAllCallable(String cacheName) {
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            ((GridEx) ignite).cachex(cacheName).clearAll();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, cacheName);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            cacheName = U.readString(in);
        }
    }

    /**
     * Internal callable which performs {@link GridCacheProjection#size()} or {@link GridCacheProjection#primarySize()}
     * operation on a cache with the given name.
     */
    @GridInternal
    private static class GlobalSizeCallable implements IgniteClosure<Object, Integer>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Cache name. */
        private String cacheName;

        /** Primary only flag. */
        private boolean primaryOnly;

        /** Injected grid instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * Empty constructor for serialization.
         */
        public GlobalSizeCallable() {
            // No-op.
        }

        /**
         * @param cacheName Cache name.
         * @param primaryOnly Primary only flag.
         */
        private GlobalSizeCallable(String cacheName, boolean primaryOnly) {
            this.cacheName = cacheName;
            this.primaryOnly = primaryOnly;
        }

        /** {@inheritDoc} */
        @Override public Integer apply(Object o) {
            GridCache<Object, Object> cache = ((GridEx) ignite).cachex(cacheName);

            return primaryOnly ? cache.primarySize() : cache.size();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, cacheName);
            out.writeBoolean(primaryOnly);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            cacheName = U.readString(in);
            primaryOnly = in.readBoolean();
        }
    }

    /**
     * Holder for last async operation future.
     */
    protected static class FutureHolder {
        /** Lock. */
        private final ReentrantLock lock = new ReentrantLock();

        /** Future. */
        private IgniteFuture fut;

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
        public IgniteFuture future() {
            return fut;
        }

        /**
         * Sets future.
         *
         * @param fut Future.
         */
        public void future(@Nullable IgniteFuture fut) {
            this.fut = fut;
        }
    }
}

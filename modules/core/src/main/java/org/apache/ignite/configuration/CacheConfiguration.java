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

package org.apache.ignite.configuration;

import java.io.Serializable;
import java.util.Collection;
import javax.cache.Cache;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cache.eviction.EvictionFilter;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.CachePluginConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * This class defines grid cache configuration. This configuration is passed to
 * grid via {@link IgniteConfiguration#getCacheConfiguration()} method. It defines all configuration
 * parameters required to start a cache within grid instance. You can have multiple caches
 * configured with different names within one grid.
 * <p>
 * Cache configuration is set on {@link
 * IgniteConfiguration#setCacheConfiguration(CacheConfiguration...)} method. This adapter is a simple bean and
 * can be configured from Spring XML files (or other DI frameworks). <p> Note that absolutely all configuration
 * properties are optional, so users should only change what they need.
 */
@SuppressWarnings("RedundantFieldInitialization")
public class CacheConfiguration<K, V> extends MutableConfiguration<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default size of rebalance thread pool. */
    public static final int DFLT_REBALANCE_THREAD_POOL_SIZE = 2;

    /** Default rebalance timeout (ms).*/
    public static final long DFLT_REBALANCE_TIMEOUT = 10000;

    /** Time in milliseconds to wait between rebalance messages to avoid overloading CPU. */
    public static final long DFLT_REBALANCE_THROTTLE = 0;

    /** Default number of backups. */
    public static final int DFLT_BACKUPS = 0;

    /** Default caching mode. */
    public static final CacheMode DFLT_CACHE_MODE = CacheMode.PARTITIONED;

    /** Default atomicity mode. */
    public static final CacheAtomicityMode DFLT_CACHE_ATOMICITY_MODE = CacheAtomicityMode.ATOMIC;

    /** Default lock timeout. */
    public static final long DFLT_LOCK_TIMEOUT = 0;

    /** Initial default cache size. */
    public static final int DFLT_START_SIZE = 1500000;

    /** Default cache size to use with eviction policy. */
    public static final int DFLT_CACHE_SIZE = 100000;

    /** Initial default near cache size. */
    public static final int DFLT_NEAR_START_SIZE = DFLT_START_SIZE / 4;

    /** Default value for 'invalidate' flag that indicates if this is invalidation-based cache. */
    public static final boolean DFLT_INVALIDATE = false;

    /** Default rebalance mode for distributed cache. */
    public static final CacheRebalanceMode DFLT_REBALANCE_MODE = CacheRebalanceMode.ASYNC;

    /** Default rebalance batch size in bytes. */
    public static final int DFLT_REBALANCE_BATCH_SIZE = 512 * 1024; // 512K

    /** Default maximum eviction queue ratio. */
    public static final float DFLT_MAX_EVICTION_OVERFLOW_RATIO = 10;

    /** Default eviction synchronized flag. */
    public static final boolean DFLT_EVICT_SYNCHRONIZED = false;

    /** Default eviction key buffer size for batching synchronized evicts. */
    public static final int DFLT_EVICT_KEY_BUFFER_SIZE = 1024;

    /** Default synchronous eviction timeout in milliseconds. */
    public static final long DFLT_EVICT_SYNCHRONIZED_TIMEOUT = 10000;

    /** Default synchronous eviction concurrency level. */
    public static final int DFLT_EVICT_SYNCHRONIZED_CONCURRENCY_LEVEL = 4;

    /** Default value for eager ttl flag. */
    public static final boolean DFLT_EAGER_TTL = true;

    /** Default off-heap storage size is {@code -1} which means that off-heap storage is disabled. */
    public static final long DFLT_OFFHEAP_MEMORY = -1;

    /** Default value for 'swapEnabled' flag. */
    public static final boolean DFLT_SWAP_ENABLED = false;

    /** Default value for 'maxConcurrentAsyncOps'. */
    public static final int DFLT_MAX_CONCURRENT_ASYNC_OPS = 500;

    /** Default value for 'writeBehindEnabled' flag. */
    public static final boolean DFLT_WRITE_BEHIND_ENABLED = false;

    /** Default flush size for write-behind cache store. */
    public static final int DFLT_WRITE_BEHIND_FLUSH_SIZE = 10240; // 10K

    /** Default critical size used when flush size is not specified. */
    public static final int DFLT_WRITE_BEHIND_CRITICAL_SIZE = 16384; // 16K

    /** Default flush frequency for write-behind cache store in milliseconds. */
    public static final long DFLT_WRITE_BEHIND_FLUSH_FREQUENCY = 5000;

    /** Default count of flush threads for write-behind cache store. */
    public static final int DFLT_WRITE_FROM_BEHIND_FLUSH_THREAD_CNT = 1;

    /** Default batch size for write-behind cache store. */
    public static final int DFLT_WRITE_BEHIND_BATCH_SIZE = 512;

    /** Default value for load previous value flag. */
    public static final boolean DFLT_LOAD_PREV_VAL = false;

    /** Default memory mode. */
    public static final CacheMemoryMode DFLT_MEMORY_MODE = CacheMemoryMode.ONHEAP_TIERED;

    /** Default value for 'readFromBackup' flag. */
    public static final boolean DFLT_READ_FROM_BACKUP = true;

    /** Filter that accepts all nodes. */
    public static final IgnitePredicate<ClusterNode> ALL_NODES = new IgniteAllNodesPredicate();

    /** Default timeout after which long query warning will be printed. */
    public static final long DFLT_LONG_QRY_WARN_TIMEOUT = 3000;

    /** Default size for onheap SQL row cache size. */
    public static final int DFLT_SQL_ONHEAP_ROW_CACHE_SIZE = 10 * 1024;

    /** Cache name. */
    private String name;

    /** Rebalance thread pool size. */
    private int rebalancePoolSize = DFLT_REBALANCE_THREAD_POOL_SIZE;

    /** Rebalance timeout. */
    private long rebalanceTimeout = DFLT_REBALANCE_TIMEOUT;

    /** Cache expiration policy. */
    private EvictionPolicy evictPlc;

    /** Flag indicating whether eviction is synchronized. */
    private boolean evictSync = DFLT_EVICT_SYNCHRONIZED;

    /** Eviction key buffer size. */
    private int evictKeyBufSize = DFLT_EVICT_KEY_BUFFER_SIZE;

    /** Synchronous eviction concurrency level. */
    private int evictSyncConcurrencyLvl = DFLT_EVICT_SYNCHRONIZED_CONCURRENCY_LEVEL;

    /** Synchronous eviction timeout. */
    private long evictSyncTimeout = DFLT_EVICT_SYNCHRONIZED_TIMEOUT;

    /** Eviction filter. */
    private EvictionFilter<?, ?> evictFilter;

    /** Maximum eviction overflow ratio. */
    private float evictMaxOverflowRatio = DFLT_MAX_EVICTION_OVERFLOW_RATIO;

    /** Eager ttl flag. */
    private boolean eagerTtl = DFLT_EAGER_TTL;

    /** Default lock timeout. */
    private long dfltLockTimeout = DFLT_LOCK_TIMEOUT;

    /** Default cache start size. */
    private int startSize = DFLT_START_SIZE;

    /** Near cache configuration. */
    private NearCacheConfiguration<K, V> nearCfg;

    /** Default value for 'copyOnRead' flag. */
    public static final boolean DFLT_COPY_ON_READ = true;

    /** Write synchronization mode. */
    private CacheWriteSynchronizationMode writeSync;

    /** */
    private Factory storeFactory;

    /** */
    private boolean loadPrevVal = DFLT_LOAD_PREV_VAL;

    /** Node group resolver. */
    private AffinityFunction aff;

    /** Cache mode. */
    private CacheMode cacheMode = DFLT_CACHE_MODE;

    /** Cache atomicity mode. */
    private CacheAtomicityMode atomicityMode;

    /** Write ordering mode. */
    private CacheAtomicWriteOrderMode atomicWriteOrderMode;

    /** Number of backups for cache. */
    private int backups = DFLT_BACKUPS;

    /** Flag indicating whether this is invalidation-based cache. */
    private boolean invalidate = DFLT_INVALIDATE;

    /** Name of class implementing GridCacheTmLookup. */
    private String tmLookupClsName;

    /** Distributed cache rebalance mode. */
    private CacheRebalanceMode rebalanceMode = DFLT_REBALANCE_MODE;

    /** Cache rebalance order. */
    private int rebalanceOrder;

    /** Rebalance batch size. */
    private int rebalanceBatchSize = DFLT_REBALANCE_BATCH_SIZE;

    /** Off-heap memory size. */
    private long offHeapMaxMem = DFLT_OFFHEAP_MEMORY;

    /** */
    private boolean swapEnabled = DFLT_SWAP_ENABLED;

    /** Maximum number of concurrent asynchronous operations. */
    private int maxConcurrentAsyncOps = DFLT_MAX_CONCURRENT_ASYNC_OPS;

    /** Write-behind feature. */
    private boolean writeBehindEnabled = DFLT_WRITE_BEHIND_ENABLED;

    /** Maximum size of write-behind cache. */
    private int writeBehindFlushSize = DFLT_WRITE_BEHIND_FLUSH_SIZE;

    /** Write-behind flush frequency in milliseconds. */
    private long writeBehindFlushFreq = DFLT_WRITE_BEHIND_FLUSH_FREQUENCY;

    /** Flush thread count for write-behind cache store. */
    private int writeBehindFlushThreadCnt = DFLT_WRITE_FROM_BEHIND_FLUSH_THREAD_CNT;

    /** Maximum batch size for write-behind cache store. */
    private int writeBehindBatchSize = DFLT_WRITE_BEHIND_BATCH_SIZE;

    /** Memory mode. */
    private CacheMemoryMode memMode = DFLT_MEMORY_MODE;

    /** */
    private AffinityKeyMapper affMapper;

    /** */
    private long rebalanceDelay;

    /** */
    private long rebalanceThrottle = DFLT_REBALANCE_THROTTLE;

    /** */
    private CacheInterceptor<?, ?> interceptor;

    /** */
    private Class<?>[] sqlFuncCls;

    /** */
    private long longQryWarnTimeout = DFLT_LONG_QRY_WARN_TIMEOUT;

    /**
     * Flag indicating whether data can be read from backup.
     * If {@code false} always get data from primary node (never from backup).
     */
    private boolean readFromBackup = DFLT_READ_FROM_BACKUP;

    /** Collection of type metadata. */
    private Collection<CacheTypeMetadata> typeMeta;

    /** Node filter specifying nodes on which this cache should be deployed. */
    private IgnitePredicate<ClusterNode> nodeFilter;

    /** */
    private boolean sqlEscapeAll;

    /** */
    private Class<?>[] indexedTypes;

    /** */
    private int sqlOnheapRowCacheSize = DFLT_SQL_ONHEAP_ROW_CACHE_SIZE;

    /** Copy on read flag. */
    private boolean cpOnRead = DFLT_COPY_ON_READ;

    /** Cache plugin configurations. */
    private CachePluginConfiguration[] pluginCfgs;

    /** Cache topology validator. */
    private TopologyValidator topValidator;

    /** Cache store session listeners. */
    private Factory<? extends CacheStoreSessionListener>[] storeSesLsnrs;

    /** Empty constructor (all values are initialized to their defaults). */
    public CacheConfiguration() {
        /* No-op. */
    }

    /** Cache name. */
    public CacheConfiguration(String name) {
        this.name = name;
    }

    /**
     * Copy constructor.
     *
     * @param cfg Configuration to copy.
     */
    public CacheConfiguration(CompleteConfiguration<K, V> cfg) {
        super(cfg);

        if (!(cfg instanceof CacheConfiguration))
            return;

        CacheConfiguration<K, V> cc = (CacheConfiguration<K, V>)cfg;

        /*
         * NOTE: MAKE SURE TO PRESERVE ALPHABETIC ORDER!
         * ==============================================
         */
        aff = cc.getAffinity();
        affMapper = cc.getAffinityMapper();
        atomicityMode = cc.getAtomicityMode();
        atomicWriteOrderMode = cc.getAtomicWriteOrderMode();
        backups = cc.getBackups();
        cacheLoaderFactory = cc.getCacheLoaderFactory();
        cacheMode = cc.getCacheMode();
        cacheWriterFactory = cc.getCacheWriterFactory();
        cpOnRead = cc.isCopyOnRead();
        dfltLockTimeout = cc.getDefaultLockTimeout();
        eagerTtl = cc.isEagerTtl();
        evictFilter = cc.getEvictionFilter();
        evictKeyBufSize = cc.getEvictSynchronizedKeyBufferSize();
        evictMaxOverflowRatio = cc.getEvictMaxOverflowRatio();
        evictPlc = cc.getEvictionPolicy();
        evictSync = cc.isEvictSynchronized();
        evictSyncConcurrencyLvl = cc.getEvictSynchronizedConcurrencyLevel();
        evictSyncTimeout = cc.getEvictSynchronizedTimeout();
        expiryPolicyFactory = cc.getExpiryPolicyFactory();
        indexedTypes = cc.getIndexedTypes();
        interceptor = cc.getInterceptor();
        invalidate = cc.isInvalidate();
        isReadThrough = cc.isReadThrough();
        isWriteThrough = cc.isWriteThrough();
        listenerConfigurations = cc.listenerConfigurations;
        loadPrevVal = cc.isLoadPreviousValue();
        longQryWarnTimeout = cc.getLongQueryWarningTimeout();
        offHeapMaxMem = cc.getOffHeapMaxMemory();
        maxConcurrentAsyncOps = cc.getMaxConcurrentAsyncOperations();
        memMode = cc.getMemoryMode();
        name = cc.getName();
        nearCfg = cc.getNearConfiguration();
        nodeFilter = cc.getNodeFilter();
        rebalanceMode = cc.getRebalanceMode();
        rebalanceBatchSize = cc.getRebalanceBatchSize();
        rebalanceDelay = cc.getRebalanceDelay();
        rebalanceOrder = cc.getRebalanceOrder();
        rebalancePoolSize = cc.getRebalanceThreadPoolSize();
        rebalanceTimeout = cc.getRebalanceTimeout();
        rebalanceThrottle = cc.getRebalanceThrottle();
        readFromBackup = cc.isReadFromBackup();
        sqlEscapeAll = cc.isSqlEscapeAll();
        sqlFuncCls = cc.getSqlFunctionClasses();
        sqlOnheapRowCacheSize = cc.getSqlOnheapRowCacheSize();
        startSize = cc.getStartSize();
        storeFactory = cc.getCacheStoreFactory();
        storeSesLsnrs = cc.getCacheStoreSessionListenerFactories();
        swapEnabled = cc.isSwapEnabled();
        tmLookupClsName = cc.getTransactionManagerLookupClassName();
        topValidator = cc.getTopologyValidator();
        typeMeta = cc.getTypeMetadata();
        writeBehindBatchSize = cc.getWriteBehindBatchSize();
        writeBehindEnabled = cc.isWriteBehindEnabled();
        writeBehindFlushFreq = cc.getWriteBehindFlushFrequency();
        writeBehindFlushSize = cc.getWriteBehindFlushSize();
        writeBehindFlushThreadCnt = cc.getWriteBehindFlushThreadCount();
        writeSync = cc.getWriteSynchronizationMode();
        pluginCfgs = cc.getPluginConfigurations();
    }

    /**
     * Cache name or {@code null} if not provided, then this will be considered a default
     * cache which can be accessed via {@link Ignite#cache(String)} method. Otherwise, if name
     * is provided, the cache will be accessed via {@link Ignite#cache(String)} method.
     *
     * @return Cache name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets cache name.
     *
     * @param name Cache name. May be <tt>null</tt>, but may not be empty string.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setName(String name) {
        A.ensure(name == null || !name.isEmpty(), "Name cannot be null or empty.");

        this.name = name;

        return this;
    }

    /**
     * Gets cache eviction policy. By default, returns {@code null}
     * which means that evictions are disabled for cache.
     *
     * @return Cache eviction policy or {@code null} if evictions should be disabled.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public EvictionPolicy<K, V> getEvictionPolicy() {
        return evictPlc;
    }

    /**
     * Sets cache eviction policy.
     *
     * @param evictPlc Cache expiration policy.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setEvictionPolicy(@Nullable EvictionPolicy evictPlc) {
        this.evictPlc = evictPlc;

        return this;
    }

    /**
     * @return Near enabled flag.
     */
    public NearCacheConfiguration<K, V> getNearConfiguration() {
        return nearCfg;
    }

    /**
     * @param nearCfg Near cache configuration.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setNearConfiguration(NearCacheConfiguration<K, V> nearCfg) {
        this.nearCfg = nearCfg;

        return this;
    }

    /**
     * Gets write synchronization mode. This mode controls whether the main
     * caller should wait for update on other nodes to complete or not.
     *
     * @return Write synchronization mode.
     */
    public CacheWriteSynchronizationMode getWriteSynchronizationMode() {
        return writeSync;
    }

    /**
     * Sets write synchronization mode.
     *
     * @param writeSync Write synchronization mode.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setWriteSynchronizationMode(CacheWriteSynchronizationMode writeSync) {
        this.writeSync = writeSync;

        return this;
    }

    /**
     * Gets filter which determines on what nodes the cache should be started.
     *
     * @return Predicate specifying on which nodes the cache should be started.
     */
    public IgnitePredicate<ClusterNode> getNodeFilter() {
        return nodeFilter;
    }

    /**
     * Sets filter which determines on what nodes the cache should be started.
     *
     * @param nodeFilter Predicate specifying on which nodes the cache should be started.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setNodeFilter(IgnitePredicate<ClusterNode> nodeFilter) {
        this.nodeFilter = nodeFilter;

        return this;
    }

    /**
     * Gets flag indicating whether eviction is synchronized between primary, backup and near nodes.
     * If this parameter is {@code true} and swap is disabled then {@link IgniteCache#localEvict(Collection)}
     * will involve all nodes where an entry is kept.  If this property is set to {@code false} then
     * eviction is done independently on different cache nodes.
     * <p>
     * Default value is defined by {@link #DFLT_EVICT_SYNCHRONIZED}.
     * <p>
     * Note that it's not recommended to set this value to {@code true} if cache
     * store is configured since it will allow to significantly improve cache
     * performance.
     *
     * @return {@code true} If eviction is synchronized with backup nodes (or the
     *      rest of the nodes in case of replicated cache), {@code false} if not.
     */
    public boolean isEvictSynchronized() {
        return evictSync;
    }

    /**
     * Sets flag indicating whether eviction is synchronized with backup nodes or near caches
     * (or the rest of the nodes for replicated cache).
     *
     * @param evictSync {@code true} if synchronized, {@code false} if not.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setEvictSynchronized(boolean evictSync) {
        this.evictSync = evictSync;

        return this;
    }

    /**
     * Gets size of the key buffer for synchronized evictions.
     * <p>
     * Default value is defined by {@link #DFLT_EVICT_KEY_BUFFER_SIZE}.
     *
     * @return Eviction key buffer size.
     */
    public int getEvictSynchronizedKeyBufferSize() {
        return evictKeyBufSize;
    }

    /**
     * Sets eviction key buffer size.
     *
     * @param evictKeyBufSize Eviction key buffer size.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setEvictSynchronizedKeyBufferSize(int evictKeyBufSize) {
        this.evictKeyBufSize = evictKeyBufSize;

        return this;
    }

    /**
     * Gets concurrency level for synchronized evictions. This flag only makes sense
     * with {@link #isEvictSynchronized()} set
     * to {@code true}. When synchronized evictions are enabled, it is possible that
     * local eviction policy will try to evict entries faster than evictions can be
     * synchronized with backup or near nodes. This value specifies how many concurrent
     * synchronous eviction sessions should be allowed before the system is forced to
     * wait and let synchronous evictions catch up with the eviction policy.
     * <p>
     * Note that if synchronous evictions start lagging, it is possible that you have either
     * too big or too small eviction key buffer size or small eviction timeout. In that case
     * you will need to adjust {@link #getEvictSynchronizedKeyBufferSize} or
     * {@link #getEvictSynchronizedTimeout()} values as well.
     * <p>
     * Default value is defined by {@link #DFLT_EVICT_SYNCHRONIZED_CONCURRENCY_LEVEL}.
     *
     * @return Synchronous eviction concurrency level.
     */
    public int getEvictSynchronizedConcurrencyLevel() {
        return evictSyncConcurrencyLvl;
    }

    /**
     * Sets concurrency level for synchronized evictions.
     *
     * @param evictSyncConcurrencyLvl Concurrency level for synchronized evictions.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setEvictSynchronizedConcurrencyLevel(int evictSyncConcurrencyLvl) {
        this.evictSyncConcurrencyLvl = evictSyncConcurrencyLvl;

        return this;
    }

    /**
     * Gets timeout for synchronized evictions.
     * <p>
     * Node that initiates eviction waits for responses
     * from remote nodes within this timeout.
     * <p>
     * Default value is defined by {@link #DFLT_EVICT_SYNCHRONIZED_TIMEOUT}.
     *
     * @return Synchronous eviction timeout.
     */
    public long getEvictSynchronizedTimeout() {
        return evictSyncTimeout;
    }

    /**
     * Sets timeout for synchronized evictions.
     *
     * @param evictSyncTimeout Timeout for synchronized evictions.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setEvictSynchronizedTimeout(long evictSyncTimeout) {
        this.evictSyncTimeout = evictSyncTimeout;

        return this;
    }

    /**
     * This value denotes the maximum size of eviction queue in percents of cache
     * size in case of distributed cache (replicated and partitioned) and using
     * synchronized eviction (that is if {@link #isEvictSynchronized()} returns
     * {@code true}).
     * <p>
     * That queue is used internally as a buffer to decrease network costs for
     * synchronized eviction. Once queue size reaches specified value all required
     * requests for all entries in the queue are sent to remote nodes and the queue
     * is cleared.
     * <p>
     * Default value is defined by {@link #DFLT_MAX_EVICTION_OVERFLOW_RATIO} and
     * equals to {@code 10%}.
     *
     * @return Maximum size of eviction queue in percents of cache size.
     */
    public float getEvictMaxOverflowRatio() {
        return evictMaxOverflowRatio;
    }

    /**
     * Sets maximum eviction overflow ratio.
     *
     * @param evictMaxOverflowRatio Maximum eviction overflow ratio.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setEvictMaxOverflowRatio(float evictMaxOverflowRatio) {
        this.evictMaxOverflowRatio = evictMaxOverflowRatio;

        return this;
    }

    /**
     * Gets eviction filter to specify which entries should not be evicted
     * (except explicit evict by calling {@link IgniteCache#localEvict(Collection)}).
     * If {@link EvictionFilter#evictAllowed(Cache.Entry)} method
     * returns {@code false} then eviction policy will not be notified and entry will
     * never be evicted.
     * <p>
     * If not provided, any entry may be evicted depending on
     * {@link #getEvictionPolicy() eviction policy} configuration.
     *
     * @return Eviction filter or {@code null}.
     */
    @SuppressWarnings("unchecked")
    public EvictionFilter<K, V> getEvictionFilter() {
        return (EvictionFilter<K, V>)evictFilter;
    }

    /**
     * Sets eviction filter.
     *
     * @param evictFilter Eviction filter.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setEvictionFilter(EvictionFilter<K, V> evictFilter) {
        this.evictFilter = evictFilter;

        return this;
    }

    /**
     * Gets flag indicating whether expired cache entries will be eagerly removed from cache. When
     * set to {@code false}, expired entries will be removed on next entry access.
     * <p>
     * When not set, default value is {@link #DFLT_EAGER_TTL}.
     * <p>
     * <b>Note</b> that this flag only matters for entries expiring based on
     * {@link ExpiryPolicy} and should not be confused with entry
     * evictions based on configured {@link EvictionPolicy}.
     *
     * @return Flag indicating whether Ignite will eagerly remove expired entries.
     */
    public boolean isEagerTtl() {
        return eagerTtl;
    }

    /**
     * Sets eager ttl flag.
     *
     * @param eagerTtl {@code True} if Ignite should eagerly remove expired cache entries.
     * @see #isEagerTtl()
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setEagerTtl(boolean eagerTtl) {
        this.eagerTtl = eagerTtl;

        return this;
    }

    /**
     * Gets initial cache size which will be used to pre-create internal
     * hash table after start. Default value is defined by {@link #DFLT_START_SIZE}.
     *
     * @return Initial cache size.
     */
    public int getStartSize() {
        return startSize;
    }

    /**
     * Initial size for internal hash map.
     *
     * @param startSize Cache start size.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setStartSize(int startSize) {
        this.startSize = startSize;

        return this;
    }

    /**
     * Gets flag indicating whether value should be loaded from store if it is not in the cache
     * for following cache operations:
     * <ul>
     *     <li>{@link IgniteCache#putIfAbsent(Object, Object)}</li>
     *     <li>{@link IgniteCache#replace(Object, Object)}</li>
     *     <li>{@link IgniteCache#replace(Object, Object, Object)}</li>
     *     <li>{@link IgniteCache#remove(Object, Object)}</li>
     *     <li>{@link IgniteCache#getAndPut(Object, Object)}</li>
     *     <li>{@link IgniteCache#getAndRemove(Object)}</li>
     *     <li>{@link IgniteCache#getAndReplace(Object, Object)}</li>
     *     <li>{@link IgniteCache#getAndPutIfAbsent(Object, Object)}</li>
     *</ul>
     *
     * @return Load previous value flag.
     */
    public boolean isLoadPreviousValue() {
        return loadPrevVal;
    }

    /**
     * Sets flag indicating whether value should be loaded from store if it is not in the cache
     * for following cache operations:
     * <ul>
     *     <li>{@link IgniteCache#putIfAbsent(Object, Object)}</li>
     *     <li>{@link IgniteCache#replace(Object, Object)}</li>
     *     <li>{@link IgniteCache#replace(Object, Object, Object)}</li>
     *     <li>{@link IgniteCache#remove(Object, Object)}</li>
     *     <li>{@link IgniteCache#getAndPut(Object, Object)}</li>
     *     <li>{@link IgniteCache#getAndRemove(Object)}</li>
     *     <li>{@link IgniteCache#getAndReplace(Object, Object)}</li>
     *     <li>{@link IgniteCache#getAndPutIfAbsent(Object, Object)}</li>
     *</ul>
     * When not set, default value is {@link #DFLT_LOAD_PREV_VAL}.
     *
     * @param loadPrevVal Load previous value flag.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setLoadPreviousValue(boolean loadPrevVal) {
        this.loadPrevVal = loadPrevVal;

        return this;
    }

    /**
     * Gets factory for underlying persistent storage for read-through and write-through operations.
     *
     * @return Cache store factory.
     */
    @SuppressWarnings("unchecked")
    public Factory<CacheStore<? super K, ? super V>> getCacheStoreFactory() {
        return (Factory<CacheStore<? super K, ? super V>>)storeFactory;
    }

    /**
     * Sets factory for persistent storage for cache data.

     * @param storeFactory Cache store factory.
     * @return {@code this} for chaining.
     */
    @SuppressWarnings("unchecked")
    public CacheConfiguration<K, V> setCacheStoreFactory(
        Factory<? extends CacheStore<? super K, ? super V>> storeFactory) {
        this.storeFactory = storeFactory;

        return this;
    }

    /**
     * Gets key topology resolver to provide mapping from keys to nodes.
     *
     * @return Key topology resolver to provide mapping from keys to nodes.
     */
    public AffinityFunction getAffinity() {
        return aff;
    }

    /**
     * Sets affinity for cache keys.
     *
     * @param aff Cache key affinity.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setAffinity(AffinityFunction aff) {
        this.aff = aff;

        return this;
    }

    /**
     * Gets caching mode to use. You can configure cache either to be local-only,
     * fully replicated, partitioned, or near. If not provided, {@link CacheMode#PARTITIONED}
     * mode will be used by default (defined by {@link #DFLT_CACHE_MODE} constant).
     *
     * @return {@code True} if cache is local.
     */
    public CacheMode getCacheMode() {
        return cacheMode;
    }

    /**
     * Sets caching mode.
     *
     * @param cacheMode Caching mode.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setCacheMode(CacheMode cacheMode) {
        this.cacheMode = cacheMode;

        return this;
    }

    /**
     * Gets cache atomicity mode.
     * <p>
     * Default value is defined by {@link #DFLT_CACHE_ATOMICITY_MODE}.
     *
     * @return Cache atomicity mode.
     */
    public CacheAtomicityMode getAtomicityMode() {
        return atomicityMode;
    }

    /**
     * Sets cache atomicity mode.
     *
     * @param atomicityMode Cache atomicity mode.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setAtomicityMode(CacheAtomicityMode atomicityMode) {
        this.atomicityMode = atomicityMode;

        return this;
    }

    /**
     * Gets cache write ordering mode. This property can be enabled only for {@link CacheAtomicityMode#ATOMIC}
     * cache (for other atomicity modes it will be ignored).
     *
     * @return Cache write ordering mode.
     */
    public CacheAtomicWriteOrderMode getAtomicWriteOrderMode() {
        return atomicWriteOrderMode;
    }

    /**
     * Sets cache write ordering mode. This property can be enabled only for {@link CacheAtomicityMode#ATOMIC}
     * cache (for other atomicity modes it will be ignored).
     *
     * @param atomicWriteOrderMode Cache write ordering mode.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setAtomicWriteOrderMode(CacheAtomicWriteOrderMode atomicWriteOrderMode) {
        this.atomicWriteOrderMode = atomicWriteOrderMode;

        return this;
    }

    /**
     * Gets number of nodes used to back up single partition for {@link CacheMode#PARTITIONED} cache.
     * <p>
     * If not set, default value is {@link #DFLT_BACKUPS}.
     *
     * @return Number of backup nodes for one partition.
     */
    public int getBackups() {
        return backups;
    }

    /**
     * Sets number of nodes used to back up single partition for {@link CacheMode#PARTITIONED} cache.
     * <p>
     * If not set, default value is {@link #DFLT_BACKUPS}.
     *
     * @param backups Number of backup nodes for one partition.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setBackups(int backups) {
        this.backups = backups;

        return this;
    }

    /**
     * Gets default lock acquisition timeout. Default value is defined by {@link #DFLT_LOCK_TIMEOUT}
     * which is {@code 0} and means that lock acquisition will never timeout.
     *
     * @return Default lock timeout.
     */
    public long getDefaultLockTimeout() {
        return dfltLockTimeout;
    }

    /**
     * Sets default lock timeout in milliseconds. By default this value is defined by {@link #DFLT_LOCK_TIMEOUT}.
     *
     * @param dfltLockTimeout Default lock timeout.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setDefaultLockTimeout(long dfltLockTimeout) {
        this.dfltLockTimeout = dfltLockTimeout;

        return this;
    }

    /**
     * Invalidation flag. If {@code true}, values will be invalidated (nullified) upon commit in near cache.
     *
     * @return Invalidation flag.
     */
    public boolean isInvalidate() {
        return invalidate;
    }

    /**
     * Sets invalidation flag for near cache entries in this transaction. Default is {@code false}.
     *
     * @param invalidate Flag to set this cache into invalidation-based mode. Default value is {@code false}.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setInvalidate(boolean invalidate) {
        this.invalidate = invalidate;

        return this;
    }

    /**
     * Gets class name of transaction manager finder for integration for JEE app servers.
     *
     * @return Transaction manager finder.
     * @deprecated Use {@link TransactionConfiguration#getTxManagerLookupClassName()} instead.
     */
    @Deprecated
    public String getTransactionManagerLookupClassName() {
        return tmLookupClsName;
    }

    /**
     * Sets look up mechanism for available {@code TransactionManager} implementation, if any.
     *
     * @param tmLookupClsName Name of class implementing GridCacheTmLookup interface that is used to
     *      receive JTA transaction manager.
     * @return {@code this} for chaining.
     * @deprecated Use {@link TransactionConfiguration#setTxManagerLookupClassName(String)} instead.
     */
    @Deprecated
    public CacheConfiguration<K, V> setTransactionManagerLookupClassName(String tmLookupClsName) {
        this.tmLookupClsName = tmLookupClsName;

        return this;
    }

    /**
     * Sets cache rebalance mode.
     *
     * @param rebalanceMode Rebalance mode.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setRebalanceMode(CacheRebalanceMode rebalanceMode) {
        this.rebalanceMode = rebalanceMode;

        return this;
    }

    /**
     * Gets rebalance mode for distributed cache.
     * <p>
     * Default is defined by {@link #DFLT_REBALANCE_MODE}.
     *
     * @return Rebalance mode.
     */
    public CacheRebalanceMode getRebalanceMode() {
        return rebalanceMode;
    }

    /**
     * Gets cache rebalance order. Rebalance order can be set to non-zero value for caches with
     * {@link CacheRebalanceMode#SYNC SYNC} or {@link CacheRebalanceMode#ASYNC ASYNC} rebalance modes only.
     * <p/>
     * If cache rebalance order is positive, rebalancing for this cache will be started only when rebalancing for
     * all caches with smaller rebalance order (except caches with rebalance order {@code 0}) will be completed.
     * <p/>
     * Note that cache with order {@code 0} does not participate in ordering. This means that cache with
     * rebalance order {@code 1} will never wait for any other caches. All caches with order {@code 0} will
     * be rebalanced right away concurrently with each other and ordered rebalance processes.
     * <p/>
     * If not set, cache order is 0, i.e. rebalancing is not ordered.
     *
     * @return Cache rebalance order.
     */
    public int getRebalanceOrder() {
        return rebalanceOrder;
    }

    /**
     * Sets cache rebalance order.
     *
     * @param rebalanceOrder Cache rebalance order.
     * @see #getRebalanceOrder()
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setRebalanceOrder(int rebalanceOrder) {
        this.rebalanceOrder = rebalanceOrder;

        return this;
    }

    /**
     * Gets size (in number bytes) to be loaded within a single rebalance message.
     * Rebalancing algorithm will split total data set on every node into multiple
     * batches prior to sending data. Default value is defined by
     * {@link #DFLT_REBALANCE_BATCH_SIZE}.
     *
     * @return Size in bytes of a single rebalance message.
     */
    public int getRebalanceBatchSize() {
        return rebalanceBatchSize;
    }

    /**
     * Sets rebalance batch size.
     *
     * @param rebalanceBatchSize Rebalance batch size.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setRebalanceBatchSize(int rebalanceBatchSize) {
        this.rebalanceBatchSize = rebalanceBatchSize;

        return this;
    }

    /**
     * Flag indicating whether Ignite should use swap storage by default. By default
     * swap is disabled which is defined via {@link #DFLT_SWAP_ENABLED} constant.
     *
     * @return {@code True} if swap storage is enabled.
     */
    public boolean isSwapEnabled() {
        return swapEnabled;
    }

    /**
     * Flag indicating whether swap storage is enabled or not.
     *
     * @param swapEnabled {@code True} if swap storage is enabled.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setSwapEnabled(boolean swapEnabled) {
        this.swapEnabled = swapEnabled;

        return this;
    }

    /**
     * Gets maximum number of allowed concurrent asynchronous operations. If 0 returned then number
     * of concurrent asynchronous operations is unlimited.
     * <p>
     * If not set, default value is {@link #DFLT_MAX_CONCURRENT_ASYNC_OPS}.
     * <p>
     * If user threads do not wait for asynchronous operations to complete, it is possible to overload
     * a system. This property enables back-pressure control by limiting number of scheduled asynchronous
     * cache operations.
     *
     * @return Maximum number of concurrent asynchronous operations or {@code 0} if unlimited.
     */
    public int getMaxConcurrentAsyncOperations() {
        return maxConcurrentAsyncOps;
    }

    /**
     * Sets maximum number of concurrent asynchronous operations.
     *
     * @param maxConcurrentAsyncOps Maximum number of concurrent asynchronous operations.
     * @see #getMaxConcurrentAsyncOperations()
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setMaxConcurrentAsyncOperations(int maxConcurrentAsyncOps) {
        this.maxConcurrentAsyncOps = maxConcurrentAsyncOps;

        return this;
    }

    /**
     * Flag indicating whether Ignite should use write-behind behaviour for the cache store.
     * By default write-behind is disabled which is defined via {@link #DFLT_WRITE_BEHIND_ENABLED}
     * constant.
     *
     * @return {@code True} if write-behind is enabled.
     */
    public boolean isWriteBehindEnabled() {
        return writeBehindEnabled;
    }

    /**
     * Sets flag indicating whether write-behind is enabled.
     *
     * @param writeBehindEnabled {@code true} if write-behind is enabled.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setWriteBehindEnabled(boolean writeBehindEnabled) {
        this.writeBehindEnabled = writeBehindEnabled;

        return this;
    }

    /**
     * Maximum size of the write-behind cache. If cache size exceeds this value,
     * all cached items are flushed to the cache store and write cache is cleared.
     * <p/>
     * If not provided, default value is {@link #DFLT_WRITE_BEHIND_FLUSH_SIZE}.
     * If this value is {@code 0}, then flush is performed according to the flush frequency interval.
     * <p/>
     * Note that you cannot set both, {@code flush} size and {@code flush frequency}, to {@code 0}.
     *
     * @return Maximum object count in write-behind cache.
     */
    public int getWriteBehindFlushSize() {
        return writeBehindFlushSize;
    }

    /**
     * Sets write-behind flush size.
     *
     * @param writeBehindFlushSize Write-behind cache flush size.
     * @see #getWriteBehindFlushSize()
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setWriteBehindFlushSize(int writeBehindFlushSize) {
        this.writeBehindFlushSize = writeBehindFlushSize;

        return this;
    }

    /**
     * Frequency with which write-behind cache is flushed to the cache store in milliseconds.
     * This value defines the maximum time interval between object insertion/deletion from the cache
     * ant the moment when corresponding operation is applied to the cache store.
     * <p>
     * If not provided, default value is {@link #DFLT_WRITE_BEHIND_FLUSH_FREQUENCY}.
     * If this value is {@code 0}, then flush is performed according to the flush size.
     * <p>
     * Note that you cannot set both, {@code flush} size and {@code flush frequency}, to {@code 0}.
     *
     * @return Write-behind flush frequency in milliseconds.
     */
    public long getWriteBehindFlushFrequency() {
        return writeBehindFlushFreq;
    }

    /**
     * Sets write-behind flush frequency.
     *
     * @param writeBehindFlushFreq Write-behind flush frequency in milliseconds.
     * @see #getWriteBehindFlushFrequency()
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setWriteBehindFlushFrequency(long writeBehindFlushFreq) {
        this.writeBehindFlushFreq = writeBehindFlushFreq;

        return this;
    }

    /**
     * Number of threads that will perform cache flushing. Cache flushing is performed
     * when cache size exceeds value defined by
     * {@link #getWriteBehindFlushSize}, or flush interval defined by
     * {@link #getWriteBehindFlushFrequency} is elapsed.
     * <p/>
     * If not provided, default value is {@link #DFLT_WRITE_FROM_BEHIND_FLUSH_THREAD_CNT}.
     *
     * @return Count of flush threads.
     */
    public int getWriteBehindFlushThreadCount() {
        return writeBehindFlushThreadCnt;
    }

    /**
     * Sets flush thread count for write-behind cache.
     *
     * @param writeBehindFlushThreadCnt Count of flush threads.
     * @see #getWriteBehindFlushThreadCount()
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setWriteBehindFlushThreadCount(int writeBehindFlushThreadCnt) {
        this.writeBehindFlushThreadCnt = writeBehindFlushThreadCnt;

        return this;
    }

    /**
     * Maximum batch size for write-behind cache store operations. Store operations (get or remove)
     * are combined in a batch of this size to be passed to
     * {@link CacheStore#writeAll(Collection)} or
     * {@link CacheStore#deleteAll(Collection)} methods.
     * <p/>
     * If not provided, default value is {@link #DFLT_WRITE_BEHIND_BATCH_SIZE}.
     *
     * @return Maximum batch size for store operations.
     */
    public int getWriteBehindBatchSize() {
        return writeBehindBatchSize;
    }

    /**
     * Sets maximum batch size for write-behind cache.
     *
     * @param writeBehindBatchSize Maximum batch size.
     * @see #getWriteBehindBatchSize()
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setWriteBehindBatchSize(int writeBehindBatchSize) {
        this.writeBehindBatchSize = writeBehindBatchSize;

        return this;
    }

    /**
     * Gets size of rebalancing thread pool. Note that size serves as a hint and implementation
     * may create more threads for rebalancing than specified here (but never less threads).
     * <p>
     * Default value is {@link #DFLT_REBALANCE_THREAD_POOL_SIZE}.
     *
     * @return Size of rebalancing thread pool.
     */
    public int getRebalanceThreadPoolSize() {
        return rebalancePoolSize;
    }

    /**
     * Sets size of rebalancing thread pool. Note that size serves as a hint and implementation may create more threads
     * for rebalancing than specified here (but never less threads).
     *
     * @param rebalancePoolSize Size of rebalancing thread pool.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setRebalanceThreadPoolSize(int rebalancePoolSize) {
        this.rebalancePoolSize = rebalancePoolSize;

        return this;
    }

    /**
     * Gets rebalance timeout (ms).
     * <p>
     * Default value is {@link #DFLT_REBALANCE_TIMEOUT}.
     *
     * @return Rebalance timeout (ms).
     */
    public long getRebalanceTimeout() {
        return rebalanceTimeout;
    }

    /**
     * Sets rebalance timeout (ms).
     *
     * @param rebalanceTimeout Rebalance timeout (ms).
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setRebalanceTimeout(long rebalanceTimeout) {
        this.rebalanceTimeout = rebalanceTimeout;

        return this;
    }

    /**
     * Gets delay in milliseconds upon a node joining or leaving topology (or crash) after which rebalancing
     * should be started automatically. Rebalancing should be delayed if you plan to restart nodes
     * after they leave topology, or if you plan to start multiple nodes at once or one after another
     * and don't want to repartition and rebalance until all nodes are started.
     * <p>
     * For better efficiency user should usually make sure that new nodes get placed on
     * the same place of consistent hash ring as the left nodes, and that nodes are
     * restarted before this delay expires. To place nodes on the same place in consistent hash ring,
     * use {@link IgniteConfiguration#setConsistentId(Serializable)}
     * to make sure that a node maps to the same hash ID event if restarted. As an example,
     * node IP address and port combination may be used in this case.
     * <p>
     * Default value is {@code 0} which means that repartitioning and rebalancing will start
     * immediately upon node leaving topology. If {@code -1} is returned, then rebalancing
     * will only be started manually by calling {@link IgniteCache#rebalance()} method or
     * from management console.
     *
     * @return Rebalancing delay, {@code 0} to start rebalancing immediately, {@code -1} to
     *      start rebalancing manually, or positive value to specify delay in milliseconds
     *      after which rebalancing should start automatically.
     */
    public long getRebalanceDelay() {
        return rebalanceDelay;
    }

    /**
     * Sets rebalance delay (see {@link #getRebalanceDelay()} for more information).
     *
     * @param rebalanceDelay Rebalance delay to set.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setRebalanceDelay(long rebalanceDelay) {
        this.rebalanceDelay = rebalanceDelay;

        return this;
    }

    /**
     * Time in milliseconds to wait between rebalance messages to avoid overloading of CPU or network.
     * When rebalancing large data sets, the CPU or network can get over-consumed with rebalancing messages,
     * which consecutively may slow down the application performance. This parameter helps tune
     * the amount of time to wait between rebalance messages to make sure that rebalancing process
     * does not have any negative performance impact. Note that application will continue to work
     * properly while rebalancing is still in progress.
     * <p>
     * Value of {@code 0} means that throttling is disabled. By default throttling is disabled -
     * the default is defined by {@link #DFLT_REBALANCE_THROTTLE} constant.
     *
     * @return Time in milliseconds to wait between rebalance messages to avoid overloading of CPU,
     *      {@code 0} to disable throttling.
     */
    public long getRebalanceThrottle() {
        return rebalanceThrottle;
    }

    /**
     * Time in milliseconds to wait between rebalance messages to avoid overloading of CPU or network. When rebalancing
     * large data sets, the CPU or network can get over-consumed with rebalancing messages, which consecutively may slow
     * down the application performance. This parameter helps tune the amount of time to wait between rebalance messages
     * to make sure that rebalancing process does not have any negative performance impact. Note that application will
     * continue to work properly while rebalancing is still in progress. <p> Value of {@code 0} means that throttling is
     * disabled. By default throttling is disabled - the default is defined by {@link #DFLT_REBALANCE_THROTTLE} constant.
     *
     * @param rebalanceThrottle Time in milliseconds to wait between rebalance messages to avoid overloading of CPU,
     * {@code 0} to disable throttling.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setRebalanceThrottle(long rebalanceThrottle) {
        this.rebalanceThrottle = rebalanceThrottle;

        return this;
    }

    /**
     * Affinity key mapper used to provide custom affinity key for any given key.
     * Affinity mapper is particularly useful when several objects need to be collocated
     * on the same node (they will also be backed up on the same nodes as well).
     * <p>
     * If not provided, then default implementation will be used. The default behavior
     * is described in {@link AffinityKeyMapper} documentation.
     *
     * @return Mapper to use for affinity key mapping.
     */
    public AffinityKeyMapper getAffinityMapper() {
        return affMapper;
    }

    /**
     * Sets custom affinity mapper. If not provided, then default implementation will be used. The default behavior is
     * described in {@link AffinityKeyMapper} documentation.
     *
     * @param affMapper Affinity mapper.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setAffinityMapper(AffinityKeyMapper affMapper) {
        this.affMapper = affMapper;

        return this;
    }

    /**
     * Gets maximum amount of memory available to off-heap storage. Possible values are
     * <ul>
     * <li>{@code -1} - Means that off-heap storage is disabled.</li>
     * <li>
     *     {@code 0} - Ignite will not limit off-heap storage (it's up to user to properly
     *     add and remove entries from cache to ensure that off-heap storage does not grow
     *     indefinitely.
     * </li>
     * <li>Any positive value specifies the limit of off-heap storage in bytes.</li>
     * </ul>
     * Default value is {@code -1}, specified by {@link #DFLT_OFFHEAP_MEMORY} constant
     * which means that off-heap storage is disabled by default.
     * <p>
     * Use off-heap storage to load gigabytes of data in memory without slowing down
     * Garbage Collection. Essentially in this case you should allocate very small amount
     * of memory to JVM and Ignite will cache most of the data in off-heap space
     * without affecting JVM performance at all.
     * <p>
     * Note that Ignite will throw an exception if max memory is set to {@code -1} and
     * {@code offHeapValuesOnly} flag is set to {@code true}.
     *
     * @return Maximum memory in bytes available to off-heap memory space.
     */
    public long getOffHeapMaxMemory() {
        return offHeapMaxMem;
    }

    /**
     * Sets maximum amount of memory available to off-heap storage. Possible values are <ul> <li>{@code -1} - Means that
     * off-heap storage is disabled.</li> <li> {@code 0} - Ignite will not limit off-heap storage (it's up to user to
     * properly add and remove entries from cache to ensure that off-heap storage does not grow infinitely. </li>
     * <li>Any positive value specifies the limit of off-heap storage in bytes.</li> </ul> Default value is {@code -1},
     * specified by {@link #DFLT_OFFHEAP_MEMORY} constant which means that off-heap storage is disabled by default. <p>
     * Use off-heap storage to load gigabytes of data in memory without slowing down Garbage Collection. Essentially in
     * this case you should allocate very small amount of memory to JVM and Ignite will cache most of the data in
     * off-heap space without affecting JVM performance at all.
     *
     * @param offHeapMaxMem Maximum memory in bytes available to off-heap memory space.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setOffHeapMaxMemory(long offHeapMaxMem) {
        this.offHeapMaxMem = offHeapMaxMem;

        return this;
    }

    /**
     * Gets memory mode for cache. Memory mode helps control whether value is stored in on-heap memory,
     * off-heap memory, or swap space. Refer to {@link CacheMemoryMode} for more info.
     * <p>
     * Default value is {@link #DFLT_MEMORY_MODE}.
     *
     * @return Memory mode.
     */
    public CacheMemoryMode getMemoryMode() {
        return memMode;
    }

    /**
     * Sets memory mode for cache.
     *
     * @param memMode Memory mode.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setMemoryMode(CacheMemoryMode memMode) {
        this.memMode = memMode;

        return this;
    }

    /**
     * Gets cache interceptor.
     *
     * @return Cache interceptor.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public CacheInterceptor<K, V> getInterceptor() {
        return (CacheInterceptor<K, V>)interceptor;
    }

    /**
     * Sets cache interceptor.
     *
     * @param interceptor Cache interceptor.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setInterceptor(CacheInterceptor<K, V> interceptor) {
        this.interceptor = interceptor;

        return this;
    }

    /**
     * Gets collection of type metadata objects.
     *
     * @return Collection of type metadata.
     */
    public Collection<CacheTypeMetadata> getTypeMetadata() {
        return typeMeta;
    }

    /**
     * Sets collection of type metadata objects.
     *
     * @param typeMeta Collection of type metadata.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setTypeMetadata(Collection<CacheTypeMetadata> typeMeta) {
        this.typeMeta = typeMeta;

        return this;
    }

    /**
     * Gets flag indicating whether data can be read from backup.
     * If {@code false} always get data from primary node (never from backup).
     * <p>
     * Default value is defined by {@link #DFLT_READ_FROM_BACKUP}.
     *
     * @return {@code true} if data can be read from backup node or {@code false} if data always
     *      should be read from primary node and never from backup.
     */
    public boolean isReadFromBackup() {
        return readFromBackup;
    }

    /**
     * Sets read from backup flag.
     *
     * @param readFromBackup {@code true} to allow reads from backups.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setReadFromBackup(boolean readFromBackup) {
        this.readFromBackup = readFromBackup;

        return this;
    }

    /**
     * Gets flag indicating whether copy of of the value stored in cache should be created
     * for cache operation implying return value. Also if this flag is set copies are created for values
     * passed to {@link CacheInterceptor} and to {@link CacheEntryProcessor}.
     *
     * @return Copy on read flag.
     */
    public boolean isCopyOnRead() {
        return cpOnRead;
    }

    /**
     * Sets copy on read flag.
     *
     * @param cpOnRead Copy on get flag.
     * @see #isCopyOnRead
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setCopyOnRead(boolean cpOnRead) {
        this.cpOnRead = cpOnRead;

        return this;
    }

    /**
     * Sets classes with methods annotated by {@link QuerySqlFunction}
     * to be used as user-defined functions from SQL queries.
     *
     * @param cls One or more classes with SQL functions.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setSqlFunctionClasses(Class<?>... cls) {
        this.sqlFuncCls = cls;

        return this;
    }

    /**
     * Gets classes with methods annotated by {@link QuerySqlFunction}
     * to be used as user-defined functions from SQL queries.
     *
     * @return Classes with SQL functions.
     */
    @Nullable public Class<?>[] getSqlFunctionClasses() {
        return sqlFuncCls;
    }

    /**
     * Gets timeout in milliseconds after which long query warning will be printed.
     *
     * @return Timeout in milliseconds.
     */
    public long getLongQueryWarningTimeout() {
        return longQryWarnTimeout;
    }

    /**
     * Gets timeout in milliseconds after which long query warning will be printed.
     *
     * @param longQryWarnTimeout Timeout in milliseconds.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setLongQueryWarningTimeout(long longQryWarnTimeout) {
        this.longQryWarnTimeout = longQryWarnTimeout;

        return this;
    }

    /**
     * If {@code true} all the SQL table and field names will be escaped with double quotes like
     * ({@code "tableName"."fieldsName"}). This enforces case sensitivity for field names and
     * also allows having special characters in table and field names.
     *
     * @return Flag value.
     */
    public boolean isSqlEscapeAll() {
        return sqlEscapeAll;
    }

    /**
     * If {@code true} all the SQL table and field names will be escaped with double quotes like
     * ({@code "tableName"."fieldsName"}). This enforces case sensitivity for field names and
     * also allows having special characters in table and field names.
     *
     * @param sqlEscapeAll Flag value.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setSqlEscapeAll(boolean sqlEscapeAll) {
        this.sqlEscapeAll = sqlEscapeAll;

        return this;
    }

    /**
     * Array of key and value type pairs to be indexed (thus array length must be always even).
     * It means each even (0,2,4...) class in the array will be considered as key type for cache entry,
     * each odd (1,3,5...) class will be considered as value type for cache entry.
     * <p>
     * The same key class can occur multiple times for different value classes, but each value class must be unique
     * because SQL table will be named as value class simple name.
     * <p>
     * To expose fields of these types onto SQL level and to index them you have to use annotations
     * from package {@link org.apache.ignite.cache.query.annotations}.
     *
     * @return Key and value type pairs.
     */
    public Class<?>[] getIndexedTypes() {
        return indexedTypes;
    }

    /**
     * Array of key and value type pairs to be indexed (thus array length must be always even).
     * It means each even (0,2,4...) class in the array will be considered as key type for cache entry,
     * each odd (1,3,5...) class will be considered as value type for cache entry.
     * <p>
     * The same key class can occur multiple times for different value classes, but each value class must be unique
     * because SQL table will be named as value class simple name.
     * <p>
     * To expose fields of these types onto SQL level and to index them you have to use annotations
     * from package {@link org.apache.ignite.cache.query.annotations}.
     *
     * @param indexedTypes Key and value type pairs.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setIndexedTypes(Class<?>... indexedTypes) {
        A.ensure(indexedTypes == null || (indexedTypes.length & 1) == 0,
            "Number of indexed types is expected to be even. Refer to method javadoc for details.");

        if (indexedTypes != null) {
            int len = indexedTypes.length;

            Class<?>[] newIndexedTypes = new Class<?>[len];

            for (int i = 0; i < len; i++)
                newIndexedTypes[i] = U.box(indexedTypes[i]);

            this.indexedTypes = newIndexedTypes;
        }
        else
            this.indexedTypes = null;

        return this;
    }

    /**
     * Number of SQL rows which will be cached onheap to avoid deserialization on each SQL index access.
     * This setting only makes sense when offheap is enabled for this cache.
     *
     * @return Cache size.
     * @see #setOffHeapMaxMemory(long)
     */
    public int getSqlOnheapRowCacheSize() {
        return sqlOnheapRowCacheSize;
    }

    /**
     * Number of SQL rows which will be cached onheap to avoid deserialization on each SQL index access.
     * This setting only makes sense when offheap is enabled for this cache.
     *
     * @param size Cache size.
     * @see #setOffHeapMaxMemory(long)
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setSqlOnheapRowCacheSize(int size) {
        this.sqlOnheapRowCacheSize = size;

        return this;
    }

    /**
     * Gets array of cache plugin configurations.
     *
     * @return Cache plugin configurations.
     */
    public CachePluginConfiguration[] getPluginConfigurations() {
        return pluginCfgs != null ? pluginCfgs : new CachePluginConfiguration[0];
    }

    /**
     * Sets cache plugin configurations.
     *
     * @param pluginCfgs Cache plugin configurations.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setPluginConfigurations(CachePluginConfiguration... pluginCfgs) {
        this.pluginCfgs = pluginCfgs;

        return this;
    }

    /**
     * Gets topology validator.
     * <p>
     * See {@link TopologyValidator} for details.
     *
     * @return validator.
     */
    public TopologyValidator getTopologyValidator() {
        return topValidator;
    }

    /**
     * Sets topology validator.
     * <p>
     * See {@link TopologyValidator} for details.
     *
     * @param topValidator validator.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setTopologyValidator(TopologyValidator topValidator) {
        this.topValidator = topValidator;

        return this;
    }

    /**
     * Gets cache store session listener factories.
     *
     * @return Cache store session listener factories.
     * @see CacheStoreSessionListener
     */
    public Factory<? extends CacheStoreSessionListener>[] getCacheStoreSessionListenerFactories() {
        return storeSesLsnrs;
    }

    /**
     * Cache store session listener factories.
     * <p>
     * These listeners override global listeners provided in
     * {@link IgniteConfiguration#setCacheStoreSessionListenerFactories(Factory[])}
     * configuration property.
     *
     * @param storeSesLsnrs Cache store session listener factories.
     * @return {@code this} for chaining.
     * @see CacheStoreSessionListener
     */
    public CacheConfiguration<K, V> setCacheStoreSessionListenerFactories(
        Factory<? extends CacheStoreSessionListener>... storeSesLsnrs) {
        this.storeSesLsnrs = storeSesLsnrs;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheConfiguration.class, this);
    }

    /**
     *  Filter that accepts all nodes.
     */
    public static class IgniteAllNodesPredicate  implements IgnitePredicate<ClusterNode> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj == null)
                return false;

            return obj.getClass().equals(this.getClass());
        }
    }
}

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
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cache.eviction.EvictionFilter;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.query.annotations.QueryGroupIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.cache.query.annotations.QueryTextField;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
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

    /** Maximum number of partitions. */
    public static final int MAX_PARTITIONS_COUNT = 0xFFFF;

    /** Default size of rebalance thread pool. */
    @Deprecated
    public static final int DFLT_REBALANCE_THREAD_POOL_SIZE = 2;

    /** Default rebalance timeout (ms).*/
    public static final long DFLT_REBALANCE_TIMEOUT = 10000;

    /** Default rebalance batches prefetch count. */
    public static final long DFLT_REBALANCE_BATCHES_PREFETCH_COUNT = 2;

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

    /** Default cache size to use with eviction policy. */
    public static final int DFLT_CACHE_SIZE = 100000;

    /** Default maximum inline size for sql indexes. */
    public static final int DFLT_SQL_INDEX_MAX_INLINE_SIZE = -1;

    /** Initial default near cache size. */
    public static final int DFLT_NEAR_START_SIZE = 1500000 / 4;

    /** Default value for 'invalidate' flag that indicates if this is invalidation-based cache. */
    public static final boolean DFLT_INVALIDATE = false;

    /** Default rebalance mode for distributed cache. */
    public static final CacheRebalanceMode DFLT_REBALANCE_MODE = CacheRebalanceMode.ASYNC;

    /** Default rebalance batch size in bytes. */
    public static final int DFLT_REBALANCE_BATCH_SIZE = 512 * 1024; // 512K

    /** Default value for eager ttl flag. */
    public static final boolean DFLT_EAGER_TTL = true;

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

    /** Default write coalescing for write-behind cache store. */
    public static final boolean DFLT_WRITE_BEHIND_COALESCING = true;

    /** Default maximum number of query iterators that can be stored. */
    public static final int DFLT_MAX_QUERY_ITERATOR_CNT = 1024;

    /** Default value for load previous value flag. */
    public static final boolean DFLT_LOAD_PREV_VAL = false;

    /** Default value for 'readFromBackup' flag. */
    public static final boolean DFLT_READ_FROM_BACKUP = true;

    /** Filter that accepts all nodes. */
    public static final IgnitePredicate<ClusterNode> ALL_NODES = new IgniteAllNodesPredicate();

    /** Default timeout after which long query warning will be printed. */
    public static final long DFLT_LONG_QRY_WARN_TIMEOUT = 3000;

    /** Default number of queries detail metrics to collect. */
    public static final int DFLT_QRY_DETAIL_METRICS_SIZE = 0;

    /** Default value for keep binary in store behavior . */
    @SuppressWarnings({"UnnecessaryBoxing", "BooleanConstructorCall"})
    public static final Boolean DFLT_STORE_KEEP_BINARY = new Boolean(false);

    /** Default threshold for concurrent loading of keys from {@link CacheStore}. */
    public static final int DFLT_CONCURRENT_LOAD_ALL_THRESHOLD = 5;

    /** Default partition loss policy. */
    public static final PartitionLossPolicy DFLT_PARTITION_LOSS_POLICY = PartitionLossPolicy.IGNORE;

    /** Default query parallelism. */
    public static final int DFLT_QUERY_PARALLELISM = 1;

    /** Cache name. */
    private String name;

    /** Name of {@link MemoryPolicyConfiguration} for this cache */
    private String memPlcName;

    /** Threshold for concurrent loading of keys from {@link CacheStore}. */
    private int storeConcurrentLoadAllThreshold = DFLT_CONCURRENT_LOAD_ALL_THRESHOLD;

    /** Rebalance thread pool size. */
    @Deprecated
    private int rebalancePoolSize = DFLT_REBALANCE_THREAD_POOL_SIZE;

    /** Rebalance timeout. */
    private long rebalanceTimeout = DFLT_REBALANCE_TIMEOUT;

    /** Cache expiration policy. */
    private EvictionPolicy evictPlc;

    /** */
    private boolean onheapCache;

    /** Eviction filter. */
    private EvictionFilter<?, ?> evictFilter;

    /** Eager ttl flag. */
    private boolean eagerTtl = DFLT_EAGER_TTL;

    /** Default lock timeout. */
    private long dfltLockTimeout = DFLT_LOCK_TIMEOUT;

    /** Near cache configuration. */
    private NearCacheConfiguration<K, V> nearCfg;

    /** Default value for 'copyOnRead' flag. */
    public static final boolean DFLT_COPY_ON_READ = true;

    /** Write synchronization mode. */
    private CacheWriteSynchronizationMode writeSync;

    /** */
    private Factory storeFactory;

    /** */
    private Boolean storeKeepBinary = DFLT_STORE_KEEP_BINARY;

    /** */
    private boolean loadPrevVal = DFLT_LOAD_PREV_VAL;

    /** Node group resolver. */
    private AffinityFunction aff;

    /** Cache mode. */
    private CacheMode cacheMode = DFLT_CACHE_MODE;

    /** Cache atomicity mode. */
    private CacheAtomicityMode atomicityMode;

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

    /** Rebalance batches prefetch count. */
    private long rebalanceBatchesPrefetchCnt = DFLT_REBALANCE_BATCHES_PREFETCH_COUNT;

    /** Maximum number of concurrent asynchronous operations. */
    private int maxConcurrentAsyncOps = DFLT_MAX_CONCURRENT_ASYNC_OPS;

    /** Maximum inline size for sql indexes. */
    private int sqlIdxMaxInlineSize = DFLT_SQL_INDEX_MAX_INLINE_SIZE;

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

    /** Write coalescing flag for write-behind cache store */
    private boolean writeBehindCoalescing = DFLT_WRITE_BEHIND_COALESCING;

    /** Maximum number of query iterators that can be stored. */
    private int maxQryIterCnt = DFLT_MAX_QUERY_ITERATOR_CNT;

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

    /** */
    private int qryDetailMetricsSz = DFLT_QRY_DETAIL_METRICS_SIZE;

    /**
     * Flag indicating whether data can be read from backup.
     * If {@code false} always get data from primary node (never from backup).
     */
    private boolean readFromBackup = DFLT_READ_FROM_BACKUP;

    /** Node filter specifying nodes on which this cache should be deployed. */
    private IgnitePredicate<ClusterNode> nodeFilter;

    /** */
    private String sqlSchema;

    /** */
    private boolean sqlEscapeAll;

    /** */
    private transient Class<?>[] indexedTypes;

    /** Copy on read flag. */
    private boolean cpOnRead = DFLT_COPY_ON_READ;

    /** Cache plugin configurations. */
    private CachePluginConfiguration[] pluginCfgs;

    /** Cache topology validator. */
    private TopologyValidator topValidator;

    /** Cache store session listeners. */
    private Factory<? extends CacheStoreSessionListener>[] storeSesLsnrs;

    /** Query entities. */
    private Collection<QueryEntity> qryEntities;

    /** Partition loss policy. */
    private PartitionLossPolicy partLossPlc = DFLT_PARTITION_LOSS_POLICY;

    /** */
    private int qryParallelism = DFLT_QUERY_PARALLELISM;

    /** Empty constructor (all values are initialized to their defaults). */
    public CacheConfiguration() {
        /* No-op. */
    }

    /**
     * @param name Cache name.
     */
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
        backups = cc.getBackups();
        cacheLoaderFactory = cc.getCacheLoaderFactory();
        cacheMode = cc.getCacheMode();
        cacheWriterFactory = cc.getCacheWriterFactory();
        cpOnRead = cc.isCopyOnRead();
        dfltLockTimeout = cc.getDefaultLockTimeout();
        eagerTtl = cc.isEagerTtl();
        evictFilter = cc.getEvictionFilter();
        evictPlc = cc.getEvictionPolicy();
        expiryPolicyFactory = cc.getExpiryPolicyFactory();
        indexedTypes = cc.getIndexedTypes();
        interceptor = cc.getInterceptor();
        invalidate = cc.isInvalidate();
        isReadThrough = cc.isReadThrough();
        qryParallelism = cc.getQueryParallelism();
        isWriteThrough = cc.isWriteThrough();
        storeKeepBinary = cc.isStoreKeepBinary() != null ? cc.isStoreKeepBinary() : DFLT_STORE_KEEP_BINARY;
        listenerConfigurations = cc.listenerConfigurations;
        loadPrevVal = cc.isLoadPreviousValue();
        longQryWarnTimeout = cc.getLongQueryWarningTimeout();
        maxConcurrentAsyncOps = cc.getMaxConcurrentAsyncOperations();
        memPlcName = cc.getMemoryPolicyName();
        sqlIdxMaxInlineSize = cc.getSqlIndexMaxInlineSize();
        name = cc.getName();
        nearCfg = cc.getNearConfiguration();
        nodeFilter = cc.getNodeFilter();
        onheapCache = cc.isOnheapCacheEnabled();
        partLossPlc = cc.getPartitionLossPolicy();
        pluginCfgs = cc.getPluginConfigurations();
        qryEntities = cc.getQueryEntities() == Collections.<QueryEntity>emptyList() ? null : cc.getQueryEntities();
        qryDetailMetricsSz = cc.getQueryDetailMetricsSize();
        readFromBackup = cc.isReadFromBackup();
        rebalanceBatchSize = cc.getRebalanceBatchSize();
        rebalanceBatchesPrefetchCnt = cc.getRebalanceBatchesPrefetchCount();
        rebalanceDelay = cc.getRebalanceDelay();
        rebalanceMode = cc.getRebalanceMode();
        rebalanceOrder = cc.getRebalanceOrder();
        rebalancePoolSize = cc.getRebalanceThreadPoolSize();
        rebalanceTimeout = cc.getRebalanceTimeout();
        rebalanceThrottle = cc.getRebalanceThrottle();
        sqlSchema = cc.getSqlSchema();
        sqlEscapeAll = cc.isSqlEscapeAll();
        sqlFuncCls = cc.getSqlFunctionClasses();
        storeFactory = cc.getCacheStoreFactory();
        storeSesLsnrs = cc.getCacheStoreSessionListenerFactories();
        tmLookupClsName = cc.getTransactionManagerLookupClassName();
        topValidator = cc.getTopologyValidator();
        writeBehindBatchSize = cc.getWriteBehindBatchSize();
        writeBehindCoalescing = cc.getWriteBehindCoalescing();
        writeBehindEnabled = cc.isWriteBehindEnabled();
        writeBehindFlushFreq = cc.getWriteBehindFlushFrequency();
        writeBehindFlushSize = cc.getWriteBehindFlushSize();
        writeBehindFlushThreadCnt = cc.getWriteBehindFlushThreadCount();
        writeSync = cc.getWriteSynchronizationMode();
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
     * @param name Cache name. Can not be <tt>null</tt> or empty.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setName(String name) {
        this.name = name;

        return this;
    }

    /**
     * @return {@link MemoryPolicyConfiguration} name.
     */
    public String getMemoryPolicyName() {
        return memPlcName;
    }

    /**
     * Sets a name of {@link MemoryPolicyConfiguration} for this cache.
     *
     * @param memPlcName MemoryPolicyConfiguration name. Can be null (default MemoryPolicyConfiguration will be used)
     *                   but should not be empty.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setMemoryPolicyName(String memPlcName) {
        A.ensure(memPlcName == null || !memPlcName.isEmpty(), "Name cannot be empty.");

        this.memPlcName = memPlcName;

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
     * Checks if the on-heap cache is enabled for the off-heap based page memory.
     *
     * @return On-heap cache enabled flag.
     */
    public boolean isOnheapCacheEnabled() {
        return onheapCache;
    }

    /**
     * Configures on-heap cache for the off-heap based page memory.
     *
     * @param onheapCache {@code True} if on-heap cache should be enabled.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setOnheapCacheEnabled(boolean onheapCache) {
        this.onheapCache = onheapCache;

        return this;
    }

    /**
     * @return Near enabled flag.
     */
    public NearCacheConfiguration<K, V> getNearConfiguration() {
        return nearCfg;
    }

    /**
     * Sets the near cache configuration to use on all cache nodes.
     *
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
     * <p>
     * Default synchronization mode is {@link CacheWriteSynchronizationMode#PRIMARY_SYNC}.
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
     * Gets flag indicating whether expired cache entries will be eagerly removed from cache.
     * If there is at least one cache configured with this flag set to {@code true}, Ignite
     * will create a single thread to clean up expired entries in background. When flag is
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
     *
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
     * Flag indicating that {@link CacheStore} implementation
     * is working with binary objects instead of Java objects.
     * Default value of this flag is {@link #DFLT_STORE_KEEP_BINARY}.
     * <p>
     * If set to {@code false}, Ignite will deserialize keys and
     * values stored in binary format before they are passed
     * to cache store.
     * <p>
     * Note that setting this flag to {@code false} can simplify
     * store implementation in some cases, but it can cause performance
     * degradation due to additional serializations and deserializations
     * of binary objects. You will also need to have key and value
     * classes on all nodes since binary will be deserialized when
     * store is called.
     *
     * @return Keep binary in store flag.
     */
    public Boolean isStoreKeepBinary() {
        return storeKeepBinary;
    }

    /**
     * Sets keep binary in store flag.
     *
     * @param storeKeepBinary Keep binary in store flag.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setStoreKeepBinary(boolean storeKeepBinary) {
        this.storeKeepBinary = storeKeepBinary;

        return this;
    }

    /**
     * Gets the threshold used in cases when values for multiple keys are being loaded from an underlying
     * {@link CacheStore} in parallel. In the situation when several threads load the same or intersecting set of keys
     * and the total number of keys to load is less or equal to this threshold then there will be no a second call to
     * the storage in order to load a key from thread A if the same key is already being loaded by thread B.
     *
     * The threshold should be controlled wisely. On the one hand if it's set to a big value then the interaction with
     * a storage during the load of missing keys will be minimal. On the other hand the big value may result in
     * significant performance degradation because it is needed to check for every key whether it's being loaded or not.
     *
     * When not set, default value is {@link #DFLT_CONCURRENT_LOAD_ALL_THRESHOLD}.
     *
     * @return The concurrent load-all threshold.
     */
    public int getStoreConcurrentLoadAllThreshold() {
        return storeConcurrentLoadAllThreshold;
    }

    /**
     * Sets the concurrent load-all threshold used for cases when keys' values are being loaded from {@link CacheStore}
     * in parallel.
     *
     * @param storeConcurrentLoadAllThreshold The concurrent load-all threshold.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setStoreConcurrentLoadAllThreshold(int storeConcurrentLoadAllThreshold) {
        this.storeConcurrentLoadAllThreshold = storeConcurrentLoadAllThreshold;

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
     * @deprecated Use {@link TransactionConfiguration#getTxManagerFactory()} instead.
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
     * @deprecated Use {@link TransactionConfiguration#setTxManagerFactory(Factory)} instead.
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
     * all caches with smaller rebalance order will be completed.
     * <p/>
     * Note that cache with order {@code 0} does not participate in ordering. This means that cache with
     * rebalance order {@code 0} will never wait for any other caches. All caches with order {@code 0} will
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
     * To gain better rebalancing performance supplier node can provide more than one batch at rebalancing start and
     * provide one new to each next demand request.
     *
     * Gets number of batches generated by supply node at rebalancing start.
     * Minimum is 1.
     *
     * @return batches count
     */
    public long getRebalanceBatchesPrefetchCount() {
        return rebalanceBatchesPrefetchCnt;
    }

    /**
     * To gain better rebalancing performance supplier node can provide more than one batch at rebalancing start and
     * provide one new to each next demand request.
     *
     * Sets number of batches generated by supply node at rebalancing start.
     * Minimum is 1.
     *
     * @param rebalanceBatchesCnt batches count.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setRebalanceBatchesPrefetchCount(long rebalanceBatchesCnt) {
        this.rebalanceBatchesPrefetchCnt = rebalanceBatchesCnt;

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
     * Gets maximum inline size for sql indexes. If -1 returned then
     * {@code IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE} system property is used.
     * <p>
     * If not set, default value is {@link #DFLT_SQL_INDEX_MAX_INLINE_SIZE}.
     *
     * @return Maximum payload size for offheap indexes.
     */
    public int getSqlIndexMaxInlineSize() {
        return sqlIdxMaxInlineSize;
    }

    /**
     * Sets maximum inline size for sql indexes.
     *
     * @param sqlIdxMaxInlineSize Maximum inline size for sql indexes.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setSqlIndexMaxInlineSize(int sqlIdxMaxInlineSize) {
        this.sqlIdxMaxInlineSize = sqlIdxMaxInlineSize;

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
     * Write coalescing flag for write-behind cache store operations. Store operations (get or remove)
     * with the same key are combined or coalesced to single, resulting operation
     * to reduce pressure to underlying cache store.
     * <p/>
     * If not provided, default value is {@link #DFLT_WRITE_BEHIND_COALESCING}.
     *
     * @return Write coalescing flag.
     */
    public boolean getWriteBehindCoalescing() {
        return writeBehindCoalescing;
    }

    /**
     * Sets write coalescing flag for write-behind cache.
     *
     * @param writeBehindCoalescing Write coalescing flag.
     * @see #getWriteBehindCoalescing()
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setWriteBehindCoalescing(boolean writeBehindCoalescing) {
        this.writeBehindCoalescing = writeBehindCoalescing;

        return this;
    }

    /**
     * Use {@link IgniteConfiguration#getRebalanceThreadPoolSize()} instead.
     *
     * @return Size of rebalancing thread pool.
     */
    @Deprecated
    public int getRebalanceThreadPoolSize() {
        return rebalancePoolSize;
    }

    /**
     * Use {@link IgniteConfiguration#getRebalanceThreadPoolSize()} instead.
     *
     * @param rebalancePoolSize Size of rebalancing thread pool.
     * @return {@code this} for chaining.
     */
    @Deprecated
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
     * Gets maximum number of query iterators that can be stored. Iterators are stored to
     * support query pagination when each page of data is sent to user's node only on demand.
     * Increase this property if you are running and processing lots of queries in parallel.
     * <p>
     * Default value is {@link #DFLT_MAX_QUERY_ITERATOR_CNT}.
     *
     * @return Maximum number of query iterators that can be stored.
     */
    public int getMaxQueryIteratorsCount() {
        return maxQryIterCnt;
    }

    /**
     * Sets maximum number of query iterators that can be stored.
     *
     * @param maxQryIterCnt Maximum number of query iterators that can be stored.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setMaxQueryIteratorsCount(int maxQryIterCnt) {
        this.maxQryIterCnt = maxQryIterCnt;

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
     * Sets timeout in milliseconds after which long query warning will be printed.
     *
     * @param longQryWarnTimeout Timeout in milliseconds.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setLongQueryWarningTimeout(long longQryWarnTimeout) {
        this.longQryWarnTimeout = longQryWarnTimeout;

        return this;
    }

    /**
     * Gets size of queries detail metrics that will be stored in memory for monitoring purposes.
     * If {@code 0} then history will not be collected.
     * Note, larger number may lead to higher memory consumption.
     *
     * @return Maximum number of query metrics that will be stored in memory.
     */
    public int getQueryDetailMetricsSize() {
        return qryDetailMetricsSz;
    }

    /**
     * Sets size of queries detail metrics that will be stored in memory for monitoring purposes.
     *
     * @param qryDetailMetricsSz Maximum number of latest queries metrics that will be stored in memory.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setQueryDetailMetricsSize(int qryDetailMetricsSz) {
        this.qryDetailMetricsSz = qryDetailMetricsSz;

        return this;
    }

    /**
     * Gets custom name of the sql schema. If custom sql schema is not set then {@code null} will be returned and
     * quoted case sensitive name will be used as sql schema.
     *
     * @return Schema name for current cache according to SQL ANSI-99. Could be {@code null}.
     */
    @Nullable public String getSqlSchema() {
        return sqlSchema;
    }

    /**
     * Sets sql schema to be used for current cache. This name will correspond to SQL ANSI-99 standard.
     * Nonquoted identifiers are not case sensitive. Quoted identifiers are case sensitive.
     * <p/>
     * Be aware of using the same string in case sensitive and case insensitive manner simultaneously, since
     * behaviour for such case is not specified.
     * <p/>
     * When sqlSchema is not specified, quoted {@code cacheName} is used instead.
     * <p/>
     * {@code sqlSchema} could not be an empty string. Has to be {@code "\"\""} instead.
     *
     * @param sqlSchema Schema name for current cache according to SQL ANSI-99. Should not be {@code null}.
     *
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setSqlSchema(String sqlSchema) {
        A.ensure((sqlSchema != null), "Schema could not be null.");
        A.ensure(!sqlSchema.isEmpty(), "Schema could not be empty.");

        this.sqlSchema = sqlSchema;

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
        if (F.isEmpty(indexedTypes))
            return this;

        int len = indexedTypes.length;

        if (len == 0)
            return this;

        A.ensure((len & 1) == 0,
            "Number of indexed types is expected to be even. Refer to method javadoc for details.");

        if (this.indexedTypes != null)
            throw new CacheException("Indexed types can be set only once.");

        Class<?>[] newIndexedTypes = new Class<?>[len];

        for (int i = 0; i < len; i++) {
            if (indexedTypes[i] == null)
                throw new NullPointerException("Indexed types array contains null at index: " + i);

            newIndexedTypes[i] = U.box(indexedTypes[i]);
        }

        if (qryEntities == null)
            qryEntities = new ArrayList<>();

        for (int i = 0; i < len; i += 2) {
            Class<?> keyCls = newIndexedTypes[i];
            Class<?> valCls = newIndexedTypes[i + 1];

            TypeDescriptor desc = processKeyAndValueClasses(keyCls, valCls);

            QueryEntity converted = convert(desc);

            boolean dup = false;

            for (QueryEntity entity : qryEntities) {
                if (F.eq(entity.findValueType(), converted.findValueType())) {
                    dup = true;

                    break;
                }
            }

            if (!dup)
                qryEntities.add(converted);
        }

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
     * Gets a collection of configured  query entities.
     *
     * @return Query entities configurations.
     */
    public Collection<QueryEntity> getQueryEntities() {
        return qryEntities != null ? qryEntities : Collections.<QueryEntity>emptyList();
    }

    /**
     * Gets partition loss policy. This policy defines how Ignite will react to a situation when all nodes for
     * some partition leave the cluster.
     *
     * @return Partition loss policy.
     * @see PartitionLossPolicy
     */
    public PartitionLossPolicy getPartitionLossPolicy() {
        return partLossPlc == null ? DFLT_PARTITION_LOSS_POLICY : partLossPlc;
    }

    /**
     * Sets partition loss policy. This policy defines how Ignite will react to a situation when all nodes for
     * some partition leave the cluster.
     *
     * @param partLossPlc Partition loss policy.
     * @return {@code this} for chaining.
     * @see PartitionLossPolicy
     */
    public CacheConfiguration<K, V> setPartitionLossPolicy(PartitionLossPolicy partLossPlc) {
        this.partLossPlc = partLossPlc;

        return this;
    }

    /**
     * Sets query entities configuration.
     *
     * @param qryEntities Query entities.
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setQueryEntities(Collection<QueryEntity> qryEntities) {
        if (this.qryEntities == null) {
            this.qryEntities = new ArrayList<>(qryEntities);

            return this;
        }

        for (QueryEntity entity : qryEntities) {
            boolean found = false;

            for (QueryEntity existing : this.qryEntities) {
                if (F.eq(entity.findValueType(), existing.findValueType())) {
                    found = true;

                    break;
                }
            }

            if (!found)
                this.qryEntities.add(entity);
        }

        return this;
    }

    /**
     * Clear query entities.
     *
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> clearQueryEntities() {
        this.qryEntities = null;

        return this;
    }

    /**
     * Defines a hint to query execution engine on desired degree of parallelism within a single node.
     * Query executor may or may not use this hint depending on estimated query costs. Query executor may define
     * certain restrictions on parallelism depending on query type and/or cache type.
     * <p>
     * As of {@code Apache Ignite 1.9} this hint is only supported for SQL queries with the following restrictions:
     * <ul>
     *     <li>All caches participating in query must have the same degree of parallelism, exception is thrown
     *     otherwise</li>
     *     <li>All queries on the given cache will follow provided degree of parallelism</li>
     * </ul>
     * These restrictions will be removed in future versions of Apache Ignite.
     * <p>
     * Defaults to {@link #DFLT_QUERY_PARALLELISM}.
     *
     * @return Query parallelism.
     */
    public int getQueryParallelism() {
        return qryParallelism;
    }

    /**
     * Sets query parallelism.
     *
     * @param qryParallelism Query parallelism.
     * @see #getQueryParallelism()
     * @return {@code this} for chaining.
     */
    public CacheConfiguration<K, V> setQueryParallelism(int qryParallelism) {
        this.qryParallelism = qryParallelism;

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
    @Override public Iterable<CacheEntryListenerConfiguration<K, V>> getCacheEntryListenerConfigurations() {
        synchronized (this) {
            return new HashSet<>(listenerConfigurations);
        }
    }

    /** {@inheritDoc} */
    @Override public MutableConfiguration<K, V> addCacheEntryListenerConfiguration(
        CacheEntryListenerConfiguration<K, V> cacheEntryLsnrCfg) {
        synchronized (this) {
            return super.addCacheEntryListenerConfiguration(cacheEntryLsnrCfg);
        }
    }

    /** {@inheritDoc} */
    @Override public MutableConfiguration<K, V> removeCacheEntryListenerConfiguration(
        CacheEntryListenerConfiguration<K, V> cacheEntryLsnrCfg) {
        synchronized (this) {
            return super.removeCacheEntryListenerConfiguration(cacheEntryLsnrCfg);
        }
    }

    /**
     * Creates a copy of current configuration and removes all cache entry listeners.
     * They are executed only locally and should never be sent to remote nodes.
     *
     * @return Configuration object that will be serialized.
     */
    protected Object writeReplace() {
        CacheConfiguration<K, V> cfg = new CacheConfiguration<>(this);

        cfg.listenerConfigurations = new HashSet<>();

        return cfg;
    }

    /**
     * @param desc Type descriptor.
     * @return Type metadata.
     */
    private static QueryEntity convert(TypeDescriptor desc) {
        QueryEntity entity = new QueryEntity();

        // Key and val types.
        entity.setKeyType(desc.keyClass().getName());
        entity.setValueType(desc.valueClass().getName());

        for (ClassProperty prop : desc.props.values())
            entity.addQueryField(prop.fullName(), U.box(prop.type()).getName(), prop.alias());

        entity.setKeyFields(desc.keyProps);

        QueryIndex txtIdx = null;

        Collection<QueryIndex> idxs = new ArrayList<>();

        for (Map.Entry<String, GridQueryIndexDescriptor> idxEntry : desc.indexes().entrySet()) {
            GridQueryIndexDescriptor idx = idxEntry.getValue();

            if (idx.type() == QueryIndexType.FULLTEXT) {
                assert txtIdx == null;

                txtIdx = new QueryIndex();

                txtIdx.setIndexType(QueryIndexType.FULLTEXT);

                txtIdx.setFieldNames(idx.fields(), true);
                txtIdx.setName(idxEntry.getKey());
            }
            else {
                Collection<String> grp = new ArrayList<>();

                for (String fieldName : idx.fields())
                    grp.add(idx.descending(fieldName) ? fieldName + " desc" : fieldName);

                QueryIndex sortedIdx = new QueryIndex();

                sortedIdx.setIndexType(idx.type());

                LinkedHashMap<String, Boolean> fields = new LinkedHashMap<>();

                for (String f : idx.fields())
                    fields.put(f, !idx.descending(f));

                sortedIdx.setFields(fields);

                sortedIdx.setName(idxEntry.getKey());

                idxs.add(sortedIdx);
            }
        }

        if (desc.valueTextIndex()) {
            if (txtIdx == null) {
                txtIdx = new QueryIndex();

                txtIdx.setIndexType(QueryIndexType.FULLTEXT);

                txtIdx.setFieldNames(Arrays.asList(QueryUtils.VAL_FIELD_NAME), true);
            }
            else
                txtIdx.getFields().put(QueryUtils.VAL_FIELD_NAME, true);
        }

        if (txtIdx != null)
            idxs.add(txtIdx);

        if (!F.isEmpty(idxs))
            entity.setIndexes(idxs);

        return entity;
    }

    /**
     * @param cls Class.
     * @return Masked class.
     */
    private static Class<?> mask(Class<?> cls) {
        assert cls != null;

        return QueryUtils.isSqlType(cls) ? cls : Object.class;
    }

    /**
     * @param keyCls Key class.
     * @param valCls Value class.
     * @return Type descriptor.
     */
    static TypeDescriptor processKeyAndValueClasses(
        Class<?> keyCls,
        Class<?> valCls
    ) {
        TypeDescriptor d = new TypeDescriptor();

        d.keyClass(keyCls);
        d.valueClass(valCls);

        processAnnotationsInClass(true, d.keyCls, d, null);
        processAnnotationsInClass(false, d.valCls, d, null);

        return d;
    }

    /**
     * Process annotations for class.
     *
     * @param key If given class relates to key.
     * @param cls Class.
     * @param type Type descriptor.
     * @param parent Parent in case of embeddable.
     */
    private static void processAnnotationsInClass(boolean key, Class<?> cls, TypeDescriptor type,
        @Nullable ClassProperty parent) {
        if (U.isJdk(cls) || QueryUtils.isGeometryClass(cls)) {
            if (parent == null && !key && QueryUtils.isSqlType(cls)) { // We have to index primitive _val.
                String idxName = cls.getSimpleName() + "_" + QueryUtils.VAL_FIELD_NAME + "_idx";

                type.addIndex(idxName, QueryUtils.isGeometryClass(cls) ?
                    QueryIndexType.GEOSPATIAL : QueryIndexType.SORTED);

                type.addFieldToIndex(idxName, QueryUtils.VAL_FIELD_NAME, 0, false);
            }

            return;
        }

        if (parent != null && parent.knowsClass(cls))
            throw new CacheException("Recursive reference found in type: " + cls.getName());

        if (parent == null) { // Check class annotation at top level only.
            QueryTextField txtAnnCls = cls.getAnnotation(QueryTextField.class);

            if (txtAnnCls != null)
                type.valueTextIndex(true);

            QueryGroupIndex grpIdx = cls.getAnnotation(QueryGroupIndex.class);

            if (grpIdx != null)
                type.addIndex(grpIdx.name(), QueryIndexType.SORTED);

            QueryGroupIndex.List grpIdxList = cls.getAnnotation(QueryGroupIndex.List.class);

            if (grpIdxList != null && !F.isEmpty(grpIdxList.value())) {
                for (QueryGroupIndex idx : grpIdxList.value())
                    type.addIndex(idx.name(), QueryIndexType.SORTED);
            }
        }

        for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                QuerySqlField sqlAnn = field.getAnnotation(QuerySqlField.class);
                QueryTextField txtAnn = field.getAnnotation(QueryTextField.class);

                if (sqlAnn != null || txtAnn != null) {
                    ClassProperty prop = new ClassProperty(field);

                    prop.parent(parent);

                    // Add parent property before its possible nested properties so that
                    // resulting parent column comes before columns corresponding to those
                    // nested properties in the resulting table - that way nested
                    // properties override will happen properly (first parent, then children).
                    type.addProperty(prop, key, true);

                    processAnnotation(key, sqlAnn, txtAnn, cls, c, field.getType(), prop, type);
                }
            }
        }
    }

    /**
     * Processes annotation at field or method.
     *
     * @param key If given class relates to key.
     * @param sqlAnn SQL annotation, can be {@code null}.
     * @param txtAnn H2 text annotation, can be {@code null}.
     * @param cls Entity class.
     * @param curCls Current entity class.
     * @param fldCls Class of field or return type for method.
     * @param prop Current property.
     * @param desc Class description.
     */
    private static void processAnnotation(boolean key, QuerySqlField sqlAnn, QueryTextField txtAnn,
        Class<?> cls, Class<?> curCls, Class<?> fldCls, ClassProperty prop, TypeDescriptor desc) {
        if (sqlAnn != null) {
            processAnnotationsInClass(key, fldCls, desc, prop);

            if (!sqlAnn.name().isEmpty())
                prop.alias(sqlAnn.name());

            if (sqlAnn.index()) {
                String idxName = curCls.getSimpleName() + "_" + prop.alias() + "_idx";

                if (cls != curCls)
                    idxName = cls.getSimpleName() + "_" + idxName;

                desc.addIndex(idxName, QueryUtils.isGeometryClass(prop.type()) ?
                    QueryIndexType.GEOSPATIAL : QueryIndexType.SORTED);

                desc.addFieldToIndex(idxName, prop.fullName(), 0, sqlAnn.descending());
            }

            if (!F.isEmpty(sqlAnn.groups())) {
                for (String group : sqlAnn.groups())
                    desc.addFieldToIndex(group, prop.fullName(), 0, false);
            }

            if (!F.isEmpty(sqlAnn.orderedGroups())) {
                for (QuerySqlField.Group idx : sqlAnn.orderedGroups())
                    desc.addFieldToIndex(idx.name(), prop.fullName(), idx.order(), idx.descending());
            }
        }

        if (txtAnn != null)
            desc.addFieldToTextIndex(prop.fullName());
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> setStatisticsEnabled(boolean enabled) {
        super.setStatisticsEnabled(enabled);

        return this;
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> setManagementEnabled(boolean enabled) {
        super.setManagementEnabled(enabled);

        return this;
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> setCacheLoaderFactory(Factory<? extends CacheLoader<K, V>> factory) {
        super.setCacheLoaderFactory(factory);

        return this;
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> setCacheWriterFactory(
        Factory<? extends CacheWriter<? super K, ? super V>> factory) {
        super.setCacheWriterFactory(factory);

        return this;
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> setExpiryPolicyFactory(Factory<? extends ExpiryPolicy> factory) {
        super.setExpiryPolicyFactory(factory);

        return this;
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> setTypes(Class<K> keyType, Class<V> valType) {
        super.setTypes(keyType, valType);

        return this;
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> setReadThrough(boolean isReadThrough) {
        super.setReadThrough(isReadThrough);

        return this;
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> setWriteThrough(boolean isWriteThrough) {
        super.setWriteThrough(isWriteThrough);

        return this;
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration<K, V> setStoreByValue(boolean isStoreByVal) {
        super.setStoreByValue(isStoreByVal);

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheConfiguration.class, this);
    }

    /**
     *  Filter that accepts all nodes.
     */
    public static class IgniteAllNodesPredicate implements IgnitePredicate<ClusterNode> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj != null && obj.getClass().equals(this.getClass());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "IgniteAllNodesPredicate []";
        }
    }

    /**
     * Descriptor of type.
     */
    private static class TypeDescriptor {
        /** Value field names and types with preserved order. */
        @GridToStringInclude
        private final Map<String, Class<?>> fields = new LinkedHashMap<>();

        /** */
        @GridToStringExclude
        private final Map<String, ClassProperty> props = new LinkedHashMap<>();

        /** */
        @GridToStringInclude
        private final Set<String> keyProps = new HashSet<>();

        /** */
        @GridToStringInclude
        private final Map<String, IndexDescriptor> indexes = new HashMap<>();

        /** */
        private IndexDescriptor fullTextIdx;

        /** */
        private Class<?> keyCls;

        /** */
        private Class<?> valCls;

        /** */
        private boolean valTextIdx;

        /**
         * @return Indexes.
         */
        public Map<String, GridQueryIndexDescriptor> indexes() {
            return Collections.<String, GridQueryIndexDescriptor>unmodifiableMap(indexes);
        }

        /**
         * Adds index.
         *
         * @param idxName Index name.
         * @param type Index type.
         * @param inlineSize Inline size.
         * @return Index descriptor.
         */
        public IndexDescriptor addIndex(String idxName, QueryIndexType type, int inlineSize) {
            IndexDescriptor idx = new IndexDescriptor(type, inlineSize);

            if (indexes.put(idxName, idx) != null)
                throw new CacheException("Index with name '" + idxName + "' already exists.");

            return idx;
        }

        /**
         * Adds index.
         *
         * @param idxName Index name.
         * @param type Index type.
         * @return Index descriptor.
         */
        public IndexDescriptor addIndex(String idxName, QueryIndexType type) {
            return addIndex(idxName, type, -1);
        }

        /**
         * Adds field to index.
         *
         * @param idxName Index name.
         * @param field Field name.
         * @param orderNum Fields order number in index.
         * @param descending Sorting order.
         */
        public void addFieldToIndex(String idxName, String field, int orderNum,
            boolean descending) {
            IndexDescriptor desc = indexes.get(idxName);

            if (desc == null)
                desc = addIndex(idxName, QueryIndexType.SORTED);

            desc.addField(field, orderNum, descending);
        }

        /**
         * Adds field to text index.
         *
         * @param field Field name.
         */
        public void addFieldToTextIndex(String field) {
            if (fullTextIdx == null) {
                fullTextIdx = new IndexDescriptor(QueryIndexType.FULLTEXT);

                indexes.put(null, fullTextIdx);
            }

            fullTextIdx.addField(field, 0, false);
        }

        /**
         * @return Value class.
         */
        public Class<?> valueClass() {
            return valCls;
        }

        /**
         * Sets value class.
         *
         * @param valCls Value class.
         */
        void valueClass(Class<?> valCls) {
            this.valCls = valCls;
        }

        /**
         * @return Key class.
         */
        public Class<?> keyClass() {
            return keyCls;
        }

        /**
         * Set key class.
         *
         * @param keyCls Key class.
         */
        void keyClass(Class<?> keyCls) {
            this.keyCls = keyCls;
        }

        /**
         * Adds property to the type descriptor.
         *
         * @param prop Property.
         * @param key Property ownership flag (key or not).
         * @param failOnDuplicate Fail on duplicate flag.
         */
        void addProperty(ClassProperty prop, boolean key, boolean failOnDuplicate) {
            String name = prop.fullName();

            if (props.put(name, prop) != null && failOnDuplicate)
                throw new CacheException("Property with name '" + name + "' already exists.");

            fields.put(name, prop.type());

            if (key)
                keyProps.add(name);
        }

        /**
         * @return {@code true} If we need to have a fulltext index on value.
         */
        public boolean valueTextIndex() {
            return valTextIdx;
        }

        /**
         * Sets if this value should be text indexed.
         *
         * @param valTextIdx Flag value.
         */
        public void valueTextIndex(boolean valTextIdx) {
            this.valTextIdx = valTextIdx;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TypeDescriptor.class, this);
        }
    }

    /**
     * Index descriptor.
     */
    private static class IndexDescriptor implements GridQueryIndexDescriptor {
        /** Fields sorted by order number. */
        private final Collection<T2<String, Integer>> fields = new TreeSet<>(
            new Comparator<T2<String, Integer>>() {
                @Override public int compare(T2<String, Integer> o1, T2<String, Integer> o2) {
                    if (o1.get2().equals(o2.get2())) // Order is equal, compare field names to avoid replace in Set.
                        return o1.get1().compareTo(o2.get1());

                    return o1.get2() < o2.get2() ? -1 : 1;
                }
            });

        /** Fields which should be indexed in descending order. */
        private Collection<String> descendings;

        /** */
        private final QueryIndexType type;

        /** */
        private final int inlineSize;

        /**
         * @param type Type.
         * @param inlineSize Inline size.
         */
        private IndexDescriptor(QueryIndexType type, int inlineSize) {
            assert type != null;

            this.type = type;
            this.inlineSize = inlineSize;
        }

        /**
         * @param type Type.
         */
        private IndexDescriptor(QueryIndexType type) {
            this(type, -1);
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<String> fields() {
            Collection<String> res = new ArrayList<>(fields.size());

            for (T2<String, Integer> t : fields)
                res.add(t.get1());

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean descending(String field) {
            return descendings != null && descendings.contains(field);
        }

        /**
         * Adds field to this index.
         *
         * @param field Field name.
         * @param orderNum Field order number in this index.
         * @param descending Sort order.
         */
        public void addField(String field, int orderNum, boolean descending) {
            fields.add(new T2<>(field, orderNum));

            if (descending) {
                if (descendings == null)
                    descendings = new HashSet<>();

                descendings.add(field);
            }
        }

        /** {@inheritDoc} */
        @Override public QueryIndexType type() {
            return type;
        }

        /** {@inheritDoc} */
        @Override public int inlineSize() {
            return inlineSize;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IndexDescriptor.class, this);
        }
    }

    /**
     * Description of type property.
     */
    private static class ClassProperty {
        /** */
        private final Member member;

        /** */
        private ClassProperty parent;

        /** */
        private String name;

        /** */
        private String alias;

        /**
         * Constructor.
         *
         * @param member Element.
         */
        ClassProperty(Member member) {
            this.member = member;

            name = member.getName();

            if (member instanceof Method) {
                if (member.getName().startsWith("get") && member.getName().length() > 3)
                    name = member.getName().substring(3);

                if (member.getName().startsWith("is") && member.getName().length() > 2)
                    name = member.getName().substring(2);
            }

            ((AccessibleObject)member).setAccessible(true);
        }

        /**
         * @param alias Alias.
         */
        public void alias(String alias) {
            this.alias = alias;
        }

        /**
         * @return Alias.
         */
        String alias() {
            return F.isEmpty(alias) ? name : alias;
        }

        /**
         * @return Type.
         */
        public Class<?> type() {
            return member instanceof Field ? ((Field)member).getType() : ((Method)member).getReturnType();
        }

        /**
         * @param parent Parent property if this is embeddable element.
         */
        public void parent(ClassProperty parent) {
            this.parent = parent;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ClassProperty.class, this);
        }

        /**
         * @param cls Class.
         * @return {@code true} If this property or some parent relates to member of the given class.
         */
        public boolean knowsClass(Class<?> cls) {
            return member.getDeclaringClass() == cls || (parent != null && parent.knowsClass(cls));
        }

        /**
         * @return Full name with all parents in dot notation.
         */
        public String fullName() {
            assert name != null;

            if (parent == null)
                return name;

            return parent.fullName() + '.' + name;
        }
    }
}

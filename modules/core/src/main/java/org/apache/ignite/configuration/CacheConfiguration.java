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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.eviction.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.indexing.*;
import org.jetbrains.annotations.*;

import javax.cache.configuration.*;
import java.util.*;

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

    /**
     * Default time to live. The value is <tt>0</tt> which means that
     * cached objects never expire based on time.
     */
    public static final long DFLT_TIME_TO_LIVE = 0;

    /** Default number of backups. */
    public static final int DFLT_BACKUPS = 0;

    /** Default caching mode. */
    public static final CacheMode DFLT_CACHE_MODE = CacheMode.REPLICATED;

    /** Default atomicity mode. */
    public static final CacheAtomicityMode DFLT_CACHE_ATOMICITY_MODE = CacheAtomicityMode.ATOMIC;

    /** Default value for cache distribution mode. */
    public static final CacheDistributionMode DFLT_DISTRIBUTION_MODE = CacheDistributionMode.PARTITIONED_ONLY;

    /** Default query timeout. */
    public static final long DFLT_QUERY_TIMEOUT = 0;

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

    /** Default near nodes eviction synchronized flag. */
    public static final boolean DFLT_EVICT_NEAR_SYNCHRONIZED = true;

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

    /** Default value for 'queryIndexEnabled' flag. */
    public static final boolean DFLT_QUERY_INDEX_ENABLED = false;

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

    /** Default maximum number of query iterators that can be stored. */
    public static final int DFLT_MAX_QUERY_ITERATOR_CNT = 1024;

    /** Default value for load previous value flag. */
    public static final boolean DFLT_LOAD_PREV_VAL = false;

    /** Default memory mode. */
    public static final CacheMemoryMode DFLT_MEMORY_MODE = CacheMemoryMode.ONHEAP_TIERED;

    /** Default value for 'readFromBackup' flag. */
    public static final boolean DFLT_READ_FROM_BACKUP = true;

    /** Cache name. */
    private String name;

    /** Rebalance thread pool size. */
    private int rebalancePoolSize = DFLT_REBALANCE_THREAD_POOL_SIZE;

    /** Rebalance timeout. */
    private long rebalanceTimeout = DFLT_REBALANCE_TIMEOUT;

    /** Default time to live for cache entries. */
    private long ttl = DFLT_TIME_TO_LIVE;

    /** Cache expiration policy. */
    private CacheEvictionPolicy evictPlc;

    /** Near cache eviction policy. */
    private CacheEvictionPolicy nearEvictPlc;

    /** Flag indicating whether eviction is synchronized. */
    private boolean evictSync = DFLT_EVICT_SYNCHRONIZED;

    /** Flag indicating whether eviction is synchronized with near nodes. */
    private boolean evictNearSync = DFLT_EVICT_NEAR_SYNCHRONIZED;

    /** Eviction key buffer size. */
    private int evictKeyBufSize = DFLT_EVICT_KEY_BUFFER_SIZE;

    /** Synchronous eviction timeout. */
    private int evictSyncConcurrencyLvl = DFLT_EVICT_SYNCHRONIZED_CONCURRENCY_LEVEL;

    /** Synchronous eviction timeout. */
    private long evictSyncTimeout = DFLT_EVICT_SYNCHRONIZED_TIMEOUT;

    /** Eviction filter. */
    private CacheEvictionFilter<?, ?> evictFilter;

    /** Maximum eviction overflow ratio. */
    private float evictMaxOverflowRatio = DFLT_MAX_EVICTION_OVERFLOW_RATIO;

    /** Eager ttl flag. */
    private boolean eagerTtl = DFLT_EAGER_TTL;

    /** Default lock timeout. */
    private long dfltLockTimeout = DFLT_LOCK_TIMEOUT;

    /** Default query timeout. */
    private long dfltQryTimeout = DFLT_QUERY_TIMEOUT;

    /** Default cache start size. */
    private int startSize = DFLT_START_SIZE;

    /** Default near cache start size. */
    private int nearStartSize = DFLT_NEAR_START_SIZE;

    /** Cache distribution mode. */
    private CacheDistributionMode distro = DFLT_DISTRIBUTION_MODE;

    /** Default value for 'copyOnRead' flag. */
    public static final boolean DFLT_COPY_ON_READ = true;

    /** Write synchronization mode. */
    private CacheWriteSynchronizationMode writeSync;

    /** */
    private Factory storeFactory;

    /** */
    private boolean loadPrevVal = DFLT_LOAD_PREV_VAL;

    /** Node group resolver. */
    private CacheAffinityFunction aff;

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

    /** */
    private boolean qryIdxEnabled = DFLT_QUERY_INDEX_ENABLED;

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

    /** Maximum number of query iterators that can be stored. */
    private int maxQryIterCnt = DFLT_MAX_QUERY_ITERATOR_CNT;

    /** Memory mode. */
    private CacheMemoryMode memMode = DFLT_MEMORY_MODE;

    /** */
    private CacheAffinityKeyMapper affMapper;

    /** */
    private String indexingSpiName;

    /** */
    private long rebalanceDelay;

    /** */
    private long rebalanceThrottle = DFLT_REBALANCE_THROTTLE;

    /** */
    private CacheInterceptor<?, ?> interceptor;

    /** Query configuration. */
    private CacheQueryConfiguration qryCfg;

    /**
     * Flag indicating whether data can be read from backup.
     * If {@code false} always get data from primary node (never from backup).
     */
    private boolean readFromBackup = DFLT_READ_FROM_BACKUP;

    /** Collection of type metadata. */
    private Collection<CacheTypeMetadata> typeMeta;

    /** Copy on read flag. */
    private boolean cpOnRead = DFLT_COPY_ON_READ;

    /** Empty constructor (all values are initialized to their defaults). */
    public CacheConfiguration() {
        /* No-op. */
    }

    /**
     * Copy constructor.
     *
     * @param cfg Configuration to copy.
     */
    public CacheConfiguration(CompleteConfiguration cfg) {
        super(cfg);

        if (!(cfg instanceof CacheConfiguration))
            return;

        CacheConfiguration cc = (CacheConfiguration)cfg;

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
        dfltQryTimeout = cc.getDefaultQueryTimeout();
        distro = cc.getDistributionMode();
        eagerTtl = cc.isEagerTtl();
        evictFilter = cc.getEvictionFilter();
        evictKeyBufSize = cc.getEvictSynchronizedKeyBufferSize();
        evictMaxOverflowRatio = cc.getEvictMaxOverflowRatio();
        evictNearSync = cc.isEvictNearSynchronized();
        evictPlc = cc.getEvictionPolicy();
        evictSync = cc.isEvictSynchronized();
        evictSyncConcurrencyLvl = cc.getEvictSynchronizedConcurrencyLevel();
        evictSyncTimeout = cc.getEvictSynchronizedTimeout();
        expiryPolicyFactory = cc.getExpiryPolicyFactory();
        indexingSpiName = cc.getIndexingSpiName();
        interceptor = cc.getInterceptor();
        invalidate = cc.isInvalidate();
        isReadThrough = cc.isReadThrough();
        isWriteThrough = cc.isWriteThrough();
        listenerConfigurations = cc.listenerConfigurations;
        loadPrevVal = cc.isLoadPreviousValue();
        offHeapMaxMem = cc.getOffHeapMaxMemory();
        maxConcurrentAsyncOps = cc.getMaxConcurrentAsyncOperations();
        maxQryIterCnt = cc.getMaximumQueryIteratorCount();
        memMode = cc.getMemoryMode();
        name = cc.getName();
        nearStartSize = cc.getNearStartSize();
        nearEvictPlc = cc.getNearEvictionPolicy();
        rebalanceMode = cc.getRebalanceMode();
        rebalanceBatchSize = cc.getRebalanceBatchSize();
        rebalanceDelay = cc.getRebalanceDelay();
        rebalanceOrder = cc.getRebalanceOrder();
        rebalancePoolSize = cc.getRebalanceThreadPoolSize();
        rebalanceTimeout = cc.getRebalanceTimeout();
        rebalanceThrottle = cc.getRebalanceThrottle();
        qryCfg = cc.getQueryConfiguration();
        qryIdxEnabled = cc.isQueryIndexEnabled();
        readFromBackup = cc.isReadFromBackup();
        startSize = cc.getStartSize();
        storeFactory = cc.getCacheStoreFactory();
        swapEnabled = cc.isSwapEnabled();
        tmLookupClsName = cc.getTransactionManagerLookupClassName();
        ttl = cc.getDefaultTimeToLive();
        writeBehindBatchSize = cc.getWriteBehindBatchSize();
        writeBehindEnabled = cc.isWriteBehindEnabled();
        writeBehindFlushFreq = cc.getWriteBehindFlushFrequency();
        writeBehindFlushSize = cc.getWriteBehindFlushSize();
        writeBehindFlushThreadCnt = cc.getWriteBehindFlushThreadCount();
        writeSync = cc.getWriteSynchronizationMode();
        typeMeta = cc.getTypeMetadata();
    }

    /**
     * Cache name. If not provided or {@code null}, then this will be considered a default
     * cache which can be accessed via {@link Ignite#jcache(String)} method. Otherwise, if name
     * is provided, the cache will be accessed via {@link Ignite#jcache(String)} method.
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
     */
    public void setName(String name) {
        A.ensure(name == null || !name.isEmpty(), "Name cannot be null or empty.");

        this.name = name;
    }

    /**
     * Gets time to live for all objects in cache. This value can be overridden for individual objects.
     * If not set, then value is {@code 0} which means that objects never expire.
     *
     * @return Time to live for all objects in cache.
     */
    public long getDefaultTimeToLive() {
        return ttl;
    }

    /**
     * Sets time to live for all objects in cache. This value can be override for individual objects.
     *
     * @param ttl Time to live for all objects in cache.
     */
    public void setDefaultTimeToLive(long ttl) {
        this.ttl = ttl;
    }

    /**
     * Gets cache eviction policy. By default, returns {@code null}
     * which means that evictions are disabled for cache.
     *
     * @return Cache eviction policy or {@code null} if evictions should be disabled.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public CacheEvictionPolicy<K, V> getEvictionPolicy() {
        return evictPlc;
    }

    /**
     * Sets cache eviction policy.
     *
     * @param evictPlc Cache expiration policy.
     */
    public void setEvictionPolicy(@Nullable CacheEvictionPolicy evictPlc) {
        this.evictPlc = evictPlc;
    }

    /**
     * Gets cache distribution mode. This parameter is taken into account only if
     * {@link #getCacheMode()} is set to {@link CacheMode#PARTITIONED} or {@link CacheMode#REPLICATED} mode.
     * <p>
     * If not set, default value is {@link #DFLT_DISTRIBUTION_MODE}.
     *
     * @return Cache distribution mode.
     */
    public CacheDistributionMode getDistributionMode() {
        return distro;
    }

    /**
     * Sets cache distribution mode.
     *
     * @param distro Distribution mode.
     */
    public void setDistributionMode(CacheDistributionMode distro) {
        this.distro = distro;
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
     */
    public void setWriteSynchronizationMode(CacheWriteSynchronizationMode writeSync) {
        this.writeSync = writeSync;
    }

    /**
     * Gets eviction policy for {@code near} cache which is different from the one used for
     * {@code partitioned} cache. By default, returns {@code null}
     * which means that evictions are disabled for near cache.
     *
     * @return Cache eviction policy or {@code null} if evictions should be disabled.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public CacheEvictionPolicy<K, V> getNearEvictionPolicy() {
        return nearEvictPlc;
    }

    /**
     * Sets eviction policy for near cache. This property is only used for {@link CacheMode#PARTITIONED} caching
     * mode.
     *
     * @param nearEvictPlc Eviction policy for near cache.
     */
    public void setNearEvictionPolicy(@Nullable CacheEvictionPolicy nearEvictPlc) {
        this.nearEvictPlc = nearEvictPlc;
    }

    /**
     * Gets flag indicating whether eviction is synchronized between primary and
     * backup nodes on partitioned cache. If this parameter is {@code true} and
     * swap is disabled then {@link CacheProjection#evict(Object)}
     * and all its variations will involve all nodes where an entry is kept -
     * this is a group of nodes responsible for partition to which
     * corresponding key belongs. If this property is set to {@code false} then
     * eviction is done independently on cache nodes.
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
     * Sets flag indicating whether eviction is synchronized with backup nodes (or the rest of the nodes for replicated
     * cache).
     *
     * @param evictSync {@code true} if synchronized, {@code false} if not.
     */
    public void setEvictSynchronized(boolean evictSync) {
        this.evictSync = evictSync;
    }

    /**
     * Sets flag indicating whether eviction is synchronized with near nodes.
     *
     * @param evictNearSync {@code true} if synchronized, {@code false} if not.
     */
    public void setEvictNearSynchronized(boolean evictNearSync) {
        this.evictNearSync = evictNearSync;
    }

    /**
     * Gets flag indicating whether eviction on primary node is synchronized with
     * near nodes where entry is kept. Default value is {@code true} and
     * is defined by {@link #DFLT_EVICT_NEAR_SYNCHRONIZED}.
     * <p>
     * Note that in most cases this property should be set to {@code true} to keep
     * cache consistency. But there may be the cases when user may use some
     * special near eviction policy to have desired control over near cache
     * entry set.
     *
     * @return {@code true} If eviction is synchronized with near nodes in
     *      partitioned cache, {@code false} if not.
     */
    public boolean isEvictNearSynchronized() {
        return evictNearSync;
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
     */
    public void setEvictSynchronizedKeyBufferSize(int evictKeyBufSize) {
        this.evictKeyBufSize = evictKeyBufSize;
    }

    /**
     * Gets concurrency level for synchronized evictions. This flag only makes sense
     * with {@link #isEvictNearSynchronized()} or {@link #isEvictSynchronized()} set
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
     */
    public void setEvictSynchronizedConcurrencyLevel(int evictSyncConcurrencyLvl) {
        this.evictSyncConcurrencyLvl = evictSyncConcurrencyLvl;
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
     */
    public void setEvictSynchronizedTimeout(long evictSyncTimeout) {
        this.evictSyncTimeout = evictSyncTimeout;
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
     */
    public void setEvictMaxOverflowRatio(float evictMaxOverflowRatio) {
        this.evictMaxOverflowRatio = evictMaxOverflowRatio;
    }

    /**
     * Gets eviction filter to specify which entries should not be evicted
     * (except explicit evict by calling {@link IgniteCache#localEvict(Collection)}).
     * If {@link org.apache.ignite.cache.eviction.CacheEvictionFilter#evictAllowed(javax.cache.Cache.Entry)} method
     * returns {@code false} then eviction policy will not be notified and entry will
     * never be evicted.
     * <p>
     * If not provided, any entry may be evicted depending on
     * {@link #getEvictionPolicy() eviction policy} configuration.
     *
     * @return Eviction filter or {@code null}.
     */
    @SuppressWarnings("unchecked")
    public CacheEvictionFilter<K, V> getEvictionFilter() {
        return (CacheEvictionFilter<K, V>)evictFilter;
    }

    /**
     * Sets eviction filter.
     *
     * @param evictFilter Eviction filter.
     */
    public void setEvictionFilter(CacheEvictionFilter<K, V> evictFilter) {
        this.evictFilter = evictFilter;
    }

    /**
     * Gets flag indicating whether expired cache entries will be eagerly removed from cache. When
     * set to {@code false}, expired entries will be removed on next entry access.
     * <p>
     * When not set, default value is {@link #DFLT_EAGER_TTL}.
     * <p>
     * <b>Note</b> that this flag only matters for entries expiring based on
     * {@link javax.cache.expiry.ExpiryPolicy} and should not be confused with entry
     * evictions based on configured {@link org.apache.ignite.cache.eviction.CacheEvictionPolicy}.
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
     */
    public void setEagerTtl(boolean eagerTtl) {
        this.eagerTtl = eagerTtl;
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
     */
    public void setStartSize(int startSize) {
        this.startSize = startSize;
    }

    /**
     * Gets initial cache size for near cache which will be used to pre-create internal
     * hash table after start. Default value is defined by {@link #DFLT_NEAR_START_SIZE}.
     *
     * @return Initial near cache size.
     */
    public int getNearStartSize() {
        return nearStartSize;
    }

    /**
     * Start size for near cache. This property is only used for {@link CacheMode#PARTITIONED} caching mode.
     *
     * @param nearStartSize Start size for near cache.
     */
    public void setNearStartSize(int nearStartSize) {
        this.nearStartSize = nearStartSize;
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
     */
    public void setLoadPreviousValue(boolean loadPrevVal) {
        this.loadPrevVal = loadPrevVal;
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
     * Sets factory fpr persistent storage for cache data.

     * @param storeFactory Cache store factory.
     */
    @SuppressWarnings("unchecked")
    public void setCacheStoreFactory(Factory<? extends CacheStore<? super K, ? super V>> storeFactory) {
        this.storeFactory = storeFactory;
    }

    /**
     * Gets key topology resolver to provide mapping from keys to nodes.
     *
     * @return Key topology resolver to provide mapping from keys to nodes.
     */
    public CacheAffinityFunction getAffinity() {
        return aff;
    }

    /**
     * Sets affinity for cache keys.
     *
     * @param aff Cache key affinity.
     */
    public void setAffinity(CacheAffinityFunction aff) {
        this.aff = aff;
    }

    /**
     * Gets caching mode to use. You can configure cache either to be local-only,
     * fully replicated, partitioned, or near. If not provided, {@link CacheMode#REPLICATED}
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
     */
    public void setCacheMode(CacheMode cacheMode) {
        this.cacheMode = cacheMode;
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
     */
    public void setAtomicityMode(CacheAtomicityMode atomicityMode) {
        this.atomicityMode = atomicityMode;
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
     */
    public void setAtomicWriteOrderMode(CacheAtomicWriteOrderMode atomicWriteOrderMode) {
        this.atomicWriteOrderMode = atomicWriteOrderMode;
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
     */
    public void setBackups(int backups) {
        this.backups = backups;
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
     */
    public void setDefaultLockTimeout(long dfltLockTimeout) {
        this.dfltLockTimeout = dfltLockTimeout;
    }

    /**
     * Gets default query timeout. Default value is defined by {@link #DFLT_QUERY_TIMEOUT}. {@code 0} (zero)
     * means that the query will never timeout and will wait for completion.
     *
     * @return Default query timeout, {@code 0} for never.
     */
    public long getDefaultQueryTimeout() {
        return dfltQryTimeout;
    }

    /**
     * Sets default query timeout, {@code 0} for never. For more information see {@link #getDefaultQueryTimeout()}.
     *
     * @param dfltQryTimeout Default query timeout.
     */
    public void setDefaultQueryTimeout(long dfltQryTimeout) {
        this.dfltQryTimeout = dfltQryTimeout;
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
     */
    public void setInvalidate(boolean invalidate) {
        this.invalidate = invalidate;
    }

    /**
     * Gets class name of transaction manager finder for integration for JEE app servers.
     *
     * @return Transaction manager finder.
     */
    public String getTransactionManagerLookupClassName() {
        return tmLookupClsName;
    }

    /**
     * Sets look up mechanism for available {@code TransactionManager} implementation, if any.
     *
     * @param tmLookupClsName Name of class implementing GridCacheTmLookup interface that is used to
     *      receive JTA transaction manager.
     */
    public void setTransactionManagerLookupClassName(String tmLookupClsName) {
        this.tmLookupClsName = tmLookupClsName;
    }

    /**
     * Sets cache rebalance mode.
     *
     * @param rebalanceMode Rebalance mode.
     */
    public void setRebalanceMode(CacheRebalanceMode rebalanceMode) {
        this.rebalanceMode = rebalanceMode;
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
     */
    public void setRebalanceOrder(int rebalanceOrder) {
        this.rebalanceOrder = rebalanceOrder;
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
     */
    public void setRebalanceBatchSize(int rebalanceBatchSize) {
        this.rebalanceBatchSize = rebalanceBatchSize;
    }

    /**
     * Flag indicating whether Ignite should use swap storage by default. By default
     * swap is disabled which is defined via {@link #DFLT_SWAP_ENABLED} constant.
     * <p>
     * Note that this flag may be overridden for cache projection created with flag
     * {@link org.apache.ignite.internal.processors.cache.CacheFlag#SKIP_SWAP}.
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
     */
    public void setSwapEnabled(boolean swapEnabled) {
        this.swapEnabled = swapEnabled;
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
     */
    public void setMaxConcurrentAsyncOperations(int maxConcurrentAsyncOps) {
        this.maxConcurrentAsyncOps = maxConcurrentAsyncOps;
    }

    /**
     * Flag indicating whether Ignite should attempt to index value and/or key instances
     * stored in cache. If this property is {@code false}, then all indexing annotations
     * inside of any class will be ignored. By default query indexing is disabled and
     * defined via {@link #DFLT_QUERY_INDEX_ENABLED} constant.
     *
     * @return {@code True} if query indexing is enabled.
     * @see #getMemoryMode()
     */
    public boolean isQueryIndexEnabled() {
        return qryIdxEnabled;
    }

    /**
     * Flag indicating whether query indexing is enabled or not.
     *
     * @param qryIdxEnabled {@code True} if query indexing is enabled.
     */
    public void setQueryIndexEnabled(boolean qryIdxEnabled) {
        this.qryIdxEnabled = qryIdxEnabled;
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
     */
    public void setWriteBehindEnabled(boolean writeBehindEnabled) {
        this.writeBehindEnabled = writeBehindEnabled;
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
     */
    public void setWriteBehindFlushSize(int writeBehindFlushSize) {
        this.writeBehindFlushSize = writeBehindFlushSize;
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
     */
    public void setWriteBehindFlushFrequency(long writeBehindFlushFreq) {
        this.writeBehindFlushFreq = writeBehindFlushFreq;
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
     */
    public void setWriteBehindFlushThreadCount(int writeBehindFlushThreadCnt) {
        this.writeBehindFlushThreadCnt = writeBehindFlushThreadCnt;
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
     */
    public void setWriteBehindBatchSize(int writeBehindBatchSize) {
        this.writeBehindBatchSize = writeBehindBatchSize;
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
     */
    public void setRebalanceThreadPoolSize(int rebalancePoolSize) {
        this.rebalancePoolSize = rebalancePoolSize;
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
     */
    public void setRebalanceTimeout(long rebalanceTimeout) {
        this.rebalanceTimeout = rebalanceTimeout;
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
     * use {@link org.apache.ignite.cache.affinity.rendezvous.CacheRendezvousAffinityFunction#setHashIdResolver(CacheAffinityNodeHashResolver)}
     * to make sure that a node maps to the same hash ID event if restarted. As an example,
     * node IP address and port combination may be used in this case.
     * <p>
     * Default value is {@code 0} which means that repartitioning and rebalancing will start
     * immediately upon node leaving topology. If {@code -1} is returned, then rebalancing
     * will only be started manually by calling {@link GridCache#forceRepartition()} method or
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
     */
    public void setRebalanceDelay(long rebalanceDelay) {
        this.rebalanceDelay = rebalanceDelay;
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
     */
    public void setRebalanceThrottle(long rebalanceThrottle) {
        this.rebalanceThrottle = rebalanceThrottle;
    }

    /**
     * Affinity key mapper used to provide custom affinity key for any given key.
     * Affinity mapper is particularly useful when several objects need to be collocated
     * on the same node (they will also be backed up on the same nodes as well).
     * <p>
     * If not provided, then default implementation will be used. The default behavior
     * is described in {@link org.apache.ignite.cache.affinity.CacheAffinityKeyMapper} documentation.
     *
     * @return Mapper to use for affinity key mapping.
     */
    public CacheAffinityKeyMapper getAffinityMapper() {
        return affMapper;
    }

    /**
     * Sets custom affinity mapper. If not provided, then default implementation will be used. The default behavior is
     * described in {@link org.apache.ignite.cache.affinity.CacheAffinityKeyMapper} documentation.
     *
     * @param affMapper Affinity mapper.
     */
    public void setAffinityMapper(CacheAffinityKeyMapper affMapper) {
        this.affMapper = affMapper;
    }

    /**
     * Gets name of the SPI to use for indexing. If not specified, the default
     * indexing SPI will be used.
     * <p>
     * This property becomes useful in rare cases when more than one indexing
     * SPI is configured. In majority of the cases default value should be used.
     *
     * @return Name of SPI to use for indexing.
     * @see IndexingSpi
     */
    public String getIndexingSpiName() {
        return indexingSpiName;
    }

    /**
     * Sets name of the SPI to use for indexing. If not specified, the default
     * indexing SPI will be used.
     * <p>
     * This property becomes useful in rare cases when more than one indexing
     * SPI is configured. In majority of the cases default value should be used.
     *
     * @param indexingSpiName Name.
     * @see IndexingSpi
     */
    public void setIndexingSpiName(String indexingSpiName) {
        this.indexingSpiName = indexingSpiName;
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
     */
    public void setOffHeapMaxMemory(long offHeapMaxMem) {
        this.offHeapMaxMem = offHeapMaxMem;
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
    public int getMaximumQueryIteratorCount() {
        return maxQryIterCnt;
    }

    /**
     * Sets maximum number of query iterators that can be stored.
     *
     * @param maxQryIterCnt Maximum number of query iterators that can be stored.
     */
    public void setMaximumQueryIteratorCount(int maxQryIterCnt) {
        this.maxQryIterCnt = maxQryIterCnt;
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
     */
    public void setMemoryMode(CacheMemoryMode memMode) {
        this.memMode = memMode;
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
     */
    public void setInterceptor(CacheInterceptor<K, V> interceptor) {
        this.interceptor = interceptor;
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
     */
    public void setTypeMetadata(Collection<CacheTypeMetadata> typeMeta) {
        this.typeMeta = typeMeta;
    }


    /**
     * Gets query configuration. Query configuration defines which fields should be indexed for objects
     * without annotations or portable objects.
     *
     * @return Cache query configuration.
     */
    public CacheQueryConfiguration getQueryConfiguration() {
        return qryCfg;
    }

    /**
     * Sets query configuration.
     *
     * @param qryCfg Query configuration.
     */
    public void setQueryConfiguration(CacheQueryConfiguration qryCfg) {
        this.qryCfg = qryCfg;
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
     */
    public void setReadFromBackup(boolean readFromBackup) {
        this.readFromBackup = readFromBackup;
    }

    /**
     * Gets flag indicating whether copy of of the value stored in cache should be created
     * for cache operation implying return value. Also if this flag is set copies are created for values
     * passed to {@link CacheInterceptor} and to {@link org.apache.ignite.cache.IgniteEntryProcessor}.
     * <p>
     * Copies are not created for immutable types, see {@link IgniteImmutable}.
     *
     * @return Copy on get flag.
     * @see IgniteImmutable
     */
    public boolean isCopyOnRead() {
        return cpOnRead;
    }

    /**
     * Set copy on get flag.
     *
     * @param cpOnGet Copy on get flag.
     * @see #isCopyOnRead
     */
    public void setCopyOnRead(boolean cpOnGet) {
        this.cpOnRead = cpOnGet;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheConfiguration.class, this);
    }
}

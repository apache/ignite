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

import java.io.Serializable;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.configuration.CacheConfiguration.DFLT_CACHE_ATOMICITY_MODE;
import static org.apache.ignite.configuration.CacheConfiguration.DFLT_CACHE_MODE;

/**
 * Cache attributes.
 * <p>
 * This class contains information on a single cache configured on some node.
 */
public class GridCacheAttributes implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache configuration. */
    private CacheConfiguration ccfg;

    /** Cache configuration enrichment. */
    private CacheConfigurationEnrichment enrichment;

    /**
     * Creates a new instance of cache attributes.
     *
     * @param cfg Cache configuration.
     */
    public GridCacheAttributes(CacheConfiguration cfg) {
        this.ccfg = cfg;
    }

    /**
     * Creates a new instance of cache attributes.
     *
     * @param cfg Cache configuration.
     * @param enrichment Cache configuration enrichment.
     */
    public GridCacheAttributes(CacheConfiguration cfg, CacheConfigurationEnrichment enrichment) {
        this.ccfg = cfg;
        this.enrichment = enrichment;
    }

    /**
     * @return Cache group name.
     */
    public String groupName() {
        return ccfg.getGroupName();
    }

    /**
     * @return Cache configuration.
     */
    public CacheConfiguration configuration() {
        return ccfg;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return ccfg.getName();
    }

    /**
     * @return Query parallelism.
     */
    public int qryParallelism() { return ccfg.getQueryParallelism(); }

    /**
     * @return Cache mode.
     */
    public CacheMode cacheMode() {
        CacheMode cacheMode = ccfg.getCacheMode();

        return cacheMode != null ? cacheMode : DFLT_CACHE_MODE;
    }

    /**
     * @return Cache atomicity mode.
     */
    public CacheAtomicityMode atomicityMode() {
        CacheAtomicityMode atomicityMode = ccfg.getAtomicityMode();

        return atomicityMode != null ? atomicityMode : DFLT_CACHE_ATOMICITY_MODE;
    }

    /**
     * @return {@code True} if near cache is enabled.
     */
    public boolean nearCacheEnabled() {
        return cacheMode() != LOCAL && ccfg.getNearConfiguration() != null;
    }

    /**
     * @return Preload mode.
     */
    public CacheRebalanceMode cacheRebalanceMode() {
        return ccfg.getRebalanceMode();
    }

    /**
     * @return Affinity class name.
     */
    public String cacheAffinityClassName() {
        return className(ccfg.getAffinity());
    }

    /**
     * @return Affinity mapper class name.
     */
    public String cacheAffinityMapperClassName() {
        return className(ccfg.getAffinityMapper());
    }

    /**
     * @return Affinity include neighbors.
     */
    public boolean affinityIncludeNeighbors() {
        AffinityFunction aff = ccfg.getAffinity();

        return aff instanceof RendezvousAffinityFunction
            && !((RendezvousAffinityFunction)aff).isExcludeNeighbors();
    }

    /**
     * @return Affinity key backups.
     */
    public int affinityKeyBackups() {
        return ccfg.getBackups();
    }

    /**
     * @return Affinity partitions count.
     */
    public int affinityPartitionsCount() {
        return ccfg.getAffinity().partitions();
    }

    /**
     * @return Eviction filter class name.
     */
    public String evictionFilterClassName() {
        if (enrichment != null)
            return enrichment.getFieldClassName("evictFilter");

        return className(ccfg.getEvictionFilter());
    }

    /**
     * @return Eviction policy class name.
     *
     * @deprecated Use evictionPolicyFactoryClassName() instead.
     */
    @Deprecated
    public String evictionPolicyClassName() {
        return className(ccfg.getEvictionPolicy());
    }

    /**
     * @return Eviction policy factory class name.
     */
    public String evictionPolicyFactoryClassName() {
        if (enrichment != null)
            return enrichment.getFieldClassName("evictPlcFactory");

        return className(ccfg.getEvictionPolicyFactory());
    }

    /**
     * @return Near eviction policy class name.
     *
     * @deprecated Use nearEvictionPolicyFactoryClassName() instead.
     */
    public String nearEvictionPolicyClassName() {
        NearCacheConfiguration nearCfg = ccfg.getNearConfiguration();

        if (nearCfg == null)
            return null;

        return className(nearCfg.getNearEvictionPolicy());
    }

    /**
     * @return Near eviction policy factory class name.
     */
    public String nearEvictionPolicyFactoryClassName() {
        NearCacheConfiguration nearCfg = ccfg.getNearConfiguration();

        if (nearCfg == null)
            return null;

        return className(nearCfg.getNearEvictionPolicyFactory());
    }

    /**
     * @return Store class name.
     */
    public String storeFactoryClassName() {
        if (enrichment != null)
            return enrichment.getFieldClassName("storeFactory");

        return className(ccfg.getCacheStoreFactory());
    }

    /**
     * @return Transaction manager lookup class name.
     * @deprecated Transaction manager lookup must be configured in
     *  {@link TransactionConfiguration#getTxManagerLookupClassName()}.
     */
    @Deprecated
    public String transactionManagerLookupClassName() {
        return ccfg.getTransactionManagerLookupClassName();
    }

    /**
     * @return Default lock timeout.
     */
    public long defaultLockTimeout() {
        return ccfg.getDefaultLockTimeout();
    }

    /**
     * @return Preload batch size.
     * @deprecated Use {@link IgniteConfiguration#getRebalanceBatchSize()} instead.
     */
    @Deprecated
    public int rebalanceBatchSize() {
        return ccfg.getRebalanceBatchSize();
    }

    /**
     * @return Rebalance delay.
     */
    public long rebalanceDelay() {
        return ccfg.getRebalanceDelay();
    }

    /**
     * @return Rebalance prefetch count.
     * @deprecated Use {@link IgniteConfiguration#getRebalanceBatchesPrefetchCount()} instead.
     */
    @Deprecated
    public long rebalanceBatchesPrefetchCount() {
        return ccfg.getRebalanceBatchesPrefetchCount();
    }

    /**
     * @return Rebalance order.
     */
    public int rebalanceOrder() {
        return ccfg.getRebalanceOrder();
    }

    /**
     * @return Rebalance throttle.
     * @deprecated Use {@link IgniteConfiguration#getRebalanceThrottle()} instead.
     */
    @Deprecated
    public long rebalanceThrottle() {
        return ccfg.getRebalanceThrottle();
    }

    /**
     * @return Rebalance timeout.
     * @deprecated Use {@link IgniteConfiguration#getRebalanceTimeout()} instead.
     */
    @Deprecated
    public long rebalanceTimeout() {
        return ccfg.getRebalanceTimeout();
    }

    /**
     * @return Synchronization mode.
     */
    public CacheWriteSynchronizationMode writeSynchronization() {
        return ccfg.getWriteSynchronizationMode();
    }

    /**
     * @return Flag indicating whether read-through behaviour is enabled.
     */
    public boolean readThrough() {
        return ccfg.isReadThrough();
    }

    /**
     * @return Flag indicating whether read-through behaviour is enabled.
     */
    public boolean writeThrough() {
        return ccfg.isWriteThrough();
    }

    /**
     * @return Flag indicating whether old value is loaded from store for cache operation.
     */
    public boolean loadPreviousValue() {
        return ccfg.isLoadPreviousValue();
    }

    /**
     * @return Flag indicating whether Ignite should use write-behind behaviour for the cache store.
     */
    public boolean writeBehindEnabled() {
        return ccfg.isWriteBehindEnabled();
    }

    /**
     * @return Maximum size of write-behind cache.
     */
    public int writeBehindFlushSize() {
        return ccfg.getWriteBehindFlushSize();
    }

    /**
     * @return Write-behind flush frequency in milliseconds.
     */
    public long writeBehindFlushFrequency() {
        return ccfg.getWriteBehindFlushFrequency();
    }

    /**
     * @return Flush thread count for write-behind cache store.
     */
    public int writeBehindFlushThreadCount() {
        return ccfg.getWriteBehindFlushThreadCount();
    }

    /**
     * @return Maximum batch size for write-behind cache store.
     */
    public int writeBehindBatchSize() {
        return ccfg.getWriteBehindBatchSize();
    }

    /**
     * @return Write coalescing flag.
     */
    public boolean writeBehindCoalescing() {
        return ccfg.getWriteBehindCoalescing();
    }

    /**
     * @return Interceptor class name.
     */
    public String interceptorClassName() {
        if (enrichment != null && enrichment.hasField("interceptor"))
            return enrichment.getFieldClassName("interceptor");

        return className(ccfg.getInterceptor());
    }

    /**
     * @return Node filter class name.
     */
    String nodeFilterClassName() {
        return className(ccfg.getNodeFilter());
    }

    /**
     * @return Topology validator class name.
     */
    String topologyValidatorClassName() {
        return className(ccfg.getTopologyValidator());
    }

    /**
     * @return Is cache encryption enabled.
     */
    public boolean isEncryptionEnabled() {
        return ccfg.isEncryptionEnabled();
    }

    /**
     * @param obj Object to get class of.
     * @return Class name or {@code null}.
     */
    @Nullable private static String className(@Nullable Object obj) {
        return obj != null ? obj.getClass().getName() : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAttributes.class, this);
    }
}

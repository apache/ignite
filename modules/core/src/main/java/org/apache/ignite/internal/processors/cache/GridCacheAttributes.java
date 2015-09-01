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
import java.io.Serializable;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
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

    /**
     * @param cfg Cache configuration.
     */
    public GridCacheAttributes(CacheConfiguration cfg) {
        ccfg = cfg;
    }

    /**
     * Public no-arg constructor for {@link Externalizable}.
     */
    public GridCacheAttributes() {
        // No-op.
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
     * @return Affinity hash ID resolver class name.
     */
    public String affinityHashIdResolverClassName() {
        AffinityFunction aff = ccfg.getAffinity();

        if (aff instanceof RendezvousAffinityFunction) {
            if (((RendezvousAffinityFunction) aff).getHashIdResolver() == null)
                return null;

            return className(((RendezvousAffinityFunction) aff).getHashIdResolver());
        }

        return null;
    }

    /**
     * @return Eviction filter class name.
     */
    public String evictionFilterClassName() {
        return className(ccfg.getEvictionFilter());
    }

    /**
     * @return Eviction policy class name.
     */
    public String evictionPolicyClassName() {
        return className(ccfg.getEvictionPolicy());
    }

    /**
     * @return Near eviction policy class name.
     */
    public String nearEvictionPolicyClassName() {
        NearCacheConfiguration nearCfg = ccfg.getNearConfiguration();

        if (nearCfg == null)
            return null;

        return className(nearCfg.getNearEvictionPolicy());
    }

    /**
     * @return Store class name.
     */
    public String storeFactoryClassName() {
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
     * @return {@code True} if swap enabled.
     */
    public boolean swapEnabled() {
        return ccfg.isSwapEnabled();
    }

    /**
     * @return Flag indicating whether eviction is synchronized.
     */
    public boolean evictSynchronized() {
        return ccfg.isEvictSynchronized();
    }

    /**
     * @return Maximum eviction overflow ratio.
     */
    public float evictMaxOverflowRatio() {
        return ccfg.getEvictMaxOverflowRatio();
    }

    /**
     * @return Default lock timeout.
     */
    public long defaultLockTimeout() {
        return ccfg.getDefaultLockTimeout();
    }

    /**
     * @return Preload batch size.
     */
    public int rebalanceBatchSize() {
        return ccfg.getRebalanceBatchSize();
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
     * @return Interceptor class name.
     */
    public String interceptorClassName() {
        return className(ccfg.getInterceptor());
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
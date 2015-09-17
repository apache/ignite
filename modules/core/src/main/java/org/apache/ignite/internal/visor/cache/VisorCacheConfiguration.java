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

package org.apache.ignite.internal.visor.cache;

import java.io.Serializable;
import java.util.Collection;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;

/**
 * Data transfer object for cache configuration properties.
 */
public class VisorCacheConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String name;

    /** Cache mode. */
    private CacheMode mode;

    /** Cache atomicity mode */
    private CacheAtomicityMode atomicityMode;

    /** Cache atomicity write ordering mode. */
    private CacheAtomicWriteOrderMode atomicWriteOrderMode;

    /** Eager ttl flag */
    private boolean eagerTtl;

    /** Write synchronization mode. */
    private CacheWriteSynchronizationMode writeSynchronizationMode;

    /** Swap enabled flag. */
    private boolean swapEnabled;

    /** Invalidate. */
    private boolean invalidate;

    /** Start size. */
    private int startSize;

    /** Off-heap max memory. */
    private long offHeapMaxMemory;

    /** Max concurrent async operations */
    private int maxConcurrentAsyncOps;

    /** Memory mode. */
    private CacheMemoryMode memoryMode;

    /** Cache interceptor. */
    private String interceptor;

    /** Cache affinityCfg config. */
    private VisorCacheAffinityConfiguration affinityCfg;

    /** Preload config. */
    private VisorCacheRebalanceConfiguration rebalanceCfg;

    /** Eviction config. */
    private VisorCacheEvictionConfiguration evictCfg;

    /** Near cache config. */
    private VisorCacheNearConfiguration nearCfg;

    /** Default config */
    private VisorCacheDefaultConfiguration dfltCfg;

    /** Store config */
    private VisorCacheStoreConfiguration storeCfg;

    /** Collection of type metadata. */
    private Collection<VisorCacheTypeMetadata> typeMeta;

    /** Whether statistics collection is enabled. */
    private boolean statisticsEnabled;

    /** Whether management is enabled. */
    private boolean mgmtEnabled;

    /** Class name of cache loader factory. */
    private String ldrFactory;

    /** Class name of cache writer factory. */
    private String writerFactory;

    /** Class name of expiry policy factory. */
    private String expiryPlcFactory;

    /** Query configuration. */
    private VisorCacheQueryConfiguration qryCfg;

    /** System cache flag. */
    private boolean sys;

    /**
     * @param ignite Grid.
     * @param ccfg Cache configuration.
     * @return Data transfer object for cache configuration properties.
     */
    public VisorCacheConfiguration from(IgniteEx ignite, CacheConfiguration ccfg) {
        name = ccfg.getName();
        mode = ccfg.getCacheMode();
        atomicityMode = ccfg.getAtomicityMode();
        atomicWriteOrderMode = ccfg.getAtomicWriteOrderMode();
        eagerTtl = ccfg.isEagerTtl();
        writeSynchronizationMode = ccfg.getWriteSynchronizationMode();
        swapEnabled = ccfg.isSwapEnabled();
        invalidate = ccfg.isInvalidate();
        startSize = ccfg.getStartSize();
        offHeapMaxMemory = ccfg.getOffHeapMaxMemory();
        maxConcurrentAsyncOps = ccfg.getMaxConcurrentAsyncOperations();
        memoryMode = ccfg.getMemoryMode();
        interceptor = compactClass(ccfg.getInterceptor());
        typeMeta = VisorCacheTypeMetadata.list(ccfg.getTypeMetadata());
        statisticsEnabled = ccfg.isStatisticsEnabled();
        mgmtEnabled = ccfg.isManagementEnabled();
        ldrFactory = compactClass(ccfg.getCacheLoaderFactory());
        writerFactory = compactClass(ccfg.getCacheWriterFactory());
        expiryPlcFactory = compactClass(ccfg.getExpiryPolicyFactory());
        sys = ignite.context().cache().systemCache(ccfg.getName());

        affinityCfg = VisorCacheAffinityConfiguration.from(ccfg);
        rebalanceCfg = VisorCacheRebalanceConfiguration.from(ccfg);
        evictCfg = VisorCacheEvictionConfiguration.from(ccfg);
        nearCfg = VisorCacheNearConfiguration.from(ccfg);
        dfltCfg = VisorCacheDefaultConfiguration.from(ccfg);
        storeCfg = VisorCacheStoreConfiguration.from(ignite, ccfg);
        qryCfg = VisorCacheQueryConfiguration.from(ccfg);

        return this;
    }

    /**
     * @return Cache name.
     */
    @Nullable public String name() {
        return name;
    }

    /**
     * @return Cache mode.
     */
    public CacheMode mode() {
        return mode;
    }

    /**
     * @return Cache atomicity mode
     */
    public CacheAtomicityMode atomicityMode() {
        return atomicityMode;
    }

    /**
     * @return Cache atomicity write ordering mode.
     */
    public CacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return atomicWriteOrderMode;
    }

    /**
     * @return Eager ttl flag
     */
    public boolean eagerTtl() {
        return eagerTtl;
    }

    /**
     * @return Write synchronization mode.
     */
    public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return writeSynchronizationMode;
    }

    /**
     * @return Swap enabled flag.
     */
    public boolean swapEnabled() {
        return swapEnabled;
    }

    /**
     * @return Invalidate.
     */
    public boolean invalidate() {
        return invalidate;
    }

    /**
     * @return Start size.
     */
    public int startSize() {
        return startSize;
    }

    /**
     * @return Off-heap max memory.
     */
    public long offsetHeapMaxMemory() {
        return offHeapMaxMemory;
    }

    /**
     * @return Max concurrent async operations
     */
    public int maxConcurrentAsyncOperations() {
        return maxConcurrentAsyncOps;
    }

    /**
     * @return Memory mode.
     */
    public CacheMemoryMode memoryMode() {
        return memoryMode;
    }

    /**
     * @return Cache interceptor.
     */
    @Nullable public String interceptor() {
        return interceptor;
    }

    /**
     * @return Collection of type metadata.
     */
    public Collection<VisorCacheTypeMetadata> typeMeta() {
        return typeMeta;
    }

    /**
     * @return {@code true} if cache statistics enabled.
     */
    public boolean statisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * @return Whether management is enabled.
     */
    public boolean managementEnabled() {
        return mgmtEnabled;
    }

    /**
     * @return Class name of cache loader factory.
     */
    public String loaderFactory() {
        return ldrFactory;
    }

    /**
     * @return Class name of cache writer factory.
     */
    public String writerFactory() {
        return writerFactory;
    }

    /**
     * @return Class name of expiry policy factory.
     */
    public String expiryPolicyFactory() {
        return expiryPlcFactory;
    }

    /**
     * @return Cache affinityCfg config.
     */
    public VisorCacheAffinityConfiguration affinityConfiguration() {
        return affinityCfg;
    }

    /**
     * @return Preload config.
     */
    public VisorCacheRebalanceConfiguration rebalanceConfiguration() {
        return rebalanceCfg;
    }

    /**
     * @return Eviction config.
     */
    public VisorCacheEvictionConfiguration evictConfiguration() {
        return evictCfg;
    }

    /**
     * @return Near cache config.
     */
    public VisorCacheNearConfiguration nearConfiguration() {
        return nearCfg;
    }

    /**
     * @return Dgc config
     */
    public VisorCacheDefaultConfiguration defaultConfiguration() {
        return dfltCfg;
    }

    /**
     * @return Store config
     */
    public VisorCacheStoreConfiguration storeConfiguration() {
        return storeCfg;
    }

    /**
     * @return Cache query configuration.
     */
    public VisorCacheQueryConfiguration queryConfiguration() {
        return qryCfg;
    }

    /**
     * @return System cache state.
     */
    public boolean system() {
        return sys;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheConfiguration.class, this);
    }
}
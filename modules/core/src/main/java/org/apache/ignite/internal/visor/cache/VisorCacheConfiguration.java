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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.jdbc.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.visor.node.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.*;

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

    /** Distribution mode. */
    private CacheDistributionMode distributionMode;

    /** Cache atomicity mode */
    private CacheAtomicityMode atomicityMode;

    /** Cache atomic sequence reserve size */
    private int atomicSeqReserveSize;

    /** Cache atomicity write ordering mode. */
    private CacheAtomicWriteOrderMode atomicWriteOrderMode;

    /** Eager ttl flag */
    private boolean eagerTtl;

    /** Write synchronization mode. */
    private CacheWriteSynchronizationMode writeSynchronizationMode;

    /** Sequence reserve size. */
    private int seqReserveSize;

    /** Swap enabled flag. */
    private boolean swapEnabled;

    /** Flag indicating whether Ignite should attempt to index value and/or key instances stored in cache. */
    private boolean qryIdxEnabled;

    /** Invalidate. */
    private boolean invalidate;

    /** Start size. */
    private int startSize;

    /** Name of class implementing GridCacheTmLookup. */
    private String tmLookupClsName;

    /** Off-heap max memory. */
    private long offHeapMaxMemory;

    /** Max query iterator count */
    private int maxQryIterCnt;

    /** Max concurrent async operations */
    private int maxConcurrentAsyncOps;

    /** Memory mode. */
    private CacheMemoryMode memoryMode;

    /** Name of SPI to use for indexing. */
    private String indexingSpiName;

    /** Cache interceptor. */
    private String interceptor;

    /** Cache affinity config. */
    private VisorCacheAffinityConfiguration affinity;

    /** Preload config. */
    private VisorCachePreloadConfiguration preload;

    /** Eviction config. */
    private VisorCacheEvictionConfiguration evict;

    /** Near cache config. */
    private VisorCacheNearConfiguration near;

    /** Default config */
    private VisorCacheDefaultConfiguration dflt;

    /** Store config */
    private VisorCacheStoreConfiguration store;

    /** Write behind config */
    private VisorCacheWriteBehindConfiguration writeBehind;

    /** Collection of type metadata. */
    private Collection<VisorCacheTypeMetadata> typeMeta;

    /** Whether cache has JDBC store. */
    private boolean jdbcStore;

    /** Whether cache should operate in read-through mode. */
    private boolean readThrough;

    /** Whether cache should operate in write-through mode. */
    private boolean writeThrough;

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

    /**
     * @param ignite Grid.
     * @param ccfg Cache configuration.
     * @return Data transfer object for cache configuration properties.
     */
    public static VisorCacheConfiguration from(Ignite ignite, CacheConfiguration ccfg) {
        GridCacheContext cctx = ((IgniteKernal)ignite).internalCache(ccfg.getName()).context();

        boolean jdbcStore = cctx.store().configuredStore() instanceof CacheAbstractJdbcStore;

        VisorCacheConfiguration cfg = new VisorCacheConfiguration();

        cfg.name(ccfg.getName());
        cfg.mode(ccfg.getCacheMode());
        cfg.distributionMode(ccfg.getDistributionMode());
        cfg.atomicityMode(ccfg.getAtomicityMode());
        cfg.atomicWriteOrderMode(ccfg.getAtomicWriteOrderMode());
        cfg.eagerTtl(ccfg.isEagerTtl());
        cfg.writeSynchronizationMode(ccfg.getWriteSynchronizationMode());
        cfg.swapEnabled(ccfg.isSwapEnabled());
        cfg.queryIndexEnabled(ccfg.isQueryIndexEnabled());
        cfg.invalidate(ccfg.isInvalidate());
        cfg.startSize(ccfg.getStartSize());
        cfg.transactionManagerLookupClassName(ccfg.getTransactionManagerLookupClassName());
        cfg.offsetHeapMaxMemory(ccfg.getOffHeapMaxMemory());
        cfg.maxQueryIteratorCount(ccfg.getMaximumQueryIteratorCount());
        cfg.maxConcurrentAsyncOperations(ccfg.getMaxConcurrentAsyncOperations());
        cfg.memoryMode(ccfg.getMemoryMode());
        cfg.indexingSpiName(ccfg.getIndexingSpiName());
        cfg.interceptor(compactClass(ccfg.getInterceptor()));
        cfg.affinityConfiguration(VisorCacheAffinityConfiguration.from(ccfg));
        cfg.preloadConfiguration(VisorCachePreloadConfiguration.from(ccfg));
        cfg.evictConfiguration(VisorCacheEvictionConfiguration.from(ccfg));
        cfg.nearConfiguration(VisorCacheNearConfiguration.from(ccfg));
        cfg.defaultConfiguration(VisorCacheDefaultConfiguration.from(ccfg));
        cfg.storeConfiguration(VisorCacheStoreConfiguration.from(ccfg));
        cfg.writeBehind(VisorCacheWriteBehindConfiguration.from(ccfg));

        cfg.typeMeta(VisorCacheTypeMetadata.list(ccfg.getTypeMetadata()));
        cfg.jdbcStore(jdbcStore);

        cfg.readThrough(ccfg.isReadThrough());
        cfg.writeThrough(ccfg.isWriteThrough());
        cfg.statisticsEnabled(ccfg.isStatisticsEnabled());
        cfg.managementEnabled(ccfg.isManagementEnabled());
        cfg.loaderFactory(compactClass(ccfg.getCacheLoaderFactory()));
        cfg.writerFactory(compactClass(ccfg.getCacheWriterFactory()));
        cfg.expiryPolicyFactory(compactClass(ccfg.getExpiryPolicyFactory()));

        cfg.queryConfiguration(VisorCacheQueryConfiguration.from(ccfg.getQueryConfiguration()));

        return cfg;
    }

    /**
     * @param ignite Grid.
     * @param caches Cache configurations.
     * @return Data transfer object for cache configurations properties.
     */
    public static Iterable<VisorCacheConfiguration> list(Ignite ignite, CacheConfiguration[] caches) {
        if (caches == null)
            return Collections.emptyList();

        final Collection<VisorCacheConfiguration> cfgs = new ArrayList<>(caches.length);

        for (CacheConfiguration cache : caches)
            cfgs.add(from(ignite, cache));

        return cfgs;
    }

    /**
     * @return Cache name.
     */
    @Nullable public String name() {
        return name;
    }

    /**
     * @param name New cache name.
     */
    public void name(@Nullable String name) {
        this.name = name;
    }

    /**
     * @return Cache mode.
     */
    public CacheMode mode() {
        return mode;
    }

    /**
     * @param mode New cache mode.
     */
    public void mode(CacheMode mode) {
        this.mode = mode;
    }

    /**
     * @return Distribution mode.
     */
    public CacheDistributionMode distributionMode() {
        return distributionMode;
    }

    /**
     * @param distributionMode New distribution mode.
     */
    public void distributionMode(CacheDistributionMode distributionMode) {
        this.distributionMode = distributionMode;
    }

    /**
     * @return Cache atomicity mode
     */
    public CacheAtomicityMode atomicityMode() {
        return atomicityMode;
    }

    /**
     * @param atomicityMode New cache atomicity mode
     */
    public void atomicityMode(CacheAtomicityMode atomicityMode) {
        this.atomicityMode = atomicityMode;
    }

    /**
     * @return Cache atomic sequence reserve size
     */
    public int atomicSequenceReserveSize() {
        return atomicSeqReserveSize;
    }

    /**
     * @param atomicSeqReserveSize New cache atomic sequence reserve size
     */
    public void atomicSequenceReserveSize(int atomicSeqReserveSize) {
        this.atomicSeqReserveSize = atomicSeqReserveSize;
    }

    /**
     * @return Cache atomicity write ordering mode.
     */
    public CacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return atomicWriteOrderMode;
    }

    /**
     * @param atomicWriteOrderMode New cache atomicity write ordering mode.
     */
    public void atomicWriteOrderMode(CacheAtomicWriteOrderMode atomicWriteOrderMode) {
        this.atomicWriteOrderMode = atomicWriteOrderMode;
    }

    /**
     * @return Eager ttl flag
     */
    public boolean eagerTtl() {
        return eagerTtl;
    }

    /**
     * @param eagerTtl New eager ttl flag
     */
    public void eagerTtl(boolean eagerTtl) {
        this.eagerTtl = eagerTtl;
    }

    /**
     * @return Write synchronization mode.
     */
    public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return writeSynchronizationMode;
    }

    /**
     * @param writeSynchronizationMode New write synchronization mode.
     */
    public void writeSynchronizationMode(CacheWriteSynchronizationMode writeSynchronizationMode) {
        this.writeSynchronizationMode = writeSynchronizationMode;
    }

    /**
     * @return Sequence reserve size.
     */
    public int sequenceReserveSize() {
        return seqReserveSize;
    }

    /**
     * @param seqReserveSize New sequence reserve size.
     */
    public void sequenceReserveSize(int seqReserveSize) {
        this.seqReserveSize = seqReserveSize;
    }

    /**
     * @return Swap enabled flag.
     */
    public boolean swapEnabled() {
        return swapEnabled;
    }

    /**
     * @param swapEnabled New swap enabled flag.
     */
    public void swapEnabled(boolean swapEnabled) {
        this.swapEnabled = swapEnabled;
    }

    /**
     * @return Flag indicating whether Ignite should attempt to index value and/or key instances stored in cache.
     */
    public boolean queryIndexEnabled() {
        return qryIdxEnabled;
    }

    /**
     * @param qryIdxEnabled New flag indicating whether Ignite should attempt to index value and/or key instances stored
     * in cache.
     */
    public void queryIndexEnabled(boolean qryIdxEnabled) {
        this.qryIdxEnabled = qryIdxEnabled;
    }

    /**
     * @return Invalidate.
     */
    public boolean invalidate() {
        return invalidate;
    }

    /**
     * @param invalidate New invalidate.
     */
    public void invalidate(boolean invalidate) {
        this.invalidate = invalidate;
    }

    /**
     * @return Start size.
     */
    public int startSize() {
        return startSize;
    }

    /**
     * @param startSize New start size.
     */
    public void startSize(int startSize) {
        this.startSize = startSize;
    }

    /**
     * @return Name of class implementing GridCacheTmLookup.
     */
    @Nullable public String transactionManagerLookupClassName() {
        return tmLookupClsName;
    }

    /**
     * @param tmLookupClsName New name of class implementing GridCacheTmLookup.
     */
    public void transactionManagerLookupClassName(@Nullable String tmLookupClsName) {
        this.tmLookupClsName = tmLookupClsName;
    }

    /**
     * @return Off-heap max memory.
     */
    public long offsetHeapMaxMemory() {
        return offHeapMaxMemory;
    }

    /**
     * @param offHeapMaxMemory New off-heap max memory.
     */
    public void offsetHeapMaxMemory(long offHeapMaxMemory) {
        this.offHeapMaxMemory = offHeapMaxMemory;
    }

    /**
     * @return Max query iterator count
     */
    public int maxQueryIteratorCount() {
        return maxQryIterCnt;
    }

    /**
     * @param maxQryIterCnt New max query iterator count
     */
    public void maxQueryIteratorCount(int maxQryIterCnt) {
        this.maxQryIterCnt = maxQryIterCnt;
    }

    /**
     * @return Max concurrent async operations
     */
    public int maxConcurrentAsyncOperations() {
        return maxConcurrentAsyncOps;
    }

    /**
     * @param maxConcurrentAsyncOps New max concurrent async operations
     */
    public void maxConcurrentAsyncOperations(int maxConcurrentAsyncOps) {
        this.maxConcurrentAsyncOps = maxConcurrentAsyncOps;
    }

    /**
     * @return Memory mode.
     */
    public CacheMemoryMode memoryMode() {
        return memoryMode;
    }

    /**
     * @param memoryMode New memory mode.
     */
    public void memoryMode(CacheMemoryMode memoryMode) {
        this.memoryMode = memoryMode;
    }

    /**
     * @return Name of SPI to use for indexing.
     */
    public String indexingSpiName() {
        return indexingSpiName;
    }

    /**
     * @param indexingSpiName New name of SPI to use for indexing.
     */
    public void indexingSpiName(String indexingSpiName) {
        this.indexingSpiName = indexingSpiName;
    }

    /**
     * @return Cache interceptor.
     */
    @Nullable public String interceptor() {
        return interceptor;
    }

    /**
     * @param interceptor New cache interceptor.
     */
    public void interceptor(@Nullable String interceptor) {
        this.interceptor = interceptor;
    }

    /**
     * @return Cache affinity config.
     */
    public VisorCacheAffinityConfiguration affinityConfiguration() {
        return affinity;
    }

    /**
     * @param affinity New cache affinity config.
     */
    public void affinityConfiguration(VisorCacheAffinityConfiguration affinity) {
        this.affinity = affinity;
    }

    /**
     * @return Preload config.
     */
    public VisorCachePreloadConfiguration preloadConfiguration() {
        return preload;
    }

    /**
     * @param preload New preload config.
     */
    public void preloadConfiguration(VisorCachePreloadConfiguration preload) {
        this.preload = preload;
    }

    /**
     * @return Eviction config.
     */
    public VisorCacheEvictionConfiguration evictConfiguration() {
        return evict;
    }

    /**
     * @param evict New eviction config.
     */
    public void evictConfiguration(VisorCacheEvictionConfiguration evict) {
        this.evict = evict;
    }

    /**
     * @return Near cache config.
     */
    public VisorCacheNearConfiguration nearConfiguration() {
        return near;
    }

    /**
     * @param near New near cache config.
     */
    public void nearConfiguration(VisorCacheNearConfiguration near) {
        this.near = near;
    }

    /**
     * @return Dgc config
     */
    public VisorCacheDefaultConfiguration defaultConfiguration() {
        return dflt;
    }

    /**
     * @param dflt New default config
     */
    public void defaultConfiguration(VisorCacheDefaultConfiguration dflt) {
        this.dflt = dflt;
    }

    /**
     * @return Store config
     */
    public VisorCacheStoreConfiguration storeConfiguration() {
        return store;
    }

    /**
     * @param store New store config
     */
    public void storeConfiguration(VisorCacheStoreConfiguration store) {
        this.store = store;
    }

    /**
     * @return Write behind config
     */
    public VisorCacheWriteBehindConfiguration writeBehind() {
        return writeBehind;
    }

    /**
     * @param writeBehind New write behind config
     */
    public void writeBehind(VisorCacheWriteBehindConfiguration writeBehind) {
        this.writeBehind = writeBehind;
    }

    /**
     * @param typeMeta New collection of type metadata.
     */
    public void typeMeta(Collection<VisorCacheTypeMetadata> typeMeta) {
        this.typeMeta = typeMeta;
    }

    /**
     * @return Collection of type metadata.
     */
    public Collection<VisorCacheTypeMetadata> typeMeta() {
        return typeMeta;
    }

    /**
     * @return {@code true} if cache has JDBC store.
     */
    public boolean jdbcStore() {
        return jdbcStore;
    }

    /**
     * @param jdbcStore {@code true} if cache has JDBC store.
     */
    public void jdbcStore(boolean jdbcStore) {
        this.jdbcStore = jdbcStore;
    }

    /**
     * @return Whether cache should operate in read-through mode.
     */
    public boolean readThrough() {
        return readThrough;
    }

    /**
     * @param readThrough New whether cache should operate in read-through mode.
     */
    public void readThrough(boolean readThrough) {
        this.readThrough = readThrough;
    }

    /**
     * @return Whether cache should operate in write-through mode.
     */
    public boolean writeThrough() {
        return writeThrough;
    }

    /**
     * @param writeThrough New whether cache should operate in write-through mode.
     */
    public void writeThrough(boolean writeThrough) {
        this.writeThrough = writeThrough;
    }

    /**
     * @return {@code true} if cache statistics enabled.
     */
    public boolean statisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * @param statisticsEnabled {@code true} if cache statistics enabled.
     */
    public void statisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
    }

    /**
     * @return Whether management is enabled.
     */
    public boolean managementEnabled() {
        return mgmtEnabled;
    }

    /**
     * @param mgmtEnabled New whether management is enabled.
     */
    public void managementEnabled(boolean mgmtEnabled) {
        this.mgmtEnabled = mgmtEnabled;
    }

    /**
     * @return Class name of cache loader factory.
     */
    public String loaderFactory() {
        return ldrFactory;
    }

    /**
     * @param ldrFactory New class name of cache loader factory.
     */
    public void loaderFactory(String ldrFactory) {
        this.ldrFactory = ldrFactory;
    }

    /**
     * @return Class name of cache writer factory.
     */
    public String writerFactory() {
        return writerFactory;
    }

    /**
     * @param writerFactory New class name of cache writer factory.
     */
    public void writerFactory(String writerFactory) {
        this.writerFactory = writerFactory;
    }

    /**
     * @return Class name of expiry policy factory.
     */
    public String expiryPolicyFactory() {
        return expiryPlcFactory;
    }

    /**
     * @param expiryPlcFactory New class name of expiry policy factory.
     */
    public void expiryPolicyFactory(String expiryPlcFactory) {
        this.expiryPlcFactory = expiryPlcFactory;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheConfiguration.class, this);
    }

    /**
     * @return Cache query configuration.
     */
    public VisorCacheQueryConfiguration queryConfiguration() {
        return qryCfg;
    }

    /**
     * @param qryCfg New cache query configuration.
     */
    public void queryConfiguration(VisorCacheQueryConfiguration qryCfg) {
        this.qryCfg = qryCfg;
    }
}

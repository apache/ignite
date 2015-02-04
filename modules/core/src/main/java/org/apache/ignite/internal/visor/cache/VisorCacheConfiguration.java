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

import org.apache.ignite.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;
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
    private int atomicSequenceReserveSize;

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
    private boolean queryIndexEnabled;

    /** Flag indicating whether to persist once on commit, or after every operation. */
    private boolean batchUpdateOnCommit;

    /** Invalidate. */
    private boolean invalidate;

    /** Start size. */
    private int startSize;

    /** Cloner. */
    private String cloner;

    /** Name of class implementing GridCacheTmLookup. */
    private String tmLookupClsName;

    /** Flag to enable/disable transaction serializable isolation level. */
    private boolean txSerializableEnabled;

    /** Off-heap max memory. */
    private long offHeapMaxMemory;

    /** Max query iterator count */
    private int maxQueryIteratorCnt;

    /** Max concurrent async operations */
    private int maxConcurrentAsyncOps;

    /** Pessimistic tx logger size */
    private int pessimisticTxLogSize;

    /** Pessimistic tx logger linger. */
    private int pessimisticTxLogLinger;

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

    /** Dgc config */
    private VisorCacheDgcConfiguration dgc;

    /** Store config */
    private VisorCacheStoreConfiguration store;

    /** Write behind config */
    private VisorCacheWriteBehindConfiguration writeBehind;

    /**
     * @param ccfg Cache configuration.
     * @return Data transfer object for cache configuration properties.
     */
    public static VisorCacheConfiguration from(CacheConfiguration ccfg) {
        // TODO gg-9141 Update Visor.

        VisorCacheConfiguration cfg = new VisorCacheConfiguration();

        cfg.name(ccfg.getName());
        cfg.mode(ccfg.getCacheMode());
        cfg.distributionMode(ccfg.getDistributionMode());
        cfg.atomicityMode(ccfg.getAtomicityMode());
        cfg.atomicSequenceReserveSize(ccfg.getAtomicSequenceReserveSize());
        cfg.atomicWriteOrderMode(ccfg.getAtomicWriteOrderMode());
        cfg.atomicSequenceReserveSize(ccfg.getAtomicSequenceReserveSize());
        cfg.eagerTtl(ccfg.isEagerTtl());
        cfg.writeSynchronizationMode(ccfg.getWriteSynchronizationMode());
        cfg.swapEnabled(ccfg.isSwapEnabled());
        cfg.queryIndexEnabled(ccfg.isQueryIndexEnabled());
//        cfg.batchUpdateOnCommit(ccfg.isBatchUpdateOnCommit());
        cfg.invalidate(ccfg.isInvalidate());
        cfg.startSize(ccfg.getStartSize());
        cfg.cloner(compactClass(ccfg.getCloner()));
        cfg.transactionManagerLookupClassName(ccfg.getTransactionManagerLookupClassName());
//        cfg.txSerializableEnabled(ccfg.isTxSerializableEnabled());
        cfg.offsetHeapMaxMemory(ccfg.getOffHeapMaxMemory());
        cfg.maxQueryIteratorCount(ccfg.getMaximumQueryIteratorCount());
        cfg.maxConcurrentAsyncOperations(ccfg.getMaxConcurrentAsyncOperations());
//        cfg.pessimisticTxLoggerSize(ccfg.getPessimisticTxLogSize());
//        cfg.pessimisticTxLoggerLinger(ccfg.getPessimisticTxLogLinger());
        cfg.memoryMode(ccfg.getMemoryMode());
        cfg.indexingSpiName(ccfg.getIndexingSpiName());
        cfg.interceptor(compactClass(ccfg.getInterceptor()));
        cfg.affinityConfiguration(VisorCacheAffinityConfiguration.from(ccfg));
        cfg.preloadConfiguration(VisorCachePreloadConfiguration.from(ccfg));
        cfg.evictConfiguration(VisorCacheEvictionConfiguration.from(ccfg));
        cfg.nearConfiguration(VisorCacheNearConfiguration.from(ccfg));
        cfg.defaultConfiguration(VisorCacheDefaultConfiguration.from(ccfg));
        cfg.dgcConfiguration(VisorCacheDgcConfiguration.from(ccfg));
        cfg.storeConfiguration(VisorCacheStoreConfiguration.from(ccfg));
        cfg.writeBehind(VisorCacheWriteBehindConfiguration.from(ccfg));

        return cfg;
    }

    /**
     * @param caches Cache configurations.
     * @return Data transfer object for cache configurations properties.
     */
    public static Iterable<VisorCacheConfiguration> list(CacheConfiguration[] caches) {
        if (caches == null)
            return Collections.emptyList();

        final Collection<VisorCacheConfiguration> cfgs = new ArrayList<>(caches.length);

        for (CacheConfiguration cache : caches)
            cfgs.add(from(cache));

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
        return atomicSequenceReserveSize;
    }

    /**
     * @param atomicSeqReserveSize New cache atomic sequence reserve size
     */
    public void atomicSequenceReserveSize(int atomicSeqReserveSize) {
        atomicSequenceReserveSize = atomicSeqReserveSize;
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
        return queryIndexEnabled;
    }

    /**
     * @param qryIdxEnabled New flag indicating whether Ignite should attempt to index value and/or key instances
     * stored in cache.
     */
    public void queryIndexEnabled(boolean qryIdxEnabled) {
        queryIndexEnabled = qryIdxEnabled;
    }

    /**
     * @return Flag indicating whether to persist once on commit, or after every operation.
     */
    public boolean batchUpdateOnCommit() {
        return batchUpdateOnCommit;
    }

    /**
     * @param batchUpdateOnCommit New batch update on commit.
     */
    public void batchUpdateOnCommit(boolean batchUpdateOnCommit) {
        this.batchUpdateOnCommit = batchUpdateOnCommit;
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
     * @return Cloner.
     */
    @Nullable public String cloner() {
        return cloner;
    }

    /**
     * @param cloner New cloner.
     */
    public void cloner(@Nullable String cloner) {
        this.cloner = cloner;
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
     * @return Flag to enable/disable transaction serializable isolation level.
     */
    public boolean txSerializableEnabled() {
        return txSerializableEnabled;
    }

    /**
     * @param txSerEnabled New flag to enable/disable transaction serializable isolation level.
     */
    public void txSerializableEnabled(boolean txSerEnabled) {
        txSerializableEnabled = txSerEnabled;
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
        return maxQueryIteratorCnt;
    }

    /**
     * @param maxQryIterCnt New max query iterator count
     */
    public void maxQueryIteratorCount(int maxQryIterCnt) {
        maxQueryIteratorCnt = maxQryIterCnt;
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
     * @return Pessimistic tx logger size
     */
    public int pessimisticTxLoggerSize() {
        return pessimisticTxLogSize;
    }

    /**
     * @param pessimisticTxLogSize New pessimistic tx logger size
     */
    public void pessimisticTxLoggerSize(int pessimisticTxLogSize) {
        this.pessimisticTxLogSize = pessimisticTxLogSize;
    }

    /**
     * @return Pessimistic tx logger linger.
     */
    public int pessimisticTxLoggerLinger() {
        return pessimisticTxLogLinger;
    }

    /**
     * @param pessimisticTxLogLinger New pessimistic tx logger linger.
     */
    public void pessimisticTxLoggerLinger(int pessimisticTxLogLinger) {
        this.pessimisticTxLogLinger = pessimisticTxLogLinger;
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
     * @return Dgc config
     */
    public VisorCacheDgcConfiguration dgcConfiguration() {
        return dgc;
    }

    /**
     * @param dgc New dgc config
     */
    public void dgcConfiguration(VisorCacheDgcConfiguration dgc) {
        this.dgc = dgc;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheConfiguration.class, this);
    }
}

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

import java.util.Collection;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cluster.CacheMetricsMessage;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Metrics snapshot.
 */
public class CacheMetricsSnapshot extends IgniteDataTransferObject implements CacheMetrics {
    /** */
    private static final long serialVersionUID = 0;

    /** Number of reads. */
    long cacheGets;

    /** Number of puts. */
    long cachePuts;

    /** Number of invokes caused updates. */
    long entryProcessorPuts;

    /** Number of invokes caused no updates. */
    long entryProcessorReadOnlyInvocations;

    /** The mean time to execute cache invokes */
    float entryProcessorAverageInvocationTime;

    /** The total number of cache invocations. */
    long entryProcessorInvocations;

    /** The total number of cache invocations, caused removal. */
    long entryProcessorRemovals;

    /** The total number of invocations on keys, which don't exist in cache. */
    long entryProcessorMisses;

    /** The total number of invocations on keys, which exist in cache. */
    long entryProcessorHits;

    /** The percentage of invocations on keys, which don't exist in cache. */
    float entryProcessorMissPercentage;

    /** The percentage of invocations on keys, which exist in cache. */
    float entryProcessorHitPercentage;

    /** So far, the maximum time to execute cache invokes. */
    float entryProcessorMaxInvocationTime;

    /** So far, the minimum time to execute cache invokes. */
    float entryProcessorMinInvocationTime;

    /** Number of hits. */
    long cacheHits;

    /** Number of misses. */
    long cacheMisses;

    /** Number of transaction commits. */
    long cacheTxCommits;

    /** Number of transaction rollbacks. */
    long cacheTxRollbacks;

    /** Number of evictions. */
    long cacheEvictions;

    /** Number of removed entries. */
    long cacheRemovals;

    /** Put time taken nanos. */
    float averagePutTime;

    /** Get time taken nanos. */
    float averageGetTime;

    /** Remove time taken nanos. */
    float averageRemoveTime;

    /** Commit transaction time taken nanos. */
    float averageTxCommitTime;

    /** Commit transaction time taken nanos. */
    float averageTxRollbackTime;

    /** Cache name */
    String cacheName;

    /** Number of reads from off-heap. */
    long offHeapGets;

    /** Number of writes to off-heap. */
    long offHeapPuts;

    /** Number of removed entries from off-heap. */
    long offHeapRemoves;

    /** Number of evictions from off-heap. */
    long offHeapEvicts;

    /** Off-heap hits number. */
    long offHeapHits;

    /** Off-heap misses number. */
    long offHeapMisses;

    /** Number of entries stored in off-heap memory. */
    long offHeapEntriesCnt;

    /** Number of entries stored in heap. */
    long heapEntriesCnt;

    /** Number of primary entries stored in off-heap memory. */
    long offHeapPrimaryEntriesCnt;

    /** Number of backup entries stored in off-heap memory. */
    long offHeapBackupEntriesCnt;

    /** Memory size allocated in off-heap. */
    long offHeapAllocatedSize;

    /** Number of non-{@code null} values in the cache. */
    int size;

    /** Cache size. */
    long cacheSize;

    /** Number of keys in the cache, possibly with {@code null} values. */
    int keySize;

    /** Cache is empty. */
    boolean empty;

    /** Gets current size of evict queue used to batch up evictions. */
    int dhtEvictQueueCurrSize;

    /** Transaction per-thread map size. */
    int txThreadMapSize;

    /** Transaction per-Xid map size. */
    int txXidMapSize;

    /** Committed transaction queue size. */
    int txCommitQueueSize;

    /** Prepared transaction queue size. */
    int txPrepareQueueSize;

    /** Start version counts map size. */
    int txStartVerCountsSize;

    /** Number of cached committed transaction IDs. */
    int txCommittedVersionsSize;

    /** Number of cached rolled back transaction IDs. */
    int txRolledbackVersionsSize;

    /** DHT thread map size. */
    int txDhtThreadMapSize;

    /** Transaction DHT per-Xid map size. */
    int txDhtXidMapSize;

    /** Committed DHT transaction queue size. */
    int txDhtCommitQueueSize;

    /** Prepared DHT transaction queue size. */
    int txDhtPrepareQueueSize;

    /** DHT start version counts map size. */
    int txDhtStartVerCountsSize;

    /** Number of cached committed DHT transaction IDs. */
    int txDhtCommittedVersionsSize;

    /** Number of cached rolled back DHT transaction IDs. */
    int txDhtRolledbackVersionsSize;

    /** Write-behind is enabled. */
    boolean writeBehindEnabled;

    /** Buffer size that triggers flush procedure. */
    int writeBehindFlushSize;

    /** Count of worker threads. */
    int writeBehindFlushThreadCnt;

    /** Flush frequency in milliseconds. */
    long writeBehindFlushFreq;

    /** Maximum size of batch. */
    int writeBehindStoreBatchSize;

    /** Count of cache overflow events since start. */
    int writeBehindTotalCriticalOverflowCnt;

    /** Count of cache overflow events since start. */
    int writeBehindCriticalOverflowCnt;

    /** Count of entries in store-retry state. */
    int writeBehindErrorRetryCnt;

    /** Total count of entries in cache store internal buffer. */
    int writeBehindBufSize;

    /** Total partitions count. */
    int totalPartitionsCnt;

    /** Rebalancing partitions count. */
    int rebalancingPartitionsCnt;

    /** Number of already rebalanced keys. */
    long rebalancedKeys;

    /** Number estimated to rebalance keys. */
    long estimatedRebalancingKeys;

    /** Keys to rebalance left. */
    long keysToRebalanceLeft;

    /** Rebalancing keys rate. */
    long rebalancingKeysRate;

    /** Get rebalancing bytes rate. */
    long rebalancingBytesRate;

    /** Start rebalance time. */
    long rebalanceStartTime;

    /** Estimate rebalance finish time. */
    long rebalanceFinishTime;

    /** The number of clearing partitions need to await before rebalance. */
    long rebalanceClearingPartitionsLeft;

    /** */
    String keyType;

    /** */
    String valType;

    /** */
    boolean storeByVal;

    /** */
    boolean statisticsEnabled;

    /** */
    boolean managementEnabled;

    /** */
    boolean readThrough;

    /** */
    boolean writeThrough;

    /** */
    boolean validForReading;

    /** */
    boolean validForWriting;

    /** Tx key collisions with appropriate queue size string representation. */
    String txKeyCollisions;

    /** Index rebuilding in progress. */
    boolean idxRebuildInProgress;

    /** Number of keys processed idxRebuildInProgressduring index rebuilding. */
    long idxRebuildKeysProcessed;

    /** The number of local node partitions that remain to be processed to complete indexing. */
    int idxBuildPartitionsLeftCount;

    /**
     * Default constructor.
     */
    public CacheMetricsSnapshot() {
        this(new CacheMetricsMessage());
    }

    /**
     * Create snapshot for given metrics.
     *
     * @param cacheMetrics Cache metrics.
     */
    public CacheMetricsSnapshot(CacheMetrics cacheMetrics) {
        this(new CacheMetricsMessage(cacheMetrics));
    }

    /**
     * Constructs merged cache metrics.
     *
     * @param loc Metrics for cache on local node.
     * @param metrics Metrics for merge.
     */
    public CacheMetricsSnapshot(CacheMetrics loc, Collection<CacheMetrics> metrics) {
        this(new CacheMetricsMessage(loc, metrics));
    }

    /**
     * Create snapshot for given metrics message.
     *
     * @param m Cache metrics message.
     */
    public CacheMetricsSnapshot(CacheMetricsMessage m) {
        cacheGets = m.cacheGets();
        cachePuts = m.cachePuts();
        entryProcessorPuts = m.entryProcessorPuts();
        entryProcessorReadOnlyInvocations = m.entryProcessorReadOnlyInvocations();
        entryProcessorAverageInvocationTime = m.entryProcessorAverageInvocationTime();
        entryProcessorInvocations = m.entryProcessorInvocations();
        entryProcessorRemovals = m.entryProcessorRemovals();
        entryProcessorMisses = m.entryProcessorMisses();
        entryProcessorHits = m.entryProcessorHits();
        entryProcessorMissPercentage = m.entryProcessorMissPercentage();
        entryProcessorHitPercentage = m.entryProcessorHitPercentage();
        entryProcessorMaxInvocationTime = m.entryProcessorMaxInvocationTime();
        entryProcessorMinInvocationTime = m.entryProcessorMinInvocationTime();
        cacheHits = m.cacheHits();
        cacheMisses = m.cacheMisses();
        cacheTxCommits = m.cacheTxCommits();
        cacheTxRollbacks = m.cacheTxRollbacks();
        cacheEvictions = m.cacheEvictions();
        cacheRemovals = m.cacheRemovals();
        averagePutTime = m.averagePutTime();
        averageGetTime = m.averageGetTime();
        averageRemoveTime = m.averageRemoveTime();
        averageTxCommitTime = m.averageTxCommitTime();
        averageTxRollbackTime = m.averageTxRollbackTime();
        cacheName = m.cacheName();
        offHeapGets = m.offHeapGets();
        offHeapPuts = m.offHeapPuts();
        offHeapRemoves = m.offHeapRemovals();
        offHeapEvicts = m.offHeapEvictions();
        offHeapHits = m.offHeapHits();
        offHeapMisses = m.offHeapMisses();
        offHeapEntriesCnt = m.offHeapEntriesCount();
        heapEntriesCnt = m.heapEntriesCount();
        offHeapPrimaryEntriesCnt = m.offHeapPrimaryEntriesCount();
        offHeapBackupEntriesCnt = m.offHeapBackupEntriesCount();
        offHeapAllocatedSize = m.offHeapAllocatedSize();
        size = m.size();
        cacheSize = m.cacheSize();
        keySize = m.keySize();
        empty = m.empty();
        dhtEvictQueueCurrSize = m.dhtEvictQueueCurrentSize();
        txThreadMapSize = m.txThreadMapSize();
        txXidMapSize = m.txXidMapSize();
        txCommitQueueSize = m.txCommitQueueSize();
        txPrepareQueueSize = m.txPrepareQueueSize();
        txStartVerCountsSize = m.txStartVersionCountsSize();
        txCommittedVersionsSize = m.txCommittedVersionsSize();
        txRolledbackVersionsSize = m.txRolledbackVersionsSize();
        txDhtThreadMapSize = m.txDhtThreadMapSize();
        txDhtXidMapSize = m.txDhtXidMapSize();
        txDhtCommitQueueSize = m.txDhtCommitQueueSize();
        txDhtPrepareQueueSize = m.txDhtPrepareQueueSize();
        txDhtStartVerCountsSize = m.txDhtStartVersionCountsSize();
        txDhtCommittedVersionsSize = m.txDhtCommittedVersionsSize();
        txDhtRolledbackVersionsSize = m.txDhtRolledbackVersionsSize();
        writeBehindEnabled = m.writeBehindEnabled();
        writeBehindFlushSize = m.writeBehindFlushSize();
        writeBehindFlushThreadCnt = m.writeBehindFlushThreadCount();
        writeBehindFlushFreq = m.writeBehindFlushFrequency();
        writeBehindStoreBatchSize = m.writeBehindStoreBatchSize();
        writeBehindTotalCriticalOverflowCnt = m.writeBehindTotalCriticalOverflowCount();
        writeBehindCriticalOverflowCnt = m.writeBehindCriticalOverflowCount();
        writeBehindErrorRetryCnt = m.writeBehindErrorRetryCount();
        writeBehindBufSize = m.writeBehindBufferSize();
        totalPartitionsCnt = m.totalPartitionsCount();
        rebalancingPartitionsCnt = m.rebalancingPartitionsCount();
        rebalancedKeys = m.rebalancedKeys();
        estimatedRebalancingKeys = m.estimatedRebalancingKeys();
        keysToRebalanceLeft = m.keysToRebalanceLeft();
        rebalancingKeysRate = m.rebalancingKeysRate();
        rebalancingBytesRate = m.rebalancingBytesRate();
        rebalanceStartTime = m.rebalancingStartTime();
        rebalanceFinishTime = m.rebalanceFinishTime();
        rebalanceClearingPartitionsLeft = m.rebalanceClearingPartitionsLeft();
        keyType = m.keyType();
        valType = m.valueType();
        storeByVal = m.storeByValue();
        statisticsEnabled = m.statisticsEnabled();
        managementEnabled = m.managementEnabled();
        readThrough = m.readThrough();
        writeThrough = m.writeThrough();
        validForReading = m.validForReading();
        validForWriting = m.validForWriting();
        txKeyCollisions = m.txKeyCollisions();
        idxRebuildInProgress = m.indexRebuildInProgress();
        idxRebuildKeysProcessed = m.indexRebuildKeysProcessed();
        idxBuildPartitionsLeftCount = m.indexBuildPartitionsLeftCount();
    }

    /** {@inheritDoc} */
    @Override public long getCacheHits() {
        return cacheHits;
    }

    /** {@inheritDoc} */
    @Override public float getCacheHitPercentage() {
        if (cacheHits == 0 || cacheGets == 0)
            return 0;

        return (float)cacheHits / cacheGets * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getCacheMisses() {
        return cacheMisses;
    }

    /** {@inheritDoc} */
    @Override public float getCacheMissPercentage() {
        if (cacheMisses == 0 || cacheGets == 0)
            return 0;

        return (float)cacheMisses / cacheGets * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getCacheGets() {
        return cacheGets;
    }

    /** {@inheritDoc} */
    @Override public long getCachePuts() {
        return cachePuts;
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorPuts() {
        return entryProcessorPuts;
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorReadOnlyInvocations() {
        return entryProcessorReadOnlyInvocations;
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorInvocations() {
        return entryProcessorInvocations;
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorHits() {
        return entryProcessorHits;
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorHitPercentage() {
        return entryProcessorHitPercentage;
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorMissPercentage() {
        return entryProcessorMissPercentage;
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorMisses() {
        return entryProcessorMisses;
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorRemovals() {
        return entryProcessorRemovals;
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorAverageInvocationTime() {
        return entryProcessorAverageInvocationTime;
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorMinInvocationTime() {
        return entryProcessorMinInvocationTime;
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorMaxInvocationTime() {
        return entryProcessorMaxInvocationTime;
    }

    /** {@inheritDoc} */
    @Override public long getCacheRemovals() {
        return cacheRemovals;
    }

    /** {@inheritDoc} */
    @Override public long getCacheEvictions() {
        return cacheEvictions;
    }

    /** {@inheritDoc} */
    @Override public float getAverageGetTime() {
        return averageGetTime;
    }

    /** {@inheritDoc} */
    @Override public float getAveragePutTime() {
        return averagePutTime;
    }

    /** {@inheritDoc} */
    @Override public float getAverageRemoveTime() {
        return averageRemoveTime;
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxCommitTime() {
        return averageTxCommitTime;
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxRollbackTime() {
        return averageTxRollbackTime;
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxCommits() {
        return cacheTxCommits;
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxRollbacks() {
        return cacheTxRollbacks;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapGets() {
        return offHeapGets;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapPuts() {
        return offHeapPuts;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapRemovals() {
        return offHeapRemoves;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapEvictions() {
        return offHeapEvicts;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapHits() {
        return offHeapHits;
    }

    /** {@inheritDoc} */
    @Override public float getOffHeapHitPercentage() {
        if (offHeapHits == 0 || offHeapGets == 0)
            return 0;

        return (float)offHeapHits / offHeapGets * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapMisses() {
        return offHeapMisses;
    }

    /** {@inheritDoc} */
    @Override public float getOffHeapMissPercentage() {
        if (offHeapMisses == 0 || offHeapGets == 0)
            return 0;

        return (float)offHeapMisses / offHeapGets * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapEntriesCount() {
        return offHeapEntriesCnt;
    }

    /** {@inheritDoc} */
    @Override public long getHeapEntriesCount() {
        return heapEntriesCnt;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapPrimaryEntriesCount() {
        return offHeapPrimaryEntriesCnt;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapBackupEntriesCount() {
        return offHeapBackupEntriesCnt;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapAllocatedSize() {
        return offHeapAllocatedSize;
    }

    /** {@inheritDoc} */
    @Override public int getSize() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public long getCacheSize() {
        return cacheSize;
    }

    /** {@inheritDoc} */
    @Override public int getKeySize() {
        return keySize;
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return empty;
    }

    /** {@inheritDoc} */
    @Override public int getDhtEvictQueueCurrentSize() {
        return dhtEvictQueueCurrSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxThreadMapSize() {
        return txThreadMapSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxXidMapSize() {
        return txXidMapSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxCommitQueueSize() {
        return txCommitQueueSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxPrepareQueueSize() {
        return txPrepareQueueSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxStartVersionCountsSize() {
        return txStartVerCountsSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxCommittedVersionsSize() {
        return txCommittedVersionsSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxRolledbackVersionsSize() {
        return txRolledbackVersionsSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtThreadMapSize() {
        return txDhtThreadMapSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtXidMapSize() {
        return txDhtXidMapSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtCommitQueueSize() {
        return txDhtCommitQueueSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtPrepareQueueSize() {
        return txDhtPrepareQueueSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtStartVersionCountsSize() {
        return txDhtStartVerCountsSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtCommittedVersionsSize() {
        return txDhtCommittedVersionsSize;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtRolledbackVersionsSize() {
        return txDhtRolledbackVersionsSize;
    }

    /** {@inheritDoc} */
    @Override public int getTotalPartitionsCount() {
        return totalPartitionsCnt;
    }

    /** {@inheritDoc} */
    @Override public long getRebalancedKeys() {
        return rebalancedKeys;
    }

    /** {@inheritDoc} */
    @Override public long getEstimatedRebalancingKeys() {
        return estimatedRebalancingKeys;
    }

    /** {@inheritDoc} */
    @Override public int getRebalancingPartitionsCount() {
        return rebalancingPartitionsCnt;
    }

    /** {@inheritDoc} */
    @Override public long getKeysToRebalanceLeft() {
        return keysToRebalanceLeft;
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingKeysRate() {
        return rebalancingKeysRate;
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingBytesRate() {
        return rebalancingBytesRate;
    }

    /** {@inheritDoc} */
    @Override public long estimateRebalancingFinishTime() {
        return rebalanceFinishTime;
    }

    /** {@inheritDoc} */
    @Override public long rebalancingStartTime() {
        return rebalanceStartTime;
    }

    /** {@inheritDoc} */
    @Override public long getEstimatedRebalancingFinishTime() {
        return rebalanceFinishTime;
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingStartTime() {
        return rebalanceStartTime;
    }

    /** {@inheritDoc} */
    @Override public long getRebalanceClearingPartitionsLeft() {
        return rebalanceClearingPartitionsLeft;
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteBehindEnabled() {
        return writeBehindEnabled;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindFlushSize() {
        return writeBehindFlushSize;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindFlushThreadCount() {
        return writeBehindFlushThreadCnt;
    }

    /** {@inheritDoc} */
    @Override public long getWriteBehindFlushFrequency() {
        return writeBehindFlushFreq;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindStoreBatchSize() {
        return writeBehindStoreBatchSize;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindTotalCriticalOverflowCount() {
        return writeBehindTotalCriticalOverflowCnt;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindCriticalOverflowCount() {
        return writeBehindCriticalOverflowCnt;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindErrorRetryCount() {
        return writeBehindErrorRetryCnt;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindBufferSize() {
        return writeBehindBufSize;
    }

    /** {@inheritDoc} */
    @Override public String getKeyType() {
        return keyType;
    }

    /** {@inheritDoc} */
    @Override public String getValueType() {
        return valType;
    }

    /** {@inheritDoc} */
    @Override public boolean isStoreByValue() {
        return storeByVal;
    }

    /** {@inheritDoc} */
    @Override public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /** {@inheritDoc} */
    @Override public boolean isManagementEnabled() {
        return managementEnabled;
    }

    /** {@inheritDoc} */
    @Override public boolean isReadThrough() {
        return readThrough;
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteThrough() {
        return writeThrough;
    }

    /** {@inheritDoc} */
    @Override public boolean isValidForReading() {
        return validForReading;
    }

    /** {@inheritDoc} */
    @Override public boolean isValidForWriting() {
        return validForWriting;
    }

    /** {@inheritDoc} */
    @Override public String getTxKeyCollisions() {
        return txKeyCollisions;
    }

    /** {@inheritDoc} */
    @Override public boolean isIndexRebuildInProgress() {
        return idxRebuildInProgress;
    }

    /** {@inheritDoc} */
    @Override public long getIndexRebuildKeysProcessed() {
        return idxRebuildKeysProcessed;
    }

    /** {@inheritDoc} */
    @Override public int getIndexBuildPartitionsLeftCount() {
        return idxBuildPartitionsLeftCount;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheMetricsSnapshot.class, this);
    }
}

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

package org.apache.ignite.internal.processors.cluster;

import java.util.Collection;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/** Cache metrics message. */
public class CacheMetricsMessage extends IgniteDataTransferObject implements Message, CacheMetrics {
    /** */
    public static final short TYPE_CODE = 136;

    /** Number of reads. */
    @Order(value = 0)
    long cacheGets;

    /** Number of puts. */
    @Order(value = 1)
    long cachePuts;

    /** Number of invokes caused updates. */
    @Order(value = 2)
    long entryProcessorPuts;

    /** Number of invokes caused no updates. */
    @Order(value = 3)
    long entryProcessorReadOnlyInvocations;

    /**
     * The mean time to execute cache invokes
     */
    @Order(value = 4)
    float entryProcessorAverageInvocationTime;

    /**
     * The total number of cache invocations.
     */
    @Order(value = 5)
    long entryProcessorInvocations;

    /**
     * The total number of cache invocations, caused removal.
     */
    @Order(value = 6)
    long entryProcessorRemovals;

    /**
     * The total number of invocations on keys, which don't exist in cache.
     */
    @Order(value = 7)
    long entryProcessorMisses;

    /**
     * The total number of invocations on keys, which exist in cache.
     */
    @Order(value = 8)
    long entryProcessorHits;

    /**
     * The percentage of invocations on keys, which don't exist in cache.
     */
    @Order(value = 9)
    float entryProcessorMissPercentage;

    /**
     * The percentage of invocations on keys, which exist in cache.
     */
    @Order(value = 10)
    float entryProcessorHitPercentage;

    /**
     * So far, the maximum time to execute cache invokes.
     */
    @Order(value = 11)
    float entryProcessorMaxInvocationTime;

    /**
     * So far, the minimum time to execute cache invokes.
     */
    @Order(value = 12)
    float entryProcessorMinInvocationTime;

    /** Number of hits. */
    @Order(value = 13)
    long cacheHits;

    /** Number of misses. */
    @Order(value = 14)
    long cacheMisses;

    /** Number of transaction commits. */
    @Order(value = 15)
    long cacheTxCommits;

    /** Number of transaction rollbacks. */
    @Order(value = 16)
    long cacheTxRollbacks;

    /** Number of evictions. */
    @Order(value = 17)
    long cacheEvictions;

    /** Number of removed entries. */
    @Order(value = 18)
    long cacheRemovals;

    /** Put time taken nanos. */
    @Order(value = 19)
    float averagePutTime;

    /** Get time taken nanos. */
    @Order(value = 20)
    float averageGetTime;

    /** Remove time taken nanos. */
    @Order(value = 21)
    float averageRemoveTime;

    /** Commit transaction time taken nanos. */
    @Order(value = 22)
    float averageTxCommitTime;

    /** Commit transaction time taken nanos. */
    @Order(value = 23)
    float averageTxRollbackTime;

    /** Cache name */
    @Order(value = 24)
    String cacheName;

    /** Number of reads from off-heap. */
    @Order(value = 25)
    long offHeapGets;

    /** Number of writes to off-heap. */
    @Order(value = 26)
    long offHeapPuts;

    /** Number of removed entries from off-heap. */
    @Order(value = 27, method = "offHeapRemovals")
    long offHeapRemoves;

    /** Number of evictions from off-heap. */
    @Order(value = 28, method = "offHeapEvictions")
    long offHeapEvicts;

    /** Off-heap hits number. */
    @Order(value = 29, method = "offHeapHits")
    long offHeapHits;

    /** Off-heap misses number. */
    @Order(value = 30)
    long offHeapMisses;

    /** Number of entries stored in off-heap memory. */
    @Order(value = 31, method = "offHeapEntriesCount")
    long offHeapEntriesCnt;

    /** Number of entries stored in heap. */
    @Order(value = 32, method = "heapEntriesCount")
    long heapEntriesCnt;

    /** Number of primary entries stored in off-heap memory. */
    @Order(value = 33, method = "offHeapPrimaryEntriesCount")
    long offHeapPrimaryEntriesCnt;

    /** Number of backup entries stored in off-heap memory. */
    @Order(value = 34, method = "offHeapBackupEntriesCount")
    long offHeapBackupEntriesCnt;

    /** Memory size allocated in off-heap. */
    @Order(value = 35)
    long offHeapAllocatedSize;

    /** Number of non-{@code null} values in the cache. */
    @Order(value = 36)
    int size;

    /** Cache size. */
    @Order(value = 37)
    long cacheSize;

    /** Number of keys in the cache, possibly with {@code null} values. */
    @Order(value = 38)
    int keySize;

    /** Cache is empty. */
    @Order(value = 39)
    boolean empty;

    /** Gets current size of evict queue used to batch up evictions. */
    @Order(value = 40, method = "dhtEvictQueueCurrentSize")
    int dhtEvictQueueCurrSize;

    /** Transaction per-thread map size. */
    @Order(value = 41)
    int txThreadMapSize;

    /** Transaction per-Xid map size. */
    @Order(value = 42)
    int txXidMapSize;

    /** Committed transaction queue size. */
    @Order(value = 43)
    int txCommitQueueSize;

    /** Prepared transaction queue size. */
    @Order(value = 44)
    int txPrepareQueueSize;

    /** Start version counts map size. */
    @Order(value = 45, method = "txStartVersionCountsSize")
    int txStartVerCountsSize;

    /** Number of cached committed transaction IDs. */
    @Order(value = 46)
    int txCommittedVersionsSize;

    /** Number of cached rolled back transaction IDs. */
    @Order(value = 47)
    int txRolledbackVersionsSize;

    /** DHT thread map size. */
    @Order(value = 48, method = "txDhtThreadMapSize")
    int txDhtThreadMapSize;

    /** Transaction DHT per-Xid map size. */
    @Order(value = 49, method = "txDhtXidMapSize")
    int txDhtXidMapSize;

    /** Committed DHT transaction queue size. */
    @Order(value = 50)
    int txDhtCommitQueueSize;

    /** Prepared DHT transaction queue size. */
    @Order(value = 51)
    int txDhtPrepareQueueSize;

    /** DHT start version counts map size. */
    @Order(value = 52, method = "txDhtStartVersionCountsSize")
    int txDhtStartVerCountsSize;

    /** Number of cached committed DHT transaction IDs. */
    @Order(value = 53)
    int txDhtCommittedVersionsSize;

    /** Number of cached rolled back DHT transaction IDs. */
    @Order(value = 54)
    int txDhtRolledbackVersionsSize;

    /** Write-behind is enabled. */
    @Order(value = 55)
    boolean writeBehindEnabled;

    /** Buffer size that triggers flush procedure. */
    @Order(value = 56)
    int writeBehindFlushSize;

    /** Count of worker threads. */
    @Order(value = 57, method = "writeBehindFlushThreadCount")
    int writeBehindFlushThreadCnt;

    /** Flush frequency in milliseconds. */
    @Order(value = 58, method = "writeBehindFlushFrequency")
    long writeBehindFlushFreq;

    /** Maximum size of batch. */
    @Order(value = 59)
    int writeBehindStoreBatchSize;

    /** Count of cache overflow events since start. */
    @Order(value = 60, method = "writeBehindTotalCriticalOverflowCount")
    int writeBehindTotalCriticalOverflowCnt;

    /** Count of cache overflow events since start. */
    @Order(value = 61, method = "writeBehindCriticalOverflowCount")
    int writeBehindCriticalOverflowCnt;

    /** Count of entries in store-retry state. */
    @Order(value = 62, method = "writeBehindErrorRetryCount")
    int writeBehindErrorRetryCnt;

    /** Total count of entries in cache store internal buffer. */
    @Order(value = 63, method = "writeBehindBufferSize")
    int writeBehindBufSize;

    /** Total partitions count. */
    @Order(value = 64, method = "totalPartitionsCount")
    int totalPartitionsCnt;

    /** Rebalancing partitions count. */
    @Order(value = 65, method = "rebalancingPartitionsCount")
    int rebalancingPartitionsCnt;

    /** Number of already rebalanced keys. */
    @Order(value = 66)
    long rebalancedKeys;

    /** Number estimated to rebalance keys. */
    @Order(value = 67)
    long estimatedRebalancingKeys;

    /** Keys to rebalance left. */
    @Order(value = 68)
    long keysToRebalanceLeft;

    /** Rebalancing keys rate. */
    @Order(value = 69)
    long rebalancingKeysRate;

    /** Get rebalancing bytes rate. */
    @Order(value = 70)
    long rebalancingBytesRate;

    /** Start rebalance time. */
    @Order(value = 71, method = "rebalancingStartTime")
    long rebalanceStartTime;

    /** Estimate rebalance finish time. */
    @Order(value = 72)
    long rebalanceFinishTime;

    /** The number of clearing partitions need to await before rebalance. */
    @Order(value = 73)
    long rebalanceClearingPartitionsLeft;

    /** */
    @Order(value = 74)
    String keyType;

    /** */
    @Order(value = 75, method = "valueType")
    String valType;

    /** */
    @Order(value = 76, method = "storeByValue")
    boolean storeByVal;

    /** */
    @Order(value = 77)
    boolean statisticsEnabled;

    /** */
    @Order(value = 78)
    boolean managementEnabled;

    /** */
    @Order(value = 79)
    boolean readThrough;

    /** */
    @Order(value = 80)
    boolean writeThrough;

    /** */
    @Order(value = 81)
    boolean validForReading;

    /** */
    @Order(value = 82)
    boolean validForWriting;

    /** Tx key collisions with appropriate queue size string representation. */
    @Order(value = 83)
    String txKeyCollisions;

    /** Index rebuilding in progress. */
    @Order(value = 84, method = "indexRebuildInProgress")
    boolean idxRebuildInProgress;

    /** Number of keys processed idxRebuildInProgressduring index rebuilding. */
    @Order(value = 85, method = "indexRebuildKeysProcessed")
    long idxRebuildKeyProcessed;

    /** The number of local node partitions that remain to be processed to complete indexing. */
    @Order(value = 86, method = "indexBuildPartitionsLeftCount")
    int idxBuildPartitionsLeftCount;

    /**
     * Default constructor for {@link GridIoMessageFactory}.
     */
    public CacheMetricsMessage() {
        // No-op.
    }

    /**
     * Create snapshot for the given metrics.
     *
     * @param m Cache metrics.
     */
    public CacheMetricsMessage(CacheMetrics m) {
        cacheGets = m.getCacheGets();
        cachePuts = m.getCachePuts();
        cacheHits = m.getCacheHits();
        cacheMisses = m.getCacheMisses();
        cacheTxCommits = m.getCacheTxCommits();
        cacheTxRollbacks = m.getCacheTxRollbacks();
        cacheEvictions = m.getCacheEvictions();
        cacheRemovals = m.getCacheRemovals();

        entryProcessorPuts = m.getEntryProcessorPuts();
        entryProcessorReadOnlyInvocations = m.getEntryProcessorReadOnlyInvocations();
        entryProcessorInvocations = m.getEntryProcessorInvocations();
        entryProcessorRemovals = m.getEntryProcessorRemovals();
        entryProcessorMisses = m.getEntryProcessorMisses();
        entryProcessorHits = m.getEntryProcessorHits();
        entryProcessorMissPercentage = m.getEntryProcessorMissPercentage();
        entryProcessorHitPercentage = m.getEntryProcessorHitPercentage();
        entryProcessorAverageInvocationTime = m.getEntryProcessorAverageInvocationTime();
        entryProcessorMaxInvocationTime = m.getEntryProcessorMaxInvocationTime();
        entryProcessorMinInvocationTime = m.getEntryProcessorMinInvocationTime();

        averagePutTime = m.getAveragePutTime();
        averageGetTime = m.getAverageGetTime();
        averageRemoveTime = m.getAverageRemoveTime();
        averageTxCommitTime = m.getAverageTxCommitTime();
        averageTxRollbackTime = m.getAverageTxRollbackTime();

        cacheName = m.name();

        offHeapGets = m.getOffHeapGets();
        offHeapPuts = m.getOffHeapPuts();
        offHeapRemoves = m.getOffHeapRemovals();
        offHeapEvicts = m.getOffHeapEvictions();
        offHeapHits = m.getOffHeapHits();
        offHeapMisses = m.getOffHeapMisses();

        offHeapEntriesCnt = m.getOffHeapEntriesCount();
        heapEntriesCnt = m.getHeapEntriesCount();
        offHeapPrimaryEntriesCnt = m.getOffHeapPrimaryEntriesCount();
        offHeapBackupEntriesCnt = m.getOffHeapBackupEntriesCount();

        offHeapAllocatedSize = m.getOffHeapAllocatedSize();

        cacheSize = m.getCacheSize();
        keySize = m.getKeySize();
        size = m.getSize();
        empty = m.isEmpty();

        dhtEvictQueueCurrSize = m.getDhtEvictQueueCurrentSize();
        txThreadMapSize = m.getTxThreadMapSize();
        txXidMapSize = m.getTxXidMapSize();
        txCommitQueueSize = m.getTxCommitQueueSize();
        txPrepareQueueSize = m.getTxPrepareQueueSize();
        txStartVerCountsSize = m.getTxStartVersionCountsSize();
        txCommittedVersionsSize = m.getTxCommittedVersionsSize();
        txRolledbackVersionsSize = m.getTxRolledbackVersionsSize();
        txDhtThreadMapSize = m.getTxDhtThreadMapSize();
        txDhtXidMapSize = m.getTxDhtXidMapSize();
        txDhtCommitQueueSize = m.getTxDhtCommitQueueSize();
        txDhtPrepareQueueSize = m.getTxDhtPrepareQueueSize();
        txDhtStartVerCountsSize = m.getTxDhtStartVersionCountsSize();
        txDhtCommittedVersionsSize = m.getTxDhtCommittedVersionsSize();
        txDhtRolledbackVersionsSize = m.getTxDhtRolledbackVersionsSize();
        writeBehindEnabled = m.isWriteBehindEnabled();
        writeBehindFlushSize = m.getWriteBehindFlushSize();
        writeBehindFlushThreadCnt = m.getWriteBehindFlushThreadCount();
        writeBehindFlushFreq = m.getWriteBehindFlushFrequency();
        writeBehindStoreBatchSize = m.getWriteBehindStoreBatchSize();
        writeBehindTotalCriticalOverflowCnt = m.getWriteBehindTotalCriticalOverflowCount();
        writeBehindCriticalOverflowCnt = m.getWriteBehindCriticalOverflowCount();
        writeBehindErrorRetryCnt = m.getWriteBehindErrorRetryCount();
        writeBehindBufSize = m.getWriteBehindBufferSize();

        keyType = m.getKeyType();
        valType = m.getValueType();
        storeByVal = m.isStoreByValue();
        statisticsEnabled = m.isStatisticsEnabled();
        managementEnabled = m.isManagementEnabled();
        readThrough = m.isReadThrough();
        writeThrough = m.isWriteThrough();
        validForReading = m.isValidForReading();
        validForWriting = m.isValidForWriting();

        totalPartitionsCnt = m.getTotalPartitionsCount();
        rebalancingPartitionsCnt = m.getRebalancingPartitionsCount();

        rebalancedKeys = m.getRebalancedKeys();
        estimatedRebalancingKeys = m.getEstimatedRebalancingKeys();
        keysToRebalanceLeft = m.getKeysToRebalanceLeft();
        rebalancingBytesRate = m.getRebalancingBytesRate();
        rebalancingKeysRate = m.getRebalancingKeysRate();
        rebalanceStartTime = m.rebalancingStartTime();
        rebalanceFinishTime = m.estimateRebalancingFinishTime();
        rebalanceClearingPartitionsLeft = m.getRebalanceClearingPartitionsLeft();
        txKeyCollisions = m.getTxKeyCollisions();

        idxRebuildInProgress = m.isIndexRebuildInProgress();
        idxRebuildKeyProcessed = m.getIndexRebuildKeysProcessed();

        idxBuildPartitionsLeftCount = m.getIndexBuildPartitionsLeftCount();
    }

    /**
     * Creates merged cache metrics.
     *
     * @param loc Metrics for cache on local node.
     * @param metrics Metrics for merge.
     */
    public CacheMetricsMessage(CacheMetrics loc, Collection<CacheMetrics> metrics) {
        cacheName = loc.name();
        empty = loc.isEmpty();
        writeBehindEnabled = loc.isWriteBehindEnabled();
        writeBehindFlushSize = loc.getWriteBehindFlushSize();
        writeBehindFlushThreadCnt = loc.getWriteBehindFlushThreadCount();
        writeBehindFlushFreq = loc.getWriteBehindFlushFrequency();
        writeBehindStoreBatchSize = loc.getWriteBehindStoreBatchSize();
        writeBehindBufSize = loc.getWriteBehindBufferSize();
        size = 0;
        cacheSize = 0;
        keySize = 0;

        keyType = loc.getKeyType();
        valType = loc.getValueType();
        storeByVal = loc.isStoreByValue();
        statisticsEnabled = loc.isStatisticsEnabled();
        managementEnabled = loc.isManagementEnabled();
        readThrough = loc.isReadThrough();
        writeThrough = loc.isWriteThrough();
        validForReading = loc.isValidForReading();
        validForWriting = loc.isValidForWriting();

        for (CacheMetrics e : metrics) {
            cacheGets += e.getCacheGets();
            cachePuts += e.getCachePuts();
            size += e.getSize();
            keySize += e.getKeySize();
            cacheSize += e.getCacheSize();
            empty &= e.isEmpty();
            cacheHits += e.getCacheHits();
            cacheMisses += e.getCacheMisses();
            cacheTxCommits += e.getCacheTxCommits();
            cacheTxRollbacks += e.getCacheTxRollbacks();
            cacheEvictions += e.getCacheEvictions();
            cacheRemovals += e.getCacheRemovals();

            entryProcessorPuts = e.getEntryProcessorPuts();
            entryProcessorReadOnlyInvocations = e.getEntryProcessorReadOnlyInvocations();
            entryProcessorInvocations = e.getEntryProcessorInvocations();
            entryProcessorRemovals = e.getEntryProcessorRemovals();
            entryProcessorMisses = e.getEntryProcessorMisses();
            entryProcessorHits = e.getEntryProcessorHits();
            entryProcessorMissPercentage = e.getEntryProcessorMissPercentage();
            entryProcessorHitPercentage = e.getEntryProcessorHitPercentage();
            entryProcessorAverageInvocationTime = e.getEntryProcessorAverageInvocationTime();
            entryProcessorMaxInvocationTime = e.getEntryProcessorMaxInvocationTime();
            entryProcessorMinInvocationTime = e.getEntryProcessorMinInvocationTime();

            averagePutTime += e.getAveragePutTime();
            averageGetTime += e.getAverageGetTime();
            averageRemoveTime += e.getAverageRemoveTime();
            averageTxCommitTime += e.getAverageTxCommitTime();
            averageTxRollbackTime += e.getAverageTxRollbackTime();

            offHeapGets += e.getOffHeapGets();
            offHeapPuts += e.getOffHeapPuts();
            offHeapRemoves += e.getOffHeapRemovals();
            offHeapEvicts += e.getOffHeapEvictions();
            offHeapHits += e.getOffHeapHits();
            offHeapMisses += e.getOffHeapMisses();
            offHeapEntriesCnt += e.getOffHeapEntriesCount();
            heapEntriesCnt += e.getHeapEntriesCount();
            offHeapPrimaryEntriesCnt += e.getOffHeapPrimaryEntriesCount();
            offHeapBackupEntriesCnt += e.getOffHeapBackupEntriesCount();
            offHeapAllocatedSize += e.getOffHeapAllocatedSize();

            if (e.getDhtEvictQueueCurrentSize() > -1)
                dhtEvictQueueCurrSize += e.getDhtEvictQueueCurrentSize();
            else
                dhtEvictQueueCurrSize = -1;

            txThreadMapSize += e.getTxThreadMapSize();
            txXidMapSize += e.getTxXidMapSize();
            txCommitQueueSize += e.getTxCommitQueueSize();
            txPrepareQueueSize += e.getTxPrepareQueueSize();
            txStartVerCountsSize += e.getTxStartVersionCountsSize();
            txCommittedVersionsSize += e.getTxCommittedVersionsSize();
            txRolledbackVersionsSize += e.getTxRolledbackVersionsSize();

            if (e.getTxDhtThreadMapSize() > -1)
                txDhtThreadMapSize += e.getTxDhtThreadMapSize();
            else
                txDhtThreadMapSize = -1;

            if (e.getTxDhtXidMapSize() > -1)
                txDhtXidMapSize += e.getTxDhtXidMapSize();
            else
                txDhtXidMapSize = -1;

            if (e.getTxDhtCommitQueueSize() > -1)
                txDhtCommitQueueSize += e.getTxDhtCommitQueueSize();
            else
                txDhtCommitQueueSize = -1;

            if (e.getTxDhtPrepareQueueSize() > -1)
                txDhtPrepareQueueSize += e.getTxDhtPrepareQueueSize();
            else
                txDhtPrepareQueueSize = -1;

            if (e.getTxDhtStartVersionCountsSize() > -1)
                txDhtStartVerCountsSize += e.getTxDhtStartVersionCountsSize();
            else
                txDhtStartVerCountsSize = -1;

            if (e.getTxDhtCommittedVersionsSize() > -1)
                txDhtCommittedVersionsSize += e.getTxDhtCommittedVersionsSize();
            else
                txDhtCommittedVersionsSize = -1;

            if (e.getTxDhtRolledbackVersionsSize() > -1)
                txDhtRolledbackVersionsSize += e.getTxDhtRolledbackVersionsSize();
            else
                txDhtRolledbackVersionsSize = -1;

            if (e.getWriteBehindTotalCriticalOverflowCount() > -1)
                writeBehindTotalCriticalOverflowCnt += e.getWriteBehindTotalCriticalOverflowCount();
            else
                writeBehindTotalCriticalOverflowCnt = -1;

            if (e.getWriteBehindCriticalOverflowCount() > -1)
                writeBehindCriticalOverflowCnt += e.getWriteBehindCriticalOverflowCount();
            else
                writeBehindCriticalOverflowCnt = -1;

            if (e.getWriteBehindErrorRetryCount() > -1)
                writeBehindErrorRetryCnt += e.getWriteBehindErrorRetryCount();
            else
                writeBehindErrorRetryCnt = -1;

            rebalancedKeys += e.getRebalancedKeys();
            estimatedRebalancingKeys += e.getEstimatedRebalancingKeys();
            totalPartitionsCnt += e.getTotalPartitionsCount();
            rebalancingPartitionsCnt += e.getRebalancingPartitionsCount();
            keysToRebalanceLeft += e.getKeysToRebalanceLeft();
            rebalancingBytesRate += e.getRebalancingBytesRate();
            rebalancingKeysRate += e.getRebalancingKeysRate();
            idxBuildPartitionsLeftCount += e.getIndexBuildPartitionsLeftCount();
        }

        int size = metrics.size();

        if (size > 1) {
            averagePutTime /= size;
            averageGetTime /= size;
            averageRemoveTime /= size;
            averageTxCommitTime /= size;
            averageTxRollbackTime /= size;
        }
    }

    /** */
    public long cacheHits() {
        return cacheHits;
    }

    /** */
    public float cacheHitPercentage() {
        if (cacheHits == 0 || cacheGets == 0)
            return 0;

        return (float)cacheHits / cacheGets * 100.0f;
    }

    /** */
    public long cacheMisses() {
        return cacheMisses;
    }

    /** */
    public float cacheMissPercentage() {
        if (cacheMisses == 0 || cacheGets == 0)
            return 0;

        return (float)cacheMisses / cacheGets * 100.0f;
    }

    /** */
    public long cacheGets() {
        return cacheGets;
    }

    /** */
    public long cachePuts() {
        return cachePuts;
    }

    /** */
    public long entryProcessorPuts() {
        return entryProcessorPuts;
    }

    /** */
    public long entryProcessorReadOnlyInvocations() {
        return entryProcessorReadOnlyInvocations;
    }

    /** */
    public long entryProcessorInvocations() {
        return entryProcessorInvocations;
    }

    /** */
    public long entryProcessorHits() {
        return entryProcessorHits;
    }

    /** */
    public float entryProcessorHitPercentage() {
        return entryProcessorHitPercentage;
    }

    /** */
    public float entryProcessorMissPercentage() {
        return entryProcessorMissPercentage;
    }

    /** */
    public long entryProcessorMisses() {
        return entryProcessorMisses;
    }

    /** */
    public long entryProcessorRemovals() {
        return entryProcessorRemovals;
    }

    /** */
    public float entryProcessorAverageInvocationTime() {
        return entryProcessorAverageInvocationTime;
    }

    /** */
    public float entryProcessorMinInvocationTime() {
        return entryProcessorMinInvocationTime;
    }

    /** */
    public float entryProcessorMaxInvocationTime() {
        return entryProcessorMaxInvocationTime;
    }

    /** */
    public long cacheRemovals() {
        return cacheRemovals;
    }

    /** */
    public long cacheEvictions() {
        return cacheEvictions;
    }

    /** */
    public float averageGetTime() {
        return averageGetTime;
    }

    /** */
    public float averagePutTime() {
        return averagePutTime;
    }

    /** */
    public float averageRemoveTime() {
        return averageRemoveTime;
    }

    /** */
    public float averageTxCommitTime() {
        return averageTxCommitTime;
    }

    /** */
    public float averageTxRollbackTime() {
        return averageTxRollbackTime;
    }

    /** */
    public long cacheTxCommits() {
        return cacheTxCommits;
    }

    /** */
    public long cacheTxRollbacks() {
        return cacheTxRollbacks;
    }

    /** */
    public String cacheName() {
        return cacheName;
    }

    /** */
    public long offHeapGets() {
        return offHeapGets;
    }

    /** */
    public long offHeapPuts() {
        return offHeapPuts;
    }

    /** */
    public long offHeapRemovals() {
        return offHeapRemoves;
    }

    /** */
    public long offHeapEvictions() {
        return offHeapEvicts;
    }

    /** */
    public long offHeapHits() {
        return offHeapHits;
    }

    /** */
    public float offHeapHitPercentage() {
        if (offHeapHits == 0 || offHeapGets == 0)
            return 0;

        return (float)offHeapHits / offHeapGets * 100.0f;
    }

    /** */
    public long offHeapMisses() {
        return offHeapMisses;
    }

    /** */
    public float offHeapMissPercentage() {
        if (offHeapMisses == 0 || offHeapGets == 0)
            return 0;

        return (float)offHeapMisses / offHeapGets * 100.0f;
    }

    /** */
    public long offHeapEntriesCount() {
        return offHeapEntriesCnt;
    }

    /** */
    public long heapEntriesCount() {
        return heapEntriesCnt;
    }

    /** */
    public long offHeapPrimaryEntriesCount() {
        return offHeapPrimaryEntriesCnt;
    }

    /** */
    public long offHeapBackupEntriesCount() {
        return offHeapBackupEntriesCnt;
    }

    /** */
    public long offHeapAllocatedSize() {
        return offHeapAllocatedSize;
    }

    /** */
    public int size() {
        return size;
    }

    /** */
    public long cacheSize() {
        return cacheSize;
    }

    /** */
    public int keySize() {
        return keySize;
    }

    /** */
    public boolean empty() {
        return empty;
    }

    /** */
    public int dhtEvictQueueCurrentSize() {
        return dhtEvictQueueCurrSize;
    }

    /** */
    public int txThreadMapSize() {
        return txThreadMapSize;
    }

    /** */
    public int txXidMapSize() {
        return txXidMapSize;
    }

    /** */
    public int txCommitQueueSize() {
        return txCommitQueueSize;
    }

    /** */
    public int txPrepareQueueSize() {
        return txPrepareQueueSize;
    }

    /** */
    public int txStartVersionCountsSize() {
        return txStartVerCountsSize;
    }

    /** */
    public int txCommittedVersionsSize() {
        return txCommittedVersionsSize;
    }

    /** */
    public int txRolledbackVersionsSize() {
        return txRolledbackVersionsSize;
    }

    /** */
    public int txDhtThreadMapSize() {
        return txDhtThreadMapSize;
    }

    /** */
    public int txDhtXidMapSize() {
        return txDhtXidMapSize;
    }

    /** */
    public int txDhtCommitQueueSize() {
        return txDhtCommitQueueSize;
    }

    /** */
    public int txDhtPrepareQueueSize() {
        return txDhtPrepareQueueSize;
    }

    /** */
    public int txDhtStartVersionCountsSize() {
        return txDhtStartVerCountsSize;
    }

    /** */
    public int txDhtCommittedVersionsSize() {
        return txDhtCommittedVersionsSize;
    }

    /** */
    public int txDhtRolledbackVersionsSize() {
        return txDhtRolledbackVersionsSize;
    }

    /** */
    public int totalPartitionsCount() {
        return totalPartitionsCnt;
    }

    /** */
    public long rebalancedKeys() {
        return rebalancedKeys;
    }

    /** */
    public long estimatedRebalancingKeys() {
        return estimatedRebalancingKeys;
    }

    /** */
    public int rebalancingPartitionsCount() {
        return rebalancingPartitionsCnt;
    }

    /** */
    public long keysToRebalanceLeft() {
        return keysToRebalanceLeft;
    }

    /** */
    public long rebalancingKeysRate() {
        return rebalancingKeysRate;
    }

    /** */
    public long rebalancingBytesRate() {
        return rebalancingBytesRate;
    }

    /** */
    public long rebalanceFinishTime() {
        return rebalanceFinishTime;
    }

    /** */
    public long rebalanceClearingPartitionsLeft() {
        return rebalanceClearingPartitionsLeft;
    }

    /** */
    public boolean writeBehindEnabled() {
        return writeBehindEnabled;
    }

    /** */
    public int writeBehindFlushSize() {
        return writeBehindFlushSize;
    }

    /** */
    public int writeBehindFlushThreadCount() {
        return writeBehindFlushThreadCnt;
    }

    /** */
    public long writeBehindFlushFrequency() {
        return writeBehindFlushFreq;
    }

    /** */
    public int writeBehindStoreBatchSize() {
        return writeBehindStoreBatchSize;
    }

    /** */
    public int writeBehindTotalCriticalOverflowCount() {
        return writeBehindTotalCriticalOverflowCnt;
    }

    /** */
    public int writeBehindCriticalOverflowCount() {
        return writeBehindCriticalOverflowCnt;
    }

    /** */
    public int writeBehindErrorRetryCount() {
        return writeBehindErrorRetryCnt;
    }

    /** */
    public int writeBehindBufferSize() {
        return writeBehindBufSize;
    }

    /** */
    public String keyType() {
        return keyType;
    }

    /** */
    public String valueType() {
        return valType;
    }

    /** */
    public boolean storeByValue() {
        return storeByVal;
    }

    /** */
    public boolean statisticsEnabled() {
        return statisticsEnabled;
    }

    /** */
    public boolean managementEnabled() {
        return managementEnabled;
    }

    /** */
    public boolean readThrough() {
        return readThrough;
    }

    /** */
    public boolean writeThrough() {
        return writeThrough;
    }

    /** */
    public boolean validForReading() {
        return validForReading;
    }

    /** */
    public boolean validForWriting() {
        return validForWriting;
    }

    /** */
    public String txKeyCollisions() {
        return txKeyCollisions;
    }

    /** */
    public boolean indexRebuildInProgress() {
        return idxRebuildInProgress;
    }

    /** */
    public long indexRebuildKeysProcessed() {
        return idxRebuildKeyProcessed;
    }

    /** */
    public int indexBuildPartitionsLeftCount() {
        return idxBuildPartitionsLeftCount;
    }

    /** */
    public void cachePuts(long puts) {
        cachePuts = puts;
    }

    /** */
    public void entryProcessorPuts(long entryProcPuts) {
        entryProcessorPuts = entryProcPuts;
    }

    /** */
    public void cacheGets(long reads) {
        cacheGets = reads;
    }

    /** */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /** */
    public void cacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
    }

    /** */
    public void averageTxCommitTime(float commitAvgTimeNanos) {
        averageTxCommitTime = commitAvgTimeNanos;
    }

    /** */
    public void dhtEvictQueueCurrentSize(int dhtEvictQueueCurrSize) {
        this.dhtEvictQueueCurrSize = dhtEvictQueueCurrSize;
    }

    /** */
    public void entryProcessorAverageInvocationTime(float entryProcessorAverageInvocationTime) {
        this.entryProcessorAverageInvocationTime = entryProcessorAverageInvocationTime;
    }

    /** */
    public void entryProcessorHitPercentage(float entryProcessorHitPercentage) {
        this.entryProcessorHitPercentage = entryProcessorHitPercentage;
    }

    /** */
    public void entryProcessorHits(long entryProcessorHits) {
        this.entryProcessorHits = entryProcessorHits;
    }

    /** */
    public void entryProcessorInvocations(long entryProcessorInvocations) {
        this.entryProcessorInvocations = entryProcessorInvocations;
    }

    /** */
    public void entryProcessorMaxInvocationTime(float entryProcMaxInvocationTime) {
        entryProcessorMaxInvocationTime = entryProcMaxInvocationTime;
    }

    /** */
    public void entryProcessorMinInvocationTime(float entryProcMinInvocationTime) {
        entryProcessorMinInvocationTime = entryProcMinInvocationTime;
    }

    /** */
    public void entryProcessorMisses(long entryProcMisses) {
        entryProcessorMisses = entryProcMisses;
    }

    /** */
    public void entryProcessorMissPercentage(float entryProcMissPercentage) {
        entryProcessorMissPercentage = entryProcMissPercentage;
    }

    /** */
    public void entryProcessorReadOnlyInvocations(long entryProcReadOnlyInvocations) {
        entryProcessorReadOnlyInvocations = entryProcReadOnlyInvocations;
    }

    /** */
    public void entryProcessorRemovals(long entryProcRemovals) {
        entryProcessorRemovals = entryProcRemovals;
    }

    /** */
    public void estimatedRebalancingKeys(long estimatedRebalancingKeys) {
        this.estimatedRebalancingKeys = estimatedRebalancingKeys;
    }

    /** */
    public void cacheEvictions(long evicts) {
        cacheEvictions = evicts;
    }

    /** */
    public void averageGetTime(float getAvgTimeNanos) {
        averageGetTime = getAvgTimeNanos;
    }

    /** */
    public void heapEntriesCount(long heapEntriesCnt) {
        this.heapEntriesCnt = heapEntriesCnt;
    }

    /** */
    public void cacheHits(long hits) {
        cacheHits = hits;
    }

    /** */
    public void indexBuildPartitionsLeftCount(int idxBuildPartitionsLeftCnt) {
        idxBuildPartitionsLeftCount = idxBuildPartitionsLeftCnt;
    }

    /** */
    public void indexRebuildInProgress(boolean idxRebuildInProgress) {
        this.idxRebuildInProgress = idxRebuildInProgress;
    }

    /** */
    public void indexRebuildKeysProcessed(long idxRebuildKeyProcessed) {
        this.idxRebuildKeyProcessed = idxRebuildKeyProcessed;
    }

    /** */
    public void empty(boolean empty) {
        this.empty = empty;
    }

    /** */
    public void managementEnabled(boolean managementEnabled) {
        this.managementEnabled = managementEnabled;
    }

    /** */
    public void readThrough(boolean readThrough) {
        this.readThrough = readThrough;
    }

    /** */
    public void statisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
    }

    /** */
    public void storeByValue(boolean storeByVal) {
        this.storeByVal = storeByVal;
    }

    /** */
    public void validForReading(boolean validForReading) {
        this.validForReading = validForReading;
    }

    /** */
    public void validForWriting(boolean validForWriting) {
        this.validForWriting = validForWriting;
    }

    /** */
    public void writeBehindEnabled(boolean writeBehindEnabled) {
        this.writeBehindEnabled = writeBehindEnabled;
    }

    /** */
    public void writeThrough(boolean writeThrough) {
        this.writeThrough = writeThrough;
    }

    /** */
    public void keySize(int keySize) {
        this.keySize = keySize;
    }

    /** */
    public void keysToRebalanceLeft(long keysToRebalanceLeft) {
        this.keysToRebalanceLeft = keysToRebalanceLeft;
    }

    /** */
    public void keyType(String keyType) {
        this.keyType = keyType;
    }

    /** */
    public void cacheMisses(long misses) {
        cacheMisses = misses;
    }

    /** */
    public void offHeapAllocatedSize(long offHeapAllocatedSize) {
        this.offHeapAllocatedSize = offHeapAllocatedSize;
    }

    /** */
    public void offHeapBackupEntriesCount(long offHeapBackupEntriesCnt) {
        this.offHeapBackupEntriesCnt = offHeapBackupEntriesCnt;
    }

    /** */
    public void offHeapEntriesCount(long offHeapEntriesCnt) {
        this.offHeapEntriesCnt = offHeapEntriesCnt;
    }

    /** */
    public void offHeapEvictions(long offHeapEvicts) {
        this.offHeapEvicts = offHeapEvicts;
    }

    /** */
    public void offHeapGets(long offHeapGets) {
        this.offHeapGets = offHeapGets;
    }

    /** */
    public void offHeapHits(long offHeapHits) {
        this.offHeapHits = offHeapHits;
    }

    /** */
    public void offHeapMisses(long offHeapMisses) {
        this.offHeapMisses = offHeapMisses;
    }

    /** */
    public void offHeapPrimaryEntriesCount(long offHeapPrimaryEntriesCnt) {
        this.offHeapPrimaryEntriesCnt = offHeapPrimaryEntriesCnt;
    }

    /** */
    public void offHeapPuts(long offHeapPuts) {
        this.offHeapPuts = offHeapPuts;
    }

    /** */
    public void offHeapRemovals(long offHeapRemoves) {
        this.offHeapRemoves = offHeapRemoves;
    }

    /** */
    public void averagePutTime(float putAvgTimeNanos) {
        averagePutTime = putAvgTimeNanos;
    }

    /** */
    public void puts(long puts) {
        cachePuts = puts;
    }

    /** */
    public void reads(long reads) {
        cacheGets = reads;
    }

    /** */
    public void rebalanceClearingPartitionsLeft(long rebalanceClearingPartitionsLeft) {
        this.rebalanceClearingPartitionsLeft = rebalanceClearingPartitionsLeft;
    }

    /** */
    public void rebalancedKeys(long rebalancedKeys) {
        this.rebalancedKeys = rebalancedKeys;
    }

    /** */
    public void rebalanceFinishTime(long rebalanceFinishTime) {
        this.rebalanceFinishTime = rebalanceFinishTime;
    }

    /** */
    public void rebalancingStartTime(long rebalanceStartTime) {
        this.rebalanceStartTime = rebalanceStartTime;
    }

    /** */
    public void rebalancingBytesRate(long rebalancingBytesRate) {
        this.rebalancingBytesRate = rebalancingBytesRate;
    }

    /** */
    public void rebalancingKeysRate(long rebalancingKeysRate) {
        this.rebalancingKeysRate = rebalancingKeysRate;
    }

    /** */
    public void rebalancingPartitionsCount(int rebalancingPartitionsCnt) {
        this.rebalancingPartitionsCnt = rebalancingPartitionsCnt;
    }

    /** */
    public void cacheRemovals(long removes) {
        cacheRemovals = removes;
    }

    /** */
    public void averageRemoveTime(float rmvAvgTimeNanos) {
        averageRemoveTime = rmvAvgTimeNanos;
    }

    /** */
    public void averageTxRollbackTime(float rollbackAvgTimeNanos) {
        averageTxRollbackTime = rollbackAvgTimeNanos;
    }

    /** */
    public void size(int size) {
        this.size = size;
    }

    /** */
    public void totalPartitionsCount(int totalPartitionsCnt) {
        this.totalPartitionsCnt = totalPartitionsCnt;
    }

    /** */
    public void txCommitQueueSize(int txCommitQueueSize) {
        this.txCommitQueueSize = txCommitQueueSize;
    }

    /** */
    public void cacheTxCommits(long txCommits) {
        cacheTxCommits = txCommits;
    }

    /** */
    public void txCommittedVersionsSize(int txCommittedVersionsSize) {
        this.txCommittedVersionsSize = txCommittedVersionsSize;
    }

    /** */
    public void txDhtCommitQueueSize(int txDhtCommitQueueSize) {
        this.txDhtCommitQueueSize = txDhtCommitQueueSize;
    }

    /** */
    public void txDhtCommittedVersionsSize(int txDhtCommittedVersionsSize) {
        this.txDhtCommittedVersionsSize = txDhtCommittedVersionsSize;
    }

    /** */
    public void txDhtPrepareQueueSize(int txDhtPrepareQueueSize) {
        this.txDhtPrepareQueueSize = txDhtPrepareQueueSize;
    }

    /** */
    public void txDhtRolledbackVersionsSize(int txDhtRolledbackVersionsSize) {
        this.txDhtRolledbackVersionsSize = txDhtRolledbackVersionsSize;
    }

    /** */
    public void txDhtStartVersionCountsSize(int txDhtStartVerCountsSize) {
        this.txDhtStartVerCountsSize = txDhtStartVerCountsSize;
    }

    /** */
    public void txDhtThreadMapSize(int txDhtThreadMapSize) {
        this.txDhtThreadMapSize = txDhtThreadMapSize;
    }

    /** */
    public void txDhtXidMapSize(int txDhtXidMapSize) {
        this.txDhtXidMapSize = txDhtXidMapSize;
    }

    /** */
    public void txKeyCollisions(String txKeyCollisions) {
        this.txKeyCollisions = txKeyCollisions;
    }

    /** */
    public void txPrepareQueueSize(int txPrepareQueueSize) {
        this.txPrepareQueueSize = txPrepareQueueSize;
    }

    /** */
    public void cacheTxRollbacks(long txRollbacks) {
        cacheTxRollbacks = txRollbacks;
    }

    /** */
    public void txRolledbackVersionsSize(int txRolledbackVersionsSize) {
        this.txRolledbackVersionsSize = txRolledbackVersionsSize;
    }

    /** */
    public void txStartVersionCountsSize(int txStartVerCountsSize) {
        this.txStartVerCountsSize = txStartVerCountsSize;
    }

    /** */
    public void txThreadMapSize(int txThreadMapSize) {
        this.txThreadMapSize = txThreadMapSize;
    }

    /** */
    public void txXidMapSize(int txXidMapSize) {
        this.txXidMapSize = txXidMapSize;
    }

    /** */
    public void valueType(String valType) {
        this.valType = valType;
    }

    /** */
    public void writeBehindBufferSize(int writeBehindBufSize) {
        this.writeBehindBufSize = writeBehindBufSize;
    }

    /** */
    public void writeBehindCriticalOverflowCount(int writeBehindCriticalOverflowCnt) {
        this.writeBehindCriticalOverflowCnt = writeBehindCriticalOverflowCnt;
    }

    /** */
    public void writeBehindErrorRetryCount(int writeBehindErrorRetryCnt) {
        this.writeBehindErrorRetryCnt = writeBehindErrorRetryCnt;
    }

    /** */
    public void writeBehindFlushFrequency(long writeBehindFlushFreq) {
        this.writeBehindFlushFreq = writeBehindFlushFreq;
    }

    /** */
    public void writeBehindFlushSize(int writeBehindFlushSize) {
        this.writeBehindFlushSize = writeBehindFlushSize;
    }

    /** */
    public void writeBehindFlushThreadCount(int writeBehindFlushThreadCnt) {
        this.writeBehindFlushThreadCnt = writeBehindFlushThreadCnt;
    }

    /** */
    public void writeBehindStoreBatchSize(int writeBehindStoreBatchSize) {
        this.writeBehindStoreBatchSize = writeBehindStoreBatchSize;
    }

    /** */
    public void writeBehindTotalCriticalOverflowCount(int writeBehindTotalCriticalOverflowCnt) {
        this.writeBehindTotalCriticalOverflowCnt = writeBehindTotalCriticalOverflowCnt;
    }

    /** {@inheritDoc} */
    @Override public long getCacheHits() {
        return cacheHits();
    }

    /** {@inheritDoc} */
    @Override public float getCacheHitPercentage() {
        return cacheHitPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getCacheMisses() {
        return cacheMisses();
    }

    /** {@inheritDoc} */
    @Override public float getCacheMissPercentage() {
        return cacheMissPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getCacheGets() {
        return cacheGets();
    }

    /** {@inheritDoc} */
    @Override public long getCachePuts() {
        return cachePuts();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorPuts() {
        return entryProcessorPuts();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorReadOnlyInvocations() {
        return entryProcessorReadOnlyInvocations();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorInvocations() {
        return entryProcessorInvocations();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorHits() {
        return entryProcessorHits();
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorHitPercentage() {
        return entryProcessorHitPercentage();
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorMissPercentage() {
        return entryProcessorMissPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorMisses() {
        return entryProcessorMisses();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorRemovals() {
        return entryProcessorRemovals();
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorAverageInvocationTime() {
        return entryProcessorAverageInvocationTime();
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorMinInvocationTime() {
        return entryProcessorMinInvocationTime();
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorMaxInvocationTime() {
        return entryProcessorMaxInvocationTime();
    }

    /** {@inheritDoc} */
    @Override public long getCacheRemovals() {
        return cacheRemovals();
    }

    /** {@inheritDoc} */
    @Override public long getCacheEvictions() {
        return cacheEvictions();
    }

    /** {@inheritDoc} */
    @Override public float getAverageGetTime() {
        return averageGetTime();
    }

    /** {@inheritDoc} */
    @Override public float getAveragePutTime() {
        return averagePutTime();
    }

    /** {@inheritDoc} */
    @Override public float getAverageRemoveTime() {
        return averageRemoveTime();
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxCommitTime() {
        return averageTxCommitTime();
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxRollbackTime() {
        return averageTxRollbackTime();
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxCommits() {
        return cacheTxCommits();
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxRollbacks() {
        return cacheTxRollbacks();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cacheName();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapGets() {
        return offHeapGets();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapPuts() {
        return offHeapPuts();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapRemovals() {
        return offHeapRemovals();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapEvictions() {
        return offHeapEvictions();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapHits() {
        return offHeapHits();
    }

    /** {@inheritDoc} */
    @Override public float getOffHeapHitPercentage() {
        return offHeapHitPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapMisses() {
        return offHeapMisses();
    }

    /** {@inheritDoc} */
    @Override public float getOffHeapMissPercentage() {
        return offHeapMissPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapEntriesCount() {
        return offHeapEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getHeapEntriesCount() {
        return heapEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapPrimaryEntriesCount() {
        return offHeapPrimaryEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapBackupEntriesCount() {
        return offHeapBackupEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapAllocatedSize() {
        return offHeapAllocatedSize();
    }

    /** {@inheritDoc} */
    @Override public int getSize() {
        return size();
    }

    /** {@inheritDoc} */
    @Override public long getCacheSize() {
        return cacheSize();
    }

    /** {@inheritDoc} */
    @Override public int getKeySize() {
        return keySize();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return empty();
    }

    /** {@inheritDoc} */
    @Override public int getDhtEvictQueueCurrentSize() {
        return dhtEvictQueueCurrentSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxThreadMapSize() {
        return txThreadMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxXidMapSize() {
        return txXidMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxCommitQueueSize() {
        return txCommitQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxPrepareQueueSize() {
        return txPrepareQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxStartVersionCountsSize() {
        return txStartVersionCountsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxCommittedVersionsSize() {
        return txCommittedVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxRolledbackVersionsSize() {
        return txRolledbackVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtThreadMapSize() {
        return txDhtThreadMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtXidMapSize() {
        return txDhtXidMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtCommitQueueSize() {
        return txDhtCommitQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtPrepareQueueSize() {
        return txDhtPrepareQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtStartVersionCountsSize() {
        return txDhtStartVersionCountsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtCommittedVersionsSize() {
        return txDhtCommittedVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtRolledbackVersionsSize() {
        return txDhtRolledbackVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTotalPartitionsCount() {
        return totalPartitionsCount();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancedKeys() {
        return rebalancedKeys();
    }

    /** {@inheritDoc} */
    @Override public long getEstimatedRebalancingKeys() {
        return estimatedRebalancingKeys();
    }

    /** {@inheritDoc} */
    @Override public int getRebalancingPartitionsCount() {
        return rebalancingPartitionsCount();
    }

    /** {@inheritDoc} */
    @Override public long getKeysToRebalanceLeft() {
        return keysToRebalanceLeft();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingKeysRate() {
        return rebalancingKeysRate();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingBytesRate() {
        return rebalancingBytesRate();
    }

    /** {@inheritDoc} */
    @Override public long estimateRebalancingFinishTime() {
        return rebalanceFinishTime();
    }

    /** {@inheritDoc} */
    @Override public long rebalancingStartTime() {
        return rebalancingStartTime();
    }

    /** {@inheritDoc} */
    @Override public long getEstimatedRebalancingFinishTime() {
        return rebalanceFinishTime();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingStartTime() {
        return rebalancingStartTime();
    }

    /** {@inheritDoc} */
    @Override public long getRebalanceClearingPartitionsLeft() {
        return rebalanceClearingPartitionsLeft();
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteBehindEnabled() {
        return writeBehindEnabled();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindFlushSize() {
        return writeBehindFlushSize();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindFlushThreadCount() {
        return writeBehindFlushThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getWriteBehindFlushFrequency() {
        return writeBehindFlushFrequency();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindStoreBatchSize() {
        return writeBehindStoreBatchSize();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindTotalCriticalOverflowCount() {
        return writeBehindTotalCriticalOverflowCount();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindCriticalOverflowCount() {
        return writeBehindCriticalOverflowCount();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindErrorRetryCount() {
        return writeBehindErrorRetryCount();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindBufferSize() {
        return writeBehindBufferSize();
    }

    /** {@inheritDoc} */
    @Override public String getKeyType() {
        return keyType();
    }

    /** {@inheritDoc} */
    @Override public String getValueType() {
        return valueType();
    }

    /** {@inheritDoc} */
    @Override public boolean isStoreByValue() {
        return storeByValue();
    }

    /** {@inheritDoc} */
    @Override public boolean isStatisticsEnabled() {
        return statisticsEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isManagementEnabled() {
        return managementEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isReadThrough() {
        return readThrough();
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteThrough() {
        return writeThrough();
    }

    /** {@inheritDoc} */
    @Override public boolean isValidForReading() {
        return validForReading();
    }

    /** {@inheritDoc} */
    @Override public boolean isValidForWriting() {
        return validForWriting();
    }

    /** {@inheritDoc} */
    @Override public String getTxKeyCollisions() {
        return txKeyCollisions();
    }

    /** {@inheritDoc} */
    @Override public boolean isIndexRebuildInProgress() {
        return indexRebuildInProgress();
    }

    /** {@inheritDoc} */
    @Override public long getIndexRebuildKeysProcessed() {
        return indexRebuildKeysProcessed();
    }

    /** {@inheritDoc} */
    @Override public int getIndexBuildPartitionsLeftCount() {
        return indexBuildPartitionsLeftCount();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** */
    public String toString() {
        return S.toString(CacheMetricsMessage.class, this);
    }
}

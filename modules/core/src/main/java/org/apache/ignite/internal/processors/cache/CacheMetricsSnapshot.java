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
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Metrics snapshot.
 */
public class CacheMetricsSnapshot implements CacheMetrics, Message {
    /** */
    public static final short TYPE_CODE = 136;

    /** Number of reads. */
    @Order(value = 0, method = "cacheGets", getter = true)
    private long reads;

    /** Number of puts. */
    @Order(value = 1, method = "cachePuts", getter = true)
    private long puts;

    /** Number of invokes caused updates. */
    @Order(value = 2, getter = true)
    private long entryProcessorPuts;

    /** Number of invokes caused no updates. */
    @Order(value = 3, getter = true)
    private long entryProcessorReadOnlyInvocations;

    /**
     * The mean time to execute cache invokes
     */
    @Order(value = 4, getter = true)
    private float entryProcessorAverageInvocationTime;

    /**
     * The total number of cache invocations.
     */
    @Order(value = 5, getter = true)
    private long entryProcessorInvocations;

    /**
     * The total number of cache invocations, caused removal.
     */
    @Order(value = 6, getter = true)
    private long entryProcessorRemovals;

    /**
     * The total number of invocations on keys, which don't exist in cache.
     */
    @Order(value = 7, getter = true)
    private long entryProcessorMisses;

    /**
     * The total number of invocations on keys, which exist in cache.
     */
    @Order(value = 8, getter = true)
    private long entryProcessorHits;

    /**
     * The percentage of invocations on keys, which don't exist in cache.
     */
    @Order(value = 9, getter = true)
    private float entryProcessorMissPercentage;

    /**
     * The percentage of invocations on keys, which exist in cache.
     */
    @Order(value = 10, getter = true)
    private float entryProcessorHitPercentage;

    /**
     * So far, the maximum time to execute cache invokes.
     */
    @Order(value = 11, getter = true)
    private float entryProcessorMaxInvocationTime;

    /**
     * So far, the minimum time to execute cache invokes.
     */
    @Order(value = 12, getter = true)
    private float entryProcessorMinInvocationTime;

    /** Number of hits. */
    @Order(value = 13, method = "cacheHits", getter = true)
    private long hits;

    /** Number of misses. */
    @Order(value = 14, method = "cacheMisses", getter = true)
    private long misses;

    /** Number of transaction commits. */
    @Order(value = 15, method = "cacheTxCommits", getter = true)
    private long txCommits;

    /** Number of transaction rollbacks. */
    @Order(value = 16, method = "cacheTxRollbacks", getter = true)
    private long txRollbacks;

    /** Number of evictions. */
    @Order(value = 17, method = "cacheEvictions", getter = true)
    private long evicts;

    /** Number of removed entries. */
    @Order(value = 18, method = "cacheRemovals", getter = true)
    private long removes;

    /** Put time taken nanos. */
    @Order(value = 19, method = "averagePutTime", getter = true)
    private float putAvgTimeNanos;

    /** Get time taken nanos. */
    @Order(value = 20, method = "averageGetTime", getter = true)
    private float getAvgTimeNanos;

    /** Remove time taken nanos. */
    @Order(value = 21, method = "averageRemoveTime", getter = true)
    private float rmvAvgTimeNanos;

    /** Commit transaction time taken nanos. */
    @Order(value = 22, method = "averageTxCommitTime", getter = true)
    private float commitAvgTimeNanos;

    /** Commit transaction time taken nanos. */
    @Order(value = 23, method = "averageTxRollbackTime", getter = true)
    private float rollbackAvgTimeNanos;

    /** Cache name */
    @Order(value = 24, method = "name")
    private String cacheName;

    /** Number of reads from off-heap. */
    @Order(value = 25, getter = true)
    private long offHeapGets;

    /** Number of writes to off-heap. */
    @Order(value = 26, getter = true)
    private long offHeapPuts;

    /** Number of removed entries from off-heap. */
    @Order(value = 27, method = "offHeapRemovals", getter = true)
    private long offHeapRemoves;

    /** Number of evictions from off-heap. */
    @Order(value = 28, method = "offHeapEvictions", getter = true)
    private long offHeapEvicts;

    /** Off-heap hits number. */
    @Order(value = 29, method = "offHeapHits", getter = true)
    private long offHeapHits;

    /** Off-heap misses number. */
    @Order(value = 30, getter = true)
    private long offHeapMisses;

    /** Number of entries stored in off-heap memory. */
    @Order(value = 31, method = "offHeapEntriesCount", getter = true)
    private long offHeapEntriesCnt;

    /** Number of entries stored in heap. */
    @Order(value = 32, method = "heapEntriesCount", getter = true)
    private long heapEntriesCnt;

    /** Number of primary entries stored in off-heap memory. */
    @Order(value = 33, method = "offHeapPrimaryEntriesCount", getter = true)
    private long offHeapPrimaryEntriesCnt;

    /** Number of backup entries stored in off-heap memory. */
    @Order(value = 34, method = "offHeapBackupEntriesCount", getter = true)
    private long offHeapBackupEntriesCnt;

    /** Memory size allocated in off-heap. */
    @Order(value = 35, getter = true)
    private long offHeapAllocatedSize;

    /** Number of non-{@code null} values in the cache. */
    @Order(value = 36, getter = true)
    private int size;

    /** Cache size. */
    @Order(value = 37, getter = true)
    private long cacheSize;

    /** Number of keys in the cache, possibly with {@code null} values. */
    @Order(value = 38, getter = true)
    private int keySize;

    /** Cache is empty. */
    @Order(value = 39, method = "empty", getter = true)
    private boolean isEmpty;

    /** Gets current size of evict queue used to batch up evictions. */
    @Order(value = 40, method = "dhtEvictQueueCurrentSize", getter = true)
    private int dhtEvictQueueCurrSize;

    /** Transaction per-thread map size. */
    @Order(value = 41, getter = true)
    private int txThreadMapSize;

    /** Transaction per-Xid map size. */
    @Order(value = 42, getter = true)
    private int txXidMapSize;

    /** Committed transaction queue size. */
    @Order(value = 43, getter = true)
    private int txCommitQueueSize;

    /** Prepared transaction queue size. */
    @Order(value = 44, getter = true)
    private int txPrepareQueueSize;

    /** Start version counts map size. */
    @Order(value = 45, method = "txStartVersionCountsSize", getter = true)
    private int txStartVerCountsSize;

    /** Number of cached committed transaction IDs. */
    @Order(value = 46, getter = true)
    private int txCommittedVersionsSize;

    /** Number of cached rolled back transaction IDs. */
    @Order(value = 47, getter = true)
    private int txRolledbackVersionsSize;

    /** DHT thread map size. */
    @Order(value = 48, method = "txDhtThreadMapSize", getter = true)
    private int txDhtThreadMapSize;

    /** Transaction DHT per-Xid map size. */
    @Order(value = 49, method = "txDhtXidMapSize", getter = true)
    private int txDhtXidMapSize;

    /** Committed DHT transaction queue size. */
    @Order(value = 50, getter = true)
    private int txDhtCommitQueueSize;

    /** Prepared DHT transaction queue size. */
    @Order(value = 51, getter = true)
    private int txDhtPrepareQueueSize;

    /** DHT start version counts map size. */
    @Order(value = 52, method = "txDhtStartVersionCountsSize", getter = true)
    private int txDhtStartVerCountsSize;

    /** Number of cached committed DHT transaction IDs. */
    @Order(value = 53, getter = true)
    private int txDhtCommittedVersionsSize;

    /** Number of cached rolled back DHT transaction IDs. */
    @Order(value = 54, getter = true)
    private int txDhtRolledbackVersionsSize;

    /** Write-behind is enabled. */
    @Order(value = 55, method = "writeBehindEnabled", getter = true)
    private boolean isWriteBehindEnabled;

    /** Buffer size that triggers flush procedure. */
    @Order(value = 56, getter = true)
    private int writeBehindFlushSize;

    /** Count of worker threads. */
    @Order(value = 57, method = "writeBehindFlushThreadCount", getter = true)
    private int writeBehindFlushThreadCnt;

    /** Flush frequency in milliseconds. */
    @Order(value = 58, method = "writeBehindFlushFrequency", getter = true)
    private long writeBehindFlushFreq;

    /** Maximum size of batch. */
    @Order(value = 59, getter = true)
    private int writeBehindStoreBatchSize;

    /** Count of cache overflow events since start. */
    @Order(value = 60, method = "writeBehindTotalCriticalOverflowCount", getter = true)
    private int writeBehindTotalCriticalOverflowCnt;

    /** Count of cache overflow events since start. */
    @Order(value = 61, method = "writeBehindCriticalOverflowCount", getter = true)
    private int writeBehindCriticalOverflowCnt;

    /** Count of entries in store-retry state. */
    @Order(value = 62, method = "writeBehindErrorRetryCount", getter = true)
    private int writeBehindErrorRetryCnt;

    /** Total count of entries in cache store internal buffer. */
    @Order(value = 63, method = "writeBehindBufferSize", getter = true)
    private int writeBehindBufSize;

    /** Total partitions count. */
    @Order(value = 64, method = "totalPartitionsCount", getter = true)
    private int totalPartitionsCnt;

    /** Rebalancing partitions count. */
    @Order(value = 65, method = "rebalancingPartitionsCount", getter = true)
    private int rebalancingPartitionsCnt;

    /** Number of already rebalanced keys. */
    @Order(value = 66, getter = true)
    private long rebalancedKeys;

    /** Number estimated to rebalance keys. */
    @Order(value = 67, getter = true)
    private long estimatedRebalancingKeys;

    /** Keys to rebalance left. */
    @Order(value = 68, getter = true)
    private long keysToRebalanceLeft;

    /** Rebalancing keys rate. */
    @Order(value = 69, getter = true)
    private long rebalancingKeysRate;

    /** Get rebalancing bytes rate. */
    @Order(value = 70, getter = true)
    private long rebalancingBytesRate;

    /** Start rebalance time. */
    @Order(value = 71, method = "rebalancingStartTime", getter = true)
    private long rebalanceStartTime;

    /** Estimate rebalance finish time. */
    @Order(value = 72, method = "estimatedRebalancingFinishTime", getter = true)
    private long rebalanceFinishTime;

    /** The number of clearing partitions need to await before rebalance. */
    @Order(value = 73, getter = true)
    private long rebalanceClearingPartitionsLeft;

    /** */
    @Order(value = 74, getter = true)
    private String keyType;

    /** */
    @Order(value = 75, method = "valueType", getter = true)
    private String valType;

    /** */
    @Order(value = 76, method = "storeByValue", getter = true)
    private boolean isStoreByVal;

    /** */
    @Order(value = 77, method = "statisticsEnabled", getter = true)
    private boolean isStatisticsEnabled;

    /** */
    @Order(value = 78, method = "managementEnabled", getter = true)
    private boolean isManagementEnabled;

    /** */
    @Order(value = 79, method = "readThrough", getter = true)
    private boolean isReadThrough;

    /** */
    @Order(value = 80, method = "writeThrough", getter = true)
    private boolean isWriteThrough;

    /** */
    @Order(value = 81, method = "validForReading", getter = true)
    private boolean isValidForReading;

    /** */
    @Order(value = 82, method = "validForWriting", getter = true)
    private boolean isValidForWriting;

    /** Tx key collisions with appropriate queue size string representation. */
    @Order(value = 83, method = "txKeyCollisions", getter = true)
    private String txKeyCollisions;

    /** Index rebuilding in progress. */
    @Order(value = 84, method = "indexRebuildInProgress", getter = true)
    private boolean idxRebuildInProgress;

    /** Number of keys processed during index rebuilding. */
    @Order(value = 85, method = "indexRebuildKeysProcessed", getter = true)
    private long idxRebuildKeyProcessed;

    /** The number of local node partitions that remain to be processed to complete indexing. */
    @Order(value = 86, method = "indexBuildPartitionsLeftCount", getter = true)
    private int idxBuildPartitionsLeftCount;

    /**
     * Default constructor.
     */
    public CacheMetricsSnapshot() {
        // No-op.
    }

    /** */
    public static CacheMetricsSnapshot of(CacheMetrics metrics) {
        return metrics instanceof CacheMetricsSnapshot ? (CacheMetricsSnapshot)metrics : new CacheMetricsSnapshot(metrics);
    }

    /**
     * Create snapshot for given metrics.
     *
     * @param m Cache metrics.
     */
    private CacheMetricsSnapshot(CacheMetrics m) {
        reads = m.getCacheGets();
        puts = m.getCachePuts();
        hits = m.getCacheHits();
        misses = m.getCacheMisses();
        txCommits = m.getCacheTxCommits();
        txRollbacks = m.getCacheTxRollbacks();
        evicts = m.getCacheEvictions();
        removes = m.getCacheRemovals();

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

        putAvgTimeNanos = m.getAveragePutTime();
        getAvgTimeNanos = m.getAverageGetTime();
        rmvAvgTimeNanos = m.getAverageRemoveTime();
        commitAvgTimeNanos = m.getAverageTxCommitTime();
        rollbackAvgTimeNanos = m.getAverageTxRollbackTime();

        cacheName = m.name();

        offHeapGets = m.getOffHeapGets();
        offHeapPuts = m.getOffHeapPuts();
        offHeapRemoves = m.getOffHeapRemovals();
        offHeapEvicts = m.getOffHeapEvictions();
        offHeapHits = m.getOffHeapHits();
        offHeapMisses = m.getOffHeapMisses();

        offHeapEntriesCnt = m.getHeapEntriesCount();
        heapEntriesCnt = m.getHeapEntriesCount();
        offHeapPrimaryEntriesCnt = m.getOffHeapPrimaryEntriesCount();
        offHeapBackupEntriesCnt = m.getOffHeapBackupEntriesCount();

        offHeapAllocatedSize = m.getOffHeapAllocatedSize();

        cacheSize = m.getCacheSize();
        keySize = m.getKeySize();
        size = m.getSize();
        isEmpty = m.isEmpty();

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
        isWriteBehindEnabled = m.isWriteBehindEnabled();
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
        isStoreByVal = m.isStoreByValue();
        isStatisticsEnabled = m.isStatisticsEnabled();
        isManagementEnabled = m.isManagementEnabled();
        isReadThrough = m.isReadThrough();
        isWriteThrough = m.isWriteThrough();
        isValidForReading = m.isValidForReading();
        isValidForWriting = m.isValidForWriting();

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
     * Constructs merged cache metrics.
     *
     * @param loc Metrics for cache on local node.
     * @param metrics Metrics for merge.
     */
    public CacheMetricsSnapshot(CacheMetrics loc, Collection<CacheMetrics> metrics) {
        cacheName = loc.name();
        isEmpty = loc.isEmpty();
        isWriteBehindEnabled = loc.isWriteBehindEnabled();
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
        isStoreByVal = loc.isStoreByValue();
        isStatisticsEnabled = loc.isStatisticsEnabled();
        isManagementEnabled = loc.isManagementEnabled();
        isReadThrough = loc.isReadThrough();
        isWriteThrough = loc.isWriteThrough();
        isValidForReading = loc.isValidForReading();
        isValidForWriting = loc.isValidForWriting();

        for (CacheMetrics e : metrics) {
            reads += e.getCacheGets();
            puts += e.getCachePuts();
            size += e.getSize();
            keySize += e.getKeySize();
            cacheSize += e.getCacheSize();
            isEmpty &= e.isEmpty();
            hits += e.getCacheHits();
            misses += e.getCacheMisses();
            txCommits += e.getCacheTxCommits();
            txRollbacks += e.getCacheTxRollbacks();
            evicts += e.getCacheEvictions();
            removes += e.getCacheRemovals();

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

            putAvgTimeNanos += e.getAveragePutTime();
            getAvgTimeNanos += e.getAverageGetTime();
            rmvAvgTimeNanos += e.getAverageRemoveTime();
            commitAvgTimeNanos += e.getAverageTxCommitTime();
            rollbackAvgTimeNanos += e.getAverageTxRollbackTime();

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
            putAvgTimeNanos /= size;
            getAvgTimeNanos /= size;
            rmvAvgTimeNanos /= size;
            commitAvgTimeNanos /= size;
            rollbackAvgTimeNanos /= size;
        }
    }

    /** {@inheritDoc} */
    @Override public long getCacheHits() {
        return hits;
    }

    /** {@inheritDoc} */
    @Override public float getCacheHitPercentage() {
        if (hits == 0 || reads == 0)
            return 0;

        return (float)hits / reads * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getCacheMisses() {
        return misses;
    }

    /** {@inheritDoc} */
    @Override public float getCacheMissPercentage() {
        if (misses == 0 || reads == 0)
            return 0;

        return (float)misses / reads * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getCacheGets() {
        return reads;
    }

    /** {@inheritDoc} */
    @Override public long getCachePuts() {
        return puts;
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
        return removes;
    }

    /** {@inheritDoc} */
    @Override public long getCacheEvictions() {
        return evicts;
    }

    /** {@inheritDoc} */
    @Override public float getAverageGetTime() {
        return getAvgTimeNanos;
    }

    /** {@inheritDoc} */
    @Override public float getAveragePutTime() {
        return putAvgTimeNanos;
    }

    /** {@inheritDoc} */
    @Override public float getAverageRemoveTime() {
        return rmvAvgTimeNanos;
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxCommitTime() {
        return commitAvgTimeNanos;
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxRollbackTime() {
        return rollbackAvgTimeNanos;
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxCommits() {
        return txCommits;
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxRollbacks() {
        return txRollbacks;
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
        return isEmpty;
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
        return isWriteBehindEnabled;
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
        return isStoreByVal;
    }

    /** {@inheritDoc} */
    @Override public boolean isStatisticsEnabled() {
        return isStatisticsEnabled;
    }

    /** {@inheritDoc} */
    @Override public boolean isManagementEnabled() {
        return isManagementEnabled;
    }

    /** {@inheritDoc} */
    @Override public boolean isReadThrough() {
        return isReadThrough;
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteThrough() {
        return isWriteThrough;
    }

    /** {@inheritDoc} */
    @Override public boolean isValidForReading() {
        return isValidForReading;
    }

    /** {@inheritDoc} */
    @Override public boolean isValidForWriting() {
        return isValidForWriting;
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
        return idxRebuildKeyProcessed;
    }

    /** {@inheritDoc} */
    @Override public int getIndexBuildPartitionsLeftCount() {
        return idxBuildPartitionsLeftCount;
    }

    /** */
    public void cachePuts(long puts) {
        this.puts = puts;
    }

    /** */
    public void entryProcessorPuts(long entryProcPuts) {
        this.entryProcessorPuts = entryProcPuts;
    }

    /** */
    public void cacheGets(long reads) {
        this.reads = reads;
    }

    /** */
    public void name(String cacheName) {
        this.cacheName = cacheName;
    }

    /** */
    public void cacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
    }

    /** */
    public void averageTxCommitTime(float commitAvgTimeNanos) {
        this.commitAvgTimeNanos = commitAvgTimeNanos;
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
    public void entryProcessorMaxInvocationTime(float entryProcessorMaxInvocationTime) {
        this.entryProcessorMaxInvocationTime = entryProcessorMaxInvocationTime;
    }

    /** */
    public void entryProcessorMinInvocationTime(float entryProcessorMinInvocationTime) {
        this.entryProcessorMinInvocationTime = entryProcessorMinInvocationTime;
    }

    /** */
    public void entryProcessorMisses(long entryProcessorMisses) {
        this.entryProcessorMisses = entryProcessorMisses;
    }

    /** */
    public void entryProcessorMissPercentage(float entryProcessorMissPercentage) {
        this.entryProcessorMissPercentage = entryProcessorMissPercentage;
    }

    /** */
    public void entryProcessorReadOnlyInvocations(long entryProcessorReadOnlyInvocations) {
        this.entryProcessorReadOnlyInvocations = entryProcessorReadOnlyInvocations;
    }

    /** */
    public void entryProcessorRemovals(long entryProcessorRemovals) {
        this.entryProcessorRemovals = entryProcessorRemovals;
    }

    /** */
    public void estimatedRebalancingKeys(long estimatedRebalancingKeys) {
        this.estimatedRebalancingKeys = estimatedRebalancingKeys;
    }

    /** */
    public void cacheEvictions(long evicts) {
        this.evicts = evicts;
    }

    /** */
    public void averageGetTime(float getAvgTimeNanos) {
        this.getAvgTimeNanos = getAvgTimeNanos;
    }

    /** */
    public void heapEntriesCount(long heapEntriesCnt) {
        this.heapEntriesCnt = heapEntriesCnt;
    }

    /** */
    public void cacheHits(long hits) {
        this.hits = hits;
    }

    /** */
    public void indexBuildPartitionsLeftCount(int idxBuildPartitionsLeftCount) {
        this.idxBuildPartitionsLeftCount = idxBuildPartitionsLeftCount;
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
        isEmpty = empty;
    }

    /** */
    public void managementEnabled(boolean managementEnabled) {
        isManagementEnabled = managementEnabled;
    }

    /** */
    public void readThrough(boolean readThrough) {
        isReadThrough = readThrough;
    }

    /** */
    public void statisticsEnabled(boolean statisticsEnabled) {
        isStatisticsEnabled = statisticsEnabled;
    }

    /** */
    public void storeByValue(boolean storeByVal) {
        isStoreByVal = storeByVal;
    }

    /** */
    public void validForReading(boolean validForReading) {
        isValidForReading = validForReading;
    }

    /** */
    public void validForWriting(boolean validForWriting) {
        isValidForWriting = validForWriting;
    }

    /** */
    public void writeBehindEnabled(boolean writeBehindEnabled) {
        isWriteBehindEnabled = writeBehindEnabled;
    }

    /** */
    public void writeThrough(boolean writeThrough) {
        isWriteThrough = writeThrough;
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
        this.misses = misses;
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
        this.putAvgTimeNanos = putAvgTimeNanos;
    }

    /** */
    public void puts(long puts) {
        this.puts = puts;
    }

    /** */
    public void reads(long reads) {
        this.reads = reads;
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
    public void estimatedRebalancingFinishTime(long rebalanceFinishTime) {
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
        this.removes = removes;
    }

    /** */
    public void averageRemoveTime(float rmvAvgTimeNanos) {
        this.rmvAvgTimeNanos = rmvAvgTimeNanos;
    }

    /** */
    public void averageTxRollbackTime(float rollbackAvgTimeNanos) {
        this.rollbackAvgTimeNanos = rollbackAvgTimeNanos;
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
        this.txCommits = txCommits;
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
        this.txRollbacks = txRollbacks;
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
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheMetricsSnapshot.class, this);
    }
}

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
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/** Cache metrics message. */
public class CacheMetricsMessage implements Message {
    /** */
    public static final short TYPE_CODE = 136;

    /** Number of reads. */
    @Order(value = 0)
    protected long cacheGets;

    /** Number of puts. */
    @Order(value = 1)
    protected long cachePuts;

    /** Number of invokes caused updates. */
    @Order(value = 2)
    protected long entryProcessorPuts;

    /** Number of invokes caused no updates. */
    @Order(value = 3)
    protected long entryProcessorReadOnlyInvocations;

    /**
     * The mean time to execute cache invokes
     */
    @Order(value = 4)
    protected float entryProcessorAverageInvocationTime;

    /**
     * The total number of cache invocations.
     */
    @Order(value = 5)
    protected long entryProcessorInvocations;

    /**
     * The total number of cache invocations, caused removal.
     */
    @Order(value = 6)
    protected long entryProcessorRemovals;

    /**
     * The total number of invocations on keys, which don't exist in cache.
     */
    @Order(value = 7)
    protected long entryProcessorMisses;

    /**
     * The total number of invocations on keys, which exist in cache.
     */
    @Order(value = 8)
    protected long entryProcessorHits;

    /**
     * The percentage of invocations on keys, which don't exist in cache.
     */
    @Order(value = 9)
    protected float entryProcessorMissPercentage;

    /**
     * The percentage of invocations on keys, which exist in cache.
     */
    @Order(value = 10)
    protected float entryProcessorHitPercentage;

    /**
     * So far, the maximum time to execute cache invokes.
     */
    @Order(value = 11)
    protected float entryProcessorMaxInvocationTime;

    /**
     * So far, the minimum time to execute cache invokes.
     */
    @Order(value = 12)
    protected float entryProcessorMinInvocationTime;

    /** Number of hits. */
    @Order(value = 13)
    protected long cacheHits;

    /** Number of misses. */
    @Order(value = 14)
    protected long cacheMisses;

    /** Number of transaction commits. */
    @Order(value = 15)
    protected long cacheTxCommits;

    /** Number of transaction rollbacks. */
    @Order(value = 16)
    protected long cacheTxRollbacks;

    /** Number of evictions. */
    @Order(value = 17)
    protected long cacheEvictions;

    /** Number of removed entries. */
    @Order(value = 18)
    protected long cacheRemovals;

    /** Put time taken nanos. */
    @Order(value = 19)
    protected float averagePutTime;

    /** Get time taken nanos. */
    @Order(value = 20)
    protected float averageGetTime;

    /** Remove time taken nanos. */
    @Order(value = 21)
    protected float averageRemoveTime;

    /** Commit transaction time taken nanos. */
    @Order(value = 22)
    protected float averageTxCommitTime;

    /** Commit transaction time taken nanos. */
    @Order(value = 23)
    protected float averageTxRollbackTime;

    /** Cache name */
    @Order(value = 24)
    protected String cacheName;

    /** Number of reads from off-heap. */
    @Order(value = 25)
    protected long offHeapGets;

    /** Number of writes to off-heap. */
    @Order(value = 26)
    protected long offHeapPuts;

    /** Number of removed entries from off-heap. */
    @Order(value = 27, method = "offHeapRemovals")
    protected long offHeapRemoves;

    /** Number of evictions from off-heap. */
    @Order(value = 28, method = "offHeapEvictions")
    protected long offHeapEvicts;

    /** Off-heap hits number. */
    @Order(value = 29, method = "offHeapHits")
    protected long offHeapHits;

    /** Off-heap misses number. */
    @Order(value = 30)
    protected long offHeapMisses;

    /** Number of entries stored in off-heap memory. */
    @Order(value = 31, method = "offHeapEntriesCount")
    protected long offHeapEntriesCnt;

    /** Number of entries stored in heap. */
    @Order(value = 32, method = "heapEntriesCount")
    protected long heapEntriesCnt;

    /** Number of primary entries stored in off-heap memory. */
    @Order(value = 33, method = "offHeapPrimaryEntriesCount")
    protected long offHeapPrimaryEntriesCnt;

    /** Number of backup entries stored in off-heap memory. */
    @Order(value = 34, method = "offHeapBackupEntriesCount")
    protected long offHeapBackupEntriesCnt;

    /** Memory size allocated in off-heap. */
    @Order(value = 35)
    protected long offHeapAllocatedSize;

    /** Number of non-{@code null} values in the cache. */
    @Order(value = 36)
    protected int size;

    /** Cache size. */
    @Order(value = 37)
    protected long cacheSize;

    /** Number of keys in the cache, possibly with {@code null} values. */
    @Order(value = 38)
    protected int keySize;

    /** Cache is empty. */
    @Order(value = 39)
    protected boolean empty;

    /** Gets current size of evict queue used to batch up evictions. */
    @Order(value = 40, method = "dhtEvictQueueCurrentSize")
    protected int dhtEvictQueueCurrSize;

    /** Transaction per-thread map size. */
    @Order(value = 41)
    protected int txThreadMapSize;

    /** Transaction per-Xid map size. */
    @Order(value = 42)
    protected int txXidMapSize;

    /** Committed transaction queue size. */
    @Order(value = 43)
    protected int txCommitQueueSize;

    /** Prepared transaction queue size. */
    @Order(value = 44)
    protected int txPrepareQueueSize;

    /** Start version counts map size. */
    @Order(value = 45, method = "txStartVersionCountsSize")
    protected int txStartVerCountsSize;

    /** Number of cached committed transaction IDs. */
    @Order(value = 46)
    protected int txCommittedVersionsSize;

    /** Number of cached rolled back transaction IDs. */
    @Order(value = 47)
    protected int txRolledbackVersionsSize;

    /** DHT thread map size. */
    @Order(value = 48, method = "txDhtThreadMapSize")
    protected int txDhtThreadMapSize;

    /** Transaction DHT per-Xid map size. */
    @Order(value = 49, method = "txDhtXidMapSize")
    protected int txDhtXidMapSize;

    /** Committed DHT transaction queue size. */
    @Order(value = 50)
    protected int txDhtCommitQueueSize;

    /** Prepared DHT transaction queue size. */
    @Order(value = 51)
    protected int txDhtPrepareQueueSize;

    /** DHT start version counts map size. */
    @Order(value = 52, method = "txDhtStartVersionCountsSize")
    protected int txDhtStartVerCountsSize;

    /** Number of cached committed DHT transaction IDs. */
    @Order(value = 53)
    protected int txDhtCommittedVersionsSize;

    /** Number of cached rolled back DHT transaction IDs. */
    @Order(value = 54)
    protected int txDhtRolledbackVersionsSize;

    /** Write-behind is enabled. */
    @Order(value = 55)
    protected boolean writeBehindEnabled;

    /** Buffer size that triggers flush procedure. */
    @Order(value = 56)
    protected int writeBehindFlushSize;

    /** Count of worker threads. */
    @Order(value = 57, method = "writeBehindFlushThreadCount")
    protected int writeBehindFlushThreadCnt;

    /** Flush frequency in milliseconds. */
    @Order(value = 58, method = "writeBehindFlushFrequency")
    protected long writeBehindFlushFreq;

    /** Maximum size of batch. */
    @Order(value = 59)
    protected int writeBehindStoreBatchSize;

    /** Count of cache overflow events since start. */
    @Order(value = 60, method = "writeBehindTotalCriticalOverflowCount")
    protected int writeBehindTotalCriticalOverflowCnt;

    /** Count of cache overflow events since start. */
    @Order(value = 61, method = "writeBehindCriticalOverflowCount")
    protected int writeBehindCriticalOverflowCnt;

    /** Count of entries in store-retry state. */
    @Order(value = 62, method = "writeBehindErrorRetryCount")
    protected int writeBehindErrorRetryCnt;

    /** Total count of entries in cache store internal buffer. */
    @Order(value = 63, method = "writeBehindBufferSize")
    protected int writeBehindBufSize;

    /** Total partitions count. */
    @Order(value = 64, method = "totalPartitionsCount")
    protected int totalPartitionsCnt;

    /** Rebalancing partitions count. */
    @Order(value = 65, method = "rebalancingPartitionsCount")
    protected int rebalancingPartitionsCnt;

    /** Number of already rebalanced keys. */
    @Order(value = 66)
    protected long rebalancedKeys;

    /** Number estimated to rebalance keys. */
    @Order(value = 67)
    protected long estimatedRebalancingKeys;

    /** Keys to rebalance left. */
    @Order(value = 68)
    protected long keysToRebalanceLeft;

    /** Rebalancing keys rate. */
    @Order(value = 69)
    protected long rebalancingKeysRate;

    /** Get rebalancing bytes rate. */
    @Order(value = 70)
    protected long rebalancingBytesRate;

    /** Start rebalance time. */
    @Order(value = 71, method = "rebalancingStartTime")
    protected long rebalanceStartTime;

    /** Estimate rebalance finish time. */
    @Order(value = 72)
    protected long rebalanceFinishTime;

    /** The number of clearing partitions need to await before rebalance. */
    @Order(value = 73)
    protected long rebalanceClearingPartitionsLeft;

    /** */
    @Order(value = 74)
    protected String keyType;

    /** */
    @Order(value = 75, method = "valueType")
    protected String valType;

    /** */
    @Order(value = 76, method = "storeByValue")
    protected boolean storeByVal;

    /** */
    @Order(value = 77)
    protected boolean statisticsEnabled;

    /** */
    @Order(value = 78)
    protected boolean managementEnabled;

    /** */
    @Order(value = 79)
    protected boolean readThrough;

    /** */
    @Order(value = 80)
    protected boolean writeThrough;

    /** */
    @Order(value = 81)
    protected boolean validForReading;

    /** */
    @Order(value = 82)
    protected boolean validForWriting;

    /** Tx key collisions with appropriate queue size string representation. */
    @Order(value = 83)
    protected String txKeyCollisions;

    /** Index rebuilding in progress. */
    @Order(value = 84, method = "indexRebuildInProgress")
    protected boolean idxRebuildInProgress;

    /** Number of keys processed idxRebuildInProgressduring index rebuilding. */
    @Order(value = 85, method = "indexRebuildKeysProcessed")
    protected long idxRebuildKeyProcessed;

    /** The number of local node partitions that remain to be processed to complete indexing. */
    @Order(value = 86, method = "indexBuildPartitionsLeftCount")
    protected int idxBuildPartitionsLeftCount;

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

        offHeapEntriesCnt = m.getHeapEntriesCount();
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
    public long estimateRebalancingFinishTime() {
        return rebalanceFinishTime;
    }

    /** */
    public long rebalancingStartTime() {
        return rebalanceStartTime;
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
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** */
    public String toString() {
        return S.toString(CacheMetricsMessage.class, this);
    }
}

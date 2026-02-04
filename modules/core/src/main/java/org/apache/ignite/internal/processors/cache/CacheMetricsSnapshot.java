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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cluster.CacheMetricsMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Metrics snapshot.
 */
public class CacheMetricsSnapshot extends IgniteDataTransferObject implements CacheMetrics {
    /** */
    private static final long serialVersionUID = 0L;

    /** Metrics values holder. */
    private CacheMetricsMessage m;

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
     * @param cacheMetricsMsg Cache metrics message.
     */
    public CacheMetricsSnapshot(CacheMetricsMessage cacheMetricsMsg) {
        m = cacheMetricsMsg;
    }

    /** {@inheritDoc} */
    @Override public long getCacheHits() {
        return m.cacheHits();
    }

    /** {@inheritDoc} */
    @Override public float getCacheHitPercentage() {
        return m.cacheHitPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getCacheMisses() {
        return m.cacheMisses();
    }

    /** {@inheritDoc} */
    @Override public float getCacheMissPercentage() {
        return m.cacheMissPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getCacheGets() {
        return m.cacheGets();
    }

    /** {@inheritDoc} */
    @Override public long getCachePuts() {
        return m.cachePuts();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorPuts() {
        return m.entryProcessorPuts();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorReadOnlyInvocations() {
        return m.entryProcessorReadOnlyInvocations();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorInvocations() {
        return m.entryProcessorInvocations();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorHits() {
        return m.entryProcessorHits();
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorHitPercentage() {
        return m.entryProcessorHitPercentage();
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorMissPercentage() {
        return m.entryProcessorMissPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorMisses() {
        return m.entryProcessorMisses();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorRemovals() {
        return m.entryProcessorRemovals();
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorAverageInvocationTime() {
        return m.entryProcessorAverageInvocationTime();
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorMinInvocationTime() {
        return m.entryProcessorMinInvocationTime();
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorMaxInvocationTime() {
        return m.entryProcessorMaxInvocationTime();
    }

    /** {@inheritDoc} */
    @Override public long getCacheRemovals() {
        return m.cacheRemovals();
    }

    /** {@inheritDoc} */
    @Override public long getCacheEvictions() {
        return m.cacheEvictions();
    }

    /** {@inheritDoc} */
    @Override public float getAverageGetTime() {
        return m.averageGetTime();
    }

    /** {@inheritDoc} */
    @Override public float getAveragePutTime() {
        return m.averagePutTime();
    }

    /** {@inheritDoc} */
    @Override public float getAverageRemoveTime() {
        return m.averageRemoveTime();
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxCommitTime() {
        return m.averageTxCommitTime();
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxRollbackTime() {
        return m.averageTxRollbackTime();
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxCommits() {
        return m.cacheTxCommits();
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxRollbacks() {
        return m.cacheTxRollbacks();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return m.cacheName();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapGets() {
        return m.offHeapGets();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapPuts() {
        return m.offHeapPuts();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapRemovals() {
        return m.offHeapRemovals();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapEvictions() {
        return m.offHeapEvictions();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapHits() {
        return m.offHeapHits();
    }

    /** {@inheritDoc} */
    @Override public float getOffHeapHitPercentage() {
        return m.offHeapHitPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapMisses() {
        return m.offHeapMisses();
    }

    /** {@inheritDoc} */
    @Override public float getOffHeapMissPercentage() {
        return m.offHeapMissPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapEntriesCount() {
        return m.offHeapEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getHeapEntriesCount() {
        return m.heapEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapPrimaryEntriesCount() {
        return m.offHeapPrimaryEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapBackupEntriesCount() {
        return m.offHeapBackupEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapAllocatedSize() {
        return m.offHeapAllocatedSize();
    }

    /** {@inheritDoc} */
    @Override public int getSize() {
        return m.size();
    }

    /** {@inheritDoc} */
    @Override public long getCacheSize() {
        return m.cacheSize();
    }

    /** {@inheritDoc} */
    @Override public int getKeySize() {
        return m.keySize();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return m.empty();
    }

    /** {@inheritDoc} */
    @Override public int getDhtEvictQueueCurrentSize() {
        return m.dhtEvictQueueCurrentSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxThreadMapSize() {
        return m.txThreadMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxXidMapSize() {
        return m.txXidMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxCommitQueueSize() {
        return m.txCommitQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxPrepareQueueSize() {
        return m.txPrepareQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxStartVersionCountsSize() {
        return m.txStartVersionCountsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxCommittedVersionsSize() {
        return m.txCommittedVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxRolledbackVersionsSize() {
        return m.txRolledbackVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtThreadMapSize() {
        return m.txDhtThreadMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtXidMapSize() {
        return m.txDhtXidMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtCommitQueueSize() {
        return m.txDhtCommitQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtPrepareQueueSize() {
        return m.txDhtPrepareQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtStartVersionCountsSize() {
        return m.txDhtStartVersionCountsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtCommittedVersionsSize() {
        return m.txDhtCommittedVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtRolledbackVersionsSize() {
        return m.txDhtRolledbackVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTotalPartitionsCount() {
        return m.totalPartitionsCount();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancedKeys() {
        return m.rebalancedKeys();
    }

    /** {@inheritDoc} */
    @Override public long getEstimatedRebalancingKeys() {
        return m.estimatedRebalancingKeys();
    }

    /** {@inheritDoc} */
    @Override public int getRebalancingPartitionsCount() {
        return m.rebalancingPartitionsCount();
    }

    /** {@inheritDoc} */
    @Override public long getKeysToRebalanceLeft() {
        return m.keysToRebalanceLeft();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingKeysRate() {
        return m.rebalancingKeysRate();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingBytesRate() {
        return m.rebalancingBytesRate();
    }

    /** {@inheritDoc} */
    @Override public long estimateRebalancingFinishTime() {
        return m.rebalanceFinishTime();
    }

    /** {@inheritDoc} */
    @Override public long rebalancingStartTime() {
        return m.rebalancingStartTime();
    }

    /** {@inheritDoc} */
    @Override public long getEstimatedRebalancingFinishTime() {
        return m.rebalanceFinishTime();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingStartTime() {
        return m.rebalancingStartTime();
    }

    /** {@inheritDoc} */
    @Override public long getRebalanceClearingPartitionsLeft() {
        return m.rebalanceClearingPartitionsLeft();
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteBehindEnabled() {
        return m.writeBehindEnabled();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindFlushSize() {
        return m.writeBehindFlushSize();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindFlushThreadCount() {
        return m.writeBehindFlushThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getWriteBehindFlushFrequency() {
        return m.writeBehindFlushFrequency();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindStoreBatchSize() {
        return m.writeBehindStoreBatchSize();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindTotalCriticalOverflowCount() {
        return m.writeBehindTotalCriticalOverflowCount();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindCriticalOverflowCount() {
        return m.writeBehindCriticalOverflowCount();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindErrorRetryCount() {
        return m.writeBehindErrorRetryCount();
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindBufferSize() {
        return m.writeBehindBufferSize();
    }

    /** {@inheritDoc} */
    @Override public String getKeyType() {
        return m.keyType();
    }

    /** {@inheritDoc} */
    @Override public String getValueType() {
        return m.valueType();
    }

    /** {@inheritDoc} */
    @Override public boolean isStoreByValue() {
        return m.storeByValue();
    }

    /** {@inheritDoc} */
    @Override public boolean isStatisticsEnabled() {
        return m.statisticsEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isManagementEnabled() {
        return m.managementEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isReadThrough() {
        return m.readThrough();
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteThrough() {
        return m.writeThrough();
    }

    /** {@inheritDoc} */
    @Override public boolean isValidForReading() {
        return m.validForReading();
    }

    /** {@inheritDoc} */
    @Override public boolean isValidForWriting() {
        return m.validForWriting();
    }

    /** {@inheritDoc} */
    @Override public String getTxKeyCollisions() {
        return m.txKeyCollisions();
    }

    /** {@inheritDoc} */
    @Override public boolean isIndexRebuildInProgress() {
        return m.indexRebuildInProgress();
    }

    /** {@inheritDoc} */
    @Override public long getIndexRebuildKeysProcessed() {
        return m.indexRebuildKeysProcessed();
    }

    /** {@inheritDoc} */
    @Override public int getIndexBuildPartitionsLeftCount() {
        return m.indexBuildPartitionsLeftCount();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheMetricsSnapshot.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternalData(ObjectOutput out) throws IOException {
        out.writeLong(m.cacheGets());
        out.writeLong(m.cachePuts());
        out.writeLong(m.cacheHits());
        out.writeLong(m.cacheMisses());
        out.writeLong(m.cacheTxCommits());
        out.writeLong(m.cacheTxRollbacks());
        out.writeLong(m.cacheEvictions());
        out.writeLong(m.cacheRemovals());

        out.writeFloat(m.averagePutTime());
        out.writeFloat(m.averageGetTime());
        out.writeFloat(m.averageRemoveTime());
        out.writeFloat(m.averageTxCommitTime());
        out.writeFloat(m.averageTxRollbackTime());

        out.writeLong(m.offHeapGets());
        out.writeLong(m.offHeapPuts());
        out.writeLong(m.offHeapRemovals());
        out.writeLong(m.offHeapEvictions());
        out.writeLong(m.offHeapHits());
        out.writeLong(m.offHeapMisses());
        out.writeLong(m.offHeapEntriesCount());
        out.writeLong(m.heapEntriesCount());
        out.writeLong(m.offHeapPrimaryEntriesCount());
        out.writeLong(m.offHeapBackupEntriesCount());
        out.writeLong(m.offHeapAllocatedSize());

        out.writeInt(m.dhtEvictQueueCurrentSize());
        out.writeInt(m.txThreadMapSize());
        out.writeInt(m.txXidMapSize());
        out.writeInt(m.txCommitQueueSize());
        out.writeInt(m.txPrepareQueueSize());
        out.writeInt(m.txStartVersionCountsSize());
        out.writeInt(m.txCommittedVersionsSize());
        out.writeInt(m.txRolledbackVersionsSize());
        out.writeInt(m.txDhtThreadMapSize());
        out.writeInt(m.txDhtXidMapSize());
        out.writeInt(m.txDhtCommitQueueSize());
        out.writeInt(m.txDhtPrepareQueueSize());
        out.writeInt(m.txDhtStartVersionCountsSize());
        out.writeInt(m.txDhtCommittedVersionsSize());
        out.writeInt(m.txDhtRolledbackVersionsSize());
        out.writeInt(m.writeBehindTotalCriticalOverflowCount());
        out.writeInt(m.writeBehindCriticalOverflowCount());
        out.writeInt(m.writeBehindErrorRetryCount());

        out.writeInt(m.totalPartitionsCount());
        out.writeInt(m.rebalancingPartitionsCount());
        out.writeLong(m.keysToRebalanceLeft());
        out.writeLong(m.rebalancingBytesRate());
        out.writeLong(m.rebalancingKeysRate());

        out.writeLong(m.rebalancedKeys());
        out.writeLong(m.estimatedRebalancingKeys());
        out.writeLong(m.rebalancingStartTime());
        out.writeLong(m.rebalanceFinishTime());
        out.writeLong(m.rebalanceClearingPartitionsLeft());

        out.writeLong(m.entryProcessorPuts());
        out.writeFloat(m.entryProcessorAverageInvocationTime());
        out.writeLong(m.entryProcessorInvocations());
        out.writeFloat(m.entryProcessorMaxInvocationTime());
        out.writeFloat(m.entryProcessorMinInvocationTime());
        out.writeLong(m.entryProcessorReadOnlyInvocations());
        out.writeFloat(m.entryProcessorHitPercentage());
        out.writeLong(m.entryProcessorHits());
        out.writeLong(m.entryProcessorMisses());
        out.writeFloat(m.entryProcessorMissPercentage());
        out.writeLong(m.entryProcessorRemovals());

        out.writeLong(m.cacheSize());
        out.writeBoolean(m.empty());
        out.writeInt(m.size());
        out.writeInt(m.keySize());
        U.writeLongString(out, m.txKeyCollisions());
        out.writeInt(m.indexBuildPartitionsLeftCount());
    }

    /** {@inheritDoc} */
    @Override public void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        m = new CacheMetricsMessage();

        m.cacheGets(in.readLong());
        m.cachePuts(in.readLong());
        m.cacheHits(in.readLong());
        m.cacheMisses(in.readLong());
        m.cacheTxCommits(in.readLong());
        m.cacheTxRollbacks(in.readLong());
        m.cacheEvictions(in.readLong());
        m.cacheRemovals(in.readLong());

        m.averagePutTime(in.readFloat());
        m.averageGetTime(in.readFloat());
        m.averageRemoveTime(in.readFloat());
        m.averageTxCommitTime(in.readFloat());
        m.averageTxRollbackTime(in.readFloat());

        m.offHeapGets(in.readLong());
        m.offHeapPuts(in.readLong());
        m.offHeapRemovals(in.readLong());
        m.offHeapEvictions(in.readLong());
        m.offHeapHits(in.readLong());
        m.offHeapMisses(in.readLong());
        m.offHeapEntriesCount(in.readLong());
        m.heapEntriesCount(in.readLong());
        m.offHeapPrimaryEntriesCount(in.readLong());
        m.offHeapBackupEntriesCount(in.readLong());
        m.offHeapAllocatedSize(in.readLong());

        m.dhtEvictQueueCurrentSize(in.readInt());
        m.txThreadMapSize(in.readInt());
        m.txXidMapSize(in.readInt());
        m.txCommitQueueSize(in.readInt());
        m.txPrepareQueueSize(in.readInt());
        m.txStartVersionCountsSize(in.readInt());
        m.txCommittedVersionsSize(in.readInt());
        m.txRolledbackVersionsSize(in.readInt());
        m.txDhtThreadMapSize(in.readInt());
        m.txDhtXidMapSize(in.readInt());
        m.txDhtCommitQueueSize(in.readInt());
        m.txDhtPrepareQueueSize(in.readInt());
        m.txDhtStartVersionCountsSize(in.readInt());
        m.txDhtCommittedVersionsSize(in.readInt());
        m.txDhtRolledbackVersionsSize(in.readInt());
        m.writeBehindTotalCriticalOverflowCount(in.readInt());
        m.writeBehindCriticalOverflowCount(in.readInt());
        m.writeBehindErrorRetryCount(in.readInt());

        m.totalPartitionsCount(in.readInt());
        m.rebalancingPartitionsCount(in.readInt());
        m.keysToRebalanceLeft(in.readLong());
        m.rebalancingBytesRate(in.readLong());
        m.rebalancingKeysRate(in.readLong());

        m.rebalancedKeys(in.readLong());
        m.estimatedRebalancingKeys(in.readLong());
        m.rebalancingStartTime(in.readLong());
        m.rebalanceFinishTime(in.readLong());
        m.rebalanceClearingPartitionsLeft(in.readLong());

        m.entryProcessorPuts(in.readLong());
        m.entryProcessorAverageInvocationTime(in.readFloat());
        m.entryProcessorInvocations(in.readLong());
        m.entryProcessorMaxInvocationTime(in.readFloat());
        m.entryProcessorMinInvocationTime(in.readFloat());
        m.entryProcessorReadOnlyInvocations(in.readLong());
        m.entryProcessorHitPercentage(in.readFloat());
        m.entryProcessorHits(in.readLong());
        m.entryProcessorMisses(in.readLong());
        m.entryProcessorMissPercentage(in.readFloat());
        m.entryProcessorRemovals(in.readLong());

        m.cacheSize(in.readLong());
        m.empty(in.readBoolean());
        m.size(in.readInt());
        m.keySize(in.readInt());
        m.txKeyCollisions(U.readLongString(in));
        m.indexBuildPartitionsLeftCount(in.readInt());
    }
}

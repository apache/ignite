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

namespace Apache.Ignite.Core.Impl.Cache
{
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// Cache metrics used to obtain statistics on cache.
    /// </summary>
    internal class CacheMetricsImpl : ICacheMetrics
    {
        /** */
        private readonly long _cacheHits;

        /** */
        private readonly float _cacheHitPercentage;

        /** */
        private readonly long _cacheMisses;

        /** */
        private readonly float _cacheMissPercentage;

        /** */
        private readonly long _cacheGets;

        /** */
        private readonly long _cachePuts;

        /** */
        private readonly long _cacheRemovals;

        /** */
        private readonly long _cacheEvictions;

        /** */
        private readonly float _averageGetTime;

        /** */
        private readonly float _averagePutTime;

        /** */
        private readonly float _averageRemoveTime;

        /** */
        private readonly float _averageTxCommitTime;

        /** */
        private readonly float _averageTxRollbackTime;

        /** */
        private readonly long _cacheTxCommits;

        /** */
        private readonly long _cacheTxRollbacks;

        /** */
        private readonly string _cacheName;

        /** */
        private readonly long _offHeapGets;

        /** */
        private readonly long _offHeapPuts;

        /** */
        private readonly long _offHeapRemovals;

        /** */
        private readonly long _offHeapEvictions;

        /** */
        private readonly long _offHeapHits;

        /** */
        private readonly float _offHeapHitPercentage;

        /** */
        private readonly long _offHeapMisses;

        /** */
        private readonly float _offHeapMissPercentage;

        /** */
        private readonly long _offHeapEntriesCount;

        /** */
        private readonly long _offHeapPrimaryEntriesCount;

        /** */
        private readonly long _offHeapBackupEntriesCount;

        /** */
        private readonly long _offHeapAllocatedSize;

        /** */
        private readonly int _size;

        /** */
        private readonly int _keySize;

        /** */
        private readonly long _cacheSize;

        /** */
        private readonly bool _isEmpty;

        /** */
        private readonly int _dhtEvictQueueCurrentSize;

        /** */
        private readonly int _txThreadMapSize;

        /** */
        private readonly int _txXidMapSize;

        /** */
        private readonly int _txCommitQueueSize;

        /** */
        private readonly int _txPrepareQueueSize;

        /** */
        private readonly int _txStartVersionCountsSize;

        /** */
        private readonly int _txCommittedVersionsSize;

        /** */
        private readonly int _txRolledbackVersionsSize;

        /** */
        private readonly int _txDhtThreadMapSize;

        /** */
        private readonly int _txDhtXidMapSize;

        /** */
        private readonly int _txDhtCommitQueueSize;

        /** */
        private readonly int _txDhtPrepareQueueSize;

        /** */
        private readonly int _txDhtStartVersionCountsSize;

        /** */
        private readonly int _txDhtCommittedVersionsSize;

        /** */
        private readonly int _txDhtRolledbackVersionsSize;

        /** */
        private readonly bool _isWriteBehindEnabled;

        /** */
        private readonly int _writeBehindFlushSize;

        /** */
        private readonly int _writeBehindFlushThreadCount;

        /** */
        private readonly long _writeBehindFlushFrequency;

        /** */
        private readonly int _writeBehindStoreBatchSize;

        /** */
        private readonly int _writeBehindTotalCriticalOverflowCount;

        /** */
        private readonly int _writeBehindCriticalOverflowCount;

        /** */
        private readonly int _writeBehindErrorRetryCount;

        /** */
        private readonly int _writeBehindBufferSize;

        /** */
        private readonly string _keyType;

        /** */
        private readonly string _valueType;

        /** */
        private readonly bool _isStoreByValue;

        /** */
        private readonly bool _isStatisticsEnabled;

        /** */
        private readonly bool _isManagementEnabled;

        /** */
        private readonly bool _isReadThrough;

        /** */
        private readonly bool _isWriteThrough;

        /** */
        private readonly bool _isValidForReading;

        /** */
        private readonly bool _isValidForWriting;

        /** */
        private readonly int _totalPartitionsCount;

        /** */
        private readonly int _rebalancingPartitionsCount;

        /** */
        private readonly long _keysToRebalanceLeft;

        /** */
        private readonly long _rebalancingKeysRate;

        /** */
        private readonly long _rebalancingBytesRate;

        /** */
        private readonly long _heapEntriesCount;

        /** */
        private readonly long _estimatedRebalancingFinishTime;

        /** */
        private readonly long _rebalancingStartTime;

        /** */
        private readonly long _rebalancingClearingPartitionsLeft;

        /** */
        private readonly long _rebalancedKeys;

        /** */
        private readonly long _estimatedRebalancedKeys;

        /** */
        private readonly long _entryProcessorPuts;

        /** */
        private readonly float _entryProcessorAverageInvocationTime;

        /** */
        private readonly long _entryProcessorInvocations;

        /** */
        private readonly float _entryProcessorMaxInvocationTime;

        /** */
        private readonly float _entryProcessorMinInvocationTime;

        /** */
        private readonly long _entryProcessorReadOnlyInvocations;

        /** */
        private readonly float _entryProcessorHitPercentage;

        /** */
        private readonly long _entryProcessorHits;

        /** */
        private readonly long _entryProcessorMisses;

        /** */
        private readonly float _entryProcessorMissPercentage;

        /** */
        private readonly long _entryProcessorRemovals;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheMetricsImpl"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public CacheMetricsImpl(IBinaryRawReader reader)
        {
            _cacheHits = reader.ReadLong();
            _cacheHitPercentage = reader.ReadFloat();
            _cacheMisses = reader.ReadLong();
            _cacheMissPercentage = reader.ReadFloat();
            _cacheGets = reader.ReadLong();
            _cachePuts = reader.ReadLong();
            _cacheRemovals = reader.ReadLong();
            _cacheEvictions = reader.ReadLong();
            _averageGetTime = reader.ReadFloat();
            _averagePutTime = reader.ReadFloat();
            _averageRemoveTime = reader.ReadFloat();
            _averageTxCommitTime = reader.ReadFloat();
            _averageTxRollbackTime = reader.ReadFloat();
            _cacheTxCommits = reader.ReadLong();
            _cacheTxRollbacks = reader.ReadLong();
            _cacheName = reader.ReadString();
            _offHeapGets = reader.ReadLong();
            _offHeapPuts = reader.ReadLong();
            _offHeapRemovals = reader.ReadLong();
            _offHeapEvictions = reader.ReadLong();
            _offHeapHits = reader.ReadLong();
            _offHeapHitPercentage = reader.ReadFloat();
            _offHeapMisses = reader.ReadLong();
            _offHeapMissPercentage = reader.ReadFloat();
            _offHeapEntriesCount = reader.ReadLong();
            _offHeapPrimaryEntriesCount = reader.ReadLong();
            _offHeapBackupEntriesCount = reader.ReadLong();
            _offHeapAllocatedSize = reader.ReadLong();
            _size = reader.ReadInt();
            _keySize = reader.ReadInt();
            _isEmpty = reader.ReadBoolean();
            _dhtEvictQueueCurrentSize = reader.ReadInt();
            _txThreadMapSize = reader.ReadInt();
            _txXidMapSize = reader.ReadInt();
            _txCommitQueueSize = reader.ReadInt();
            _txPrepareQueueSize = reader.ReadInt();
            _txStartVersionCountsSize = reader.ReadInt();
            _txCommittedVersionsSize = reader.ReadInt();
            _txRolledbackVersionsSize = reader.ReadInt();
            _txDhtThreadMapSize = reader.ReadInt();
            _txDhtXidMapSize = reader.ReadInt();
            _txDhtCommitQueueSize = reader.ReadInt();
            _txDhtPrepareQueueSize = reader.ReadInt();
            _txDhtStartVersionCountsSize = reader.ReadInt();
            _txDhtCommittedVersionsSize = reader.ReadInt();
            _txDhtRolledbackVersionsSize = reader.ReadInt();
            _isWriteBehindEnabled = reader.ReadBoolean();
            _writeBehindFlushSize = reader.ReadInt();
            _writeBehindFlushThreadCount = reader.ReadInt();
            _writeBehindFlushFrequency = reader.ReadLong();
            _writeBehindStoreBatchSize = reader.ReadInt();
            _writeBehindTotalCriticalOverflowCount = reader.ReadInt();
            _writeBehindCriticalOverflowCount = reader.ReadInt();
            _writeBehindErrorRetryCount = reader.ReadInt();
            _writeBehindBufferSize = reader.ReadInt();
            _keyType = reader.ReadString();
            _valueType = reader.ReadString();
            _isStoreByValue = reader.ReadBoolean();
            _isStatisticsEnabled = reader.ReadBoolean();
            _isManagementEnabled = reader.ReadBoolean();
            _isReadThrough = reader.ReadBoolean();
            _isWriteThrough = reader.ReadBoolean();
            _isValidForReading = reader.ReadBoolean();
            _isValidForWriting = reader.ReadBoolean();
            _totalPartitionsCount = reader.ReadInt();
            _rebalancingPartitionsCount = reader.ReadInt();
            _keysToRebalanceLeft = reader.ReadLong();
            _rebalancingKeysRate = reader.ReadLong();
            _rebalancingBytesRate = reader.ReadLong();
            _heapEntriesCount = reader.ReadLong();
            _estimatedRebalancingFinishTime = reader.ReadLong();
            _rebalancingStartTime = reader.ReadLong();
            _rebalancingClearingPartitionsLeft = reader.ReadLong();
            _cacheSize = reader.ReadLong();
            _rebalancedKeys = reader.ReadLong();
            _estimatedRebalancedKeys = reader.ReadLong();
            _entryProcessorPuts = reader.ReadLong();
            _entryProcessorAverageInvocationTime = reader.ReadFloat();
            _entryProcessorInvocations = reader.ReadLong();
            _entryProcessorMaxInvocationTime = reader.ReadFloat();
            _entryProcessorMinInvocationTime = reader.ReadFloat();
            _entryProcessorReadOnlyInvocations = reader.ReadLong();
            _entryProcessorHitPercentage = reader.ReadFloat();
            _entryProcessorHits = reader.ReadLong();
            _entryProcessorMisses = reader.ReadLong();
            _entryProcessorMissPercentage = reader.ReadFloat();
            _entryProcessorRemovals = reader.ReadLong();
        }

        /** <inheritDoc /> */
        public long CacheHits { get { return _cacheHits; } }

        /** <inheritDoc /> */
        public float CacheHitPercentage { get { return _cacheHitPercentage; } }

        /** <inheritDoc /> */
        public long CacheMisses { get { return _cacheMisses; } }

        /** <inheritDoc /> */
        public float CacheMissPercentage { get { return _cacheMissPercentage; } }

        /** <inheritDoc /> */
        public long CacheGets { get { return _cacheGets; } }

        /** <inheritDoc /> */
        public long CachePuts { get { return _cachePuts; } }

        /** <inheritDoc /> */
        public long CacheRemovals { get { return _cacheRemovals; } }

        /** <inheritDoc /> */
        public long CacheEvictions { get { return _cacheEvictions; } }

        /** <inheritDoc /> */
        public float AverageGetTime { get { return _averageGetTime; } }

        /** <inheritDoc /> */
        public float AveragePutTime { get { return _averagePutTime; } }

        /** <inheritDoc /> */
        public float AverageRemoveTime { get { return _averageRemoveTime; } }

        /** <inheritDoc /> */
        public float AverageTxCommitTime { get { return _averageTxCommitTime; } }

        /** <inheritDoc /> */
        public float AverageTxRollbackTime { get { return _averageTxRollbackTime; } }

        /** <inheritDoc /> */
        public long CacheTxCommits { get { return _cacheTxCommits; } }

        /** <inheritDoc /> */
        public long CacheTxRollbacks { get { return _cacheTxRollbacks; } }

        /** <inheritDoc /> */
        public string CacheName { get { return _cacheName; } }

        /** <inheritDoc /> */
        public long OffHeapGets { get { return _offHeapGets; } }

        /** <inheritDoc /> */
        public long OffHeapPuts { get { return _offHeapPuts; } }

        /** <inheritDoc /> */
        public long OffHeapRemovals { get { return _offHeapRemovals; } }

        /** <inheritDoc /> */
        public long OffHeapEvictions { get { return _offHeapEvictions; } }

        /** <inheritDoc /> */
        public long OffHeapHits { get { return _offHeapHits; } }

        /** <inheritDoc /> */
        public float OffHeapHitPercentage { get { return _offHeapHitPercentage; } }

        /** <inheritDoc /> */
        public long OffHeapMisses { get { return _offHeapMisses; } }

        /** <inheritDoc /> */
        public float OffHeapMissPercentage { get { return _offHeapMissPercentage; } }

        /** <inheritDoc /> */
        public long OffHeapEntriesCount { get { return _offHeapEntriesCount; } }

        /** <inheritDoc /> */
        public long OffHeapPrimaryEntriesCount { get { return _offHeapPrimaryEntriesCount; } }

        /** <inheritDoc /> */
        public long OffHeapBackupEntriesCount { get { return _offHeapBackupEntriesCount; } }

        /** <inheritDoc /> */
        public long OffHeapAllocatedSize { get { return _offHeapAllocatedSize; } }

        /** <inheritDoc /> */
        public int Size { get { return _size; } }

        /** <inheritDoc /> */
        public long CacheSize { get { return _cacheSize; } }

        /** <inheritDoc /> */
        public int KeySize { get { return _keySize; } }

        /** <inheritDoc /> */
        public bool IsEmpty { get { return _isEmpty; } }

        /** <inheritDoc /> */
        public int DhtEvictQueueCurrentSize { get { return _dhtEvictQueueCurrentSize; } }

        /** <inheritDoc /> */
        public int TxThreadMapSize { get { return _txThreadMapSize; } }

        /** <inheritDoc /> */
        public int TxXidMapSize { get { return _txXidMapSize; } }

        /** <inheritDoc /> */
        public int TxCommitQueueSize { get { return _txCommitQueueSize; } }

        /** <inheritDoc /> */
        public int TxPrepareQueueSize { get { return _txPrepareQueueSize; } }

        /** <inheritDoc /> */
        public int TxStartVersionCountsSize { get { return _txStartVersionCountsSize; } }

        /** <inheritDoc /> */
        public int TxCommittedVersionsSize { get { return _txCommittedVersionsSize; } }

        /** <inheritDoc /> */
        public int TxRolledbackVersionsSize { get { return _txRolledbackVersionsSize; } }

        /** <inheritDoc /> */
        public int TxDhtThreadMapSize { get { return _txDhtThreadMapSize; } }

        /** <inheritDoc /> */
        public int TxDhtXidMapSize { get { return _txDhtXidMapSize; } }

        /** <inheritDoc /> */
        public int TxDhtCommitQueueSize { get { return _txDhtCommitQueueSize; } }

        /** <inheritDoc /> */
        public int TxDhtPrepareQueueSize { get { return _txDhtPrepareQueueSize; } }

        /** <inheritDoc /> */
        public int TxDhtStartVersionCountsSize { get { return _txDhtStartVersionCountsSize; } }

        /** <inheritDoc /> */
        public int TxDhtCommittedVersionsSize { get { return _txDhtCommittedVersionsSize; } }

        /** <inheritDoc /> */
        public int TxDhtRolledbackVersionsSize { get { return _txDhtRolledbackVersionsSize; } }

        /** <inheritDoc /> */
        public bool IsWriteBehindEnabled { get { return _isWriteBehindEnabled; } }

        /** <inheritDoc /> */
        public int WriteBehindFlushSize { get { return _writeBehindFlushSize; } }

        /** <inheritDoc /> */
        public int WriteBehindFlushThreadCount { get { return _writeBehindFlushThreadCount; } }

        /** <inheritDoc /> */
        public long WriteBehindFlushFrequency { get { return _writeBehindFlushFrequency; } }

        /** <inheritDoc /> */
        public int WriteBehindStoreBatchSize { get { return _writeBehindStoreBatchSize; } }

        /** <inheritDoc /> */
        public int WriteBehindTotalCriticalOverflowCount { get { return _writeBehindTotalCriticalOverflowCount; } }

        /** <inheritDoc /> */
        public int WriteBehindCriticalOverflowCount { get { return _writeBehindCriticalOverflowCount; } }

        /** <inheritDoc /> */
        public int WriteBehindErrorRetryCount { get { return _writeBehindErrorRetryCount; } }

        /** <inheritDoc /> */
        public int WriteBehindBufferSize { get { return _writeBehindBufferSize; } }

        /** <inheritDoc /> */
        public string KeyType { get { return _keyType; } }

        /** <inheritDoc /> */
        public string ValueType { get { return _valueType; } }

        /** <inheritDoc /> */
        public bool IsStoreByValue { get { return _isStoreByValue; } }

        /** <inheritDoc /> */
        public bool IsStatisticsEnabled { get { return _isStatisticsEnabled; } }

        /** <inheritDoc /> */
        public bool IsManagementEnabled { get { return _isManagementEnabled; } }

        /** <inheritDoc /> */
        public bool IsReadThrough { get { return _isReadThrough; } }

        /** <inheritDoc /> */
        public bool IsWriteThrough { get { return _isWriteThrough; } }

        /** <inheritDoc /> */
        public bool IsValidForReading { get { return _isValidForReading; } }

        /** <inheritDoc /> */
        public bool IsValidForWriting { get { return _isValidForWriting; } }

        /** <inheritDoc /> */
        public int TotalPartitionsCount { get { return _totalPartitionsCount; } }

        /** <inheritDoc /> */
        public int RebalancingPartitionsCount { get { return _rebalancingPartitionsCount; } }

        /** <inheritDoc /> */
        public long KeysToRebalanceLeft { get { return _keysToRebalanceLeft; } }

        /** <inheritDoc /> */
        public long RebalancingKeysRate { get { return _rebalancingKeysRate; } }

        /** <inheritDoc /> */
        public long RebalancingBytesRate { get { return _rebalancingBytesRate; } }

        /** <inheritDoc /> */
        public long HeapEntriesCount { get { return _heapEntriesCount; } }

        /** <inheritDoc /> */
        public long EstimatedRebalancingFinishTime { get { return _estimatedRebalancingFinishTime; } }

        /** <inheritDoc /> */
        public long RebalancingStartTime { get { return _rebalancingStartTime; } }

        /** <inheritDoc /> */
        public long RebalanceClearingPartitionsLeft { get { return _rebalancingClearingPartitionsLeft; } }

        /** <inheritDoc /> */
        public long RebalancedKeys { get { return _rebalancedKeys; } }

        /** <inheritDoc /> */
        public long EstimatedRebalancingKeys { get { return _estimatedRebalancedKeys; } }

        /** <inheritDoc /> */
        public long EntryProcessorPuts { get { return _entryProcessorPuts; } }

        /** <inheritDoc /> */
        public float EntryProcessorAverageInvocationTime { get { return _entryProcessorAverageInvocationTime; } }

        /** <inheritDoc /> */
        public long EntryProcessorInvocations { get { return _entryProcessorInvocations; } }

        /** <inheritDoc /> */
        public float EntryProcessorMaxInvocationTime { get { return _entryProcessorMaxInvocationTime; } }

        /** <inheritDoc /> */
        public float EntryProcessorMinInvocationTime { get { return _entryProcessorMinInvocationTime; } }

        /** <inheritDoc /> */
        public long EntryProcessorReadOnlyInvocations { get { return _entryProcessorReadOnlyInvocations; } }

        /** <inheritDoc /> */
        public float EntryProcessorHitPercentage { get { return _entryProcessorHitPercentage; } }

        /** <inheritDoc /> */
        public long EntryProcessorHits { get { return _entryProcessorHits; } }

        /** <inheritDoc /> */
        public long EntryProcessorMisses { get { return _entryProcessorMisses; } }

        /** <inheritDoc /> */
        public float EntryProcessorMissPercentage { get { return _entryProcessorMissPercentage; } }

        /** <inheritDoc /> */
        public long EntryProcessorRemovals { get { return _entryProcessorRemovals; } }
    }
}
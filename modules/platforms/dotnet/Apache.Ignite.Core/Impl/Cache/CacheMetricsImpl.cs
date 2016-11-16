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
        private readonly long _overflowSize;

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
        private readonly long _offHeapMaxSize;

        /** */
        private readonly long _swapGets;

        /** */
        private readonly long _swapPuts;

        /** */
        private readonly long _swapRemovals;

        /** */
        private readonly long _swapHits;

        /** */
        private readonly long _swapMisses;

        /** */
        private readonly long _swapEntriesCount;

        /** */
        private readonly long _swapSize;

        /** */
        private readonly float _swapHitPercentage;

        /** */
        private readonly float _swapMissPercentage;

        /** */
        private readonly int _size;

        /** */
        private readonly int _keySize;

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

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheMetricsImpl"/> class.
        /// </summary>
        /// <param name="r">The reader.</param>
        public CacheMetricsImpl(IBinaryRawReader r)
        {
            _cacheHits = r.ReadLong();
            _cacheHitPercentage = r.ReadFloat();
            _cacheMisses = r.ReadLong();
            _cacheMissPercentage = r.ReadFloat();
            _cacheGets = r.ReadLong();
            _cachePuts = r.ReadLong();
            _cacheRemovals = r.ReadLong();
            _cacheEvictions = r.ReadLong();
            _averageGetTime = r.ReadFloat();
            _averagePutTime = r.ReadFloat();
            _averageRemoveTime = r.ReadFloat();
            _averageTxCommitTime = r.ReadFloat();
            _averageTxRollbackTime = r.ReadFloat();
            _cacheTxCommits = r.ReadLong();
            _cacheTxRollbacks = r.ReadLong();
            _cacheName = r.ReadString();
            _overflowSize = r.ReadLong();
            _offHeapGets = r.ReadLong();
            _offHeapPuts = r.ReadLong();
            _offHeapRemovals = r.ReadLong();
            _offHeapEvictions = r.ReadLong();
            _offHeapHits = r.ReadLong();
            _offHeapHitPercentage = r.ReadFloat();
            _offHeapMisses = r.ReadLong();
            _offHeapMissPercentage = r.ReadFloat();
            _offHeapEntriesCount = r.ReadLong();
            _offHeapPrimaryEntriesCount = r.ReadLong();
            _offHeapBackupEntriesCount = r.ReadLong();
            _offHeapAllocatedSize = r.ReadLong();
            _offHeapMaxSize = r.ReadLong();
            _swapGets = r.ReadLong();
            _swapPuts = r.ReadLong();
            _swapRemovals = r.ReadLong();
            _swapHits = r.ReadLong();
            _swapMisses = r.ReadLong();
            _swapEntriesCount = r.ReadLong();
            _swapSize = r.ReadLong();
            _swapHitPercentage = r.ReadFloat();
            _swapMissPercentage = r.ReadFloat();
            _size = r.ReadInt();
            _keySize = r.ReadInt();
            _isEmpty = r.ReadBoolean();
            _dhtEvictQueueCurrentSize = r.ReadInt();
            _txThreadMapSize = r.ReadInt();
            _txXidMapSize = r.ReadInt();
            _txCommitQueueSize = r.ReadInt();
            _txPrepareQueueSize = r.ReadInt();
            _txStartVersionCountsSize = r.ReadInt();
            _txCommittedVersionsSize = r.ReadInt();
            _txRolledbackVersionsSize = r.ReadInt();
            _txDhtThreadMapSize = r.ReadInt();
            _txDhtXidMapSize = r.ReadInt();
            _txDhtCommitQueueSize = r.ReadInt();
            _txDhtPrepareQueueSize = r.ReadInt();
            _txDhtStartVersionCountsSize = r.ReadInt();
            _txDhtCommittedVersionsSize = r.ReadInt();
            _txDhtRolledbackVersionsSize = r.ReadInt();
            _isWriteBehindEnabled = r.ReadBoolean();
            _writeBehindFlushSize = r.ReadInt();
            _writeBehindFlushThreadCount = r.ReadInt();
            _writeBehindFlushFrequency = r.ReadLong();
            _writeBehindStoreBatchSize = r.ReadInt();
            _writeBehindTotalCriticalOverflowCount = r.ReadInt();
            _writeBehindCriticalOverflowCount = r.ReadInt();
            _writeBehindErrorRetryCount = r.ReadInt();
            _writeBehindBufferSize = r.ReadInt();
            _keyType = r.ReadString();
            _valueType = r.ReadString();
            _isStoreByValue = r.ReadBoolean();
            _isStatisticsEnabled = r.ReadBoolean();
            _isManagementEnabled = r.ReadBoolean();
            _isReadThrough = r.ReadBoolean();
            _isWriteThrough = r.ReadBoolean();
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
        public long OverflowSize { get { return _overflowSize; } }

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
        public long OffHeapMaxSize { get { return _offHeapMaxSize; } }

        /** <inheritDoc /> */
        public long SwapGets { get { return _swapGets; } }

        /** <inheritDoc /> */
        public long SwapPuts { get { return _swapPuts; } }

        /** <inheritDoc /> */
        public long SwapRemovals { get { return _swapRemovals; } }

        /** <inheritDoc /> */
        public long SwapHits { get { return _swapHits; } }

        /** <inheritDoc /> */
        public long SwapMisses { get { return _swapMisses; } }

        /** <inheritDoc /> */
        public long SwapEntriesCount { get { return _swapEntriesCount; } }

        /** <inheritDoc /> */
        public long SwapSize { get { return _swapSize; } }

        /** <inheritDoc /> */
        public float SwapHitPercentage { get { return _swapHitPercentage; } }

        /** <inheritDoc /> */
        public float SwapMissPercentage { get { return _swapMissPercentage; } }

        /** <inheritDoc /> */
        public int Size { get { return _size; } }

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
    }
}
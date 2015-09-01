/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Cache
{
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Cache metrics used to obtain statistics on cache.
    /// </summary>
    internal class CacheMetricsImpl : ICacheMetrics
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CacheMetricsImpl"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public CacheMetricsImpl(IPortableRawReader reader)
        {
            CacheGets = reader.ReadLong();
            CachePuts = reader.ReadLong();
            CacheHits = reader.ReadLong();
            CacheMisses = reader.ReadLong();
            CacheTxCommits = reader.ReadLong();
            CacheTxRollbacks = reader.ReadLong();
            CacheEvictions = reader.ReadLong();
            CacheRemovals = reader.ReadLong();
            AveragePutTime = reader.ReadFloat();
            AverageGetTime = reader.ReadFloat();
            AverageRemoveTime = reader.ReadFloat();
            AverageTxCommitTime = reader.ReadFloat();
            AverageTxRollbackTime = reader.ReadFloat();
            CacheName = reader.ReadString();
            OverflowSize = reader.ReadLong();
            OffHeapEntriesCount = reader.ReadLong();
            OffHeapAllocatedSize = reader.ReadLong();
            Size = reader.ReadInt();
            KeySize = reader.ReadInt();
            IsEmpty = reader.ReadBoolean();
            DhtEvictQueueCurrentSize = reader.ReadInt();
            TxThreadMapSize = reader.ReadInt();
            TxXidMapSize = reader.ReadInt();
            TxCommitQueueSize = reader.ReadInt();
            TxPrepareQueueSize = reader.ReadInt();
            TxStartVersionCountsSize = reader.ReadInt();
            TxCommittedVersionsSize = reader.ReadInt();
            TxRolledbackVersionsSize = reader.ReadInt();
            TxDhtThreadMapSize = reader.ReadInt();
            TxDhtXidMapSize = reader.ReadInt();
            TxDhtCommitQueueSize = reader.ReadInt();
            TxDhtPrepareQueueSize = reader.ReadInt();
            TxDhtStartVersionCountsSize = reader.ReadInt();
            TxDhtCommittedVersionsSize = reader.ReadInt();
            TxDhtRolledbackVersionsSize = reader.ReadInt();
            IsWriteBehindEnabled = reader.ReadBoolean();
            WriteBehindFlushSize = reader.ReadInt();
            WriteBehindFlushThreadCount = reader.ReadInt();
            WriteBehindFlushFrequency = reader.ReadLong();
            WriteBehindStoreBatchSize = reader.ReadInt();
            WriteBehindTotalCriticalOverflowCount = reader.ReadInt();
            WriteBehindCriticalOverflowCount = reader.ReadInt();
            WriteBehindErrorRetryCount = reader.ReadInt();
            WriteBehindBufferSize = reader.ReadInt();
            KeyType = reader.ReadString();
            ValueType = reader.ReadString();
            IsStoreByValue = reader.ReadBoolean();
            IsStatisticsEnabled = reader.ReadBoolean();
            IsManagementEnabled = reader.ReadBoolean();
            IsReadThrough = reader.ReadBoolean();
            IsWriteThrough = reader.ReadBoolean();
            CacheHitPercentage = reader.ReadFloat();
            CacheMissPercentage = reader.ReadFloat();
        }

        /** <inheritdoc /> */
        public long CacheHits { get; private set; }

        /** <inheritdoc /> */
        public float CacheHitPercentage { get; private set; }

        /** <inheritdoc /> */
        public long CacheMisses { get; private set; }

        /** <inheritdoc /> */
        public float CacheMissPercentage { get; private set; }

        /** <inheritdoc /> */
        public long CacheGets { get; private set; }

        /** <inheritdoc /> */
        public long CachePuts { get; private set; }

        /** <inheritdoc /> */
        public long CacheRemovals { get; private set; }

        /** <inheritdoc /> */
        public long CacheEvictions { get; private set; }

        /** <inheritdoc /> */
        public float AverageGetTime { get; private set; }

        /** <inheritdoc /> */
        public float AveragePutTime { get; private set; }

        /** <inheritdoc /> */
        public float AverageRemoveTime { get; private set; }

        /** <inheritdoc /> */
        public float AverageTxCommitTime { get; private set; }

        /** <inheritdoc /> */
        public float AverageTxRollbackTime { get; private set; }

        /** <inheritdoc /> */
        public long CacheTxCommits { get; private set; }

        /** <inheritdoc /> */
        public long CacheTxRollbacks { get; private set; }

        /** <inheritdoc /> */
        public string CacheName { get; private set; }

        /** <inheritdoc /> */
        public long OverflowSize { get; private set; }

        /** <inheritdoc /> */
        public long OffHeapEntriesCount { get; private set; }

        /** <inheritdoc /> */
        public long OffHeapAllocatedSize { get; private set; }

        /** <inheritdoc /> */
        public int Size { get; private set; }

        /** <inheritdoc /> */
        public int KeySize { get; private set; }

        /** <inheritdoc /> */
        public bool IsEmpty { get; private set; }

        /** <inheritdoc /> */
        public int DhtEvictQueueCurrentSize { get; private set; }

        /** <inheritdoc /> */
        public int TxThreadMapSize { get; private set; }

        /** <inheritdoc /> */
        public int TxXidMapSize { get; private set; }

        /** <inheritdoc /> */
        public int TxCommitQueueSize { get; private set; }

        /** <inheritdoc /> */
        public int TxPrepareQueueSize { get; private set; }

        /** <inheritdoc /> */
        public int TxStartVersionCountsSize { get; private set; }

        /** <inheritdoc /> */
        public int TxCommittedVersionsSize { get; private set; }

        /** <inheritdoc /> */
        public int TxRolledbackVersionsSize { get; private set; }

        /** <inheritdoc /> */
        public int TxDhtThreadMapSize { get; private set; }

        /** <inheritdoc /> */
        public int TxDhtXidMapSize { get; private set; }

        /** <inheritdoc /> */
        public int TxDhtCommitQueueSize { get; private set; }

        /** <inheritdoc /> */
        public int TxDhtPrepareQueueSize { get; private set; }

        /** <inheritdoc /> */
        public int TxDhtStartVersionCountsSize { get; private set; }

        /** <inheritdoc /> */
        public int TxDhtCommittedVersionsSize { get; private set; }

        /** <inheritdoc /> */
        public int TxDhtRolledbackVersionsSize { get; private set; }

        /** <inheritdoc /> */
        public bool IsWriteBehindEnabled { get; private set; }

        /** <inheritdoc /> */
        public int WriteBehindFlushSize { get; private set; }

        /** <inheritdoc /> */
        public int WriteBehindFlushThreadCount { get; private set; }

        /** <inheritdoc /> */
        public long WriteBehindFlushFrequency { get; private set; }

        /** <inheritdoc /> */
        public int WriteBehindStoreBatchSize { get; private set; }

        /** <inheritdoc /> */
        public int WriteBehindTotalCriticalOverflowCount { get; private set; }

        /** <inheritdoc /> */
        public int WriteBehindCriticalOverflowCount { get; private set; }

        /** <inheritdoc /> */
        public int WriteBehindErrorRetryCount { get; private set; }

        /** <inheritdoc /> */
        public int WriteBehindBufferSize { get; private set; }

        /** <inheritdoc /> */
        public string KeyType { get; private set; }

        /** <inheritdoc /> */
        public string ValueType { get; private set; }

        /** <inheritdoc /> */
        public bool IsStoreByValue { get; private set; }

        /** <inheritdoc /> */
        public bool IsStatisticsEnabled { get; private set; }

        /** <inheritdoc /> */
        public bool IsManagementEnabled { get; private set; }

        /** <inheritdoc /> */
        public bool IsReadThrough { get; private set; }

        /** <inheritdoc /> */
        public bool IsWriteThrough { get; private set; }
    }
}
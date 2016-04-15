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

namespace Apache.Ignite.Core.Cache
{
    /// <summary>
    /// Cache metrics used to obtain statistics on cache itself.
    /// </summary>
    public interface ICacheMetrics
    {
        /// <summary>
        /// The number of get requests that were satisfied by the cache.
        /// </summary>
        /// <returns>
        /// The number of hits
        /// </returns>
        long CacheHits { get; }

        /// <summary>
        /// This is a measure of cache efficiency.
        /// </summary>
        /// <returns>
        /// The percentage of successful hits, as a decimal e.g 75.
        /// </returns>
        float CacheHitPercentage { get; }

        /// <summary>
        /// A miss is a get request that is not satisfied.
        /// </summary>
        /// <returns>
        /// The number of misses
        /// </returns>
        long CacheMisses { get; }

        /// <summary>
        /// Returns the percentage of cache accesses that did not find a requested entry in the cache.
        /// </summary>
        /// <returns>
        /// The percentage of accesses that failed to find anything.
        /// </returns>
        float CacheMissPercentage { get; }

        /// <summary>
        /// The total number of requests to the cache. This will be equal to the sum of the hits and misses.
        /// </summary>
        /// <returns>
        /// The number of gets.
        /// </returns>
        long CacheGets { get; }

        /// <summary>
        /// The total number of puts to the cache.
        /// </summary>
        /// <returns>
        /// The number of puts.
        /// </returns>
        long CachePuts { get; }

        /// <summary>
        /// The total number of removals from the cache. This does not include evictions, where the cache itself
        /// initiates the removal to make space.
        /// </summary>
        /// <returns>
        /// The number of removals.
        /// </returns>
        long CacheRemovals { get; }

        /// <summary>
        /// The total number of evictions from the cache. An eviction is a removal initiated by the cache itself 
        /// to free up space. An eviction is not treated as a removal and does not appear in the removal counts.
        /// </summary>
        /// <returns>
        /// The number of evictions.
        /// </returns>
        long CacheEvictions { get; }

        /// <summary>
        /// The mean time to execute gets.
        /// </summary>
        /// <returns>
        /// The time in �s.
        /// </returns>
        float AverageGetTime { get; }

        /// <summary>
        /// The mean time to execute puts.
        /// </summary>
        /// <returns>
        /// The time in �s.
        /// </returns>
        float AveragePutTime { get; }

        /// <summary>
        /// The mean time to execute removes.
        /// </summary>
        /// <returns>
        /// The time in �s.
        /// </returns>
        float AverageRemoveTime { get; }

        /// <summary>
        /// The mean time to execute tx commit.
        /// </summary>
        /// <returns>
        /// The time in �s.
        /// </returns>
        float AverageTxCommitTime { get; }

        /// <summary>
        /// The mean time to execute tx rollbacks.
        /// </summary>
        /// <returns>
        /// Number of transaction rollbacks.
        /// </returns>
        float AverageTxRollbackTime { get; }

        /// <summary>
        /// Gets total number of transaction commits.
        /// </summary>
        /// <returns>
        /// Number of transaction commits.
        /// </returns>
        long CacheTxCommits { get; }

        /// <summary>
        /// Gets total number of transaction rollbacks.
        /// </summary>
        /// <returns>
        /// Number of transaction rollbacks.
        /// </returns>
        long CacheTxRollbacks { get; }

        /// <summary>
        /// Gets cache name.
        /// </summary>
        /// <returns>
        /// Cache name.
        /// </returns>
        string CacheName { get; }

        /// <summary>
        /// Gets number of entries that was swapped to disk.
        /// </summary>
        /// <returns>
        /// Number of entries that was swapped to disk.
        /// </returns>
        long OverflowSize { get; }

        /// <summary>
        /// Gets number of entries stored in off-heap memory.
        /// </summary>
        /// <returns>
        /// Number of entries stored in off-heap memory.
        /// </returns>
        long OffHeapEntriesCount { get; }

        /// <summary>
        /// Gets memory size allocated in off-heap.
        /// </summary>
        /// <returns>
        /// Memory size allocated in off-heap.
        /// </returns>
        long OffHeapAllocatedSize { get; }

        /// <summary>
        /// Gets number of non-null values in the cache.
        /// </summary>
        /// <returns>
        /// Number of non-null values in the cache.
        /// </returns>
        int Size { get; }

        /// <summary>
        /// Gets number of keys in the cache, possibly with null values.
        /// </summary>
        /// <returns>
        /// Number of keys in the cache.
        /// </returns>
        int KeySize { get; }

        /// <summary>
        /// Returns true if this cache is empty.
        /// </summary>
        /// <returns>
        /// True if this cache is empty.
        /// </returns>
        bool IsEmpty { get; }

        /// <summary>
        /// Gets current size of evict queue used to batch up evictions.
        /// </summary>
        /// <returns>
        /// Current size of evict queue.
        /// </returns>
        int DhtEvictQueueCurrentSize { get; }

        /// <summary>
        /// Gets transaction per-thread map size.
        /// </summary>
        /// <returns>
        /// Thread map size.
        /// </returns>
        int TxThreadMapSize { get; }

        /// <summary>
        /// Gets transaction per-Xid map size.
        /// </summary>
        /// <returns>
        /// Transaction per-Xid map size.
        /// </returns>
        int TxXidMapSize { get; }

        /// <summary>
        /// Gets committed transaction queue size.
        /// </summary>
        /// <returns>
        /// Committed transaction queue size.
        /// </returns>
        int TxCommitQueueSize { get; }

        /// <summary>
        /// Gets prepared transaction queue size.
        /// </summary>
        /// <returns>
        /// Prepared transaction queue size.
        /// </returns>
        int TxPrepareQueueSize { get; }

        /// <summary>
        /// Gets start version counts map size.
        /// </summary>
        /// <returns>
        /// Start version counts map size.
        /// </returns>
        int TxStartVersionCountsSize { get; }

        /// <summary>
        /// Gets number of cached committed transaction IDs.
        /// </summary>
        /// <returns>
        /// Number of cached committed transaction IDs.
        /// </returns>
        int TxCommittedVersionsSize { get; }

        /// <summary>
        /// Gets number of cached rolled back transaction IDs.
        /// </summary>
        /// <returns>
        /// Number of cached rolled back transaction IDs.
        /// </returns>
        int TxRolledbackVersionsSize { get; }

        /// <summary>
        /// Gets transaction DHT per-thread map size.
        /// </summary>
        /// <returns>
        /// DHT thread map size.
        /// </returns>
        int TxDhtThreadMapSize { get; }

        /// <summary>
        /// Gets transaction DHT per-Xid map size.
        /// </summary>
        /// <returns>
        /// Transaction DHT per-Xid map size.
        /// </returns>
        int TxDhtXidMapSize { get; }

        /// <summary>
        /// Gets committed DHT transaction queue size.
        /// </summary>
        /// <returns>
        /// Committed DHT transaction queue size.
        /// </returns>
        int TxDhtCommitQueueSize { get; }

        /// <summary>
        /// Gets prepared DHT transaction queue size.
        /// </summary>
        /// <returns>
        /// Prepared DHT transaction queue size.
        /// </returns>
        int TxDhtPrepareQueueSize { get; }

        /// <summary>
        /// Gets DHT start version counts map size.
        /// </summary>
        /// <returns>
        /// DHT start version counts map size.
        /// </returns>
        int TxDhtStartVersionCountsSize { get; }

        /// <summary>
        /// Gets number of cached committed DHT transaction IDs.
        /// </summary>
        /// <returns>
        /// Number of cached committed DHT transaction IDs.
        /// </returns>
        int TxDhtCommittedVersionsSize { get; }

        /// <summary>
        /// Gets number of cached rolled back DHT transaction IDs.
        /// </summary>
        /// <returns>
        /// Number of cached rolled back DHT transaction IDs.
        /// </returns>
        int TxDhtRolledbackVersionsSize { get; }

        /// <summary>
        /// Returns true if write-behind is enabled.
        /// </summary>
        /// <returns>
        /// True if write-behind is enabled.
        /// </returns>
        bool IsWriteBehindEnabled { get; }

        /// <summary>
        /// Gets the maximum size of the write-behind buffer. When the count of unique keys in write buffer exceeds 
        /// this value, the buffer is scheduled for write to the underlying store. 
        /// <para /> 
        /// If this value is 0, then flush is performed only on time-elapsing basis. 
        /// </summary>
        /// <returns>
        /// Buffer size that triggers flush procedure.
        /// </returns>
        int WriteBehindFlushSize { get; }

        /// <summary>
        /// Gets the number of flush threads that will perform store update operations.
        /// </summary>
        /// <returns>
        /// Count of worker threads.
        /// </returns>
        int WriteBehindFlushThreadCount { get; }

        /// <summary>
        /// Gets the cache flush frequency. All pending operations on the underlying store will be performed 
        /// within time interval not less then this value. 
        /// <para /> If this value is 0, then flush is performed only when buffer size exceeds flush size.
        /// </summary>
        /// <returns>
        /// Flush frequency in milliseconds.
        /// </returns>
        long WriteBehindFlushFrequency { get; }

        /// <summary>
        /// Gets the maximum count of similar (put or remove) operations that can be grouped to a single batch.
        /// </summary>
        /// <returns>
        /// Maximum size of batch.
        /// </returns>
        int WriteBehindStoreBatchSize { get; }

        /// <summary>
        /// Gets count of write buffer overflow events since initialization. 
        /// Each overflow event causes the ongoing flush operation to be performed synchronously.
        /// </summary>
        /// <returns>
        /// Count of cache overflow events since start.
        /// </returns>
        int WriteBehindTotalCriticalOverflowCount { get; }

        /// <summary>
        /// Gets count of write buffer overflow events in progress at the moment. 
        /// Each overflow event causes the ongoing flush operation to be performed synchronously.
        /// </summary>
        /// <returns>
        /// Count of cache overflow events since start.
        /// </returns>
        int WriteBehindCriticalOverflowCount { get; }

        /// <summary>
        /// Gets count of cache entries that are in a store-retry state. 
        /// An entry is assigned a store-retry state when underlying store failed due some reason 
        /// and cache has enough space to retain this entry till the next try.
        /// </summary>
        /// <returns>
        /// Count of entries in store-retry state.
        /// </returns>
        int WriteBehindErrorRetryCount { get; }

        /// <summary>
        /// Gets count of entries that were processed by the write-behind store 
        /// and have not been flushed to the underlying store yet.
        /// </summary>
        /// <returns>
        /// Total count of entries in cache store internal buffer.
        /// </returns>
        int WriteBehindBufferSize { get; }

        /// <summary>
        /// Determines the required type of keys for this cache, if any.
        /// </summary>
        /// <returns>
        /// The fully qualified class name of the key type, or "java.lang.Object" if the type is undefined.
        /// </returns>
        string KeyType { get; }

        /// <summary>
        /// Determines the required type of values for this cache, if any.
        /// </summary>
        /// <returns>
        /// The fully qualified class name of the value type, or "java.lang.Object" if the type is undefined.
        /// </returns>
        string ValueType { get; }

        /// <summary>
        /// Whether storeByValue true or storeByReference false. When true, both keys and values are stored by value. 
        /// <para /> 
        /// When false, both keys and values are stored by reference. Caches stored by reference are capable of 
        /// mutation by any threads holding the reference. 
        /// The effects are: 
        /// - if the key is mutated, then the key may not be retrievable or removable
        /// - if the value is mutated, then all threads in the JVM can potentially observe those mutations, subject
        /// to the normal Java Memory Model rules.
        /// Storage by reference only applies to the local heap. 
        /// If an entry is moved off heap it will need to be transformed into a representation. 
        /// Any mutations that occur after transformation may not be reflected in the cache. 
        /// <para /> 
        /// When a cache is storeByValue, any mutation to the key or value does not affect the key of value 
        /// stored in the cache. 
        /// <para /> 
        /// The default value is true.
        /// </summary>
        /// <returns>
        /// True if the cache is store by value
        /// </returns>
        bool IsStoreByValue { get; }

        /// <summary>
        /// Checks whether statistics collection is enabled in this cache. 
        /// <para /> 
        /// The default value is false.
        /// </summary>
        /// <returns>
        /// True if statistics collection is enabled
        /// </returns>
        bool IsStatisticsEnabled { get; }

        /// <summary>
        /// Checks whether management is enabled on this cache. 
        /// <para /> 
        /// The default value is false.
        /// </summary>
        /// <returns>
        /// True if management is enabled
        /// </returns>
        bool IsManagementEnabled { get; }

        /// <summary>
        /// Determines if a cache should operate in read-through mode. 
        /// <para /> 
        /// The default value is false
        /// </summary>
        /// <returns>
        /// True when a cache is in "read-through" mode.
        /// </returns>
        bool IsReadThrough { get; }

        /// <summary>
        /// Determines if a cache should operate in "write-through" mode. 
        /// <para /> 
        /// Will appropriately cause the configured CacheWriter to be invoked. 
        /// <para /> 
        /// The default value is false
        /// </summary>
        /// <returns>
        /// True when a cache is in "write-through" mode.
        /// </returns>
        bool IsWriteThrough { get; }
    }
}
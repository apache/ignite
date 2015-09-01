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

package org.apache.ignite.cache;

import javax.cache.Cache;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import org.apache.ignite.IgniteCache;

/**
 * Cache metrics used to obtain statistics on cache itself.
 * Use {@link IgniteCache#metrics()} to obtain metrics for a cache.
 */
public interface CacheMetrics {
    /**
     * The number of get requests that were satisfied by the cache.
     *
     * @return The number of hits.
     */
    public long getCacheHits();

    /**
     * This is a measure of cache efficiency.
     *
     * @return The percentage of successful hits, as a decimal e.g 75.
     */
    public float getCacheHitPercentage();

    /**
     * A miss is a get request that is not satisfied.
     *
     * @return The number of misses.
     */
    public long getCacheMisses();

    /**
     * Returns the percentage of cache accesses that did not find a requested entry
     * in the cache.
     *
     * @return The percentage of accesses that failed to find anything.
     */
    public float getCacheMissPercentage();

    /**
     * The total number of requests to the cache. This will be equal to the sum of
     * the hits and misses.
     *
     * @return The number of gets.
     */
    public long getCacheGets();

    /**
     * The total number of puts to the cache.
     *
     * @return The number of puts.
     */
    public long getCachePuts();

    /**
     * The total number of removals from the cache. This does not include evictions,
     * where the cache itself initiates the removal to make space.
     *
     * @return The number of removals.
     */
    public long getCacheRemovals();

    /**
     * The total number of evictions from the cache. An eviction is a removal
     * initiated by the cache itself to free up space. An eviction is not treated as
     * a removal and does not appear in the removal counts.
     *
     * @return The number of evictions.
     */
    public long getCacheEvictions();

    /**
     * The mean time to execute gets.
     *
     * @return The time in µs.
     */
    public float getAverageGetTime();

    /**
     * The mean time to execute puts.
     *
     * @return The time in µs.
     */
    public float getAveragePutTime();

    /**
     * The mean time to execute removes.
     *
     * @return The time in µs.
     */
    public float getAverageRemoveTime();


    /**
     * The mean time to execute tx commit.
     *
     * @return The time in µs.
     */
    public float getAverageTxCommitTime();

    /**
     * The mean time to execute tx rollbacks.
     *
     * @return Number of transaction rollbacks.
     */
    public float getAverageTxRollbackTime();

    /**
     * Gets total number of transaction commits.
     *
     * @return Number of transaction commits.
     */
    public long getCacheTxCommits();

    /**
     * Gets total number of transaction rollbacks.
     *
     * @return Number of transaction rollbacks.
     */
    public long getCacheTxRollbacks();

    /**
     * Gets name of this cache.
     *
     * @return Cache name.
     */
    public String name();

    /**
     * Gets number of entries that was swapped to disk.
     *
     * @return Number of entries that was swapped to disk.
     */
    public long getOverflowSize();

    /**
     * The total number of get requests to the off-heap memory.
     *
     * @return The number of gets.
     */
    public long getOffHeapGets();

    /**
     * The total number of put requests to the off-heap memory.
     *
     * @return The number of puts.
     */
    public long getOffHeapPuts();

    /**
     * The total number of removals from the off-heap memory. This does not include evictions.
     *
     * @return The number of removals.
     */
    public long getOffHeapRemovals();

    /**
     * The total number of evictions from the off-heap memory.
     *
     * @return The number of evictions.
     */
    public long getOffHeapEvictions();

    /**
     * The number of get requests that were satisfied by the off-heap memory.
     *
     * @return The off-heap hits number.
     */
    public long getOffHeapHits();

    /**
     * Gets the percentage of hits on off-heap memory.
     *
     * @return The percentage of hits on off-heap memory.
     */
    public float getOffHeapHitPercentage();

    /**
     * A miss is a get request that is not satisfied by off-heap memory.
     *
     * @return The off-heap misses number.
     */
    public long getOffHeapMisses();

    /**
     * Gets the percentage of misses on off-heap memory.
     *
     * @return The percentage of misses on off-heap memory.
     */
    public float getOffHeapMissPercentage();

    /**
     * Gets number of entries stored in off-heap memory.
     *
     * @return Number of entries stored in off-heap memory.
     */
    public long getOffHeapEntriesCount();

    /**
     * Gets number of primary entries stored in off-heap memory.
     *
     * @return Number of primary entries stored in off-heap memory.
     */
    public long getOffHeapPrimaryEntriesCount();

    /**
     * Gets number of backup entries stored in off-heap memory.
     *
     * @return Number of backup entries stored in off-heap memory.
     */
    public long getOffHeapBackupEntriesCount();

    /**
     * Gets memory size allocated in off-heap.
     *
     * @return Memory size allocated in off-heap.
     */
    public long getOffHeapAllocatedSize();

    /**
     * Gets off-heap memory maximum size.
     *
     * @return Off-heap memory maximum size.
     */
    public long getOffHeapMaxSize();

    /**
     * The total number of get requests to the swap.
     *
     * @return The number of gets.
     */
    public long getSwapGets();

    /**
     * The total number of put requests to the swap.
     *
     * @return The number of puts.
     */
    public long getSwapPuts();

    /**
     * The total number of removals from the swap.
     *
     * @return The number of removals.
     */
    public long getSwapRemovals();

    /**
     * The number of get requests that were satisfied by the swap.
     *
     * @return The swap hits number.
     */
    public long getSwapHits();

    /**
     * A miss is a get request that is not satisfied by swap.
     *
     * @return The swap misses number.
     */
    public long getSwapMisses();

    /**
     * Gets number of entries stored in swap.
     *
     * @return Number of entries stored in swap.
     */
    public long getSwapEntriesCount();

    /**
     * Gets size of swap.
     *
     * @return Size of swap.
     */
    public long getSwapSize();

    /**
     * Gets the percentage of hits on swap.
     *
     * @return The percentage of hits on swap.
     */
    public float getSwapHitPercentage();

    /**
     * Gets the percentage of misses on swap.
     *
     * @return The percentage of misses on swap.
     */
    public float getSwapMissPercentage();

    /**
     * Gets number of non-{@code null} values in the cache.
     *
     * @return Number of non-{@code null} values in the cache.
     */
    public int getSize();

    /**
     * Gets number of keys in the cache, possibly with {@code null} values.
     *
     * @return Number of keys in the cache.
     */
    public int getKeySize();

    /**
     * Returns {@code true} if this cache is empty.
     *
     * @return {@code True} if this cache is empty.
     */
    public boolean isEmpty();

    /**
     * Gets current size of evict queue used to batch up evictions.
     *
     * @return Current size of evict queue.
     */
    public int getDhtEvictQueueCurrentSize();

    /**
     * Gets transaction per-thread map size.
     *
     * @return Thread map size.
     */
    public int getTxThreadMapSize();

    /**
     * Gets transaction per-Xid map size.
     *
     * @return Transaction per-Xid map size.
     */
    public int getTxXidMapSize();

    /**
     * Gets committed transaction queue size.
     *
     * @return Committed transaction queue size.
     */
    public int getTxCommitQueueSize();

    /**
     * Gets prepared transaction queue size.
     *
     * @return Prepared transaction queue size.
     */
    public int getTxPrepareQueueSize();

    /**
     * Gets start version counts map size.
     *
     * @return Start version counts map size.
     */
    public int getTxStartVersionCountsSize();

    /**
     * Gets number of cached committed transaction IDs.
     *
     * @return Number of cached committed transaction IDs.
     */
    public int getTxCommittedVersionsSize();

    /**
     * Gets number of cached rolled back transaction IDs.
     *
     * @return Number of cached rolled back transaction IDs.
     */
    public int getTxRolledbackVersionsSize();

    /**
     * Gets transaction DHT per-thread map size.
     *
     * @return DHT thread map size.
     */
    public int getTxDhtThreadMapSize();

    /**
     * Gets transaction DHT per-Xid map size.
     *
     * @return Transaction DHT per-Xid map size.
     */
    public int getTxDhtXidMapSize();

    /**
     * Gets committed DHT transaction queue size.
     *
     * @return Committed DHT transaction queue size.
     */
    public int getTxDhtCommitQueueSize();

    /**
     * Gets prepared DHT transaction queue size.
     *
     * @return Prepared DHT transaction queue size.
     */
    public int getTxDhtPrepareQueueSize();

    /**
     * Gets DHT start version counts map size.
     *
     * @return DHT start version counts map size.
     */
    public int getTxDhtStartVersionCountsSize();

    /**
     * Gets number of cached committed DHT transaction IDs.
     *
     * @return Number of cached committed DHT transaction IDs.
     */
    public int getTxDhtCommittedVersionsSize();

    /**
     * Gets number of cached rolled back DHT transaction IDs.
     *
     * @return Number of cached rolled back DHT transaction IDs.
     */
    public int getTxDhtRolledbackVersionsSize();

    /**
     * Returns {@code true} if write-behind is enabled.
     *
     * @return {@code True} if write-behind is enabled.
     */
    public boolean isWriteBehindEnabled();

    /**
     * Gets the maximum size of the write-behind buffer. When the count of unique keys
     * in write buffer exceeds this value, the buffer is scheduled for write to the underlying store.
     * <p/>
     * If this value is {@code 0}, then flush is performed only on time-elapsing basis. However,
     * when this value is {@code 0}, the cache critical size is set to
     * {@link org.apache.ignite.configuration.CacheConfiguration#DFLT_WRITE_BEHIND_CRITICAL_SIZE}
     *
     * @return Buffer size that triggers flush procedure.
     */
    public int getWriteBehindFlushSize();

    /**
     * Gets the number of flush threads that will perform store update operations.
     *
     * @return Count of worker threads.
     */
    public int getWriteBehindFlushThreadCount();

    /**
     * Gets the cache flush frequency. All pending operations on the underlying store will be performed
     * within time interval not less then this value.
     * <p/>
     * If this value is {@code 0}, then flush is performed only when buffer size exceeds flush size.
     *
     * @return Flush frequency in milliseconds.
     */
    public long getWriteBehindFlushFrequency();

    /**
     * Gets the maximum count of similar (put or remove) operations that can be grouped to a single batch.
     *
     * @return Maximum size of batch.
     */
    public int getWriteBehindStoreBatchSize();

    /**
     * Gets count of write buffer overflow events since initialization. Each overflow event causes
     * the ongoing flush operation to be performed synchronously.
     *
     * @return Count of cache overflow events since start.
     */
    public int getWriteBehindTotalCriticalOverflowCount();

    /**
     * Gets count of write buffer overflow events in progress at the moment. Each overflow event causes
     * the ongoing flush operation to be performed synchronously.
     *
     * @return Count of cache overflow events since start.
     */
    public int getWriteBehindCriticalOverflowCount();

    /**
     * Gets count of cache entries that are in a store-retry state. An entry is assigned a store-retry state
     * when underlying store failed due some reason and cache has enough space to retain this entry till
     * the next try.
     *
     * @return Count of entries in store-retry state.
     */
    public int getWriteBehindErrorRetryCount();

    /**
     * Gets count of entries that were processed by the write-behind store and have not been
     * flushed to the underlying store yet.
     *
     * @return Total count of entries in cache store internal buffer.
     */
    public int getWriteBehindBufferSize();

    /**
     * Determines the required type of keys for this {@link Cache}, if any.
     *
     * @return The fully qualified class name of the key type,
     * or {@code "java.lang.Object"} if the type is undefined.
     */
    public String getKeyType();

    /**
     * Determines the required type of values for this {@link Cache}, if any.
     *
     * @return The fully qualified class name of the value type,
     *         or {@code "java.lang.Object"} if the type is undefined.
     */
    public String getValueType();

    /**
     * Whether storeByValue {@code true} or storeByReference {@code false}.
     * When {@code true}, both keys and values are stored by value.
     * <p>
     * When {@code false}, both keys and values are stored by reference.
     * Caches stored by reference are capable of mutation by any threads holding
     * the reference. The effects are:
     * <ul>
     * <li>if the key is mutated, then the key may not be retrievable or
     * removable</li>
     * <li>if the value is mutated, then all threads in the JVM can potentially
     * observe those mutations, subject to the normal Java Memory Model rules.</li>
     * </ul>
     * Storage by reference only applies to the local heap. If an entry is moved off
     * heap it will need to be transformed into a representation. Any mutations that
     * occur after transformation may not be reflected in the cache.
     * <p>
     * When a cache is storeByValue, any mutation to the key or value does not affect
     * the key of value stored in the cache.
     * <p>
     * The default value is {@code true}.
     *
     * @return {@code True} if the cache is store by value.
     */
    public boolean isStoreByValue();

    /**
     * Checks whether statistics collection is enabled in this cache.
     * <p>
     * The default value is {@code false}.
     *
     * @return {@code True} if statistics collection is enabled.
     */
    public boolean isStatisticsEnabled();

    /**
     * Checks whether management is enabled on this cache.
     * <p>
     * The default value is {@code false}.
     *
     * @return {@code true} if management is enabled.
     */
    public boolean isManagementEnabled();

    /**
     * Determines if a {@link Cache} should operate in read-through mode.
     * <p>
     * The default value is {@code false}
     *
     * @return {@code True} when a {@link Cache} is in
     *         "read-through" mode.
     * @see CacheLoader
     */
    public boolean isReadThrough();

    /**
     * Determines if a {@link Cache} should operate in "write-through"
     * mode.
     * <p>
     * will appropriately cause the configured {@link CacheWriter} to be invoked.
     * <p>
     * The default value is {@code false}
     *
     * @return {@code True} when a {@link Cache} is in "write-through" mode.
     * @see CacheWriter
     */
    public boolean isWriteThrough();
}
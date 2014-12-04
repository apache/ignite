/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache;

import org.gridgain.grid.util.mbean.*;

/**
 * This interface defines JMX view on {@link GridCache}.
 */
@GridMBeanDescription("MBean that provides access to cache descriptor.")
public interface GridCacheMBean {
    /**
     * Gets name of this cache.
     *
     * @return Cache name.
     */
    @GridMBeanDescription("Cache name.")
    public String name();

    /**
     * Gets metrics (statistics) for this cache.
     *
     * @return Cache metrics.
     */
    @GridMBeanDescription("Formatted cache metrics.")
    public String metricsFormatted();

    /**
     * Gets number of entries that was swapped to disk.
     *
     * @return Number of entries that was swapped to disk.
     */
    @GridMBeanDescription("Number of entries that was swapped to disk.")
    public long getOverflowSize();

    /**
     * Gets number of entries stored in off-heap memory.
     *
     * @return Number of entries stored in off-heap memory.
     */
    @GridMBeanDescription("Number of entries stored in off-heap memory.")
    public long getOffHeapEntriesCount();

    /**
     * Gets memory size allocated in off-heap.
     *
     * @return Memory size allocated in off-heap.
     */
    @GridMBeanDescription("Memory size allocated in off-heap.")
    public long getOffHeapAllocatedSize();

    /**
     * Returns number of non-{@code null} values in the cache.
     *
     * @return Number of non-{@code null} values in the cache.
     */
    @GridMBeanDescription("Number of non-null values in the cache.")
    public int getSize();

    /**
     * Gets number of keys in the cache, possibly with {@code null} values.
     *
     * @return Number of keys in the cache.
     */
    @GridMBeanDescription("Number of keys in the cache (possibly with null values).")
    public int getKeySize();

    /**
     * Returns {@code true} if this cache is empty.
     *
     * @return {@code true} if this cache is empty.
     */
    @GridMBeanDescription("True if cache is empty.")
    public boolean isEmpty();

    /**
     * Gets current size of evict queue used to batch up evictions.
     *
     * @return Current size of evict queue.
     */
    @GridMBeanDescription("Current size of evict queue.")
    public int getDhtEvictQueueCurrentSize();

    /**
     * Gets transaction per-thread map size.
     *
     * @return Thread map size.
     */
    @GridMBeanDescription("Transaction per-thread map size.")
    public int getTxThreadMapSize();

    /**
     * Gets transaction per-Xid map size.
     *
     * @return Transaction per-Xid map size.
     */
    @GridMBeanDescription("Transaction per-Xid map size.")
    public int getTxXidMapSize();

    /**
     * Gets committed transaction queue size.
     *
     * @return Committed transaction queue size.
     */
    @GridMBeanDescription("Transaction committed queue size.")
    public int getTxCommitQueueSize();

    /**
     * Gets prepared transaction queue size.
     *
     * @return Prepared transaction queue size.
     */
    @GridMBeanDescription("Transaction prepared queue size.")
    public int getTxPrepareQueueSize();

    /**
     * Gets start version counts map size.
     *
     * @return Start version counts map size.
     */
    @GridMBeanDescription("Transaction start version counts map size.")
    public int getTxStartVersionCountsSize();

    /**
     * Gets number of cached committed transaction IDs.
     *
     * @return Number of cached committed transaction IDs.
     */
    @GridMBeanDescription("Transaction committed ID map size.")
    public int getTxCommittedVersionsSize();

    /**
     * Gets number of cached rolled back transaction IDs.
     *
     * @return Number of cached rolled back transaction IDs.
     */
    @GridMBeanDescription("Transaction rolled back ID map size.")
    public int getTxRolledbackVersionsSize();

    /**
     * Gets transaction DHT per-thread map size.
     *
     * @return DHT thread map size.
     */
    @GridMBeanDescription("Transaction DHT per-thread map size.")
    public int getTxDhtThreadMapSize();

    /**
     * Gets transaction DHT per-Xid map size.
     *
     * @return Transaction DHT per-Xid map size.
     */
    @GridMBeanDescription("Transaction DHT per-Xid map size.")
    public int getTxDhtXidMapSize();

    /**
     * Gets committed DHT transaction queue size.
     *
     * @return Committed DHT transaction queue size.
     */
    @GridMBeanDescription("Transaction DHT committed queue size.")
    public int getTxDhtCommitQueueSize();

    /**
     * Gets prepared DHT transaction queue size.
     *
     * @return Prepared DHT transaction queue size.
     */
    @GridMBeanDescription("Transaction DHT prepared queue size.")
    public int getTxDhtPrepareQueueSize();

    /**
     * Gets DHT start version counts map size.
     *
     * @return DHT start version counts map size.
     */
    @GridMBeanDescription("Transaction DHT start version counts map size.")
    public int getTxDhtStartVersionCountsSize();

    /**
     * Gets number of cached committed DHT transaction IDs.
     *
     * @return Number of cached committed DHT transaction IDs.
     */
    @GridMBeanDescription("Transaction DHT committed ID map size.")
    public int getTxDhtCommittedVersionsSize();

    /**
     * Gets number of cached rolled back DHT transaction IDs.
     *
     * @return Number of cached rolled back DHT transaction IDs.
     */
    @GridMBeanDescription("Transaction DHT rolled back ID map size.")
    public int getTxDhtRolledbackVersionsSize();

    /**
     * Returns {@code True} if write-behind is enabled.
     *
     * @return {@code True} if write-behind is enabled.
     */
    @GridMBeanDescription("True if write-behind is enabled for this cache.")
    public boolean isWriteBehindEnabled();

    /**
     * Gets the maximum size of the write-behind buffer. When the count of unique keys
     * in write buffer exceeds this value, the buffer is scheduled for write to the underlying store.
     * <p/>
     * If this value is {@code 0}, then flush is performed only on time-elapsing basis. However,
     * when this value is {@code 0}, the cache critical size is set to
     * {@link GridCacheConfiguration#DFLT_WRITE_BEHIND_CRITICAL_SIZE}
     *
     * @return Buffer size that triggers flush procedure.
     */
    @GridMBeanDescription("Size of internal buffer that triggers flush procedure.")
    public int getWriteBehindFlushSize();

    /**
     * Gets the number of flush threads that will perform store update operations.
     *
     * @return Count of worker threads.
     */
    @GridMBeanDescription("Count of flush threads.")
    public int getWriteBehindFlushThreadCount();

    /**
     * Gets the cache flush frequency. All pending operations on the underlying store will be performed
     * within time interval not less then this value.
     * <p/>
     * If this value is {@code 0}, then flush is performed only when buffer size exceeds flush size.
     *
     * @return Flush frequency in milliseconds.
     */
    @GridMBeanDescription("Flush frequency interval in milliseconds.")
    public long getWriteBehindFlushFrequency();

    /**
     * Gets the maximum count of similar (put or remove) operations that can be grouped to a single batch.
     *
     * @return Maximum size of batch.
     */
    @GridMBeanDescription("Maximum size of batch for similar operations.")
    public int getWriteBehindStoreBatchSize();

    /**
     * Gets count of write buffer overflow events since initialization. Each overflow event causes
     * the ongoing flush operation to be performed synchronously.
     *
     * @return Count of cache overflow events since start.
     */
    @GridMBeanDescription("Count of cache overflow events since write-behind cache has started.")
    public int getWriteBehindTotalCriticalOverflowCount();

    /**
     * Gets count of write buffer overflow events in progress at the moment. Each overflow event causes
     * the ongoing flush operation to be performed synchronously.
     *
     * @return Count of cache overflow events since start.
     */
    @GridMBeanDescription("Count of cache overflow events since write-behind cache has started.")
    public int getWriteBehindCriticalOverflowCount();

    /**
     * Gets count of cache entries that are in a store-retry state. An entry is assigned a store-retry state
     * when underlying store failed due some reason and cache has enough space to retain this entry till
     * the next try.
     *
     * @return Count of entries in store-retry state.
     */
    @GridMBeanDescription("Count of cache cache entries that are currently in retry state.")
    public int getWriteBehindErrorRetryCount();

    /**
     * Gets count of entries that were processed by the write-behind store and have not been
     * flushed to the underlying store yet.
     *
     * @return Total count of entries in cache store internal buffer.
     */
    @GridMBeanDescription("Count of cache entries that are waiting to be flushed.")
    public int getWriteBehindBufferSize();
}

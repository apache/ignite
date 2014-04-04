/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.offheap;

import org.gridgain.grid.util.offheap.unsafe.*;
import org.jetbrains.annotations.*;

/**
 * Factory for off-heap maps.
 */
public class GridOffHeapMapFactory {
    /**
     * Creates off-heap map based on {@code Unsafe} implementation with
     * unlimited memory.
     *
     * @param initCap Initial capacity.
     * @return Off-heap map.
     */
    public static <K> GridOffHeapMap<K> unsafeMap(long initCap) {
        return new GridUnsafeMap<>(128, 0.75f, initCap, 0, (short)0, null);
    }

    /**
     * Creates off-heap map based on {@code Unsafe} implementation.
     *
     * @param concurrency Concurrency.
     * @param initCap Initial capacity.
     * @return Off-heap map.
     */
    public static <K> GridOffHeapMap<K> unsafeMap(int concurrency, long initCap) {
        return new GridUnsafeMap<>(concurrency, 0.75f, initCap, 0, (short)0, null);
    }

    /**
     * Creates off-heap map based on {@code Unsafe} implementation.
     *
     * @param concurrency Concurrency.
     * @param load Load factor.
     * @param initCap Initial capacity.
     * @return Off-heap map.
     */
    public static <K> GridOffHeapMap<K> unsafeMap(int concurrency, float load, long initCap) {
        return new GridUnsafeMap<>(concurrency, load, initCap, 0, (short)0, null);
    }

    /**
     * Creates off-heap map based on {@code Unsafe} implementation with limited
     * memory and LRU-based eviction.
     *
     * @param initCap Initial capacity.
     * @param totalMem Total memory.
     * @param lruStripes Number of LRU stripes.
     * @return Off-heap map.
     */
    public static <K> GridOffHeapMap<K> unsafeMap(long initCap, long totalMem, short lruStripes) {
        return new GridUnsafeMap<>(128, 0.75f, initCap, totalMem, lruStripes, null);
    }

    /**
     * Creates off-heap map based on {@code Unsafe} implementation with limited
     * memory and LRU-based eviction.
     *
     * @param initCap Initial capacity.
     * @param totalMem Total memory.
     * @param lruStripes Number of LRU stripes.
     * @param lsnr Optional eviction listener which gets notified every time an entry is evicted.
     * @return Off-heap map.
     */
    public static <K> GridOffHeapMap<K> unsafeMap(long initCap, long totalMem, short lruStripes,
        @Nullable GridOffHeapEvictListener lsnr) {
        return new GridUnsafeMap<>(128, 0.75f, initCap, totalMem, lruStripes, lsnr);
    }

    /**
     * Creates off-heap map based on {@code Unsafe} implementation with limited
     * memory and LRU-based eviction.
     *
     * @param concurrency Concurrency.
     * @param initCap Initial capacity.
     * @param totalMem Total memory.
     * @param lruStripes Number of LRU stripes.
     * @param lsnr Optional eviction listener which gets notified every time an entry is evicted.
     * @return Off-heap map.
     */
    public static <K> GridOffHeapMap<K> unsafeMap(int concurrency, long initCap, long totalMem, short lruStripes,
        @Nullable GridOffHeapEvictListener lsnr) {
        return new GridUnsafeMap<>(concurrency, 0.75f, initCap, totalMem, lruStripes, lsnr);
    }

    /**
     * Creates off-heap map based on {@code Unsafe} implementation with limited
     * memory and LRU-based eviction.
     *
     * @param concurrency Concurrency.
     * @param load Load factor.
     * @param initCap Initial capacity.
     * @param totalMem Total memory.
     * @param lruStripes Number of LRU stripes.
     * @param lsnr Optional eviction listener which gets notified every time an entry is evicted.
     * @return Off-heap map.
     */
    public static <K> GridOffHeapMap<K> unsafeMap(int concurrency, float load, long initCap, long totalMem,
        short lruStripes, @Nullable GridOffHeapEvictListener lsnr) {
        return new GridUnsafeMap<>(concurrency, load, initCap, totalMem, lruStripes, lsnr);
    }

    /**
     * Creates off-heap map based on {@code Unsafe} implementation with
     * unlimited memory.
     *
     * @param parts Partitions.
     * @param initCap Initial capacity.
     * @return Off-heap map.
     */
    public static GridOffHeapPartitionedMap unsafePartitionedMap(int parts, long initCap) {
        return new GridUnsafePartitionedMap(parts, 128, 0.75f, initCap, 0, (short)0, null);
    }

    /**
     * Creates off-heap map based on {@code Unsafe} implementation.
     *
     * @param parts Partitions.
     * @param concurrency Concurrency.
     * @param initCap Initial capacity.
     * @return Off-heap map.
     */
    public static GridOffHeapPartitionedMap unsafePartitionedMap(int parts, int concurrency, long initCap) {
        return new GridUnsafePartitionedMap(parts, concurrency, 0.75f, initCap, 0, (short)0, null);
    }

    /**
     * Creates off-heap map based on {@code Unsafe} implementation.
     *
     * @param parts Partitions.
     * @param concurrency Concurrency.
     * @param load Load factor.
     * @param initCap Initial capacity.
     * @return Off-heap map.
     */
    public static GridOffHeapPartitionedMap unsafePartitionedMap(int parts, int concurrency, float load,
        long initCap) {
        return new GridUnsafePartitionedMap(parts, concurrency, load, initCap, 0, (short)0, null);
    }

    /**
     * Creates off-heap map based on {@code Unsafe} implementation with limited
     * memory and LRU-based eviction.
     *
     * @param parts Partitions.
     * @param initCap Initial capacity.
     * @param totalMem Total memory.
     * @param lruStripes Number of LRU stripes.
     * @return Off-heap map.
     */
    public static GridOffHeapPartitionedMap unsafePartitionedMap(int parts, long initCap, long totalMem,
        short lruStripes) {
        return new GridUnsafePartitionedMap(parts, 128, 0.75f, initCap, totalMem, lruStripes, null);
    }

    /**
     * Creates off-heap map based on {@code Unsafe} implementation with limited
     * memory and LRU-based eviction.
     *
     * @param parts Partitions.
     * @param initCap Initial capacity.
     * @param totalMem Total memory.
     * @param lruStripes Number of LRU stripes.
     * @param lsnr Optional eviction listener which gets notified every time an entry is evicted.
     * @return Off-heap map.
     */
    public static GridOffHeapPartitionedMap unsafePartitionedMap(int parts, long initCap, long totalMem,
        short lruStripes, @Nullable GridOffHeapEvictListener lsnr) {
        return new GridUnsafePartitionedMap(parts, 128, 0.75f, initCap, totalMem, lruStripes, lsnr);
    }

    /**
     * Creates off-heap map based on {@code Unsafe} implementation with limited
     * memory and LRU-based eviction.
     *
     * @param parts Partitions.
     * @param concurrency Concurrency.
     * @param initCap Initial capacity.
     * @param totalMem Total memory.
     * @param lruStripes Number of LRU stripes.
     * @param lsnr Optional eviction listener which gets notified every time an entry is evicted.
     * @return Off-heap map.
     */
    public static GridOffHeapPartitionedMap unsafePartitionedMap(int parts, int concurrency, long initCap,
        long totalMem, short lruStripes, @Nullable GridOffHeapEvictListener lsnr) {
        return new GridUnsafePartitionedMap(parts, concurrency, 0.75f, initCap, totalMem, lruStripes, lsnr);
    }

    /**
     * Creates off-heap map based on {@code Unsafe} implementation with limited
     * memory and LRU-based eviction.
     *
     * @param parts Partitions.
     * @param concurrency Concurrency.
     * @param load Load factor.
     * @param initCap Initial capacity.
     * @param totalMem Total memory.
     * @param lruStripes Number of LRU stripes.
     * @param lsnr Optional eviction listener which gets notified every time an entry is evicted.
     * @return Off-heap map.
     */
    public static GridOffHeapPartitionedMap unsafePartitionedMap(int parts, int concurrency, float load,
        long initCap, long totalMem, short lruStripes, @Nullable GridOffHeapEvictListener lsnr) {
        return new GridUnsafePartitionedMap(parts, concurrency, load, initCap, totalMem, lruStripes, lsnr);
    }
}

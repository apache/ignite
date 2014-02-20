// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.store.local;

/**
 * Write mode for file local store. Defines when and how internal buffers will be flushed to the file system.
 * Delay for buffered modes can be configured by {@link GridCacheFileLocalStore#setWriteDelay(long)} method.
 *
 * @see GridCacheFileLocalStore#setWriteMode(GridCacheFileLocalStoreWriteMode)
 * @author @java.author
 * @version @java.version
 */
public enum GridCacheFileLocalStoreWriteMode {
    /**
     * Store will attempt to flush buffers right away. The mode is synchronous, the store will not loose updates in case
     * of crash of the process. The store will have better latency for rare uncontended updates
     * than {@link #SYNC_BUFFERED} and the worst throughput for multithreaded updates.
     */
    SYNC,

    /**
     * Store will not attempt to flush buffers until they are full or flush will occur in a background
     * thread after the specified delay. The mode is asynchronous, thus it can loose delayed updates in case of
     * crash of the process but it is the fastest in terms of both latency and throughput.
     *
     * @see GridCacheFileLocalStore#setWriteDelay(long)
     */
    ASYNC_BUFFERED,

    /**
     * Store will attempt to flush buffers only after the specified delay in hope that other threads will write to the
     * same buffer for better batching effect. The difference from {@link #ASYNC_BUFFERED} is that flush always happens
     * synchronously but not in a background thread. The mode is synchronous, the store will not loose updates.
     * This mode trades latency for better multithreaded throughput.
     *
     * @see GridCacheFileLocalStore#setWriteDelay(long)
     */
    SYNC_BUFFERED
}

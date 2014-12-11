/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;

/**
 * Cache manager shared across all caches.
 */
public interface GridCacheSharedManager <K, V> {
    /**
     * Starts manager.
     *
     * @param cctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void start(GridCacheSharedContext<K, V> cctx) throws IgniteCheckedException;

    /**
     * Stops manager.
     *
     * @param cancel Cancel flag.
     */
    public void stop(boolean cancel);

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void onKernalStart() throws IgniteCheckedException;

    /**
     * @param cancel Cancel flag.
     */
    public void onKernalStop(boolean cancel);

    /**
     * Prints memory statistics (sizes of internal data structures, etc.).
     *
     * NOTE: this method is for testing and profiling purposes only.
     */
    public void printMemoryStats();
}

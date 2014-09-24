/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import java.util.*;

/**
 * Update future for atomic cache.
 */
public interface GridCacheAtomicFuture<K, R> extends GridCacheFuture<R> {
    /**
     * @return {@code True} if partition exchange should wait for this future to complete.
     */
    public boolean waitForPartitionExchange();

    /**
     * @return Future topology version.
     */
    public long topologyVersion();

    /**
     * @return Future keys.
     */
    public Collection<? extends K> keys();

    /**
     * Checks if timeout occurred.
     *
     * @param timeout Timeout to check.
     */
    public void checkTimeout(long timeout);
}

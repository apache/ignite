// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;

import java.util.*;

/**
 * Cache preloader that is responsible for loading cache entries either from remote
 * nodes (for distributed cache) or anywhere else at cache startup.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridCachePreloader<K, V> {
    /**
     * Starts preloading.
     *
     * @throws GridException If start failed.
     */
    public void start() throws GridException;

    /**
     * Stops preloading.
     */
    public void stop();

    /**
     * Kernal start callback.
     *
     * @throws GridException If failed.
     */
    public void onKernalStart() throws GridException;

    /**
     * Kernal stop callback.
     */
    public void onKernalStop();

    /**
     * @param p Preload predicate.
     */
    public void preloadPredicate(GridPredicate<GridCacheEntryInfo<K, V>> p);

    /**
     * @return Preload predicate. If not {@code null}, will evaluate each preloaded entry during
     *      send and receive, and if predicate evaluates to {@code false}, entry will be skipped.
     */
    public GridPredicate<GridCacheEntryInfo<K, V>> preloadPredicate();

    /**
     * @return Future which will complete when preloader is safe to use.
     */
    public GridFuture<?> startFuture();

    /**
     * @return Future which will complete when preloading is finished.
     */
    public GridFuture<?> syncFuture();

    /**
     * Requests that preloader sends the request for the key.
     *
     * @param keys Keys to request.
     * @param topVer Topology version, {@code -1} if not required.
     * @return Future to complete when all keys are preloaded.
     */
    public GridFuture<Object> request(Collection<? extends K> keys, long topVer);

    /**
     * Force preload process.
     */
    public void forcePreload();

    /**
     * Unwinds undeploys.
     */
    public void unwindUndeploys();
}

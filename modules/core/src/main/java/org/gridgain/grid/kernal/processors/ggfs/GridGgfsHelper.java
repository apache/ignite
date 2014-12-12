/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.*;
import org.gridgain.grid.cache.*;

/**
 * GGFS utility processor adapter.
 */
public interface GridGgfsHelper {
    /**
     * Pre-process cache configuration.
     *
     * @param cfg Cache configuration.
     */
    public abstract void preProcessCacheConfiguration(GridCacheConfiguration cfg);

    /**
     * Validate cache configuration for GGFS.
     *
     * @param cfg Cache configuration.
     * @throws IgniteCheckedException If validation failed.
     */
    public abstract void validateCacheConfiguration(GridCacheConfiguration cfg) throws IgniteCheckedException;

    /**
     * Check whether object is of type {@code GridGgfsBlockKey}
     *
     * @param key Key.
     * @return {@code True} if GGFS block key.
     */
    public abstract boolean isGgfsBlockKey(Object key);
}

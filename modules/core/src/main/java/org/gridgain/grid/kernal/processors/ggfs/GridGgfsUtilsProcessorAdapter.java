/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;

/**
 * GGFS utility processor adapter.
 */
public abstract class GridGgfsUtilsProcessorAdapter extends GridProcessorAdapter {
    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    protected GridGgfsUtilsProcessorAdapter(GridKernalContext ctx) {
        super(ctx);
    }

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
     * @throws org.gridgain.grid.GridException If validation failed.
     */
    public abstract void validateCacheConfiguration(GridCacheConfiguration cfg) throws GridException;

    /**
     * Check whether object is of type {@code GridGgfsBlockKey}
     *
     * @param key Key.
     * @return {@code True} if GGFS block key.
     */
    public abstract boolean isGgfsBlockKey(Object key);
}

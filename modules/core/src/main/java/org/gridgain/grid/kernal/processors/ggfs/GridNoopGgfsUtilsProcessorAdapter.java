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

/**
 * No-op utils processor adapter.
 */
public class GridNoopGgfsUtilsProcessorAdapter extends GridGgfsUtilsProcessorAdapter {
    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public GridNoopGgfsUtilsProcessorAdapter(GridKernalContext ctx) {
        super(ctx);
    }

    @Override public void preProcessCacheConfiguration(GridCacheConfiguration cfg) {
        // No-op.
    }

    @Override public void validateCacheConfiguration(GridCacheConfiguration cfg) throws GridException {
        // No-op.
    }

    @Override public boolean isGgfsBlockKey(Object key) {
        return false;
    }
}

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
 * No-op utils processor adapter.
 */
public class GridNoopGgfsHelper implements GridGgfsHelper {

    /** {@inheritDoc} */
    @Override public void preProcessCacheConfiguration(GridCacheConfiguration cfg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void validateCacheConfiguration(GridCacheConfiguration cfg) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isGgfsBlockKey(Object key) {
        return false;
    }
}

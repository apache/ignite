/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.jta;

import org.apache.ignite.*;
import org.gridgain.grid.cache.*;
import org.jetbrains.annotations.*;

/**
 * No-op implementation of {@link GridCacheJtaManagerAdapter}.
 */
public class GridCacheNoopJtaManager<K, V> extends GridCacheJtaManagerAdapter<K, V> {
    /** {@inheritDoc} */
    @Override public void checkJta() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void createTmLookup(GridCacheConfiguration ccfg) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object tmLookup() {
        return null;
    }
}

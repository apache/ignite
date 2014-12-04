/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.colocation;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.resources.*;

/**
 * Lifecycle bean.
 */
public class GridTestLifecycleBean implements GridLifecycleBean {
    @GridInstanceResource
    private Ignite g;

    @Override public void onLifecycleEvent(GridLifecycleEventType type) throws GridException {
        if (type == GridLifecycleEventType.AFTER_GRID_START) {
            GridCache<GridTestKey, Long> cache = g.cache("partitioned");

            assert cache != null;

            cache.loadCache(null, 0, GridTestConstants.LOAD_THREADS, GridTestConstants.ENTRY_COUNT);
        }
    }
}

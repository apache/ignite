/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.eviction.ggfs;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.*;
import org.gridgain.grid.kernal.processors.ggfs.*;

/**
 * GGFS eviction filter which will not evict blocks of particular files.
 */
public class GridCacheGgfsEvictionFilter implements GridCacheEvictionFilter {
    /** {@inheritDoc} */
    @Override public boolean evictAllowed(GridCacheEntry entry) {
        Object key = entry.getKey();

        return !(key instanceof GridGgfsBlockKey && ((GridGgfsBlockKey)key).evictExclude());
    }
}

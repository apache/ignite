/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.spring;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.springframework.beans.factory.*;
import org.springframework.cache.*;

import java.util.*;

/**
 * Cache manager implementation.
 */
public class GridSpringCacheManager implements InitializingBean, CacheManager {
    /** Grid name. */
    private String gridName;

    /** Grid instance. */
    private Grid grid;

    /**
     * @return Grid name.
     */
    public String getGridName() {
        return gridName;
    }

    /**
     * @param gridName Grid name.
     */
    public void setGridName(String gridName) {
        this.gridName = gridName;
    }

    /** {@inheritDoc} */
    @Override public void afterPropertiesSet() throws Exception {
        assert grid == null;

        grid = GridGain.grid(gridName);
    }

    /** {@inheritDoc} */
    @Override public Cache getCache(String name) {
        assert grid != null;

        return new GridSpringCache(grid.cache(name));
    }

    /** {@inheritDoc} */
    @Override public Collection<String> getCacheNames() {
        assert grid != null;

        return F.viewReadOnly(grid.caches(), new GridClosure<GridCache<?, ?>, String>() {
            @Override public String apply(GridCache<?, ?> c) {
                return c.name();
            }
        });
    }
}

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
import org.springframework.cache.*;
import org.springframework.cache.support.*;

/**
 * Cache implementation.
 */
class GridSpringCache implements Cache {
    /** */
    private final GridCache<Object, Object> cache;

    /**
     * @param cache Cache.
     */
    GridSpringCache(GridCache<Object, Object> cache) {
        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return cache.name();
    }

    /** {@inheritDoc} */
    @Override public Object getNativeCache() {
        return cache;
    }

    /** {@inheritDoc} */
    @Override public ValueWrapper get(Object key) {
        try {
            Object val = cache.get(key);

            return val != null ? new SimpleValueWrapper(cache.get(key)) : null;
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(Object key, Object val) {
        try {
            cache.putx(key, val);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void evict(Object key) {
        // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        // TODO: implement.
    }
}

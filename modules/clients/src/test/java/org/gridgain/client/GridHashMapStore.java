/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.store.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Simple HashMap based cache store emulation.
 */
public class GridHashMapStore extends GridCacheStoreAdapter {
    /** Map for cache store. */
    private final Map<Object, Object> map = new HashMap<>();

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure c, Object... args)
        throws IgniteCheckedException {
        for (Map.Entry e : map.entrySet())
            c.apply(e.getKey(), e.getValue());
    }

    /** {@inheritDoc} */
    @Override public Object load(@Nullable IgniteTx tx, Object key)
        throws IgniteCheckedException {
        return map.get(key);
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable IgniteTx tx, Object key,
        @Nullable Object val) throws IgniteCheckedException {
        map.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable IgniteTx tx, Object key)
        throws IgniteCheckedException {
        map.remove(key);
    }
}

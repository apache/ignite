/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.store;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.resources.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Test store that generates objects on demand.
 */
public class GridGeneratingTestStore implements GridCacheStore<String, String> {
    /** Number of entries to be generated. */
    private static final int DFLT_GEN_CNT = 100;

    /** */
    @GridCacheNameResource
    private String cacheName;

    /** {@inheritDoc} */
    @Override public String load(@Nullable GridCacheTx tx, String key)
        throws GridException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<String, String> clo,
        @Nullable Object... args) throws GridException {
        if (args.length > 0) {
            try {
                int cnt = ((Number)args[0]).intValue();
                int postfix = ((Number)args[1]).intValue();

                for (int i = 0; i < cnt; i++)
                    clo.apply("key" + i, "val." + cacheName + "." + postfix);
            }
            catch (Exception e) {
                X.println("Unexpected exception in loadAll: " + e);

                throw new GridException(e);
            }
        }
        else {
            for (int i = 0; i < DFLT_GEN_CNT; i++)
                clo.apply("key" + i, "val." + cacheName + "." + i);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadAll(@Nullable GridCacheTx tx,
        @Nullable Collection<? extends String> keys, IgniteBiInClosure<String, String> c) throws GridException {
        for (String key : keys)
            c.apply(key, "val" + key);
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable GridCacheTx tx, String key, @Nullable String val)
        throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void putAll(@Nullable GridCacheTx tx,
        @Nullable Map<? extends String, ? extends String> map) throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable GridCacheTx tx, String key)
        throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable GridCacheTx tx,
        @Nullable Collection<? extends String> keys) throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void txEnd(GridCacheTx tx, boolean commit) throws GridException {
        // No-op.
    }
}

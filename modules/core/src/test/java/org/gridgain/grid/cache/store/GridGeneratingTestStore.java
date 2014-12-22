/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.store;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Test store that generates objects on demand.
 */
public class GridGeneratingTestStore implements GridCacheStore<String, String> {
    /** Number of entries to be generated. */
    private static final int DFLT_GEN_CNT = 100;

    /** */
    @IgniteCacheNameResource
    private String cacheName;

    /** {@inheritDoc} */
    @Override public String load(@Nullable IgniteTx tx, String key)
        throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<String, String> clo,
        @Nullable Object... args) throws IgniteCheckedException {
        if (args.length > 0) {
            try {
                int cnt = ((Number)args[0]).intValue();
                int postfix = ((Number)args[1]).intValue();

                for (int i = 0; i < cnt; i++)
                    clo.apply("key" + i, "val." + cacheName + "." + postfix);
            }
            catch (Exception e) {
                X.println("Unexpected exception in loadAll: " + e);

                throw new IgniteCheckedException(e);
            }
        }
        else {
            for (int i = 0; i < DFLT_GEN_CNT; i++)
                clo.apply("key" + i, "val." + cacheName + "." + i);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadAll(@Nullable IgniteTx tx,
        @Nullable Collection<? extends String> keys, IgniteBiInClosure<String, String> c) throws IgniteCheckedException {
        for (String key : keys)
            c.apply(key, "val" + key);
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable IgniteTx tx, String key, @Nullable String val)
        throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void putAll(@Nullable IgniteTx tx,
        @Nullable Map<? extends String, ? extends String> map) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable IgniteTx tx, String key)
        throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable IgniteTx tx,
        @Nullable Collection<? extends String> keys) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void txEnd(IgniteTx tx, boolean commit) throws IgniteCheckedException {
        // No-op.
    }
}

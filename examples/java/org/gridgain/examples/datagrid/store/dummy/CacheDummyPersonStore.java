// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.store.dummy;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Dummy cache store implementation.
 *
 * @author @java.author
 * @version @java.version
 */
public class CacheDummyPersonStore extends GridCacheStoreAdapter<Object, Object> {
    /** Auto-injected grid instance. */
    @GridInstanceResource
    private Grid grid;

    /** Auto-injected logger. */
    @GridLoggerResource
    private GridLogger log;

    /** Cache name. */
    @GridCacheNameResource
    private String cacheName;

    /** Dummy database. */
    private Map<Object, Object> dummyDB = new HashMap<>();

    /** {@inheritDoc} */
    @Override public void loadCache(final GridBiInClosure<Object, Object> clo, Object... args) throws GridException {
        // Number of entries is passed by caller.
        int entryCnt = (Integer)args[0];

        GridCache<Object, Object> cache = grid.cache(cacheName);

        log.info("Number of cache entries to load: " + entryCnt);

        for (int i = 0; i < entryCnt; i++) {
            // Only add to cache if key is mapped to local node.
            // We check for local mapping just to demonstrate that
            // you can do it from your logic - GridGain will always
            // check internally as well.
            //
            // In real life you can store partition IDs for your keys
            // in your persistent store and only load the partition IDs
            // mapped to the local node here. You would use similar check
            // as below to find out if partition ID is mapped to local node.
            if (cache.affinity().isPrimaryOrBackup(grid.localNode(), i)) {
                // Load key-value pair into cache.
                clo.apply(i, Integer.toString(i));

                // Add loaded value to database as well, since we generated it here.
                // In real life, the values would most likely be loaded from underlying database.
                dummyDB.put(i, Integer.toString(i));
            }

            if (i % 100000 == 0)
                log.info("Loaded " + i + " keys.");
        }
    }

    /** {@inheritDoc} */
    @Override public Object load(@Nullable GridCacheTx tx, Object key) throws GridException {
        return dummyDB.get(key);
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable GridCacheTx tx, Object key, Object val) throws GridException {
        dummyDB.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable GridCacheTx tx, Object key) throws GridException {
        dummyDB.remove(key);
    }
}

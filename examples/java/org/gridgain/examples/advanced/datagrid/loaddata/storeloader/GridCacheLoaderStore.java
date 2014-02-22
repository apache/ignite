// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.datagrid.loaddata.storeloader;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.resources.*;
import org.jetbrains.annotations.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * Store loader responsible for loading bulk of data from persistent store.
 * The only implemented method is {@link #loadAll(GridBiInClosure, Object...)}
 * method which randomly generates values to load and passes them to cache.
 * <p>
 * Other {@link GridCacheStore} methods are not implemented simply because they are not used in
 * this example. Take a look at examples defined in {@link org.gridgain.examples.advanced.datagrid.store}
 * package for various cache store implementations.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class GridCacheLoaderStore extends GridCacheStoreAdapter<String, Integer> {
    /** Auto-injected grid instance. */
    @GridInstanceResource
    private Grid grid;

    /** Auto-injected logger. */
    @GridLoggerResource
    private GridLogger log;

    /** {@inheritDoc} */
    @Override public void loadAll(final GridBiInClosure<String, Integer> clo,
        Object... args) throws GridException {
        // Number of entries is passed by caller.
        int entryCnt = (Integer)args[0];

        GridCache<String, Integer> cache = grid.cache("partitioned");

        log.info("Number of cache entries to load: " + entryCnt);

        for (int i = 0; i < entryCnt; i++) {
            String key = Integer.toString(i);

            // Only add to cache if key is mapped to local node.
            // We check for local mapping just to demonstrate that
            // you can do it from your logic - GridGain will always
            // check internally as well.
            //
            // In real life you can store partition IDs for your keys
            // in your persistent store and only load the partition IDs
            // mapped to the local node here. You would use similar check
            // as below to find out if partition ID is mapped to local node.
            if (cache.affinity().primaryOrBackup(grid.localNode(), key))
                clo.apply(key, i);

            if (i % 100000 == 0)
                log.info("Loaded " + i + " keys.");
        }
    }

    /** {@inheritDoc} */
    @Override public Integer load(@Nullable GridCacheTx tx, String key)
        throws GridException {
        assert false : "This method is never called in our example.";

        return null;
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable GridCacheTx tx, String key,
        @Nullable Integer val) throws GridException {
        assert false : "This method is never called in our example.";
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable GridCacheTx tx, String key)
        throws GridException {
        assert false : "This method is never called in our example.";
    }
}

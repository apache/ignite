// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.datagrid.affinity;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.product.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * This example demonstrates the simplest code that populates the distributed cache
 * and co-locates simple closure execution with each key. The goal of this particular
 * example is to provide the simplest code example of this logic.
 * <p>
 * Note that other examples in this package provide more detailed examples
 * of affinity co-location.
 * <p>
 * Affinity routing is enabled for all caches.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache: {@code 'ggstart.sh examples/config/example-cache.xml'}. Local node should
 * be started with cache.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public final class GridCacheAffinitySimpleExample {
    /** Number of keys. */
    private static final int KEY_CNT = 20;

    /**
     * Executes cache affinity example.
     * <p>
     * Note that in case of {@code LOCAL} configuration,
     * since there is no distribution, values may come back as {@code nulls}.
     *
     * @param args Command line arguments
     * @throws GridException If failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start("examples/config/example-cache.xml")) {
            GridCache<String, String> cache = g.cache("partitioned");

            if (cache == null) {
                System.err.println("Cache with name 'partitioned' not found (is configuration correct?)");

                return;
            }

            // If you run this example multiple times - make sure
            // to comment this call in order not to re-populate twice.
            populate(cache);

            // Co-locates closures with data in in-memory data grid.
            visit(g, cache);
        }
    }

    /**
     * Visits every in-memory data grid entry on the remote node it resides by co-locating visiting
     * closure with the cache key.
     *
     * @param g Grid.
     * @param c Cache to use.
     * @throws GridException If failed.
     */
    private static void visit(GridProjection g, final GridCache<String, String> c) throws GridException {
        for (int i = 0; i < KEY_CNT; i++) {
            // Affinity key is cache key for this example.
            final String key = Integer.toString(i);

            g.compute().affinityRun("partitioned", key, new GridRunnable() {
                // This closure will execute on the remote node where
                // data with the 'key' is located. Since it will be co-located
                // we can use local 'peek' operation safely.
                @Override public void run() {
                    // Value should never be 'null' as we are co-located and using local 'peek'
                    // access method.
                    System.out.println("Co-located [key= " + key + ", value=" + c.peek(key) + ']');
                }
            }).get();
        }
    }

    /**
     * Populates given cache.
     *
     * @param c Cache to populate.
     * @throws GridException Thrown in case of any cache error.
     */
    private static void populate(GridCache<String, String> c) throws GridException {
        for (int i = 0; i < KEY_CNT; i++)
            c.put(Integer.toString(i), Integer.toString(i));
    }
}

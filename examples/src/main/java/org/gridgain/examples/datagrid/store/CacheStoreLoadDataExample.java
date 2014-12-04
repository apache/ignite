/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.store;

import org.gridgain.examples.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;

/**
 * Loads data from persistent store at cache startup by calling
 * {@link GridCache#loadCache(GridBiPredicate, long, Object...)} method on
 * all nodes.
 * <p>
 * Remote nodes should always be started using {@link CacheNodeWithStoreStartup}.
 * Also you can change type of underlying store modifying configuration in the
 * {@link CacheNodeWithStoreStartup#configure()} method.
 */
public class CacheStoreLoadDataExample {
    /** Heap size required to run this example. */
    public static final int MIN_MEMORY = 1024 * 1024 * 1024;

    /** Number of entries to load. */
    private static final int ENTRY_COUNT = 1000000;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        ExamplesUtils.checkMinMemory(MIN_MEMORY);

        try (Grid g = GridGain.start(CacheNodeWithStoreStartup.configure())) {
            System.out.println();
            System.out.println(">>> Cache store load data example started.");

            final GridCache<String, Integer> cache = g.cache(null);

            // Clean up caches on all nodes before run.
            cache.globalClearAll(0);

            long start = System.currentTimeMillis();

            // Start loading cache on all caching nodes.
            g.compute(g.cluster().forCache(null)).broadcast(new GridCallable<Object>() {
                @Override public Object call() throws Exception {
                    // Load cache from persistent store.
                    cache.loadCache(null, 0, ENTRY_COUNT);

                    return null;
                }
            });

            long end = System.currentTimeMillis();

            System.out.println(">>> Loaded " + ENTRY_COUNT + " keys with backups in " + (end - start) + "ms.");
        }
    }
}

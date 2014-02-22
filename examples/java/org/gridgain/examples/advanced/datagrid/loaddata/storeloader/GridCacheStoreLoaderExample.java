// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.datagrid.loaddata.storeloader;

import org.gridgain.examples.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.dataload.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.product.*;

import java.util.concurrent.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * Loads data from persistent store at cache startup by calling
 * {@link GridCache#loadCache(GridBiPredicate, long, Object...)} method on
 * all nodes.
 * <p>
 * For this example you should startup remote nodes only by calling
 * {@link GridCacheStoreLoaderNodeStartup} class.
 * <p>
 * You should not be using stand-alone nodes (started with {@code 'ggstart.sh})
 * because GridGain nodes do not know about the {@link GridCacheLoaderStore}
 * we define in this example. However, users can always add JAR-files with their classes to
 * {@code GRIDGAIN_HOME/libs/ext} folder to make them available to GridGain.
 * If this was done here (i.e. we had JAR-file containing custom cache store built
 * and put to {@code GRIDGAIN_HOME/libs/ext} folder), we could easily startup
 * remote nodes with {@code 'ggstart.sh examples/config/example-cache-storeloader.xml'}
 * command.
 * <p>
 * Please note that this example loads large amount of data into memory and therefore
 * requires larger heap size. Please add {@code -Xmx1g} to JVM startup options.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class GridCacheStoreLoaderExample {
    /** Heap size required to run this example. */
    public static final int MIN_MEMORY = 1024 * 1024 * 1024;

    /** Number of entries to load. */
    private static final int ENTRY_COUNT = 1000000;

    /**
     * Generates and loads data onto in-memory data grid directly form {@link GridDataLoader}
     * public API.
     *
     * @param args Parameters.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        GridExamplesUtils.checkMinMemory(MIN_MEMORY);

        Grid g = GridGain.start("examples/config/example-cache-storeloader.xml");

        try {
            // Warm up.
            load(g, 100000);

            System.out.println(">>> JVM is warmed up.");

            // Load.
            load(g, ENTRY_COUNT);
        }
        finally {
            GridGain.stop(false);
        }
    }

    /**
     * Loads specified number of keys into cache using provided {@link GridDataLoader} instance.
     *
     * @param g Grid instance.
     * @param cnt Number of keys to load.
     * @throws GridException If failed.
     */
    private static void load(Grid g, final int cnt) throws GridException {
        final GridCache<String, Integer> cache = g.cache("partitioned");

        long start = System.currentTimeMillis();

        // Start loading cache on all nodes.
        g.compute().call(new Callable<Object>() {
            @Override public Object call() throws Exception {
                cache.loadCache(null, 0, cnt);

                return null;
            }
        }).get();

        long end = System.currentTimeMillis();

        System.out.println(">>> Loaded " + cnt + " keys with backups in " + (end - start) + "ms.");
    }
}

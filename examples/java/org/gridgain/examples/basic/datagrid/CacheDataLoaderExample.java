// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.basic.datagrid;

import org.gridgain.examples.*;
import org.gridgain.grid.*;
import org.gridgain.grid.dataload.*;

/**
 * Loads data from one node onto the rest of the in-memory data grid by utilizing {@link GridDataLoader}
 * API. {@link GridDataLoader} is a lot more efficient to use than standard
 * {@code GridCacheProjection.put(...)} operation as it properly buffers cache requests
 * together and properly manages load on remote nodes.
 * <p>
 * You can startup remote nodes either by starting {@link CacheNodeStartup}
 * class or stand alone. In case of stand alone node startup, remote nodes should always
 * be started with configuration which includes cache using following command:
 * {@code 'ggstart.sh examples/config/example-cache-dataloader.xml'}.
 * <p>
 * Please note that this example loads large amount of data into memory and therefore
 * requires larger heap size. Please add {@code -Xmx512m} to JVM startup options.
 *
 * @author @java.author
 * @version @java.version
 */
public class CacheDataLoaderExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";
    //private static final String CACHE_NAME = "replicated";
    //private static final String CACHE_NAME = "local";

    /** Number of entries to load. */
    private static final int ENTRY_COUNT = 1000000;

    /** Heap size required to run this example. */
    public static final int MIN_MEMORY = 512 * 1024 * 1024;

    /**
     * Generates and loads data onto in-memory data grid directly form {@link GridDataLoader}
     * public API.
     *
     * @param args Parameters.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        GridExamplesUtils.checkMinMemory(MIN_MEMORY);

        try (Grid g = GridGain.start("examples/config/example-cache-tuned.xml")) {
            try (GridDataLoader<Integer, String> ldr = g.dataLoader(CACHE_NAME)) {
                // Configure loader.
                ldr.perNodeBufferSize(1024);

                // Warm up.
                load(ldr, 100000);

                System.out.println(">>> JVM is warmed up.");

                // Load.
                load(ldr, ENTRY_COUNT);
            }
        }
    }

    /**
     * Loads specified number of keys into cache using provided {@link GridDataLoader} instance.
     *
     * @param ldr Data loader.
     * @param cnt Number of keys to load.
     * @throws GridException If failed.
     */
    private static void load(GridDataLoader<Integer, String> ldr, int cnt) throws GridException {
        long start = System.currentTimeMillis();

        for (int i = 0; i < cnt; i++) {
            ldr.addData(i, Integer.toString(i));

            // Print out progress while loading cache.
            if (i > 0 && i % 10000 == 0)
                System.out.println("Loaded " + i + " keys.");
        }

        long end = System.currentTimeMillis();

        System.out.println(">>> Loaded " + cnt + " keys in " + (end - start) + "ms.");
    }
}

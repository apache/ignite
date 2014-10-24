/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid;

import org.gridgain.examples.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.dataload.*;

import java.util.*;

/**
 * Demonstrates how cache can be populated with data utilizing {@link GridDataLoader} API.
 * {@link GridDataLoader} is a lot more efficient to use than standard
 * {@code GridCacheProjection.put(...)} operation as it properly buffers cache requests
 * together and properly manages load on remote nodes.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start GridGain node with {@code examples/config/example-cache.xml} configuration.
 */
public class CacheDataLoaderExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /** Number of entries to load. */
    private static final int ENTRY_COUNT = 500_000;

    /** Load buffer size. */
    private static final int LDR_BUFF = 64 * 1024;

    /** Heap size required to run this example. */
    public static final int MIN_MEMORY = 512 * 1024 * 1024;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        loaderDirectExample();

        loaderMapExample();
    }

    /**
     * Example to show how to load data via loader.
     *
     * @throws Exception
     */
    private static void loaderDirectExample() throws Exception {
        ExamplesUtils.checkMinMemory(MIN_MEMORY);

        try (Grid g = GridGain.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache data loader direct example started.");

            // Clean up caches on all nodes before run.
            g.cache(CACHE_NAME).globalClearAll(0);

            try (GridDataLoader<Integer, String> ldr = g.dataLoader(CACHE_NAME)) {
                // Configure loader.
                ldr.perNodeBufferSize(LDR_BUFF);

                long start = System.currentTimeMillis();

                for (int i = 0; i < ENTRY_COUNT; i++) {
                    ldr.addData(i, Integer.toString(i));

                    // Print out progress while loading cache.
                    if (i > 0 && i % 10000 == 0)
                        System.out.println("Loaded " + i + " keys.");
                }

                ldr.close(false);

                System.out.println(">>> Loaded " + ENTRY_COUNT + " keys in " + (System.currentTimeMillis() - start) + "ms.");

                checkCache(g);

                System.out.println(">>> Cache data loader direct example finished.");
            }
        }
    }

    /**
     * Example to show how to load data via map.
     *
     * @throws Exception
     */
    public static void loaderMapExample() throws Exception {
        ExamplesUtils.checkMinMemory(MIN_MEMORY);

        try (Grid g = GridGain.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache data loader example started.");

            // Clean up caches on all nodes before run.
            g.cache(CACHE_NAME).globalClearAll(0);

            try (GridDataLoader<Integer, String> ldr = g.dataLoader(CACHE_NAME)) {
                // Configure loader.
                ldr.perNodeBufferSize(LDR_BUFF);

                long start = System.currentTimeMillis();

                Map<Integer, String> m = new HashMap<>(ENTRY_COUNT);

                for (int i = 0; i < ENTRY_COUNT; i++) {
                    m.put(i, Integer.toString(i));
                }

                System.out.println(">>> Map prepared for " + ENTRY_COUNT + " keys in " +
                    (System.currentTimeMillis() - start) + "ms.");

                start = System.currentTimeMillis();

                ldr.addData(m);
                ldr.close(false);

                System.out.println(">>> Loaded " + ENTRY_COUNT + " keys in " +
                    (System.currentTimeMillis() - start) + "ms.");

                checkCache(g);

                System.out.println(">>> Cache data loader example with map finished.");
            }
        }
    }

    /**
     * Check random keys in cache.
     *
     * @param g Grid instance.
     * @throws GridException
     */
    private static void checkCache(Grid g) throws GridException {
        System.out.println(">>> Check that keys were loaded (read 10 random keys)...");

        GridCache<Integer, String> c = g.cache(CACHE_NAME);

        Random rnd = new Random();

        for (int i = 0; i < 10; i++) {
            System.out.println(c.get(rnd.nextInt(ENTRY_COUNT)));
        }
   }
}

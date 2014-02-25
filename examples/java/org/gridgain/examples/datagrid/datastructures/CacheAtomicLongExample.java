// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.lang.*;

import java.util.*;

/**
 * Demonstrates a simple usage of distributed atomic long.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache configuration, e.g. {@code 'ggstart.sh examples/config/example-cache.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
public final class CacheAtomicLongExample {
    /** Cache name. */
    //private static final String CACHE_NAME = "replicated";
    private static final String CACHE_NAME = "partitioned_tx";

    /** Number of retries */
    private static final int RETRIES = 20;

    /**
     * Executes this example on the grid.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start("examples/config/example-cache.xml")) {
            print("Starting atomic long example on nodes: " + g.nodes().size());

            // Number nodes in grid.
            int nodes = g.nodes().size();

            // Make name for atomic long (by which it will be known in the grid).
            String atomicName = UUID.randomUUID().toString();

            // Initialize atomic long in grid.
            GridCacheAtomicLong atomicLong = g.cache(CACHE_NAME).dataStructures().atomicLong(atomicName, 0, true);

            print("Atomic long initial value : " + atomicLong.get() + '.');

            atomicLong.incrementAndGet();

            print("Atomic long value after increment: " + atomicLong.get());

            atomicLong.compareAndSet(2, new GridPredicate<Long>() {
                @Override public boolean apply(Long val) {
                    return val == 0;
                }
            });

            print("Atomic long value after failed CAS: " + atomicLong.get());

            atomicLong.compareAndSet(2, new GridPredicate<Long>() {
                @Override public boolean apply(Long val) {
                    return val == 1;
                }
            });

            print("Atomic long value after successful CAS: " + atomicLong.get());
            print("");
            print("Finished atomic long example...");
            print("Check all nodes for output (this node is also part of the grid).");
            print("");
        }
    }

    /**
     * Prints out given object to standard out.
     *
     * @param o Object to print.
     */
    private static void print(Object o) {
        System.out.println(">>> " + o);
    }
}

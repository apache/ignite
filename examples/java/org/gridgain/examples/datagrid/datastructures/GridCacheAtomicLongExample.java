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
import org.gridgain.grid.product.*;

import java.util.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * Demonstrates a simple usage of distributed atomic long.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache configuration, e.g. {@code 'ggstart.sh examples/config/example-cache.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public final class GridCacheAtomicLongExample {
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

            print("Atomic long initial size : " + atomicLong.get() + '.');

            // Try increment atomic long from all grid nodes.
            // Note that this node is also part of the grid.
            g.compute().call(new IncrementClosure(CACHE_NAME, atomicName)).get();

            print("");
            print("AtomicLong after incrementing [expected=" + (nodes * RETRIES) + ", actual=" + atomicLong.get() + ']');
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

    /**
     * Performs increments on an atomic variable in cache.
     */
    private static class IncrementClosure extends GridCallable<Object> {
        /** Cache name. */
        private final String cacheName;

        /** Atomic variable name. */
        private final String atomicName;

        /**
         * @param cacheName Cache name.
         * @param atomicName Atomic variable name.
         */
        IncrementClosure(String cacheName, String atomicName) {
            this.cacheName = cacheName;
            this.atomicName = atomicName;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            GridCacheAtomicLong atomicLong = GridGain.grid().cache(cacheName).dataStructures().
                atomicLong(atomicName, 0, true);

            for (int i = 0; i < RETRIES; i++)
                print("AtomicLong value has been incremented " + atomicLong.incrementAndGet() + '.');

            return null;
        }
    }
}

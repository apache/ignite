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
 * Demonstrates a simple usage of distributed atomic reference.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache configuration, e.g. {@code 'ggstart.sh examples/config/example-cache.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
public final class CacheAtomicReferenceExample {
    /** Cache name. */
    // private static final String CACHE_NAME = "replicated";
    private static final String CACHE_NAME = "partitioned_tx";

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
            print("Starting atomic reference example on nodes: " + g.nodes().size());

            // Make name of atomic reference.
            final String refName = UUID.randomUUID().toString();

            // Make value of atomic reference.
            String val = UUID.randomUUID().toString();

            // Initialize atomic reference in grid.
            GridCacheAtomicReference<String> ref = g.cache(CACHE_NAME).dataStructures().
                atomicReference(refName, val, true);

            print("Atomic reference initial value : " + ref.get() + '.');

            // Make new value of atomic reference.
            String newVal = UUID.randomUUID().toString();

            print("Try to change value of atomic reference with wrong expected value.");

            ref.compareAndSet("WRONG EXPECTED VALUE", newVal); // Won't change.

            print("Atomic reference value : " + ref.get() + '.');

            print("Try to change value of atomic reference with correct expected value.");

            ref.compareAndSet(val, newVal);

            print("Atomic reference value : " + ref.get() + '.');
        }

        print("");
        print("Finished atomic reference example...");
        print("Check all nodes for output (this node is also part of the grid).");
        print("");
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

// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.datagrid.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.product.*;

import java.util.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * Demonstrates a simple usage of distributed atomic reference.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache configuration, e.g. {@code 'ggstart.sh examples/config/example-cache.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public final class GridCacheAtomicReferenceExample {
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

            // Make closure for checking atomic reference value on grid.
            Runnable c = new ReferenceClosure(CACHE_NAME, refName);

            // Check atomic reference on all grid nodes.
            g.compute().run(c).get();

            // Make new value of atomic reference.
            String newVal = UUID.randomUUID().toString();

            print("Try to change value of atomic reference with wrong expected value.");

            ref.compareAndSet("WRONG EXPECTED VALUE", newVal); // Won't change.

            // Check atomic reference on all grid nodes.
            // Atomic reference value shouldn't be changed.
            g.compute().run(c).get();

            print("Try to change value of atomic reference with correct expected value.");

            ref.compareAndSet(val, newVal);

            // Check atomic reference on all grid nodes.
            // Atomic reference value should be changed.
            g.compute().run(c).get();
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

    /**
     * Obtains atomic reference.
     */
    private static class ReferenceClosure extends GridRunnable {
        /** Cache name. */
        private final String cacheName;

        /** Reference name. */
        private final String refName;

        /**
         * @param cacheName Cache name.
         * @param refName Reference name.
         */
        ReferenceClosure(String cacheName, String refName) {
            this.cacheName = cacheName;
            this.refName = refName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                GridCacheAtomicReference<String> ref = GridGain.grid().cache(cacheName).dataStructures().
                    atomicReference(refName, null, true);

                print("Atomic reference value is " + ref.get() + '.');
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }
    }
}

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

import static org.gridgain.grid.GridClosureCallMode.*;
import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * Demonstrates a simple usage of distributed atomic stamped.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache configuration, e.g. {@code 'ggstart.sh examples/config/example-cache.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public final class GridCacheAtomicStampedExample {
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
            print("Starting atomic stamped example on nodes: " + g.nodes().size());

            // Make name of atomic stamped.
            String stampedName = UUID.randomUUID().toString();

            // Make value of atomic stamped.
            String val = UUID.randomUUID().toString();

            // Make stamp of atomic stamped.
            String stamp = UUID.randomUUID().toString();

            // Initialize atomic stamped in cache.
            GridCacheAtomicStamped<String, String> stamped = g.cache(CACHE_NAME).dataStructures().
                atomicStamped(stampedName, val, stamp, true);

            print("Atomic stamped initial [value=" + stamped.value() + ", stamp=" + stamped.stamp() + ']');

            // Make closure for checking atomic stamped on grid.
            Runnable c = new StampedUpdateClosure(CACHE_NAME, stampedName);

            // Check atomic stamped on all grid nodes.
            g.compute().run(BROADCAST, c).get();

            // Make new value of atomic stamped.
            String newVal = UUID.randomUUID().toString();

            // Make new stamp of atomic stamped.
            String newStamp = UUID.randomUUID().toString();

            print("Try to change value and stamp of atomic stamped with wrong expected value and stamp.");

            stamped.compareAndSet("WRONG EXPECTED VALUE", newVal, "WRONG EXPECTED STAMP", newStamp);

            // Check atomic stamped on all grid nodes.
            // Atomic stamped value and stamp shouldn't be changed.
            g.compute().run(BROADCAST, c).get();

            print("Try to change value and stamp of atomic stamped with correct value and stamp.");

            stamped.compareAndSet(val, newVal, stamp, newStamp);

            // Check atomic stamped on all grid nodes.
            // Atomic stamped value and stamp should be changed.
            g.compute().run(BROADCAST, c).get();
        }

        print("");
        print("Finished atomic stamped example...");
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
     * Performs update of on an atomic stamped variable in cache.
     */
    private static class StampedUpdateClosure extends GridRunnable {
        /** Cache name. */
        private final String cacheName;

        /** Atomic stamped variable name. */
        private final String stampedName;

        /**
         * @param cacheName Cache name.
         * @param stampedName Atomic stamped variable name.
         */
        StampedUpdateClosure(String cacheName, String stampedName) {
            this.cacheName = cacheName;
            this.stampedName = stampedName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                GridCacheAtomicStamped<String, String> stamped = GridGain.grid().cache(cacheName).dataStructures().
                    atomicStamped(stampedName, null, null, true);

                print("Atomic stamped [value=" + stamped.value() + ", stamp=" + stamped.stamp() + ']');
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }
    }
}

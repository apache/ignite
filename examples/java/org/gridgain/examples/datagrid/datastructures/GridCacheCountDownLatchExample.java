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
 * Demonstrates a simple usage of distributed count down latch.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache configuration, e.g. {@code 'ggstart.sh examples/config/example-cache.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class GridCacheCountDownLatchExample {
    /** Cache name. */
    // private static final String CACHE_NAME = "replicated";
    private static final String CACHE_NAME = "partitioned_tx";

    /** Number of latch initial count */
    private static final int INITIAL_COUNT = 10;

    /**
     * Executes this example on the grid.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("examples/config/example-cache.xml")) {
            print("Starting count down latch example on grid of size: " + g.nodes().size());

            // Make name of count down latch.
            final String latchName = UUID.randomUUID().toString();

            // Initialize count down latch in grid.
            GridCacheCountDownLatch latch = g.cache(CACHE_NAME).dataStructures().
                countDownLatch(latchName, INITIAL_COUNT, false, true);

            print("Latch initial value: " + latch.count());

            // Start waiting on the latch on all grid nodes.
            for (int i = 0; i < INITIAL_COUNT; i++)
                g.compute().run(new LatchClosure(CACHE_NAME, latchName)).get();

            // Wait for latch to go down which essentially means that all remote closures completed.
            latch.await();

            print("All latch closures have completed.");
        }

        print("");
        print("Finished count down latch example...");
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
     * Closure which simply waits on the latch on all nodes.
     */
    private static class LatchClosure extends GridRunnable {
        /** Cache name. */
        private final String cacheName;

        /** Latch name. */
        private final String latchName;

        /**
         * @param cacheName Cache name.
         * @param latchName Latch name.
         */
        LatchClosure(String cacheName, String latchName) {
            this.cacheName = cacheName;
            this.latchName = latchName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                GridCacheCountDownLatch latch = GridGain.grid().cache(cacheName).dataStructures().
                    countDownLatch(latchName, 1, false, true);

                int newCnt = latch.countDown();

                print("Counted down [newCnt=" + newCnt + ", nodeId=" + GridGain.grid().localNode().id() + ']');
            }
            catch (GridException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

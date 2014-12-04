/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.datastructures;

import org.gridgain.examples.datagrid.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.lang.*;

import java.util.*;

/**
 * Demonstrates a simple usage of distributed count down latch.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start GridGain node with {@code examples/config/example-cache.xml} configuration.
 */
public class CacheCountDownLatchExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned_tx";

    /** Number of latch initial count */
    private static final int INITIAL_COUNT = 10;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite g = GridGain.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache atomic countdown latch example started.");

            // Make name of count down latch.
            final String latchName = UUID.randomUUID().toString();

            // Initialize count down latch in grid.
            GridCacheCountDownLatch latch = g.cache(CACHE_NAME).dataStructures().
                countDownLatch(latchName, INITIAL_COUNT, false, true);

            System.out.println("Latch initial value: " + latch.count());

            // Start waiting on the latch on all grid nodes.
            for (int i = 0; i < INITIAL_COUNT; i++)
                g.compute().run(new LatchClosure(CACHE_NAME, latchName));

            // Wait for latch to go down which essentially means that all remote closures completed.
            latch.await();

            System.out.println("All latch closures have completed.");
        }

        System.out.println();
        System.out.println("Finished count down latch example...");
        System.out.println("Check all nodes for output (this node is also part of the grid).");
    }

    /**
     * Closure which simply waits on the latch on all nodes.
     */
    private static class LatchClosure implements GridRunnable {
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

                System.out.println("Counted down [newCnt=" + newCnt + ", nodeId=" + GridGain.grid().cluster().localNode().id() + ']');
            }
            catch (GridException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

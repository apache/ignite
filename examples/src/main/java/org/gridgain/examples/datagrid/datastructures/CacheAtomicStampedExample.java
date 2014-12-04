/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.examples.datagrid.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.datastructures.*;

import java.util.*;

/**
 * Demonstrates a simple usage of distributed atomic stamped.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start GridGain node with {@code examples/config/example-cache.xml} configuration.
 */
public final class CacheAtomicStampedExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned_tx";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Ignite g = Ignition.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache atomic stamped example started.");

            // Make name of atomic stamped.
            String stampedName = UUID.randomUUID().toString();

            // Make value of atomic stamped.
            String val = UUID.randomUUID().toString();

            // Make stamp of atomic stamped.
            String stamp = UUID.randomUUID().toString();

            // Initialize atomic stamped in cache.
            GridCacheAtomicStamped<String, String> stamped = g.cache(CACHE_NAME).dataStructures().
                atomicStamped(stampedName, val, stamp, true);

            System.out.println("Atomic stamped initial [value=" + stamped.value() + ", stamp=" + stamped.stamp() + ']');

            // Make closure for checking atomic stamped on grid.
            Runnable c = new StampedUpdateClosure(CACHE_NAME, stampedName);

            // Check atomic stamped on all grid nodes.
            g.compute().run(c);

            // Make new value of atomic stamped.
            String newVal = UUID.randomUUID().toString();

            // Make new stamp of atomic stamped.
            String newStamp = UUID.randomUUID().toString();

            System.out.println("Try to change value and stamp of atomic stamped with wrong expected value and stamp.");

            stamped.compareAndSet("WRONG EXPECTED VALUE", newVal, "WRONG EXPECTED STAMP", newStamp);

            // Check atomic stamped on all grid nodes.
            // Atomic stamped value and stamp shouldn't be changed.
            g.compute().run(c);

            System.out.println("Try to change value and stamp of atomic stamped with correct value and stamp.");

            stamped.compareAndSet(val, newVal, stamp, newStamp);

            // Check atomic stamped on all grid nodes.
            // Atomic stamped value and stamp should be changed.
            g.compute().run(c);
        }

        System.out.println();
        System.out.println("Finished atomic stamped example...");
        System.out.println("Check all nodes for output (this node is also part of the grid).");
    }

    /**
     * Performs update of on an atomic stamped variable in cache.
     */
    private static class StampedUpdateClosure implements IgniteRunnable {
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
                GridCacheAtomicStamped<String, String> stamped = Ignition.grid().cache(cacheName).dataStructures().
                    atomicStamped(stampedName, null, null, true);

                System.out.println("Atomic stamped [value=" + stamped.value() + ", stamp=" + stamped.stamp() + ']');
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }
    }
}

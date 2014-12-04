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
 * Demonstrates a simple usage of distributed atomic reference.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start GridGain node with {@code examples/config/example-cache.xml} configuration.
 */
public final class CacheAtomicReferenceExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned_tx";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Ignite g = GridGain.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache atomic reference example started.");

            // Make name of atomic reference.
            final String refName = UUID.randomUUID().toString();

            // Make value of atomic reference.
            String val = UUID.randomUUID().toString();

            // Initialize atomic reference in grid.
            GridCacheAtomicReference<String> ref = g.cache(CACHE_NAME).dataStructures().
                atomicReference(refName, val, true);

            System.out.println("Atomic reference initial value : " + ref.get() + '.');

            // Make closure for checking atomic reference value on grid.
            Runnable c = new ReferenceClosure(CACHE_NAME, refName);

            // Check atomic reference on all grid nodes.
            g.compute().run(c);

            // Make new value of atomic reference.
            String newVal = UUID.randomUUID().toString();

            System.out.println("Try to change value of atomic reference with wrong expected value.");

            ref.compareAndSet("WRONG EXPECTED VALUE", newVal); // Won't change.

            // Check atomic reference on all grid nodes.
            // Atomic reference value shouldn't be changed.
            g.compute().run(c);

            System.out.println("Try to change value of atomic reference with correct expected value.");

            ref.compareAndSet(val, newVal);

            // Check atomic reference on all grid nodes.
            // Atomic reference value should be changed.
            g.compute().run(c);
        }

        System.out.println();
        System.out.println("Finished atomic reference example...");
        System.out.println("Check all nodes for output (this node is also part of the grid).");
    }

    /**
     * Obtains atomic reference.
     */
    private static class ReferenceClosure implements IgniteRunnable {
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

                System.out.println("Atomic reference value is " + ref.get() + '.');
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }
    }
}

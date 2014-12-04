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
 * Demonstrates a simple usage of distributed atomic long.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start GridGain node with {@code examples/config/example-cache.xml} configuration.
 */
public final class CacheAtomicLongExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned_tx";

    /** Number of retries */
    private static final int RETRIES = 20;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Ignite g = GridGain.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache atomic long example started.");

            // Make name for atomic long (by which it will be known in the grid).
            String atomicName = UUID.randomUUID().toString();

            // Initialize atomic long in grid.
            final GridCacheAtomicLong atomicLong = g.cache(CACHE_NAME).dataStructures().atomicLong(atomicName, 0, true);

            System.out.println();
            System.out.println("Atomic long initial value : " + atomicLong.get() + '.');

            // Try increment atomic long from all grid nodes.
            // Note that this node is also part of the grid.
            g.compute(g.cluster().forCache(CACHE_NAME)).call(new IgniteCallable<Object>() {
                @Override public Object call() throws  Exception {
                    for (int i = 0; i < RETRIES; i++)
                        System.out.println("AtomicLong value has been incremented: " + atomicLong.incrementAndGet());

                    return null;
                }
            });

            System.out.println();
            System.out.println("Atomic long value after successful CAS: " + atomicLong.get());
        }
    }
}

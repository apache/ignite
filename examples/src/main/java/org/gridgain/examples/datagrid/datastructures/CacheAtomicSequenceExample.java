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
 * Demonstrates a simple usage of distributed atomic sequence.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start GridGain node with {@code examples/config/example-cache.xml} configuration.
 */
public final class CacheAtomicSequenceExample {
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
        try (Grid g = GridGain.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache atomic sequence example started.");

            // Make name of sequence.
            final String seqName = UUID.randomUUID().toString();

            // Initialize atomic sequence in grid.
            GridCacheAtomicSequence seq = g.cache(CACHE_NAME).dataStructures().atomicSequence(seqName, 0, true);

            // First value of atomic sequence on this node.
            long firstVal = seq.get();

            System.out.println("Sequence initial value: " + firstVal);

            // Try increment atomic sequence on all grid nodes. Note that this node is also part of the grid.
            g.compute().run(new SequenceClosure(CACHE_NAME, seqName));

            System.out.println("Sequence after incrementing [expected=" + (firstVal + RETRIES) + ", actual=" +
                seq.get() + ']');
        }

        System.out.println();
        System.out.println("Finished atomic sequence example...");
        System.out.println("Check all nodes for output (this node is also part of the grid).");
    }

    /**
     * Obtains atomic sequence.
     */
    private static class SequenceClosure implements GridRunnable {
        /** Cache name. */
        private final String cacheName;

        /** Sequence name. */
        private final String seqName;

        /**
         * @param cacheName Cache name.
         * @param seqName Sequence name.
         */
        SequenceClosure(String cacheName, String seqName) {
            this.cacheName = cacheName;
            this.seqName = seqName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                GridCacheAtomicSequence seq = GridGain.grid().cache(cacheName).dataStructures().
                    atomicSequence(seqName, 0, true);

                for (int i = 0; i < RETRIES; i++)
                    System.out.println("Sequence [currentValue=" + seq.get() + ", afterIncrement=" +
                        seq.incrementAndGet() + ']');

            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }
    }
}

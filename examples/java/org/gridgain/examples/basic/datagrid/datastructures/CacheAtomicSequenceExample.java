// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.basic.datagrid.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.lang.*;

import java.util.*;

/**
 * Demonstrates a simple usage of distributed atomic sequence.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache configuration, e.g. {@code 'ggstart.sh examples/config/example-cache.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
public final class CacheAtomicSequenceExample {
    /** Cache name. */
    // private static final String CACHE_NAME = "replicated";
    private static final String CACHE_NAME = "partitioned_tx";

    /** Number of retries */
    private static final int RETRIES = 20;

    /**
     * Executes this example on the grid.
     * <p>
     * Note that atomic sequence reserves on each node region of ids for best performance.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start("examples/config/example-cache.xml")) {
            print("Starting atomic sequence example on nodes: " + g.nodes().size());

            // Make name of sequence.
            final String seqName = UUID.randomUUID().toString();

            // Initialize atomic sequence in grid.
            GridCacheAtomicSequence seq = g.cache(CACHE_NAME).dataStructures().atomicSequence(seqName, 0, true);

            // First value of atomic sequence on this node.
            long firstVal = seq.get();

            print("Sequence initial value: " + firstVal);

            // Try increment atomic sequence on all grid nodes. Note that this node is also part of the grid.
            g.compute().run(new SequenceClosure(CACHE_NAME, seqName)).get();

            print("Sequence after incrementing [expected=" + (firstVal + RETRIES) + ", actual=" + seq.get() + ']');
        }

        print("");
        print("Finished atomic sequence example...");
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
     * Obtains atomic sequence.
     */
    private static class SequenceClosure extends GridRunnable {
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
                    print("Sequence [currentValue=" + seq.get() + ", afterIncrement=" + seq.incrementAndGet() + ']');

            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }
    }
}

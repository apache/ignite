/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.examples.datagrid.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.cache.datastructures.*;
import org.apache.ignite.examples.datagrid.*;
import org.apache.ignite.lang.*;

import java.util.*;

/**
 * Demonstrates a simple usage of distributed atomic sequence.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-cache.xml} configuration.
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
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        try (Ignite ignite = Ignition.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache atomic sequence example started.");

            // Make name of sequence.
            final String seqName = UUID.randomUUID().toString();

            // Initialize atomic sequence in grid.
            CacheAtomicSequence seq = ignite.cache(CACHE_NAME).dataStructures().atomicSequence(seqName, 0, true);

            // First value of atomic sequence on this node.
            long firstVal = seq.get();

            System.out.println("Sequence initial value: " + firstVal);

            // Try increment atomic sequence on all grid nodes. Note that this node is also part of the grid.
            ignite.compute().run(new SequenceClosure(CACHE_NAME, seqName));

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
    private static class SequenceClosure implements IgniteRunnable {
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
                CacheAtomicSequence seq = Ignition.ignite().cache(cacheName).dataStructures().
                    atomicSequence(seqName, 0, true);

                for (int i = 0; i < RETRIES; i++)
                    System.out.println("Sequence [currentValue=" + seq.get() + ", afterIncrement=" +
                        seq.incrementAndGet() + ']');

            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }
}

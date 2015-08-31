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

package org.apache.ignite.examples.datastructures;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.lang.IgniteRunnable;

/**
 * Demonstrates a simple usage of distributed atomic sequence.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public final class IgniteAtomicSequenceExample {
    /** Number of retries */
    private static final int RETRIES = 20;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache atomic sequence example started.");

            // Try increment atomic sequence on all cluster nodes. Note that this node is also part of the cluster.
            ignite.compute().broadcast(new SequenceClosure("example-sequence"));

            System.out.println();
            System.out.println("Finished atomic sequence example...");
            System.out.println("Check all nodes for output (this node is also part of the cluster).");
            System.out.println();
        }
    }

    /**
     * Obtains atomic sequence.
     */
    private static class SequenceClosure implements IgniteRunnable {
        /** Sequence name. */
        private final String seqName;

        /**
         * @param seqName Sequence name.
         */
        SequenceClosure(String seqName) {
            this.seqName = seqName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            // Create sequence. Only one concurrent call will succeed in creation.
            // Rest of the callers will get already created instance.
            IgniteAtomicSequence seq = Ignition.ignite().atomicSequence(seqName, 0, true);

            // First value of atomic sequence on this node.
            long firstVal = seq.get();

            System.out.println("Sequence initial value on local node: " + firstVal);

            for (int i = 0; i < RETRIES; i++)
                System.out.println("Sequence [currentValue=" + seq.get() + ", afterIncrement=" +
                    seq.incrementAndGet() + ']');

            System.out.println("Sequence after incrementing [expected=" + (firstVal + RETRIES) + ", actual=" +
                seq.get() + ']');
        }
    }
}
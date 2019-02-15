/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
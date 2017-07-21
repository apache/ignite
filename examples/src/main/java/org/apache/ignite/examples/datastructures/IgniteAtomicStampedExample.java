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

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.lang.IgniteRunnable;

/**
 * Demonstrates a simple usage of distributed atomic stamped.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public final class IgniteAtomicStampedExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Atomic stamped example started.");

            // Make name of atomic stamped.
            String stampedName = UUID.randomUUID().toString();

            // Make value of atomic stamped.
            String val = UUID.randomUUID().toString();

            // Make stamp of atomic stamped.
            String stamp = UUID.randomUUID().toString();

            // Initialize atomic stamped.
            IgniteAtomicStamped<String, String> stamped = ignite.atomicStamped(stampedName, val, stamp, true);

            System.out.println("Atomic stamped initial [value=" + stamped.value() + ", stamp=" + stamped.stamp() + ']');

            // Make closure for checking atomic stamped.
            IgniteRunnable c = new StampedUpdateClosure(stampedName);

            // Check atomic stamped on all cluster nodes.
            ignite.compute().broadcast(c);

            // Make new value of atomic stamped.
            String newVal = UUID.randomUUID().toString();

            // Make new stamp of atomic stamped.
            String newStamp = UUID.randomUUID().toString();

            System.out.println("Try to change value and stamp of atomic stamped with wrong expected value and stamp.");

            stamped.compareAndSet("WRONG EXPECTED VALUE", newVal, "WRONG EXPECTED STAMP", newStamp);

            // Check atomic stamped on all cluster nodes.
            // Atomic stamped value and stamp shouldn't be changed.
            ignite.compute().run(c);

            System.out.println("Try to change value and stamp of atomic stamped with correct value and stamp.");

            stamped.compareAndSet(val, newVal, stamp, newStamp);

            // Check atomic stamped on all cluster nodes.
            // Atomic stamped value and stamp should be changed.
            ignite.compute().run(c);
        }

        System.out.println();
        System.out.println("Finished atomic stamped example...");
        System.out.println("Check all nodes for output (this node is also part of the cluster).");
    }

    /**
     * Performs update of on an atomic stamped variable in cache.
     */
    private static class StampedUpdateClosure implements IgniteRunnable {
        /** Atomic stamped variable name. */
        private final String stampedName;

        /**
         * @param stampedName Atomic stamped variable name.
         */
        StampedUpdateClosure(String stampedName) {
            this.stampedName = stampedName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteAtomicStamped<String, String> stamped = Ignition.ignite().
                atomicStamped(stampedName, null, null, true);

            System.out.println("Atomic stamped [value=" + stamped.value() + ", stamp=" + stamped.stamp() + ']');
        }
    }
}
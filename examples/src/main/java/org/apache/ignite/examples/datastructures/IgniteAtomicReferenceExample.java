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
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.lang.IgniteRunnable;

/**
 * Demonstrates a simple usage of distributed atomic reference.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public final class IgniteAtomicReferenceExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Atomic reference example started.");

            // Make name of atomic reference.
            final String refName = UUID.randomUUID().toString();

            // Make value of atomic reference.
            String val = UUID.randomUUID().toString();

            // Initialize atomic reference.
            IgniteAtomicReference<String> ref = ignite.atomicReference(refName, val, true);

            System.out.println("Atomic reference initial value : " + ref.get() + '.');

            // Make closure for checking atomic reference value on cluster.
            IgniteRunnable c = new ReferenceClosure(refName);

            // Check atomic reference on all cluster nodes.
            ignite.compute().run(c);

            // Make new value of atomic reference.
            String newVal = UUID.randomUUID().toString();

            System.out.println("Try to change value of atomic reference with wrong expected value.");

            ref.compareAndSet("WRONG EXPECTED VALUE", newVal); // Won't change.

            // Check atomic reference on all cluster nodes.
            // Atomic reference value shouldn't be changed.
            ignite.compute().run(c);

            System.out.println("Try to change value of atomic reference with correct expected value.");

            ref.compareAndSet(val, newVal);

            // Check atomic reference on all cluster nodes.
            // Atomic reference value should be changed.
            ignite.compute().run(c);
        }

        System.out.println();
        System.out.println("Finished atomic reference example...");
        System.out.println("Check all nodes for output (this node is also part of the cluster).");
    }

    /**
     * Obtains atomic reference.
     */
    private static class ReferenceClosure implements IgniteRunnable {
        /** Reference name. */
        private final String refName;

        /**
         * @param refName Reference name.
         */
        ReferenceClosure(String refName) {
            this.refName = refName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteAtomicReference<String> ref = Ignition.ignite().atomicReference(refName, null, true);

            System.out.println("Atomic reference value is " + ref.get() + '.');
        }
    }
}
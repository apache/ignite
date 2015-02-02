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
 * Demonstrates a simple usage of distributed atomic reference.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-cache.xml} configuration.
 */
public final class CacheAtomicReferenceExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned_tx";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        try (Ignite ignite = Ignition.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache atomic reference example started.");

            // Make name of atomic reference.
            final String refName = UUID.randomUUID().toString();

            // Make value of atomic reference.
            String val = UUID.randomUUID().toString();

            // Initialize atomic reference in grid.
            CacheAtomicReference<String> ref = ignite.cache(CACHE_NAME).dataStructures().
                atomicReference(refName, val, true);

            System.out.println("Atomic reference initial value : " + ref.get() + '.');

            // Make closure for checking atomic reference value on grid.
            Runnable c = new ReferenceClosure(CACHE_NAME, refName);

            // Check atomic reference on all grid nodes.
            ignite.compute().run(c);

            // Make new value of atomic reference.
            String newVal = UUID.randomUUID().toString();

            System.out.println("Try to change value of atomic reference with wrong expected value.");

            ref.compareAndSet("WRONG EXPECTED VALUE", newVal); // Won't change.

            // Check atomic reference on all grid nodes.
            // Atomic reference value shouldn't be changed.
            ignite.compute().run(c);

            System.out.println("Try to change value of atomic reference with correct expected value.");

            ref.compareAndSet(val, newVal);

            // Check atomic reference on all grid nodes.
            // Atomic reference value should be changed.
            ignite.compute().run(c);
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
                CacheAtomicReference<String> ref = Ignition.ignite().cache(cacheName).dataStructures().
                    atomicReference(refName, null, true);

                System.out.println("Atomic reference value is " + ref.get() + '.');
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }
}

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
 * Demonstrates a simple usage of distributed atomic stamped.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-cache.xml} configuration.
 */
public final class CacheAtomicStampedExample {
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
            System.out.println(">>> Cache atomic stamped example started.");

            // Make name of atomic stamped.
            String stampedName = UUID.randomUUID().toString();

            // Make value of atomic stamped.
            String val = UUID.randomUUID().toString();

            // Make stamp of atomic stamped.
            String stamp = UUID.randomUUID().toString();

            // Initialize atomic stamped in cache.
            CacheAtomicStamped<String, String> stamped = ignite.cache(CACHE_NAME).dataStructures().
                atomicStamped(stampedName, val, stamp, true);

            System.out.println("Atomic stamped initial [value=" + stamped.value() + ", stamp=" + stamped.stamp() + ']');

            // Make closure for checking atomic stamped on ignite.
            IgniteRunnable c = new StampedUpdateClosure(CACHE_NAME, stampedName);

            // Check atomic stamped on all cluster nodes.
            ignite.compute().run(c);

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
        /** Cache name. */
        private final String cacheName;

        /** Atomic stamped variable name. */
        private final String stampedName;

        /**
         * @param cacheName Cache name.
         * @param stampedName Atomic stamped variable name.
         */
        StampedUpdateClosure(String cacheName, String stampedName) {
            this.cacheName = cacheName;
            this.stampedName = stampedName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                CacheAtomicStamped<String, String> stamped = Ignition.ignite().cache(cacheName).dataStructures().
                    atomicStamped(stampedName, null, null, true);

                System.out.println("Atomic stamped [value=" + stamped.value() + ", stamp=" + stamped.stamp() + ']');
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }
}

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
 * Grid cache distributed set example.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-cache.xml} configuration.
 */
public class CacheSetExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned_tx";

    /** Set instance. */
    private static CacheSet<String> set;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        try (Ignite ignite = Ignition.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache set example started.");

            // Make set name.
            String setName = UUID.randomUUID().toString();

            set = initializeSet(ignite, setName);

            writeToSet(ignite);

            clearAndRemoveSet(ignite);
        }

        System.out.println("Cache set example finished.");
    }

    /**
     * Initialize set.
     *
     * @param ignite Ignite.
     * @param setName Name of set.
     * @return Set.
     * @throws IgniteCheckedException If execution failed.
     */
    private static CacheSet<String> initializeSet(Ignite ignite, String setName) throws IgniteCheckedException {
        // Initialize new set.
        CacheSet<String> set = ignite.cache(CACHE_NAME).dataStructures().set(setName, false, true);

        // Initialize set items.
        for (int i = 0; i < 10; i++)
            set.add(Integer.toString(i));

        System.out.println("Set size after initializing: " + set.size());

        return set;
    }

    /**
     * Write items into set.
     *
     * @param ignite Ignite.
     * @throws IgniteCheckedException If failed.
     */
    private static void writeToSet(Ignite ignite) throws IgniteCheckedException {
        final String setName = set.name();

        // Write set items on each node.
        ignite.compute().broadcast(new SetClosure(CACHE_NAME, setName));

        System.out.println("Set size after writing [expected=" + (10 + ignite.cluster().nodes().size() * 5) +
            ", actual=" + set.size() + ']');

        System.out.println("Iterate over set.");

        // Iterate over set.
        for (String item : set)
            System.out.println("Set item: " + item);

        // Set API usage examples.
        if (!set.contains("0"))
            throw new RuntimeException("Set should contain '0' among its elements.");

        if (set.add("0"))
            throw new RuntimeException("Set should not allow duplicates.");

        if (!set.remove("0"))
            throw new RuntimeException("Set should correctly remove elements.");

        if (set.contains("0"))
            throw new RuntimeException("Set should not contain '0' among its elements.");

        if (!set.add("0"))
            throw new RuntimeException("Set should correctly add new elements.");
    }

    /**
     * Clear and remove set.
     *
     * @param ignite Ignite.
     * @throws IgniteCheckedException If execution failed.
     */
    private static void clearAndRemoveSet(Ignite ignite) throws IgniteCheckedException {
        System.out.println("Set size before clearing: " + set.size());

        // Clear set.
        set.clear();

        System.out.println("Set size after clearing: " + set.size());

        // Remove set from cache.
        ignite.cache(CACHE_NAME).dataStructures().removeSet(set.name());

        System.out.println("Set was removed: " + set.removed());

        // Try to work with removed set.
        try {
            set.contains("1");
        }
        catch (IgniteException expected) {
            System.out.println("Expected exception - " + expected.getMessage());
        }
    }

    /**
     * Closure to populate the set.
     */
    private static class SetClosure implements IgniteRunnable {
        /** Cache name. */
        private final String cacheName;

        /** Set name. */
        private final String setName;

        /**
         * @param cacheName Cache name.
         * @param setName Set name.
         */
        SetClosure(String cacheName, String setName) {
            this.cacheName = cacheName;
            this.setName = setName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                CacheSet<String> set = Ignition.ignite().cache(cacheName).dataStructures().set(setName, false, true);

                UUID locId = Ignition.ignite().cluster().localNode().id();

                for (int i = 0; i < 5; i++) {
                    String item = locId + "_" + Integer.toString(i);

                    set.add(item);

                    System.out.println("Set item has been added: " + item);
                }
            }
            catch (IgniteCheckedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

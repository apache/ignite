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
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.lang.IgniteRunnable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Ignite cache distributed set example.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class IgniteSetExample {
    /** Set instance. */
    private static IgniteSet<String> set;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Ignite set example started.");

            // Make set name.
            String setName = UUID.randomUUID().toString();

            set = initializeSet(ignite, setName);

            writeToSet(ignite);

            clearAndRemoveSet();
        }

        System.out.println("Ignite set example finished.");
    }

    /**
     * Initialize set.
     *
     * @param ignite Ignite.
     * @param setName Name of set.
     * @return Set.
     * @throws IgniteException If execution failed.
     */
    private static IgniteSet<String> initializeSet(Ignite ignite, String setName) throws IgniteException {
        CollectionConfiguration setCfg = new CollectionConfiguration();

        setCfg.setAtomicityMode(TRANSACTIONAL);
        setCfg.setCacheMode(PARTITIONED);

        // Initialize new set.
        IgniteSet<String> set = ignite.set(setName, setCfg);

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
     * @throws IgniteException If failed.
     */
    private static void writeToSet(Ignite ignite) throws IgniteException {
        final String setName = set.name();

        // Write set items on each node.
        ignite.compute().broadcast(new SetClosure(setName));

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
     * @throws IgniteException If execution failed.
     */
    private static void clearAndRemoveSet() throws IgniteException {
        System.out.println("Set size before clearing: " + set.size());

        // Clear set.
        set.clear();

        System.out.println("Set size after clearing: " + set.size());

        // Remove set.
        set.close();

        System.out.println("Set was removed: " + set.removed());

        // Try to work with removed set.
        try {
            set.contains("1");
        }
        catch (IllegalStateException expected) {
            System.out.println("Expected exception - " + expected.getMessage());
        }
    }

    /**
     * Closure to populate the set.
     */
    private static class SetClosure implements IgniteRunnable {
        /** Set name. */
        private final String setName;

        /**
         * @param setName Set name.
         */
        SetClosure(String setName) {
            this.setName = setName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteSet<String> set = Ignition.ignite().set(setName, null);

            UUID locId = Ignition.ignite().cluster().localNode().id();

            for (int i = 0; i < 5; i++) {
                String item = locId + "_" + Integer.toString(i);

                set.add(item);

                System.out.println("Set item has been added: " + item);
            }
        }
    }
}
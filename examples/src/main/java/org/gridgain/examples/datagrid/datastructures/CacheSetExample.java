/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.datastructures;

import org.gridgain.examples.datagrid.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.lang.*;

import java.util.*;

/**
 * Grid cache distributed set example.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start GridGain node with {@code examples/config/example-cache.xml} configuration.
 */
public class CacheSetExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned_tx";

    /** Set instance. */
    private static GridCacheSet<String> set;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Ignite g = GridGain.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache set example started.");

            // Make set name.
            String setName = UUID.randomUUID().toString();

            set = initializeSet(g, setName);

            writeToSet(g);

            clearAndRemoveSet(g);
        }

        System.out.println("Cache set example finished.");
    }

    /**
     * Initialize set.
     *
     * @param g Grid.
     * @param setName Name of set.
     * @return Set.
     * @throws GridException If execution failed.
     */
    private static GridCacheSet<String> initializeSet(Ignite g, String setName) throws GridException {
        // Initialize new set.
        GridCacheSet<String> set = g.cache(CACHE_NAME).dataStructures().set(setName, false, true);

        // Initialize set items.
        for (int i = 0; i < 10; i++)
            set.add(Integer.toString(i));

        System.out.println("Set size after initializing: " + set.size());

        return set;
    }

    /**
     * Write items into set.
     *
     * @param g Grid.
     * @throws GridException If failed.
     */
    private static void writeToSet(Ignite g) throws GridException {
        final String setName = set.name();

        // Write set items on each node.
        g.compute().broadcast(new SetClosure(CACHE_NAME, setName));

        System.out.println("Set size after writing [expected=" + (10 + g.cluster().nodes().size() * 5) +
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
     * @param g Grid.
     * @throws GridException If execution failed.
     */
    private static void clearAndRemoveSet(Ignite g) throws GridException {
        System.out.println("Set size before clearing: " + set.size());

        // Clear set.
        set.clear();

        System.out.println("Set size after clearing: " + set.size());

        // Remove set from cache.
        g.cache(CACHE_NAME).dataStructures().removeSet(set.name());

        System.out.println("Set was removed: " + set.removed());

        // Try to work with removed set.
        try {
            set.contains("1");
        }
        catch (GridRuntimeException expected) {
            System.out.println("Expected exception - " + expected.getMessage());
        }
    }

    /**
     * Closure to populate the set.
     */
    private static class SetClosure implements GridRunnable {
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
                GridCacheSet<String> set = GridGain.grid().cache(cacheName).dataStructures().set(setName, false, true);

                UUID locId = GridGain.grid().cluster().localNode().id();

                for (int i = 0; i < 5; i++) {
                    String item = locId + "_" + Integer.toString(i);

                    set.add(item);

                    System.out.println("Set item has been added: " + item);
                }
            }
            catch (GridException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

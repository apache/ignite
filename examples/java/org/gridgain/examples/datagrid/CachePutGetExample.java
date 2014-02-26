// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;

import java.util.*;

/**
 * This example demonstrates very basic operations on cache, such as 'put' and 'get'.
 * We first populate cache by putting values into it, and then we 'peek' at values on
 * remote nodes, and then we 'get' those values. For replicated cache, values should
 * be everywhere at all times. For partitioned cache, 'peek' on some nodes may return
 * {@code null} due to partitioning, however, 'get' operation should never return {@code null}.
 * <p>
 * When starting remote nodes, make sure to use the same configuration file as follows:
 * <pre>
 *     GRIDGAIN_HOME/bin/ggstart.sh examples/config/example-cache.xml
 * </pre>
 *
 * @author @java.author
 * @version @java.version
 */
public class CachePutGetExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";
    //private static final String CACHE_NAME = "replicated";
    //private static final String CACHE_NAME = "local";

    /**
     * Runs basic cache example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("examples/config/example-cache.xml")) {
            // Individual puts and gets.
            putGet();

            // Bulk puts and gets.
            putAllGetAll();
        }
    }

    /**
     * Execute individual puts and gets.
     *
     * @throws GridException If failed.
     */
    private static void putGet() throws GridException {
        System.out.println();
        System.out.println(">>> Starting put-get example.");

        Grid g = GridGain.grid();

        final GridCache<Integer, String> cache = g.cache(CACHE_NAME);

        final int keyCnt = 20;

        // Store keys in cache.
        for (int i = 0; i < keyCnt; i++)
            cache.putx(i, Integer.toString(i));

        System.out.println(">>> Stored values in cache.");

        // Projection (view) for remote nodes that have cache running.
        GridProjection rmts = g.forCache(CACHE_NAME).forRemotes();

        // If no other cache nodes are started.
        if (rmts.nodes().isEmpty()) {
            System.out.println(">>> Need to start remote nodes to complete example.");

            return;
        }

        // Get and print out values on all remote nodes.
        rmts.compute().broadcast(new GridCallable<Object>() {
            @Override public Object call() throws GridException {
                for (int i = 0; i < keyCnt; i++)
                    System.out.println("Got [key=" + i + ", val=" + cache.get(i) + ']');

                return null;
            }
        }).get();
    }

    /**
     * Execute bulk {@code putAll(...)} and {@code getAll(...)} operations.
     *
     * @throws GridException If failed.
     */
    private static void putAllGetAll() throws GridException {
        System.out.println();
        System.out.println(">>> Starting putAll-getAll example.");

        Grid g = GridGain.grid();

        final GridCache<Integer, String> cache = g.cache(CACHE_NAME);

        final int keyCnt = 20;

        // Create batch.
        Map<Integer, String> batch = new HashMap<>(keyCnt);

        for (int i = 0; i < keyCnt; i++)
            batch.put(i, "bulk-" + Integer.toString(i));

        // Bulk-store entries in cache.
        cache.putAll(batch);

        System.out.println(">>> Bulk-stored values in cache.");

        // Projection (view) for remote nodes that have cache running.
        GridProjection rmts = g.forCache(CACHE_NAME).forRemotes();

        // If no other cache nodes are started.
        if (rmts.nodes().isEmpty()) {
            System.out.println(">>> Need to start remote nodes to complete example.");

            return;
        }

        final Collection<Integer> keys = new ArrayList<>(batch.keySet());

        // Get values from all remote cache nodes.
        Collection<Map<Integer, String>> retMaps = rmts.compute().broadcast(
            new GridCallable<Map<Integer, String>>() {
                @Override public Map<Integer, String> call() throws GridException {
                    Map<Integer, String> vals = cache.getAll(keys);

                    for (Map.Entry<Integer, String> e : vals.entrySet())
                        System.out.println("Got entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');

                    return vals;
                }
            }).get();

        System.out.println(">>> Got all entries from all remote nodes.");

        // Since we get the same keys on all nodes, values should be equal to the initial batch.
        for (Map<Integer, String> map : retMaps)
            assert map.equals(batch);
    }
}

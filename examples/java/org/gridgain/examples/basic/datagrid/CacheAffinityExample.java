// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.basic.datagrid;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.lang.*;

import java.util.*;

/**
 * This example demonstrates the simplest code that populates the distributed cache
 * and co-locates simple closure execution with each key. The goal of this particular
 * example is to provide the simplest code example of this logic.
 * <p>
 * Note that other examples in this package provide more detailed examples
 * of affinity co-location.
 * <p>
 * Affinity routing is enabled for all caches.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache: {@code 'ggstart.sh examples/config/example-cache.xml'}. Local node should
 * be started with cache.
 *
 * @author @java.author
 * @version @java.version
 */
public final class CacheAffinityExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /** Number of keys. */
    private static final int KEY_CNT = 20;

    /**
     * Executes cache affinity example.
     * <p>
     * Note that in case of {@code LOCAL} configuration,
     * since there is no distribution, values may come back as {@code nulls}.
     *
     * @param args Command line arguments
     * @throws GridException If failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start("examples/config/example-cache.xml")) {
            GridCache<Integer, String> cache = g.cache(CACHE_NAME);

            if (cache == null) {
                System.err.println("Cache with name 'partitioned' not found (is configuration correct?)");

                return;
            }

            for (int i = 0; i < KEY_CNT; i++)
                cache.putx(i, Integer.toString(i));

            // Co-locates closures with data in in-memory data grid using GridCompute.affinityRun(...) method.
            visitUsingAffinityRun();
        }
    }

    /**
     * Collocates jobs with keys they need to work on using {@link GridCompute#affinityRun(String, Object, Runnable)}
     * method.
     *
     * @throws GridException If failed.
     */
    private static void visitUsingAffinityRun() throws GridException {
        Grid g = GridGain.grid();

        final GridCache<Integer, String> cache = g.cache(CACHE_NAME);

        for (int i = 0; i < KEY_CNT; i++) {
            final int key = i;

            // This runnable will execute on the remote node where
            // data with the 'key' is located. Since it will be co-located
            // we can use local 'peek' operation safely.
            g.compute().affinityRun(CACHE_NAME, key, new GridRunnable() {
                @Override public void run() {
                    // Peek is a local memory lookup, however, value should never be 'null'
                    // as we are co-located with node that has a given key.
                    System.out.println("Co-located using affinityRun [key= " + key + ", value=" + cache.peek(key) + ']');
                }
            }).get();
        }
    }

    /**
     * Collocates jobs with keys they need to work on using {@link Grid#mapKeysToNodes(String, Collection)}
     * method. The difference from {@code affinityRun(...)} method is that here we process multiple keys
     * in a single job.
     *
     * @throws GridException If failed.
     */
    private static void visitUsingMapKeysToNodes() throws GridException {
        final Grid g = GridGain.grid();

        Collection<Integer> keys = new ArrayList<>(KEY_CNT);

        for (int i = 0; i < KEY_CNT; i++)
            keys.add(i);

        // Map all keys to nodes.
        Map<GridNode, Collection<Integer>> mappings = g.mapKeysToNodes(CACHE_NAME, keys);

        for (Map.Entry<GridNode, Collection<Integer>> mapping : mappings.entrySet()) {
            GridNode node = mapping.getKey();

            final Collection<Integer> mappedKeys = mapping.getValue();

            if (node != null) {
                // Bring computations to the nodes where the data resides (i.e. collocation).
                g.forNode(node).compute().run(new GridRunnable() {
                    @Override public void run() {
                        GridCache<Integer, String> cache = g.cache(CACHE_NAME);

                        // Peek is a local memory lookup, however, value should never be 'null'
                        // as we are co-located with node that has a given key.
                        for (Integer key : mappedKeys)
                            System.out.println("Co-located using mapKeysToNodes [key= " + key +
                                ", value=" + cache.peek(key) + ']');
                    }
                }).get();
            }
        }
    }
}

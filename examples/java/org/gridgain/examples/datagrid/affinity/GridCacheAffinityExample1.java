// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.affinity;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.product.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.GridClosureCallMode.*;
import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * Example of how to collocate computations and data in GridGain using
 * {@link GridCacheAffinityMapped} annotation as opposed to direct API calls. This
 * example will first populate cache on some node where cache is available, and then
 * will send jobs to the nodes where keys reside and print out values for those
 * keys.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache: {@code 'ggstart.sh examples/config/example-cache.xml'}. Local node can
 * be started with or without cache.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class GridCacheAffinityExample1 {
    /**
     * Configuration file name.
     */
    //private static final String CONFIG = "examples/config/example-cache-none.xml"; // No cache - need a remote node with cache.
    private static final String CONFIG = "examples/config/example-cache.xml"; // Cache.

    /** Name of cache specified in spring configuration. */
    private static final String NAME = "partitioned";

    /**
     * Executes cache affinity example.
     * <p>
     * Note that in case of {@code LOCAL} configuration,
     * since there is no distribution, values may come back as {@code nulls}.
     *
     * @param args Command line arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start(args.length == 0 ? CONFIG : args[0])) {
            Collection<String> keys = new ArrayList<>('Z' - 'A' + 1);

            // Create collection of capital letters of English alphabet.
            for (char c = 'A'; c <= 'Z'; c++)
                keys.add(Character.toString(c));

            // Populate cache.
            populateCache(g, keys);

            // Result map (ordered by key for readability).
            Map<String, String> results = new TreeMap<>();

            // Bring computations to the nodes where the data resides (i.e. collocation).
            for (final String key : keys) {
                String res = g.compute().call(
                    BALANCE,
                    new GridCallable<String>() {
                        // This annotation allows to route job to the node
                        // where the key is cached.
                        @GridCacheAffinityMapped
                        public String affinityKey() {
                            return key;
                        }

                        // Specify name of cache to use for affinity.
                        @GridCacheName
                        public String cacheName() {
                            return NAME;
                        }

                        @Nullable @Override public String call() {
                            info("Executing affinity job for key: " + key);

                            // Get cache with name 'partitioned'.
                            GridCache<String, String> cache = g.cache(NAME);

                            // If cache is not defined at this point then it means that
                            // job was not routed by affinity.
                            if (cache == null) {
                                info("Cache not found [nodeId=" + g.localNode().id() + ", cacheName=" + NAME + ']');

                                return "Error";
                            }

                            // Check cache without loading the value.
                            return cache.peek(key);
                        }
                    }
                ).get();

                results.put(key, res);
            }

            // Print out results.
            for (Map.Entry<String, String> e : results.entrySet())
                info("Affinity job result for key '" + e.getKey() + "': " + e.getValue());
        }
    }

    /**
     * Populates cache with given keys. This method accounts for the case when
     * cache is not started on local node. In that case a job which populates
     * the cache will be sent to the node where cache is started.
     *
     * @param g Grid.
     * @param keys Keys to populate.
     * @throws GridException If failed.
     */
    private static void populateCache(final Grid g, Collection<String> keys) throws GridException {
        GridProjection prj = g.forCaches(NAME);

        // Give preference to local node.
        if (prj.nodes().contains(g.localNode()))
            prj = g.forLocal();

        // Populate cache on some node (possibly this node) which has cache with given name started.
        // Note that CIX1 is a short type alias for GridInClosureX class. If you
        // find it too cryptic, you can use GridInClosureX class directly.
        prj.compute().call(UNICAST, new GridClosure<Collection<String>, Object>() {
            @Override public Object apply(Collection<String> keys) {
                info("Storing keys in cache: " + keys);

                GridCache<String, String> c = g.cache(NAME);

                try {
                    for (String key : keys)
                        c.put(key, key.toLowerCase());
                }
                catch (GridException e) {
                    throw new RuntimeException(e);
                }

                return null;
            }
        }, keys).get();
    }

    /**
     * Prints out message to standard out with given parameters.
     *
     * @param msg Message to print.
     */
    private static void info(String msg) {
        System.out.println(">>> " + msg);
    }
}

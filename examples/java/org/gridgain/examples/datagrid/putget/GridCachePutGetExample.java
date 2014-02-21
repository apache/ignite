// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.putget;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.resources.*;

import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.product.GridProductEdition.*;

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
@GridOnlyAvailableIn(DATA_GRID)
public class GridCachePutGetExample {
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
        try (Grid g = args.length == 0 ? GridGain.start("examples/config/example-cache.xml") : GridGain.start(args[0])) {
            // Subscribe to events on every node, so we can visualize what's
            // happening in remote caches.
            g.events().consumeRemote(
                null,
                new GridPredicate<GridCacheEvent>() {
                    @Override public boolean apply(GridCacheEvent evt) {
                        System.out.println("Cache event [name=" + evt.name() + ", key=" + evt.key() + ']');

                        return true;
                    }
                },
                EVT_CACHE_OBJECT_PUT,
                EVT_CACHE_OBJECT_READ,
                EVT_CACHE_OBJECT_REMOVED).get();

            GridCacheProjection<String, String> cache = g.cache(CACHE_NAME).
                projection(String.class, String.class);

            final int keyCnt = 20;

            // Store keys in cache.
            for (int i = 0; i < keyCnt; i++)
                cache.putx(Integer.toString(i), Integer.toString(i));

            // Peek and get on local node.
            for (int i = 0; i < keyCnt; i++) {
                System.out.println("Peeked [key=" + i + ", val=" + cache.peek(Integer.toString(i)) + ']');
                System.out.println("Got [key=" + i + ", val=" + cache.get(Integer.toString(i)) + ']');
            }

            // Projection (view) for remote nodes.
            GridProjection rmts = g.forRemotes();

            if (!rmts.nodes().isEmpty()) {
                // Peek and get on remote nodes (comment it out if output gets too crowded).
                rmts.compute().run(new GridRunnable() {
                    @GridInstanceResource
                    private transient Grid g;

                    @Override public void run() {
                        GridCacheProjection<String, String> cache = g.cache(CACHE_NAME).
                            projection(String.class, String.class);

                        if (cache == null) {
                            System.out.println("Cache was not found [locNodeId=" + g.localNode().id() + ", cacheName=" +
                                CACHE_NAME + ']');

                            return;
                        }

                        try {
                            for (int i = 0; i < keyCnt; i++) {
                                System.out.println("Peeked [key=" + i + ", val=" + cache.peek(Integer.toString(i)) + ']');
                                System.out.println("Got [key=" + i + ", val=" + cache.get(Integer.toString(i)) + ']');
                            }
                        }
                        catch (GridException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }).get();
            }
        }
    }
}

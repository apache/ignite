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
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;

import java.util.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * This examples demonstrates continuous query API.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache: {@code 'ggstart.sh examples/config/example-cache.xml'}.
 * <h2 class="header">NOTE</h2>
 * Under some concurrent circumstances callback may get several notifications
 * for one cache update.
 *
 * @author @java.author
 * @version @java.version
 */
public class CacheContinuousEventsExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /**
     * Executes example on the grid.
     *
     * @param args Command line arguments. None required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void main(String[] args) throws GridException, InterruptedException {
        try (Grid g = GridGain.start("examples/config/example-cache.xml")) {
            GridCache<Integer, String> cache = g.cache(CACHE_NAME);

            // Subscribe to events on every node that has cache running.
            GridFuture<UUID> fut = GridGain.grid().forCache(CACHE_NAME).events().remoteListen(
                new GridBiPredicate<UUID, GridCacheEvent>() {
                    @Override public boolean apply(UUID uuid, GridCacheEvent evt) {
                        System.out.println("Received event [evt=" + evt.name() + ", key=" + evt.key() +
                            ", oldVal=" + evt.oldValue() + ", newVal=" + evt.newValue());

                        return true; // Continue listening.
                    }
                },
                // Only accept events for keys that are greater or equal than 10
                // and if local node is primary for this key.
                new GridPredicate<GridCacheEvent>() {
                    @Override public boolean apply(GridCacheEvent evt) {
                        System.out.println("Cache event [name=" + evt.name() + ", key=" + evt.key() + ']');

                        int key = evt.key();

                        return key >= 10 && g.cache(CACHE_NAME).affinity().isPrimary(g.localNode(), key);
                    }
                },
                EVT_CACHE_OBJECT_PUT,
                EVT_CACHE_OBJECT_READ,
                EVT_CACHE_OBJECT_REMOVED);

            // Wait until event listeners are subscribed on all nodes.
            fut.get();

            int keyCnt = 20;

            // Generate events.
            for (int i = 0; i < keyCnt; i++)
                cache.putx(i, Integer.toString(i));

            // Wait for a while while callback is notified about remaining puts.
            Thread.sleep(2000);
        }
    }
}

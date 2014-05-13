/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.events;

import org.gridgain.examples.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;

import java.util.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * Demonstrates event consume API that allows to register event listeners on remote nodes.
 * <p>
 * Remote nodes should always be started with configuration: {@code 'ggstart.sh examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start
 * GridGain node with {@code examples/config/example-compute.xml} configuration.
 */
public class EventsApiExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache events example started.");

            final GridCache<Integer, String> cache = g.cache(CACHE_NAME);

            // This optional local callback is called for each event notification
            // that passed remote predicate listener.
            GridBiPredicate<UUID, GridCacheEvent> locLsnr = (uuid, evt) -> {
                System.out.println("Received event [evt=" + evt.name() + ", key=" + evt.key() +
                    ", oldVal=" + evt.oldValue() + ", newVal=" + evt.newValue());

                return true; // Continue listening.
            };

            // Remote listener which only accepts events for keys that are
            // greater or equal than 10 and if event node is primary for this key.
            GridPredicate<GridCacheEvent> rmtLsnr = evt -> {
                System.out.println("Cache event [name=" + evt.name() + ", key=" + evt.key() + ']');

                int key = evt.key();

                return key >= 10 && cache.affinity().isPrimary(g.localNode(), key);
            };

            // Subscribe to specified cache events on all nodes that have cache running.
            GridFuture<UUID> fut = g.forCache(CACHE_NAME).events().remoteListen(locLsnr, rmtLsnr,
                EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_REMOVED);

            // Wait until event listeners are subscribed on all nodes.
            fut.get();

            // Generate cache events.
            for (int i = 0; i < 20; i++)
                cache.putx(i, Integer.toString(i));

            // Wait for a while while callback is notified about remaining puts.
            Thread.sleep(2000);
        }
    }
}

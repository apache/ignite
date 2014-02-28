// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.events;

import org.gridgain.examples.datagrid.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;

import java.util.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * Demonstrates event consume API that allows to register event listeners on remote nodes.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache: {@code 'ggstart.sh examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will start
 * GridGain node with {@code examples/config/example-cache.xml} configuration.
 *
 * @author @java.author
 * @version @java.version
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
        try (Grid grid = GridGain.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Events API example started.");

            // Listen to events happening on local node.
            localListen();

            // Listen to events happening on all grid nodes.
            remoteListen();

            // Wait for a while while callback is notified about remaining puts.
            Thread.sleep(1000);
        }
    }

    /**
     * Listen to events that happen only on local node.
     *
     * @throws GridException If failed.
     */
    private static void localListen() throws GridException {
        Grid g = GridGain.grid();

        // Register event listener for all local task execution events.
        g.events().localListen(new GridPredicate<GridEvent>() {
            @Override public boolean apply(GridEvent evt) {
                GridTaskEvent taskEvt = (GridTaskEvent) evt;

                System.out.println();
                System.out.println("Git event notification [evt=" + evt.name() + ", taskName=" + taskEvt.taskName() + ']');

                return true;
            }
        }, EVTS_TASK_EXECUTION);

        // Generate task events.
        g.compute().withName("example-event-task").run(new GridRunnable() {
            @Override public void run() {
                System.out.println();
                System.out.println("Executing sample job.");
            }
        }).get();
    }

    /**
     * Listen to events coming from all grid nodes.
     *
     * @throws GridException If failed.
     */
    private static void remoteListen() throws GridException {
        Grid g = GridGain.grid();

        GridCache<Integer, String> cache = g.cache(CACHE_NAME);

        // Register remote event listeners on all nodes running cache.
        GridFuture<?> fut = g.forCache(CACHE_NAME).events().remoteListen(
            // This optional local callback is called for each event notification
            // that passed remote predicate filter.
            new GridBiPredicate<UUID, GridCacheEvent>() {
                @Override public boolean apply(UUID nodeId, GridCacheEvent evt) {
                    System.out.println();
                    System.out.println("Received event [evt=" + evt.name() + ", key=" + evt.key() +
                        ", oldVal=" + evt.oldValue() + ", newVal=" + evt.newValue());

                    return true; // Return true to continue listening.
                }
            },
            // Remote filter which only accepts events for keys that are
            // greater or equal than 10 and if local node is primary for this key.
            new GridPredicate<GridCacheEvent>() {
                /** Auto-inject grid instance. */
                @GridInstanceResource
                private Grid g;

                @Override public boolean apply(GridCacheEvent evt) {
                    Integer key = evt.key();

                    return key >= 10 && g.cache(CACHE_NAME).affinity().isPrimary(g.localNode(), key);
                }
            },
            // Types of events for which listeners are registered.
            EVT_CACHE_OBJECT_PUT,
            EVT_CACHE_OBJECT_READ,
            EVT_CACHE_OBJECT_REMOVED);

        // Wait until event listeners are subscribed on all nodes.
        fut.get();

        int keyCnt = 20;

        // Generate cache events.
        for (int i = 0; i < keyCnt; i++)
            cache.putx(i, Integer.toString(i));
    }
}

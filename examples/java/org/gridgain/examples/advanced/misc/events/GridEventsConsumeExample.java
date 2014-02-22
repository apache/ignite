// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.misc.events;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;

import java.util.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * This examples demonstrates event consume API that allows to register
 * event listeners on remote nodes.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache: {@code 'ggstart.sh examples/config/example-cache.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridEventsConsumeExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /**
     * Executes example on the grid.
     *
     * @param args Command line arguments. None required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid grid = GridGain.start(args.length == 0 ? "examples/config/example-cache.xml" : args[0])) {
            UUID consumeId = null;

            try {
                // Register remote event listeners and get continuous ID
                // (it will be used later to unregister listeners).
                consumeId = grid.events().consumeRemote(
                    // This callback is called locally on each event (event
                    // itself can occur locally or on one of remote nodes).
                    new GridBiPredicate<UUID, GridCacheEvent>() {
                        @Override public boolean apply(UUID uuid, GridCacheEvent evt) {
                            System.out.println(">>>");

                            // The callback simply prints out event information.
                            switch (evt.type()) {
                                case EVT_CACHE_OBJECT_PUT:
                                    System.out.println(">>> Cache object was put on node " + evt.nodeId() +
                                        ". Key: " + evt.key());

                                    break;

                                case EVT_CACHE_OBJECT_REMOVED:
                                    System.out.println(">>> Cache object was removed on node " + evt.nodeId() +
                                        ". Key: " + evt.key());

                                    break;

                                default:
                                    // This will never happen as we register listeners
                                    // only for two types of events.
                                    assert false : "Unexpected event received: " + evt;
                            }

                            System.out.println(">>>");

                            // Always return true (we will unregister listeners manually).
                            return true;
                        }
                    },
                    // This filter retains events only for keys that are greater or equal than 10
                    // and if local node is primary for this key.
                    new GridPredicate<GridCacheEvent>() {
                        @GridInstanceResource
                        private Grid g;

                        @Override public boolean apply(GridCacheEvent evt) {
                            Integer key = evt.key();

                            return key >= 10 && g.cache(CACHE_NAME).affinity().
                                primary(g.localNode(), key); // FIXME cast to rich node
                        }
                    },
                    // Types of events for which listeners are registered.
                    EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED
                ).get();

                GridCache<Integer, String> cache = grid.cache(CACHE_NAME);

                // Put some object with key less than 10 (will be filtered out).
                cache.putx(2, "value2");
                cache.putx(5, "value5");
                cache.putx(8, "value8");

                // Put some object with key greater or equal than 10.
                cache.putx(10, "value10");
                cache.putx(25, "value25");
                cache.putx(33, "value33");

                // Remove some objects with key less than 10 (will be filtered out).
                cache.removex(2);
                cache.removex(5);

                // Remove some objects with key greater or equal than 10.
                cache.removex(10);
                cache.removex(33);
            }
            finally {
                // Unregister all listeners.
                grid.events().stopConsume(consumeId).get();
            }
        }
    }
}

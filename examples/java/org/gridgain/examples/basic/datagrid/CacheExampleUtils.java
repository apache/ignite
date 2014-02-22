// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.basic.datagrid;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class CacheExampleUtils {
    /**
     * Prints cache events by subscribing global listener on all nodes that have cache running.
     *
     * @throws GridException If failed.
     */
    public static void printGlobalCacheEvents(String cacheName) throws GridException {
        // Subscribe to events on every node with cache,
        // so we can visualize what's happening in remote caches.
        GridGain.grid().forCache(cacheName).events().consumeRemote(
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
    }
}

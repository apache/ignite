/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.local;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Tests local cache with daemon node.
 */
public class GridCacheDaemonNodeLocalSelfTest extends GridCacheDaemonNodeAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return LOCAL;
    }

    /** {@inheritDoc} */
    @Override public void testMapKeyToNode() throws Exception {
        try {
            // Start normal nodes.
            Grid g1 = startGridsMultiThreaded(3);

            // Start daemon node.
            daemon = true;

            Grid g2 = startGrid(4);

            for (long i = 0; i < Integer.MAX_VALUE; i = (i << 1) + 1) {
                // Call mapKeyToNode for normal node.
                g1.cluster().mapKeyToNode(null, i);

                try {
                    g2.cluster().mapKeyToNode(null, i);

                    assert false;
                }
                catch (GridException e) {
                    info("Caught expected exception: " + e);
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }
}

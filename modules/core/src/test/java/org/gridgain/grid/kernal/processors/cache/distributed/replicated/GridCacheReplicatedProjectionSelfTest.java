/* @java.file.header */

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache.distributed.replicated;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheFlag.*;

/**
 * Projection tests for replicated cache.
 */
public class GridCacheReplicatedProjectionSelfTest extends GridCacheAbstractProjectionSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return REPLICATED;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testInvalidateFlag() throws Exception {
        try {
            for (int i = 1; i < 3; i++)
                startGrid(i);

            String key = "1";
            Integer val = Integer.valueOf(key);

            // Put value into cache.
            cache(0).put(key, val);

            for (int i = 0; i < 3; i++)
                assertEquals(val, grid(i).cache(null).peek(key));

            // Put value again, it should be invalidated on remote nodes.
            cache(0).flagsOn(INVALIDATE).put(key, val);

            for (int i = 0; i < 3; i++) {
                Object peeked = grid(i).cache(null).peek(key);

                if (i == 0)
                    assertEquals(val, peeked);
                else
                    assertNull(peeked);
            }
        }
        finally {
            for (int i = 1; i < 3; i++)
                stopGrid(i);
        }
    }
}

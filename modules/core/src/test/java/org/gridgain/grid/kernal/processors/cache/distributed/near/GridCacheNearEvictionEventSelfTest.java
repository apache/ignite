/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;

/**
 * Test local cache eviction event.
 */
public class GridCacheNearEvictionEventSelfTest extends GridCacheEvictionEventAbstractTest {
    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return GridCacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected boolean isNearEvictSynchronized() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void testEvictionEvent() throws Exception {
        super.testEvictionEvent();
    }
}

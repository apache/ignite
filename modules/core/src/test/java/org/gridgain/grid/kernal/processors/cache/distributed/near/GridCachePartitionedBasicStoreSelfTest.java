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

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Test store with partitioned cache.
 */
public class GridCachePartitionedBasicStoreSelfTest extends GridCacheBasicStoreAbstractTest {
    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }
}

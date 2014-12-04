/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.eviction;

import org.apache.ignite.*;
import org.gridgain.grid.cache.*;

/**
 * Test allow empty entries flag on partitioned cache.
 */
public class GridCacheEmptyEntriesPartitionedSelfTest extends GridCacheEmptyEntriesAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected Ignite startGrids() throws Exception {
        return startGridsMultiThreaded(3);
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return GridCacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override public void testFifo() throws Exception {
        super.testFifo();
    }
}

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

/**
 * Tests colocated cache with values being stored off-heap.
 */
public class GridCachePartitionedNearDisabledOffHeapFullApiSelfTest extends
    GridCachePartitionedNearDisabledFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean offHeapValues() {
        return true;
    }
}

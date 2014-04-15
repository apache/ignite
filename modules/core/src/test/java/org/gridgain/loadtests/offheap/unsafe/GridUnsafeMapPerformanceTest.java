/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.offheap.unsafe;

import org.gridgain.grid.util.offheap.*;

/**
 * Unsafe map test.
 */
public class GridUnsafeMapPerformanceTest extends GridOffHeapMapPerformanceAbstractTest {
    /** {@inheritDoc} */
    @Override protected <K> GridOffHeapMap<K> newMap() {
        return GridOffHeapMapFactory.unsafeMap(concurrency, load, initCap, mem, lruStripes, evictClo);
    }
}

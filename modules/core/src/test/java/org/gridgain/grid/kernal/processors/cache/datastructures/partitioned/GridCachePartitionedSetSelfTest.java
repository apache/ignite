package org.gridgain.grid.kernal.processors.cache.datastructures.partitioned;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.datastructures.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Set tests.
 */
public class GridCachePartitionedSetSelfTest extends GridCacheSetAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }
}

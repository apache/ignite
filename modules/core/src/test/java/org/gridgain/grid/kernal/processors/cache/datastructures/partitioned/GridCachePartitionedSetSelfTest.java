package org.gridgain.grid.kernal.processors.cache.datastructures.partitioned;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.datastructures.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 * Set tests.
 */
public class GridCachePartitionedSetSelfTest extends GridCacheSetAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }
}

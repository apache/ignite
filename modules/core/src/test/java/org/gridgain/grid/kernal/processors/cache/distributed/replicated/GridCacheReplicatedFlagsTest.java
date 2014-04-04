package org.gridgain.grid.kernal.processors.cache.distributed.replicated;

import org.gridgain.grid.cache.GridCacheMode;
import org.gridgain.grid.kernal.processors.cache.GridCacheAbstractFlagsTest;

public class GridCacheReplicatedFlagsTest extends GridCacheAbstractFlagsTest {
    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return GridCacheMode.REPLICATED;
    }
}

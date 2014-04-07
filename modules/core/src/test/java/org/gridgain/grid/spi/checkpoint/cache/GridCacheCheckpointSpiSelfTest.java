package org.gridgain.grid.spi.checkpoint.cache;

import org.gridgain.grid.spi.checkpoint.GridCheckpointSpiAbstractTest;
import org.gridgain.testframework.junits.spi.GridSpiTest;

/**
 * Grid cache checkpoint SPI self test.
 */
@GridSpiTest(spi = GridCacheCheckpointSpi.class, group = "Checkpoint SPI")
public class GridCacheCheckpointSpiSelfTest extends GridCheckpointSpiAbstractTest<GridCacheCheckpointSpi> {
    // No-op.
}

package org.gridgain.grid.spi.checkpoint.cache;

import org.gridgain.grid.spi.*;
import org.gridgain.testframework.junits.spi.*;

/**
 * Grid cache checkpoint SPI start stop self test.
 */
@GridSpiTest(spi = GridCacheCheckpointSpi.class, group = "Checkpoint SPI")
public class GridCacheCheckpointSpiStartStopSelfTest extends GridSpiStartStopAbstractTest<GridCacheCheckpointSpi> {
    /** {@inheritDoc} */
    @Override protected void spiConfigure(GridCacheCheckpointSpi spi) throws Exception {
        spi.setCacheName("test-checkpoints");

        super.spiConfigure(spi);
    }
}

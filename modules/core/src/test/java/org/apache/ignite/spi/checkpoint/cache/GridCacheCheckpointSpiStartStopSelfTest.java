package org.apache.ignite.spi.checkpoint.cache;

import org.gridgain.grid.spi.*;
import org.gridgain.testframework.junits.spi.*;

/**
 * Grid cache checkpoint SPI start stop self test.
 */
@GridSpiTest(spi = CacheCheckpointSpi.class, group = "Checkpoint SPI")
public class GridCacheCheckpointSpiStartStopSelfTest extends GridSpiStartStopAbstractTest<CacheCheckpointSpi> {
    /** {@inheritDoc} */
    @Override protected void spiConfigure(CacheCheckpointSpi spi) throws Exception {
        spi.setCacheName("test-checkpoints");

        super.spiConfigure(spi);
    }
}

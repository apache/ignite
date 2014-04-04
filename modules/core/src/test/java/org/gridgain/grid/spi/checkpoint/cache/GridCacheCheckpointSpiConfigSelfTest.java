package org.gridgain.grid.spi.checkpoint.cache;

import org.gridgain.testframework.junits.spi.*;

/**
 * Grid cache checkpoint SPI config self test.
 */
@GridSpiTest(spi = GridCacheCheckpointSpi.class, group = "Checkpoint SPI")
public class GridCacheCheckpointSpiConfigSelfTest extends GridSpiAbstractConfigTest<GridCacheCheckpointSpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new GridCacheCheckpointSpi(), "cacheName", null);
        checkNegativeSpiProperty(new GridCacheCheckpointSpi(), "cacheName", "");
    }
}

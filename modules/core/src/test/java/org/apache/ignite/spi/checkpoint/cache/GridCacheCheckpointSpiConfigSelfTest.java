package org.apache.ignite.spi.checkpoint.cache;

import org.gridgain.testframework.junits.spi.*;

/**
 * Grid cache checkpoint SPI config self test.
 */
@GridSpiTest(spi = CacheCheckpointSpi.class, group = "Checkpoint SPI")
public class GridCacheCheckpointSpiConfigSelfTest extends GridSpiAbstractConfigTest<CacheCheckpointSpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new CacheCheckpointSpi(), "cacheName", null);
        checkNegativeSpiProperty(new CacheCheckpointSpi(), "cacheName", "");
    }
}

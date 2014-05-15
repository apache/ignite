package org.gridgain.grid.spi.checkpoint.s3;

import org.gridgain.testframework.junits.spi.*;

/**
 * Grid S3 checkpoint SPI config self test.
 */
@GridSpiTest(spi = GridS3CheckpointSpi.class, group = "Checkpoint SPI")
public class GridS3CheckpointSpiConfigSelfTest extends GridSpiAbstractConfigTest<GridS3CheckpointSpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new GridS3CheckpointSpi(), "awsCredentials", null);
    }
}

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.collision.jobstealing;

import org.gridgain.testframework.junits.spi.*;

/**
 * Job stealing collision SPI config test.
 */
@GridSpiTest(spi = GridJobStealingCollisionSpi.class, group = "Collision SPI")
public class GridJobStealingCollisionSpiConfigSelfTest extends GridSpiAbstractConfigTest<GridJobStealingCollisionSpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new GridJobStealingCollisionSpi(), "messageExpireTime", 0);
        checkNegativeSpiProperty(new GridJobStealingCollisionSpi(), "waitJobsThreshold", -1);
        checkNegativeSpiProperty(new GridJobStealingCollisionSpi(), "activeJobsThreshold", -1);
        checkNegativeSpiProperty(new GridJobStealingCollisionSpi(), "maximumStealingAttempts", 0);
    }
}

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.collision.fifoqueue;

import org.gridgain.testframework.junits.spi.*;

/**
 * Unit tests for {@link GridFifoQueueCollisionSpi} config.
 */
@GridSpiTest(spi = GridFifoQueueCollisionSpi.class, group = "Collision SPI")
public class GridFifoQueueCollisionSpiConfigSelfTest extends GridSpiAbstractConfigTest<GridFifoQueueCollisionSpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new GridFifoQueueCollisionSpi(), "parallelJobsNumber", 0);
        checkNegativeSpiProperty(new GridFifoQueueCollisionSpi(), "waitingJobsNumber", -1);
    }
}

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.collision.priorityqueue;

import org.gridgain.testframework.junits.spi.*;

/**
 * Priority queue collision SPI config test.
 */
@GridSpiTest(spi = GridPriorityQueueCollisionSpi.class, group = "Collision SPI")
public class GridPriorityQueueCollisionSpiConfigSelfTest
    extends GridSpiAbstractConfigTest<GridPriorityQueueCollisionSpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new GridPriorityQueueCollisionSpi(), "parallelJobsNumber", 0);
        checkNegativeSpiProperty(new GridPriorityQueueCollisionSpi(), "waitingJobsNumber", -1);
        checkNegativeSpiProperty(new GridPriorityQueueCollisionSpi(), "starvationIncrement", -1);
        checkNegativeSpiProperty(new GridPriorityQueueCollisionSpi(), "priorityAttributeKey", null);
        checkNegativeSpiProperty(new GridPriorityQueueCollisionSpi(), "jobPriorityAttributeKey", null);
    }
}

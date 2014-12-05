/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.collision.priorityqueue;

import org.gridgain.testframework.junits.spi.*;

/**
 * Priority queue collision SPI config test.
 */
@GridSpiTest(spi = PriorityQueueCollisionSpi.class, group = "Collision SPI")
public class GridPriorityQueueCollisionSpiConfigSelfTest
    extends GridSpiAbstractConfigTest<PriorityQueueCollisionSpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new PriorityQueueCollisionSpi(), "parallelJobsNumber", 0);
        checkNegativeSpiProperty(new PriorityQueueCollisionSpi(), "waitingJobsNumber", -1);
        checkNegativeSpiProperty(new PriorityQueueCollisionSpi(), "starvationIncrement", -1);
        checkNegativeSpiProperty(new PriorityQueueCollisionSpi(), "priorityAttributeKey", null);
        checkNegativeSpiProperty(new PriorityQueueCollisionSpi(), "jobPriorityAttributeKey", null);
    }
}

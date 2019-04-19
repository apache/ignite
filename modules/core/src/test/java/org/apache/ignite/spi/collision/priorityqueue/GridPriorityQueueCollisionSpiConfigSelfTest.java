/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.collision.priorityqueue;

import org.apache.ignite.testframework.junits.spi.GridSpiAbstractConfigTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.junit.Test;

/**
 * Priority queue collision SPI config test.
 */
@GridSpiTest(spi = PriorityQueueCollisionSpi.class, group = "Collision SPI")
public class GridPriorityQueueCollisionSpiConfigSelfTest
    extends GridSpiAbstractConfigTest<PriorityQueueCollisionSpi> {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new PriorityQueueCollisionSpi(), "parallelJobsNumber", 0);
        checkNegativeSpiProperty(new PriorityQueueCollisionSpi(), "waitingJobsNumber", -1);
        checkNegativeSpiProperty(new PriorityQueueCollisionSpi(), "starvationIncrement", -1);
        checkNegativeSpiProperty(new PriorityQueueCollisionSpi(), "priorityAttributeKey", null);
        checkNegativeSpiProperty(new PriorityQueueCollisionSpi(), "jobPriorityAttributeKey", null);
    }
}

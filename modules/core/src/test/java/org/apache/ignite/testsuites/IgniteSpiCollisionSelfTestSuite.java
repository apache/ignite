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

package org.apache.ignite.testsuites;

import org.apache.ignite.spi.collision.fifoqueue.GridFifoQueueCollisionSpiConfigSelfTest;
import org.apache.ignite.spi.collision.fifoqueue.GridFifoQueueCollisionSpiSelfTest;
import org.apache.ignite.spi.collision.fifoqueue.GridFifoQueueCollisionSpiStartStopSelfTest;
import org.apache.ignite.spi.collision.jobstealing.GridJobStealingCollisionSpiAttributesSelfTest;
import org.apache.ignite.spi.collision.jobstealing.GridJobStealingCollisionSpiConfigSelfTest;
import org.apache.ignite.spi.collision.jobstealing.GridJobStealingCollisionSpiCustomTopologySelfTest;
import org.apache.ignite.spi.collision.jobstealing.GridJobStealingCollisionSpiSelfTest;
import org.apache.ignite.spi.collision.jobstealing.GridJobStealingCollisionSpiStartStopSelfTest;
import org.apache.ignite.spi.collision.priorityqueue.GridPriorityQueueCollisionSpiConfigSelfTest;
import org.apache.ignite.spi.collision.priorityqueue.GridPriorityQueueCollisionSpiSelfTest;
import org.apache.ignite.spi.collision.priorityqueue.GridPriorityQueueCollisionSpiStartStopSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Collision SPI self-test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridPriorityQueueCollisionSpiSelfTest.class,
    GridPriorityQueueCollisionSpiStartStopSelfTest.class,
    GridPriorityQueueCollisionSpiConfigSelfTest.class,

    // FIFO.
    GridFifoQueueCollisionSpiSelfTest.class,
    GridFifoQueueCollisionSpiStartStopSelfTest.class,
    GridFifoQueueCollisionSpiConfigSelfTest.class,

    // Job Stealing.
    GridJobStealingCollisionSpiSelfTest.class,
    GridJobStealingCollisionSpiAttributesSelfTest.class,
    GridJobStealingCollisionSpiCustomTopologySelfTest.class,
    GridJobStealingCollisionSpiStartStopSelfTest.class,
    GridJobStealingCollisionSpiConfigSelfTest.class
})
public class IgniteSpiCollisionSelfTestSuite {
}

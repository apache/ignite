/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testsuites;

import junit.framework.*;
import org.apache.ignite.spi.collision.fifoqueue.*;
import org.apache.ignite.spi.collision.jobstealing.*;
import org.apache.ignite.spi.collision.priorityqueue.*;

/**
 * Collision SPI self-test suite.
 */
public class IgniteSpiCollisionSelfTestSuite extends TestSuite {
    /**
     * @return Failover SPI tests suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Collision SPI Test Suite");

        // Priority.
        suite.addTestSuite(GridPriorityQueueCollisionSpiSelfTest.class);
        suite.addTestSuite(GridPriorityQueueCollisionSpiStartStopSelfTest.class);
        suite.addTestSuite(GridPriorityQueueCollisionSpiConfigSelfTest.class);

        // FIFO.
        suite.addTestSuite(GridFifoQueueCollisionSpiSelfTest.class);
        suite.addTestSuite(GridFifoQueueCollisionSpiStartStopSelfTest.class);
        suite.addTestSuite(GridFifoQueueCollisionSpiConfigSelfTest.class);

        // Job Stealing.
        suite.addTestSuite(GridJobStealingCollisionSpiSelfTest.class);
        suite.addTestSuite(GridJobStealingCollisionSpiAttributesSelfTest.class);
        suite.addTestSuite(GridJobStealingCollisionSpiCustomTopologySelfTest.class);
        suite.addTestSuite(GridJobStealingCollisionSpiStartStopSelfTest.class);
        suite.addTestSuite(GridJobStealingCollisionSpiConfigSelfTest.class);

        return suite;
    }
}

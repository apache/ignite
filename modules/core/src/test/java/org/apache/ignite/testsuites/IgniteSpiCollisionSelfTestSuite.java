/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.testsuites;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
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
import org.junit.runners.AllTests;

/**
 * Collision SPI self-test suite.
 */
@RunWith(AllTests.class)
public class IgniteSpiCollisionSelfTestSuite {
    /**
     * @return Failover SPI tests suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Collision SPI Test Suite");

        // Priority.
        suite.addTest(new JUnit4TestAdapter(GridPriorityQueueCollisionSpiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridPriorityQueueCollisionSpiStartStopSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridPriorityQueueCollisionSpiConfigSelfTest.class));

        // FIFO.
        suite.addTest(new JUnit4TestAdapter(GridFifoQueueCollisionSpiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridFifoQueueCollisionSpiStartStopSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridFifoQueueCollisionSpiConfigSelfTest.class));

        // Job Stealing.
        suite.addTest(new JUnit4TestAdapter(GridJobStealingCollisionSpiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridJobStealingCollisionSpiAttributesSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridJobStealingCollisionSpiCustomTopologySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridJobStealingCollisionSpiStartStopSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridJobStealingCollisionSpiConfigSelfTest.class));

        return suite;
    }
}

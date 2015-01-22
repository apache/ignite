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
import org.apache.ignite.spi.failover.always.*;
import org.apache.ignite.spi.failover.jobstealing.*;
import org.apache.ignite.spi.failover.never.*;

/**
 * Failover SPI self-test suite.
 */
public class GridSpiFailoverSelfTestSuite extends TestSuite {
    /**
     * @return Failover SPI tests suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Failover SPI Test Suite");

        // Always failover.
        suite.addTest(new TestSuite(GridAlwaysFailoverSpiSelfTest.class));
        suite.addTest(new TestSuite(GridAlwaysFailoverSpiStartStopSelfTest.class));
        suite.addTest(new TestSuite(GridAlwaysFailoverSpiConfigSelfTest.class));

        // Never failover.
        suite.addTest(new TestSuite(GridNeverFailoverSpiSelfTest.class));
        suite.addTest(new TestSuite(GridNeverFailoverSpiStartStopSelfTest.class));

        // Job stealing failover.
        suite.addTest(new TestSuite(GridJobStealingFailoverSpiSelfTest.class));
        suite.addTest(new TestSuite(GridJobStealingFailoverSpiOneNodeSelfTest.class));
        suite.addTest(new TestSuite(GridJobStealingFailoverSpiStartStopSelfTest.class));
        suite.addTest(new TestSuite(GridJobStealingFailoverSpiConfigSelfTest.class));

        return suite;
    }
}

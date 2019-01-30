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

import org.apache.ignite.spi.failover.always.GridAlwaysFailoverSpiConfigSelfTest;
import org.apache.ignite.spi.failover.always.GridAlwaysFailoverSpiSelfTest;
import org.apache.ignite.spi.failover.always.GridAlwaysFailoverSpiStartStopSelfTest;
import org.apache.ignite.spi.failover.jobstealing.GridJobStealingFailoverSpiConfigSelfTest;
import org.apache.ignite.spi.failover.jobstealing.GridJobStealingFailoverSpiOneNodeSelfTest;
import org.apache.ignite.spi.failover.jobstealing.GridJobStealingFailoverSpiSelfTest;
import org.apache.ignite.spi.failover.jobstealing.GridJobStealingFailoverSpiStartStopSelfTest;
import org.apache.ignite.spi.failover.never.GridNeverFailoverSpiSelfTest;
import org.apache.ignite.spi.failover.never.GridNeverFailoverSpiStartStopSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Failover SPI self-test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridAlwaysFailoverSpiSelfTest.class,
    GridAlwaysFailoverSpiStartStopSelfTest.class,
    GridAlwaysFailoverSpiConfigSelfTest.class,

    // Never failover.
    GridNeverFailoverSpiSelfTest.class,
    GridNeverFailoverSpiStartStopSelfTest.class,

    // Job stealing failover.
    GridJobStealingFailoverSpiSelfTest.class,
    GridJobStealingFailoverSpiOneNodeSelfTest.class,
    GridJobStealingFailoverSpiStartStopSelfTest.class,
    GridJobStealingFailoverSpiConfigSelfTest.class
})
public class IgniteSpiFailoverSelfTestSuite {
}

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
import org.apache.ignite.spi.checkpoint.cache.*;
import org.apache.ignite.spi.checkpoint.jdbc.*;
import org.apache.ignite.spi.checkpoint.sharedfs.*;

/**
 * Grid SPI checkpoint self test suite.
 */
public class IgniteSpiCheckpointSelfTestSuite extends TestSuite {
    /**
     * @return Checkpoint test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Checkpoint Test Suite");

        // Cache.
        suite.addTest(new TestSuite(GridCacheCheckpointSpiConfigSelfTest.class));
        suite.addTest(new TestSuite(GridCacheCheckpointSpiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheCheckpointSpiStartStopSelfTest.class));
        suite.addTest(new TestSuite(GridCacheCheckpointSpiSecondCacheSelfTest.class));

        // JDBC.
        suite.addTest(new TestSuite(GridJdbcCheckpointSpiConfigSelfTest.class));
        suite.addTest(new TestSuite(GridJdbcCheckpointSpiCustomConfigSelfTest.class));
        suite.addTest(new TestSuite(GridJdbcCheckpointSpiDefaultConfigSelfTest.class));
        suite.addTest(new TestSuite(GridJdbcCheckpointSpiStartStopSelfTest.class));

        // Shared FS.
        suite.addTest(new TestSuite(GridSharedFsCheckpointSpiMultipleDirectoriesSelfTest.class));
        suite.addTest(new TestSuite(GridSharedFsCheckpointSpiSelfTest.class));
        suite.addTest(new TestSuite(GridSharedFsCheckpointSpiStartStopSelfTest.class));
        suite.addTest(new TestSuite(GridSharedFsCheckpointSpiConfigSelfTest.class));

        return suite;
    }
}

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
import org.apache.ignite.spi.checkpoint.cache.CacheCheckpointSpiConfigSelfTest;
import org.apache.ignite.spi.checkpoint.cache.CacheCheckpointSpiSecondCacheSelfTest;
import org.apache.ignite.spi.checkpoint.cache.CacheCheckpointSpiSelfTest;
import org.apache.ignite.spi.checkpoint.cache.CacheCheckpointSpiStartStopSelfTest;
import org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpiConfigSelfTest;
import org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpiCustomConfigSelfTest;
import org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpiDefaultConfigSelfTest;
import org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpiStartStopSelfTest;
import org.apache.ignite.spi.checkpoint.sharedfs.GridSharedFsCheckpointSpiConfigSelfTest;
import org.apache.ignite.spi.checkpoint.sharedfs.GridSharedFsCheckpointSpiMultiThreadedSelfTest;
import org.apache.ignite.spi.checkpoint.sharedfs.GridSharedFsCheckpointSpiMultipleDirectoriesSelfTest;
import org.apache.ignite.spi.checkpoint.sharedfs.GridSharedFsCheckpointSpiSelfTest;
import org.apache.ignite.spi.checkpoint.sharedfs.GridSharedFsCheckpointSpiStartStopSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Grid SPI checkpoint self test suite.
 */
@RunWith(AllTests.class)
public class IgniteSpiCheckpointSelfTestSuite {
    /**
     * @return Checkpoint test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Checkpoint Test Suite");

        // Cache.
        suite.addTest(new JUnit4TestAdapter(CacheCheckpointSpiConfigSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheCheckpointSpiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheCheckpointSpiStartStopSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheCheckpointSpiSecondCacheSelfTest.class));

        // JDBC.
        suite.addTest(new JUnit4TestAdapter(JdbcCheckpointSpiConfigSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcCheckpointSpiCustomConfigSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcCheckpointSpiDefaultConfigSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcCheckpointSpiStartStopSelfTest.class));

        // Shared FS.
        suite.addTest(new JUnit4TestAdapter(GridSharedFsCheckpointSpiMultipleDirectoriesSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSharedFsCheckpointSpiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSharedFsCheckpointSpiStartStopSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSharedFsCheckpointSpiConfigSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridSharedFsCheckpointSpiMultiThreadedSelfTest.class));

        return suite;
    }
}

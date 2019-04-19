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

import org.apache.ignite.spi.checkpoint.cache.CacheCheckpointSpiConfigSelfTest;
import org.apache.ignite.spi.checkpoint.cache.CacheCheckpointSpiSecondCacheSelfTest;
import org.apache.ignite.spi.checkpoint.cache.CacheCheckpointSpiSelfTest;
import org.apache.ignite.spi.checkpoint.cache.CacheCheckpointSpiStartStopSelfTest;
import org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpiConfigSelfTest;
import org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpiCustomConfigSelfTest;
import org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpiDefaultConfigSelfTest;
import org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpiStartStopSelfTest;
import org.apache.ignite.spi.checkpoint.sharedfs.GridSharedFsCheckpointSpiConfigSelfTest;
import org.apache.ignite.spi.checkpoint.sharedfs.GridSharedFsCheckpointSpiMultipleDirectoriesSelfTest;
import org.apache.ignite.spi.checkpoint.sharedfs.GridSharedFsCheckpointSpiSelfTest;
import org.apache.ignite.spi.checkpoint.sharedfs.GridSharedFsCheckpointSpiStartStopSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Grid SPI checkpoint self test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    CacheCheckpointSpiConfigSelfTest.class,
    CacheCheckpointSpiSelfTest.class,
    CacheCheckpointSpiStartStopSelfTest.class,
    CacheCheckpointSpiSecondCacheSelfTest.class,

    // JDBC.
    JdbcCheckpointSpiConfigSelfTest.class,
    JdbcCheckpointSpiCustomConfigSelfTest.class,
    JdbcCheckpointSpiDefaultConfigSelfTest.class,
    JdbcCheckpointSpiStartStopSelfTest.class,

    // Shared FS.
    GridSharedFsCheckpointSpiMultipleDirectoriesSelfTest.class,
    GridSharedFsCheckpointSpiSelfTest.class,
    GridSharedFsCheckpointSpiStartStopSelfTest.class,
    GridSharedFsCheckpointSpiConfigSelfTest.class,
    //GridSharedFsCheckpointSpiMultiThreadedSelfTest.class,
})
public class IgniteSpiCheckpointSelfTestSuite {
}

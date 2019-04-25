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

import org.apache.ignite.logger.log4j.GridLog4jConfigUpdateTest;
import org.apache.ignite.logger.log4j.GridLog4jCorrectFileNameTest;
import org.apache.ignite.logger.log4j.GridLog4jInitializedTest;
import org.apache.ignite.logger.log4j.GridLog4jLoggingFileTest;
import org.apache.ignite.logger.log4j.GridLog4jLoggingPathTest;
import org.apache.ignite.logger.log4j.GridLog4jLoggingUrlTest;
import org.apache.ignite.logger.log4j.GridLog4jNotInitializedTest;
import org.apache.ignite.logger.log4j.GridLog4jWatchDelayTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Log4j logging tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridLog4jInitializedTest.class,
    GridLog4jNotInitializedTest.class,
    GridLog4jCorrectFileNameTest.class,
    GridLog4jLoggingFileTest.class,
    GridLog4jLoggingPathTest.class,
    GridLog4jLoggingUrlTest.class,
    GridLog4jConfigUpdateTest.class,
    GridLog4jWatchDelayTest.class,
})
public class IgniteLog4jTestSuite { }

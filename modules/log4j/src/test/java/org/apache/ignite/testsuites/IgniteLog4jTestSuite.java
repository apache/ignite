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

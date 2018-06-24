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

import junit.framework.TestSuite;
import org.apache.ignite.internal.commandline.CommandHandlerParsingTest;
import org.apache.ignite.util.GridCommandHandlerTest;
import org.apache.ignite.util.GridInternalTaskUnusedWalSegmentsTest;

import java.util.Set;

/**
 * Test suite for Ignite utility classes.
 */
public class IgniteUtilSelfTestWithPersistenceSuite extends TestSuite {
    /**
     * @return Grid utility methods tests suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests don't include in the execution.
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite(Set<Class> ignoredTests) throws Exception {
        TestSuite suite = new TestSuite("Ignite Util Test Suite");

        // control.sh
        suite.addTestSuite(CommandHandlerParsingTest.class);
        suite.addTestSuite(GridInternalTaskUnusedWalSegmentsTest.class);
        suite.addTestSuite(GridCommandHandlerTest.class);

        return suite;
    }
}

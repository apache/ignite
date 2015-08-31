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
import org.apache.ignite.logger.log4j.GridLog4jCorrectFileNameTest;
import org.apache.ignite.logger.log4j.GridLog4jInitializedTest;
import org.apache.ignite.logger.log4j.GridLog4jNotInitializedTest;

/**
 * Log4j logging tests.
 */
public class IgniteLog4jTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Log4j Logging Test Suite");

        suite.addTest(new TestSuite(GridLog4jInitializedTest.class));
        suite.addTest(new TestSuite(GridLog4jNotInitializedTest.class));
        suite.addTest(new TestSuite(GridLog4jCorrectFileNameTest.class));

        return suite;
    }
}
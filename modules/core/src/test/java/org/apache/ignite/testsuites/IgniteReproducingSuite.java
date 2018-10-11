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

/**
 * Test suite for cycled run tests on PR code. <br>
 * This empty suite may be used in case it is needed to run
 * some test subset to reproduce an issue.<br>
 *
 * You may launch and check results on
 * https://ci.ignite.apache.org/viewType.html?buildTypeId=Ignite20Tests_IgniteReproducingSuite
 *
 * This suite is not included into main build
 */
public class IgniteReproducingSuite extends TestSuite {
    /**
     * @return suite with test(s) for reproduction some problem.
     * @throws Exception if failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Issue Reproducing Test Suite");

        //uncomment to add some test
        //for (int i = 0; i < 100; i++)
        //    suite.addTestSuite(IgniteCheckpointDirtyPagesForLowLoadTest.class);

        return suite;
    }
}

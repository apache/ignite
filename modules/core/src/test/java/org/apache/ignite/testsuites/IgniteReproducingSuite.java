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

import junit.framework.TestSuite;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

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
@RunWith(AllTests.class)
public class IgniteReproducingSuite {
    /**
     * @return suite with test(s) for reproduction some problem.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Issue Reproducing Test Suite");

        //uncomment to add some test
        //for (int i = 0; i < 100; i++)
        //    suite.addTest(new JUnit4TestAdapter(IgniteCheckpointDirtyPagesForLowLoadTest.class));

        return suite;
    }
}

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
import org.apache.ignite.testframework.IgniteTestSuite;

/**
 * Special test suite with ignored tests for Binary mode.
 */
public class IgniteIgnoredBinaryTestSuite extends TestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        IgniteTestSuite.ignoreDefault(true);

        IgniteTestSuite suite = new IgniteTestSuite(null, "Ignite Ignored Binary Test Suite");

        /* --- QUERY --- */
        suite.addTest(IgniteBinaryCacheQueryTestSuite.suite());
        suite.addTest(IgniteBinaryCacheQueryTestSuite2.suite());

        return suite;
    }
}

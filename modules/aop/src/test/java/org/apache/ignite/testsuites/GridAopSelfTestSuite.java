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
import org.apache.ignite.gridify.*;
import org.apache.ignite.testframework.*;
import org.test.gridify.*;

import static org.apache.ignite.IgniteSystemProperties.*;

/**
 * AOP test suite.
 */
public class GridAopSelfTestSuite extends TestSuite {
    /**
     * @return AOP test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain AOP Test Suite");

        // Test configuration.
        suite.addTest(new TestSuite(GridBasicAopSelfTest.class));

        suite.addTest(new TestSuite(GridSpringAopSelfTest.class));
        suite.addTest(new TestSuite(GridNonSpringAopSelfTest.class));
        suite.addTest(new TestSuite(GridifySetToXXXSpringAopSelfTest.class));
        suite.addTest(new TestSuite(GridifySetToXXXNonSpringAopSelfTest.class));
        suite.addTest(new TestSuite(GridExternalNonSpringAopSelfTest.class));

        // Examples
        System.setProperty(GG_OVERRIDE_MCAST_GRP, GridTestUtils.getNextMulticastGroup(GridAopSelfTestSuite.class));

        return suite;
    }
}

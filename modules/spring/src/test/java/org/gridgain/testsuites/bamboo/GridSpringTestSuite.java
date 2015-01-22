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

package org.gridgain.testsuites.bamboo;

import junit.framework.*;
import org.gridgain.grid.cache.spring.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.p2p.*;
import org.gridgain.testsuites.*;

/**
 * Spring tests.
 */
public class GridSpringTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Spring Test Suite");

        suite.addTestSuite(GridSpringBeanSerializationSelfTest.class);
        suite.addTestSuite(GridFactorySelfTest.class);

        suite.addTest(GridResourceSelfTestSuite.suite());

        // Tests moved to this suite since they require Spring functionality.
        suite.addTest(new TestSuite(GridP2PUserVersionChangeSelfTest.class));

        suite.addTest(new TestSuite(GridSpringCacheManagerSelfTest.class));
        suite.addTest(new TestSuite(GridSpringDynamicCacheManagerSelfTest.class));

        return suite;
    }
}

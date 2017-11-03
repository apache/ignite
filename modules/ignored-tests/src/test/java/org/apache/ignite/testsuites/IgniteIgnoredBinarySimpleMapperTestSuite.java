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
import org.apache.ignite.testframework.IgniteTestSuite;

/**
 * Special test suite with ignored tests for Binary mode.
 */
public class IgniteIgnoredBinarySimpleMapperTestSuite extends TestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        IgniteTestSuite.ignoreDefault(true);

        IgniteTestSuite suite = new IgniteTestSuite(null, "Ignite Ignored Binary Simple Mapper Test Suite");

        /* --- QUERY --- */
        suite.addTest(IgniteBinarySimpleNameMapperCacheQueryTestSuite.suite());

        return suite;
    }
}

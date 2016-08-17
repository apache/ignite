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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Special test suite with ignored tests.
 */
public class IgniteIgnoredTestSuite extends TestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        IgniteTestSuite suite = new IgniteTestSuite("Ignite Ignored Test Suite");

        suite.addTestSuite(SampleTestClass.class, true);

        return suite;
    }

    /**
     * Sample test class. To be removed once the very first really ignored test class is there.
     */
    public static class SampleTestClass extends GridCommonAbstractTest {
        /**
         * Test 1.
         *
         * @throws Exception If failed.
         */
        public void testMethod1() throws Exception {
            System.out.println("Normal test method called.");
        }

        /**
         * Test 2.
         *
         * @throws Exception If failed.
         */
        @IgniteIgnore
        public void testMethod2() throws Exception {
            System.out.println("Ignored method called.");
        }
    }
}

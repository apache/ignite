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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.query.h2.CacheQueryEntityWithJsr310Java8DateTimeApiFieldsTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Test suite for JSR-310 Java 8 Date and Time API queries.
 */
@RunWith(AllTests.class)
public class CacheQueryJsr310Java8DateTimeApiSupportTestSuite {
    /**
     * @return Test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("JSR-310 Java 8 Date and Time API Cache Queries Test Suite");

        suite.addTest(new JUnit4TestAdapter(CacheQueryEntityWithJsr310Java8DateTimeApiFieldsTest.class));

        return suite;
    }
}

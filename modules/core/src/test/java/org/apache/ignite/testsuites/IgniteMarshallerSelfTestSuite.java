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

import java.util.Set;
import junit.framework.TestSuite;
import org.apache.ignite.internal.util.io.GridUnsafeDataOutputArraySizingSelfTest;
import org.apache.ignite.marshaller.jdk.GridJdkMarshallerSelfTest;
import org.apache.ignite.marshaller.optimized.OptimizedMarshallerEnumSelfTest;
import org.apache.ignite.marshaller.optimized.OptimizedMarshallerNodeFailoverTest;
import org.apache.ignite.marshaller.optimized.OptimizedMarshallerSelfTest;
import org.apache.ignite.marshaller.optimized.OptimizedMarshallerSerialPersistentFieldsSelfTest;
import org.apache.ignite.marshaller.optimized.OptimizedMarshallerTest;
import org.apache.ignite.marshaller.optimized.OptimizedObjectStreamSelfTest;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Test suite for all marshallers.
 */
public class IgniteMarshallerSelfTestSuite extends TestSuite {
    /**
     * @return Kernal test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        return suite(null);
    }

    /**
     * @param ignoredTests
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite(Set<Class> ignoredTests) throws Exception {
        TestSuite suite = new TestSuite("Ignite Marshaller Test Suite");

        GridTestUtils.addTestIfNeeded(suite, GridJdkMarshallerSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OptimizedMarshallerEnumSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OptimizedMarshallerSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OptimizedMarshallerTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OptimizedObjectStreamSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridUnsafeDataOutputArraySizingSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OptimizedMarshallerNodeFailoverTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OptimizedMarshallerSerialPersistentFieldsSelfTest.class, ignoredTests);

        return suite;
    }
}
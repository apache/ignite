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
import org.apache.ignite.internal.direct.stream.v2.DirectByteBufferStreamImplV2ByteOrderSelfTest;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshallerEnumSelfTest;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshallerNodeFailoverTest;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshallerPooledSelfTest;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshallerSelfTest;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshallerSerialPersistentFieldsSelfTest;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshallerTest;
import org.apache.ignite.internal.marshaller.optimized.OptimizedObjectStreamSelfTest;
import org.apache.ignite.internal.util.GridHandleTableSelfTest;
import org.apache.ignite.internal.util.io.GridUnsafeDataInputOutputByteOrderSelfTest;
import org.apache.ignite.internal.util.io.GridUnsafeDataOutputArraySizingSelfTest;
import org.apache.ignite.marshaller.MarshallerEnumDeadlockMultiJvmTest;
import org.apache.ignite.marshaller.jdk.GridJdkMarshallerSelfTest;
import org.apache.ignite.testframework.GridTestUtils;

import java.util.Set;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Test suite for all marshallers.
 */
@RunWith(AllTests.class)
public class IgniteMarshallerSelfTestSuite {
    /**
     * @return Kernal test suite.
     */
    public static TestSuite suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Ignored tests.
     * @return Test suite.
     */
    public static TestSuite suite(Set<Class> ignoredTests) {
        TestSuite suite = new TestSuite("Ignite Marshaller Test Suite");

        GridTestUtils.addTestIfNeeded(suite, GridUnsafeDataOutputArraySizingSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridJdkMarshallerSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OptimizedMarshallerEnumSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OptimizedMarshallerSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OptimizedMarshallerTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OptimizedObjectStreamSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridUnsafeDataInputOutputByteOrderSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OptimizedMarshallerNodeFailoverTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OptimizedMarshallerSerialPersistentFieldsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DirectByteBufferStreamImplV2ByteOrderSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridHandleTableSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OptimizedMarshallerPooledSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, MarshallerEnumDeadlockMultiJvmTest.class, ignoredTests);

        return suite;
    }
}

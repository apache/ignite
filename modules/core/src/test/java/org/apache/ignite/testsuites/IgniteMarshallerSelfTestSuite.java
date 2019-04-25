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

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all marshallers.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridUnsafeDataOutputArraySizingSelfTest.class,
    GridJdkMarshallerSelfTest.class,
    OptimizedMarshallerEnumSelfTest.class,
    OptimizedMarshallerSelfTest.class,
    OptimizedMarshallerTest.class,
    OptimizedObjectStreamSelfTest.class,
    GridUnsafeDataInputOutputByteOrderSelfTest.class,
    OptimizedMarshallerNodeFailoverTest.class,
    OptimizedMarshallerSerialPersistentFieldsSelfTest.class,
    DirectByteBufferStreamImplV2ByteOrderSelfTest.class,
    GridHandleTableSelfTest.class,
    OptimizedMarshallerPooledSelfTest.class,
    MarshallerEnumDeadlockMultiJvmTest.class
})
public class IgniteMarshallerSelfTestSuite {
}

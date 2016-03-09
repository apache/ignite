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

import java.util.HashSet;
import java.util.Set;
import junit.framework.TestSuite;
import org.apache.ignite.internal.ClusterGroupSelfTest;
import org.apache.ignite.internal.GridReleaseTypeSelfTest;
import org.apache.ignite.internal.GridVersionSelfTest;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.managers.deployment.GridDeploymentMessageCountSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheP2pUnmarshallingErrorTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheP2pUnmarshallingNearErrorTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheP2pUnmarshallingRebalanceErrorTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheP2pUnmarshallingTxErrorTest;
import org.apache.ignite.internal.processors.cache.IgniteDaemonNodeMarshallerCacheTest;
import org.apache.ignite.internal.util.GridHandleTableSelfTest;
import org.apache.ignite.internal.util.GridStartupWithSpecifiedWorkDirectorySelfTest;
import org.apache.ignite.internal.util.IgniteUtilsSelfTest;
import org.apache.ignite.internal.util.io.GridUnsafeDataOutputArraySizingSelfTest;
import org.apache.ignite.internal.util.nio.GridNioSelfTest;
import org.apache.ignite.internal.util.nio.GridNioSslSelfTest;
import org.apache.ignite.marshaller.DynamicProxySerializationMultiJvmSelfTest;
import org.apache.ignite.marshaller.jdk.GridJdkMarshallerSelfTest;
import org.apache.ignite.marshaller.optimized.OptimizedMarshallerEnumSelfTest;
import org.apache.ignite.marshaller.optimized.OptimizedMarshallerNodeFailoverTest;
import org.apache.ignite.marshaller.optimized.OptimizedMarshallerPooledSelfTest;
import org.apache.ignite.marshaller.optimized.OptimizedMarshallerSelfTest;
import org.apache.ignite.marshaller.optimized.OptimizedMarshallerSerialPersistentFieldsSelfTest;
import org.apache.ignite.marshaller.optimized.OptimizedMarshallerTest;
import org.apache.ignite.marshaller.optimized.OptimizedObjectStreamSelfTest;
import org.apache.ignite.messaging.GridMessagingNoPeerClassLoadingSelfTest;
import org.apache.ignite.messaging.GridMessagingSelfTest;
import org.apache.ignite.testframework.config.GridTestProperties;

/**
 * Basic test suite.
 */
public class IgniteBinaryBasicTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        GridTestProperties.setProperty(GridTestProperties.MARSH_CLASS_NAME, BinaryMarshaller.class.getName());

        TestSuite suite = new TestSuite("GridGain Binary Basic Test Suite");

        Set<Class> ignoredTests = new HashSet<>();

        // Tests that are not ready to be used with PortableMarshaller
        ignoredTests.add(GridJdkMarshallerSelfTest.class);
        ignoredTests.add(OptimizedMarshallerEnumSelfTest.class);
        ignoredTests.add(OptimizedMarshallerSelfTest.class);
        ignoredTests.add(OptimizedMarshallerTest.class);
        ignoredTests.add(OptimizedObjectStreamSelfTest.class);
        ignoredTests.add(GridUnsafeDataOutputArraySizingSelfTest.class);
        ignoredTests.add(OptimizedMarshallerNodeFailoverTest.class);
        ignoredTests.add(OptimizedMarshallerSerialPersistentFieldsSelfTest.class);
        ignoredTests.add(GridNioSslSelfTest.class);
        ignoredTests.add(GridNioSelfTest.class);
        ignoredTests.add(IgniteCacheP2pUnmarshallingErrorTest.class);
        ignoredTests.add(IgniteCacheP2pUnmarshallingTxErrorTest.class);
        ignoredTests.add(IgniteCacheP2pUnmarshallingNearErrorTest.class);
        ignoredTests.add(IgniteCacheP2pUnmarshallingRebalanceErrorTest.class);
        ignoredTests.add(GridReleaseTypeSelfTest.class);
        ignoredTests.add(GridStartupWithSpecifiedWorkDirectorySelfTest.class);
        ignoredTests.add(IgniteUtilsSelfTest.class);
        ignoredTests.add(ClusterGroupSelfTest.class);
        ignoredTests.add(GridMessagingNoPeerClassLoadingSelfTest.class);
        ignoredTests.add(GridMessagingSelfTest.class);
        ignoredTests.add(GridVersionSelfTest.class);
        ignoredTests.add(GridDeploymentMessageCountSelfTest.class);
        ignoredTests.add(DynamicProxySerializationMultiJvmSelfTest.class);
        ignoredTests.add(GridHandleTableSelfTest.class);
        ignoredTests.add(OptimizedMarshallerPooledSelfTest.class);

        // TODO: check and delete if pass.
        ignoredTests.add(IgniteDaemonNodeMarshallerCacheTest.class);

        suite.addTest(IgniteBasicTestSuite.suite(ignoredTests));

        return suite;
    }
}

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
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheClassNameConflictTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheClientRequestsMappingOnMissTest;
import org.apache.ignite.internal.util.GridHandleTableSelfTest;
import org.apache.ignite.internal.util.IgniteUtilsSelfTest;
import org.apache.ignite.internal.util.io.GridUnsafeDataOutputArraySizingSelfTest;
import org.apache.ignite.internal.util.nio.GridNioSelfTest;
import org.apache.ignite.internal.util.nio.GridNioSslSelfTest;
import org.apache.ignite.marshaller.DynamicProxySerializationMultiJvmSelfTest;
import org.apache.ignite.marshaller.jdk.GridJdkMarshallerSelfTest;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshallerEnumSelfTest;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshallerNodeFailoverTest;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshallerPooledSelfTest;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshallerSelfTest;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshallerSerialPersistentFieldsSelfTest;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshallerTest;
import org.apache.ignite.internal.marshaller.optimized.OptimizedObjectStreamSelfTest;
import org.apache.ignite.messaging.GridMessagingNoPeerClassLoadingSelfTest;
import org.apache.ignite.messaging.GridMessagingSelfTest;
import org.apache.ignite.testframework.config.GridTestProperties;

/**
 * Basic test suite.
 * May be removed soon as all tests were moved to {@link IgniteBasicTestSuite}
 */
@Deprecated
public class IgniteBinaryBasicTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        return new TestSuite("GridGain Binary Basic Test Suite: migrated to Ignite Basic Test Suite");
    }
}

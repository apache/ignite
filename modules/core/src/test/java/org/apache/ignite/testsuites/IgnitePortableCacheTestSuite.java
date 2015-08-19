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

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.expiry.*;
import org.apache.ignite.internal.processors.cache.portable.*;
import org.apache.ignite.internal.processors.cache.portable.datastreaming.*;
import org.apache.ignite.internal.processors.cache.portable.distributed.dht.*;
import org.apache.ignite.internal.processors.datastreamer.*;
import org.apache.ignite.marshaller.portable.*;
import org.apache.ignite.testframework.config.*;

import junit.framework.*;

import java.util.*;

/**
 * Cache suite with portable marshaller.
 */
public class IgnitePortableCacheTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception In case of error.
     */
    public static TestSuite suite() throws Exception {
        GridTestProperties.setProperty(GridTestProperties.MARSH_CLASS_NAME, PortableMarshaller.class.getName());

        TestSuite suite = new TestSuite("Portable Cache Test Suite");

        HashSet<Class> ignoredTests = new HashSet<>();

        // Tests below have a special version for Portable Marshaller
        ignoredTests.add(DataStreamProcessorSelfTest.class);
        ignoredTests.add(GridCacheOffHeapTieredEvictionAtomicSelfTest.class);
        ignoredTests.add(GridCacheOffHeapTieredEvictionSelfTest.class);
        ignoredTests.add(GridCacheOffHeapTieredSelfTest.class);
        ignoredTests.add(GridCacheOffHeapTieredAtomicSelfTest.class);
        ignoredTests.add(GridCacheAffinityRoutingSelfTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalExpiryPolicyTest.class);
        ignoredTests.add(GridCacheEntryMemorySizeSelfTest.class);

        // Tests that are not ready to be used with PortableMarshaller
        ignoredTests.add(GridCacheMvccSelfTest.class);

        suite.addTest(IgniteCacheTestSuite.suite(ignoredTests));
        suite.addTest(IgniteCacheExpiryPolicyTestSuite.suite());

        suite.addTestSuite(GridCacheMemoryModePortableSelfTest.class);
        suite.addTestSuite(GridCacheOffHeapTieredEvictionAtomicPortableSelfTest.class);
        suite.addTestSuite(GridCacheOffHeapTieredEvictionPortableSelfTest.class);

        suite.addTestSuite(GridCachePortablesPartitionedOnlyByteArrayValuesSelfTest.class);
        suite.addTestSuite(GridCachePortablesNearPartitionedByteArrayValuesSelfTest.class);
        suite.addTestSuite(GridCacheOffHeapTieredPortableSelfTest.class);
        suite.addTestSuite(GridCacheOffHeapTieredAtomicPortableSelfTest.class);

        suite.addTestSuite(GridDataStreamerImplSelfTest.class);
        suite.addTestSuite(DataStreamProcessorPortableSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPartitionedOnlyPortableDataStreamerMultiNodeSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPartitionedOnlyPortableDataStreamerMultithreadedSelfTest.class);

        suite.addTestSuite(GridCacheAtomicPartitionedOnlyPortableMultiNodeSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPartitionedOnlyPortableMultithreadedSelfTest.class);

        suite.addTestSuite(GridCacheAffinityRoutingPortableSelfTest.class);
        suite.addTestSuite(GridPortableCacheEntryMemorySizeSelfTest.class);

        return suite;
    }
}

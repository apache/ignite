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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityRoutingSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheEntryMemorySizeSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMvccSelfTest;
import org.apache.ignite.internal.processors.cache.binary.CacheKeepBinaryWithInterceptorTest;
import org.apache.ignite.internal.processors.cache.binary.GridBinaryCacheEntryMemorySizeSelfTest;
import org.apache.ignite.internal.processors.cache.binary.datastreaming.DataStreamProcessorBinarySelfTest;
import org.apache.ignite.internal.processors.cache.binary.datastreaming.DataStreamProcessorPersistenceBinarySelfTest;
import org.apache.ignite.internal.processors.cache.binary.datastreaming.GridDataStreamerImplSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAffinityRoutingBinarySelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAtomicPartitionedOnlyBinaryDataStreamerMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAtomicPartitionedOnlyBinaryDataStreamerMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAtomicPartitionedOnlyBinaryMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAtomicPartitionedOnlyBinaryMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinariesNearPartitionedByteArrayValuesSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinariesPartitionedOnlyByteArrayValuesSelfTest;
import org.apache.ignite.internal.processors.cache.expiry.IgniteCacheAtomicLocalExpiryPolicyTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Cache suite with binary marshaller.
 */
@RunWith(DynamicSuite.class)
public class IgniteBinaryCacheTestSuite {
    /**
     * @return Suite.
     */
    public static List<Class<?>> suite() {
        return suite(new HashSet<>());
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     */
    public static List<Class<?>> suite(Collection<Class> ignoredTests) {
        // Tests below have a special version for Binary Marshaller
        ignoredTests.add(GridCacheAffinityRoutingSelfTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalExpiryPolicyTest.class);
        ignoredTests.add(GridCacheEntryMemorySizeSelfTest.class);

        // Tests that are not ready to be used with BinaryMarshaller
        ignoredTests.add(GridCacheMvccSelfTest.class);

        List<Class<?>> suite = new ArrayList<>(IgniteCacheTestSuite.suite(ignoredTests));

        GridTestUtils.addTestIfNeeded(suite, GridCacheBinariesPartitionedOnlyByteArrayValuesSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheBinariesNearPartitionedByteArrayValuesSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridDataStreamerImplSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DataStreamProcessorBinarySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DataStreamProcessorPersistenceBinarySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicPartitionedOnlyBinaryDataStreamerMultiNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicPartitionedOnlyBinaryDataStreamerMultithreadedSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicPartitionedOnlyBinaryMultiNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicPartitionedOnlyBinaryMultithreadedSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheAffinityRoutingBinarySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridBinaryCacheEntryMemorySizeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheKeepBinaryWithInterceptorTest.class, ignoredTests);

        return suite;
    }
}

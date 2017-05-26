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
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityRoutingSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheEntryMemorySizeSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMvccSelfTest;
import org.apache.ignite.internal.processors.cache.binary.CacheKeepBinaryWithInterceptorTest;
import org.apache.ignite.internal.processors.cache.expiry.IgniteCacheAtomicLocalExpiryPolicyTest;
import org.apache.ignite.internal.processors.cache.binary.GridBinaryCacheEntryMemorySizeSelfTest;
import org.apache.ignite.internal.processors.cache.binary.datastreaming.DataStreamProcessorBinarySelfTest;
import org.apache.ignite.internal.processors.cache.binary.datastreaming.GridDataStreamerImplSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAffinityRoutingBinarySelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAtomicPartitionedOnlyBinaryDataStreamerMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAtomicPartitionedOnlyBinaryDataStreamerMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAtomicPartitionedOnlyBinaryMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAtomicPartitionedOnlyBinaryMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinariesNearPartitionedByteArrayValuesSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinariesPartitionedOnlyByteArrayValuesSelfTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamProcessorSelfTest;

/**
 * Cache suite with binary marshaller.
 */
public class IgniteBinaryCacheTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception In case of error.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Binary Cache Test Suite");

        HashSet<Class> ignoredTests = new HashSet<>();

        // Tests below have a special version for Binary Marshaller
        ignoredTests.add(DataStreamProcessorSelfTest.class);
        ignoredTests.add(GridCacheAffinityRoutingSelfTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalExpiryPolicyTest.class);
        ignoredTests.add(GridCacheEntryMemorySizeSelfTest.class);

        // Tests that are not ready to be used with BinaryMarshaller
        ignoredTests.add(GridCacheMvccSelfTest.class);

        suite.addTest(IgniteCacheTestSuite.suite(ignoredTests));

        // TODO GG-11148
        // suite.addTestSuite(GridCacheMemoryModeBinarySelfTest.class);

        suite.addTestSuite(GridCacheBinariesPartitionedOnlyByteArrayValuesSelfTest.class);
        suite.addTestSuite(GridCacheBinariesNearPartitionedByteArrayValuesSelfTest.class);

        suite.addTestSuite(GridDataStreamerImplSelfTest.class);
        suite.addTestSuite(DataStreamProcessorBinarySelfTest.class);
        suite.addTestSuite(GridCacheAtomicPartitionedOnlyBinaryDataStreamerMultiNodeSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPartitionedOnlyBinaryDataStreamerMultithreadedSelfTest.class);

        suite.addTestSuite(GridCacheAtomicPartitionedOnlyBinaryMultiNodeSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPartitionedOnlyBinaryMultithreadedSelfTest.class);

        suite.addTestSuite(GridCacheAffinityRoutingBinarySelfTest.class);
        suite.addTestSuite(GridBinaryCacheEntryMemorySizeSelfTest.class);
        suite.addTestSuite(CacheKeepBinaryWithInterceptorTest.class);

        return suite;
    }
}

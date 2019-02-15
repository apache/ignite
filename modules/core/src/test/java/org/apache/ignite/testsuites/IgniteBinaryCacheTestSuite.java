/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.testsuites;

import java.util.Collection;
import java.util.HashSet;
import junit.framework.TestSuite;
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
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Cache suite with binary marshaller.
 */
@RunWith(AllTests.class)
public class IgniteBinaryCacheTestSuite {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        return suite(new HashSet<>());
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     */
    public static TestSuite suite(Collection<Class> ignoredTests) {
        TestSuite suite = new TestSuite("Binary Cache Test Suite");

        // Tests below have a special version for Binary Marshaller
        ignoredTests.add(GridCacheAffinityRoutingSelfTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalExpiryPolicyTest.class);
        ignoredTests.add(GridCacheEntryMemorySizeSelfTest.class);

        // Tests that are not ready to be used with BinaryMarshaller
        ignoredTests.add(GridCacheMvccSelfTest.class);

        suite.addTest(IgniteCacheTestSuite.suite(ignoredTests));

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

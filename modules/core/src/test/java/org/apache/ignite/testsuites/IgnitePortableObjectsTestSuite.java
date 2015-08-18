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

import org.apache.ignite.internal.portable.*;

import junit.framework.*;

/**
 * Test for portable objects stored in cache.
 */
public class IgnitePortableObjectsTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("GridGain Portable Objects Test Suite");

        suite.addTestSuite(GridPortableMarshallerSelfTest.class);
        suite.addTestSuite(GridPortableMarshallerCtxDisabledSelfTest.class);
        suite.addTestSuite(GridPortableBuilderSelfTest.class);
        suite.addTestSuite(GridPortableBuilderStringAsCharsSelfTest.class);
        suite.addTestSuite(GridPortableMetaDataSelfTest.class);
        suite.addTestSuite(GridPortableMetaDataDisabledSelfTest.class);
        suite.addTestSuite(GridPortableAffinityKeySelfTest.class);
        suite.addTestSuite(GridPortableWildcardsSelfTest.class);
        suite.addTestSuite(GridPortableBuilderAdditionalSelfTest.class);
        suite.addTestSuite(GridPortableBuilderStringAsCharsAdditionalSelfTest.class);
//
//        suite.addTestSuite(GridCachePortableObjectsLocalSelfTest.class);
//        suite.addTestSuite(GridCachePortableObjectsAtomicLocalSelfTest.class);
//        suite.addTestSuite(GridCachePortableObjectsReplicatedSelfTest.class);
//        suite.addTestSuite(GridCachePortableObjectsPartitionedSelfTest.class);
//        suite.addTestSuite(GridCachePortableObjectsPartitionedNearDisabledSelfTest.class);
//        suite.addTestSuite(GridCachePortableObjectsAtomicSelfTest.class);
//        suite.addTestSuite(GridCachePortableObjectsAtomicNearDisabledSelfTest.class);
//
//        suite.addTestSuite(GridCachePortableObjectsLocalOffheapTieredSelfTest.class);
//        suite.addTestSuite(GridCachePortableObjectsAtomicOffheapTieredSelfTest.class);
//        suite.addTestSuite(GridCachePortableObjectsAtomicNearDisabledOffheapTieredSelfTest.class);
//        suite.addTestSuite(GridCachePortableObjectsPartitionedOffheapTieredSelfTest.class);
//        suite.addTestSuite(GridCachePortableObjectsPartitionedNearDisabledOffheapTieredSelfTest.class);
//
//        suite.addTestSuite(GridCacheLocalPortableEnabledFullApiSelfTest.class);
//        suite.addTestSuite(GridCacheAtomicLocalPortableEnabledFullApiSelfTest.class);
//        suite.addTestSuite(GridCacheReplicatedPortableEnabledFullApiSelfTest.class);
//        suite.addTestSuite(GridCachePartitionedPortableEnabledFullApiSelfTest.class);
//        suite.addTestSuite(GridCachePartitionedNearDisabledPortableEnabledFullApiSelfTest.class);
//        suite.addTestSuite(GridCacheAtomicPortableEnabledFullApiSelfTest.class);
//        suite.addTestSuite(GridCacheAtomicNearEnabledPortableEnabledFullApiSelfTest.class);
//
//        /** Off-heap tiered mode. */
//        suite.addTestSuite(GridCacheOffHeapTieredLocalPortableEnabledFullApiSelfTest.class);
//        suite.addTestSuite(GridCacheOffHeapTieredAtomicLocalPortableEnabledFullApiSelfTest.class);
//        suite.addTestSuite(GridCacheOffHeapTieredReplicatedPortableEnabledFullApiSelfTest.class);
//        suite.addTestSuite(GridCacheOffHeapTieredPartitionedPortableEnabledFullApiSelfTest.class);
//        suite.addTestSuite(GridCacheOffHeapTieredPartitionedNearDisabledPortableEnabledFullApiSelfTest.class);
//        suite.addTestSuite(GridCacheOffHeapTieredAtomicPortableEnabledFullApiSelfTest.class);
//        suite.addTestSuite(GridCacheOffHeapTieredAtomicNearEnabledPortableEnabledFullApiSelfTest.class);
//
//        suite.addTestSuite(GridCachePartitionedPortableEnabledFullApiMultiNodeSelfTest.class);
//        suite.addTestSuite(GridCacheAtomicPortableEnabledFullApiMultiNodeSelfTest.class);
//
//        suite.addTestSuite(GridCacheOffHeapTieredPartitionedPortableEnabledFullApiMultiNodeSelfTest.class);
//        suite.addTestSuite(GridCacheOffHeapTieredAtomicPortableEnabledFullApiMultiNodeSelfTest.class);
//
//        suite.addTestSuite(GridCacheAtomicPortableEnabledRollingUpdatesFullApiMultiNodeSelfTest.class);
//        suite.addTestSuite(GridCachePartitionedPortableEnabledRollingUpdatesFullApiMultiNodeSelfTest.class);
//
//        suite.addTestSuite(GridCacheOffHeapTieredAtomicPortableEnabledRollingUpdatesFullApiMultiNodeSelfTest.class);
//        suite.addTestSuite(GridCacheOffHeapTieredPartitionedPortableEnabledRollingUpdatesFullApiMultiNodeSelfTest.class);
//
//        suite.addTestSuite(GridCacheAtomicPartitionedOnlyPortableMultiNodeSelfTest.class);
//        suite.addTestSuite(GridCacheAtomicPartitionedOnlyPortableMultithreadedSelfTest.class);
//
//        suite.addTestSuite(GridCacheAtomicPartitionedOnlyPortableDataStreamerMultiNodeSelfTest.class);
//        suite.addTestSuite(GridCacheAtomicPartitionedOnlyPortableDataStreamerMultithreadedSelfTest.class);
//
//        suite.addTestSuite(GridCachePortableDuplicateIndexObjectPartitionedAtomicSelfTest.class);
//        suite.addTestSuite(GridCachePortableDuplicateIndexObjectPartitionedTransactionalSelfTest.class);
//
//        suite.addTestSuite(GridCacheMemoryModePortableSelfTest.class);
//        suite.addTestSuite(GridCacheOffHeapAtomicPortableMultiThreadedUpdateSelfTest.class);
//        suite.addTestSuite(GridCacheOffHeapTieredEvictionAtomicPortableSelfTest.class);
//        suite.addTestSuite(GridCacheOffHeapTieredEvictionPortableSelfTest.class);
//        suite.addTestSuite(GridCachePortablesPartitionedOnlyByteArrayValuesSelfTest.class);
//        suite.addTestSuite(GridCachePortablesNearPartitionedByteArrayValuesSelfTest.class);
//        suite.addTestSuite(GridCacheOffHeapTieredPortableSelfTest.class);
//        suite.addTestSuite(GridCacheOffHeapTieredAtomicPortableSelfTest.class);
//
//        suite.addTestSuite(GridDataStreamerImplEntSelfTest.class);
//
//        suite.addTestSuite(GridCachePortableStoreObjectsSelfTest.class);
//        suite.addTestSuite(GridCachePortableStorePortablesSelfTest.class);
//
//        suite.addTestSuite(GridCacheClientNodePortableMetadataTest.class);
//        suite.addTestSuite(GridCacheClientNodePortableMetadataMultinodeTest.class);
//
//        suite.addTestSuite(DrBasicSelfTest.class);

        return suite;
    }
}


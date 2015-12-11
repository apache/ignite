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

import junit.framework.TestSuite;
import org.apache.ignite.internal.binary.BinaryEnumsSelfTest;
import org.apache.ignite.internal.binary.GridPortableAffinityKeySelfTest;
import org.apache.ignite.internal.binary.BinaryObjectBuilderAdditionalSelfTest;
import org.apache.ignite.internal.binary.BinaryObjectBuilderSelfTest;
import org.apache.ignite.internal.binary.GridPortableMarshallerCtxDisabledSelfTest;
import org.apache.ignite.internal.binary.BinaryMarshallerSelfTest;
import org.apache.ignite.internal.binary.GridPortableMetaDataSelfTest;
import org.apache.ignite.internal.binary.GridPortableWildcardsSelfTest;
import org.apache.ignite.internal.binary.BinaryFooterOffsetsHeapSelfTest;
import org.apache.ignite.internal.binary.BinaryFooterOffsetsOffheapSelfTest;
import org.apache.ignite.internal.binary.BinaryFieldsHeapSelfTest;
import org.apache.ignite.internal.binary.BinaryFieldsOffheapSelfTest;
import org.apache.ignite.internal.binary.noncompact.BinaryFieldsHeapNonCompactSelfTest;
import org.apache.ignite.internal.binary.noncompact.BinaryFieldsOffheapNonCompactSelfTest;
import org.apache.ignite.internal.binary.noncompact.BinaryFooterOffsetsHeapNonCompactSelfTest;
import org.apache.ignite.internal.binary.noncompact.BinaryFooterOffsetsOffheapNonCompactSelfTest;
import org.apache.ignite.internal.binary.noncompact.BinaryMarshallerNonCompactSelfTest;
import org.apache.ignite.internal.binary.noncompact.BinaryObjectBuilderAdditionalNonCompactSelfTest;
import org.apache.ignite.internal.binary.noncompact.BinaryObjectBuilderNonCompactSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteBinaryMetadataUpdateChangingTopologySelfTest;
import org.apache.ignite.internal.processors.cache.binary.GridCacheClientNodeBinaryObjectMetadataMultinodeTest;
import org.apache.ignite.internal.processors.cache.binary.GridCacheClientNodeBinaryObjectMetadataTest;
import org.apache.ignite.internal.processors.cache.binary.GridCachePortableStoreObjectsSelfTest;
import org.apache.ignite.internal.processors.cache.binary.GridCachePortableStorePortablesSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsAtomicNearDisabledOffheapTieredSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsAtomicNearDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsAtomicOffheapTieredSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsPartitionedNearDisabledOffheapTieredSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsPartitionedNearDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsPartitionedOffheapTieredSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.replicated.GridCacheBinaryObjectsReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.binary.local.GridCacheBinaryObjectsAtomicLocalSelfTest;
import org.apache.ignite.internal.processors.cache.binary.local.GridCacheBinaryObjectsLocalOffheapTieredSelfTest;
import org.apache.ignite.internal.processors.cache.binary.local.GridCacheBinaryObjectsLocalSelfTest;

/**
 * Test for portable objects stored in cache.
 */
public class IgnitePortableObjectsTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Binary Objects Test Suite");

        suite.addTestSuite(BinaryMarshallerSelfTest.class);
        suite.addTestSuite(GridPortableMarshallerCtxDisabledSelfTest.class);
        suite.addTestSuite(BinaryObjectBuilderSelfTest.class);
        suite.addTestSuite(BinaryObjectBuilderAdditionalSelfTest.class);
        suite.addTestSuite(BinaryFieldsHeapSelfTest.class);
        suite.addTestSuite(BinaryFieldsOffheapSelfTest.class);
        suite.addTestSuite(BinaryFooterOffsetsHeapSelfTest.class);
        suite.addTestSuite(BinaryFooterOffsetsOffheapSelfTest.class);
        suite.addTestSuite(BinaryEnumsSelfTest.class);
        suite.addTestSuite(GridPortableMetaDataSelfTest.class);
        suite.addTestSuite(GridPortableAffinityKeySelfTest.class);
        suite.addTestSuite(GridPortableWildcardsSelfTest.class);

        // Tests for objects with non-compact footers.
        suite.addTestSuite(BinaryMarshallerNonCompactSelfTest.class);
        suite.addTestSuite(BinaryObjectBuilderNonCompactSelfTest.class);
        suite.addTestSuite(BinaryObjectBuilderAdditionalNonCompactSelfTest.class);
        suite.addTestSuite(BinaryFieldsHeapNonCompactSelfTest.class);
        suite.addTestSuite(BinaryFieldsOffheapNonCompactSelfTest.class);
        suite.addTestSuite(BinaryFooterOffsetsHeapNonCompactSelfTest.class);
        suite.addTestSuite(BinaryFooterOffsetsOffheapNonCompactSelfTest.class);

        suite.addTestSuite(GridCacheBinaryObjectsLocalSelfTest.class);
        suite.addTestSuite(GridCacheBinaryObjectsAtomicLocalSelfTest.class);
        suite.addTestSuite(GridCacheBinaryObjectsReplicatedSelfTest.class);
        suite.addTestSuite(GridCacheBinaryObjectsPartitionedSelfTest.class);
        suite.addTestSuite(GridCacheBinaryObjectsPartitionedNearDisabledSelfTest.class);
        suite.addTestSuite(GridCacheBinaryObjectsAtomicSelfTest.class);
        suite.addTestSuite(GridCacheBinaryObjectsAtomicNearDisabledSelfTest.class);

        suite.addTestSuite(GridCacheBinaryObjectsLocalOffheapTieredSelfTest.class);
        suite.addTestSuite(GridCacheBinaryObjectsAtomicOffheapTieredSelfTest.class);
        suite.addTestSuite(GridCacheBinaryObjectsAtomicNearDisabledOffheapTieredSelfTest.class);
        suite.addTestSuite(GridCacheBinaryObjectsPartitionedOffheapTieredSelfTest.class);
        suite.addTestSuite(GridCacheBinaryObjectsPartitionedNearDisabledOffheapTieredSelfTest.class);

        suite.addTestSuite(GridCachePortableStoreObjectsSelfTest.class);
        suite.addTestSuite(GridCachePortableStorePortablesSelfTest.class);

        suite.addTestSuite(GridCacheClientNodeBinaryObjectMetadataTest.class);
        suite.addTestSuite(GridCacheClientNodeBinaryObjectMetadataMultinodeTest.class);
        suite.addTestSuite(IgniteBinaryMetadataUpdateChangingTopologySelfTest.class);

        return suite;
    }
}

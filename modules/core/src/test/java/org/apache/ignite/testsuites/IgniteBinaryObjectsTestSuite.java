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
import org.apache.ignite.internal.binary.BinaryArrayIdentityResolverSelfTest;
import org.apache.ignite.internal.binary.BinaryBasicIdMapperSelfTest;
import org.apache.ignite.internal.binary.BinaryBasicNameMapperSelfTest;
import org.apache.ignite.internal.binary.BinaryConfigurationConsistencySelfTest;
import org.apache.ignite.internal.binary.BinaryConfigurationCustomSerializerSelfTest;
import org.apache.ignite.internal.binary.BinaryEnumsSelfTest;
import org.apache.ignite.internal.binary.BinaryFieldExtractionSelfTest;
import org.apache.ignite.internal.binary.BinaryFieldsHeapSelfTest;
import org.apache.ignite.internal.binary.BinaryFieldsOffheapSelfTest;
import org.apache.ignite.internal.binary.BinaryFooterOffsetsHeapSelfTest;
import org.apache.ignite.internal.binary.BinaryFooterOffsetsOffheapSelfTest;
import org.apache.ignite.internal.binary.BinaryMarshallerSelfTest;
import org.apache.ignite.internal.binary.BinaryObjectBuilderAdditionalSelfTest;
import org.apache.ignite.internal.binary.BinaryObjectBuilderDefaultMappersSelfTest;
import org.apache.ignite.internal.binary.BinaryObjectBuilderSimpleNameLowerCaseMappersSelfTest;
import org.apache.ignite.internal.binary.BinaryObjectExceptionSelfTest;
import org.apache.ignite.internal.binary.BinaryObjectToStringSelfTest;
import org.apache.ignite.internal.binary.BinaryObjectTypeCompatibilityTest;
import org.apache.ignite.internal.binary.BinarySerialiedFieldComparatorSelfTest;
import org.apache.ignite.internal.binary.BinarySimpleNameTestPropertySelfTest;
import org.apache.ignite.internal.binary.BinaryTreeSelfTest;
import org.apache.ignite.internal.binary.GridBinaryAffinityKeySelfTest;
import org.apache.ignite.internal.binary.GridBinaryMarshallerCtxDisabledSelfTest;
import org.apache.ignite.internal.binary.GridBinaryWildcardsSelfTest;
import org.apache.ignite.internal.binary.GridDefaultBinaryMappersBinaryMetaDataSelfTest;
import org.apache.ignite.internal.binary.GridSimpleLowerCaseBinaryMappersBinaryMetaDataSelfTest;
import org.apache.ignite.internal.binary.noncompact.BinaryFieldsHeapNonCompactSelfTest;
import org.apache.ignite.internal.binary.noncompact.BinaryFieldsOffheapNonCompactSelfTest;
import org.apache.ignite.internal.binary.noncompact.BinaryFooterOffsetsHeapNonCompactSelfTest;
import org.apache.ignite.internal.binary.noncompact.BinaryFooterOffsetsOffheapNonCompactSelfTest;
import org.apache.ignite.internal.binary.noncompact.BinaryMarshallerNonCompactSelfTest;
import org.apache.ignite.internal.binary.noncompact.BinaryObjectBuilderAdditionalNonCompactSelfTest;
import org.apache.ignite.internal.binary.noncompact.BinaryObjectBuilderNonCompactDefaultMappersSelfTest;
import org.apache.ignite.internal.binary.noncompact.BinaryObjectBuilderNonCompactSimpleNameLowerCaseMappersSelfTest;
import org.apache.ignite.internal.binary.streams.BinaryAbstractOutputStreamTest;
import org.apache.ignite.internal.binary.streams.BinaryHeapStreamByteOrderSelfTest;
import org.apache.ignite.internal.binary.streams.BinaryOffheapStreamByteOrderSelfTest;
import org.apache.ignite.internal.processors.cache.binary.BinaryAtomicCacheLocalEntriesSelfTest;
import org.apache.ignite.internal.processors.cache.binary.BinaryMetadataUpdatesFlowTest;
import org.apache.ignite.internal.processors.cache.binary.BinaryTxCacheLocalEntriesSelfTest;
import org.apache.ignite.internal.processors.cache.binary.GridCacheBinaryObjectMetadataExchangeMultinodeTest;
import org.apache.ignite.internal.processors.cache.binary.GridCacheBinaryObjectUserClassloaderSelfTest;
import org.apache.ignite.internal.processors.cache.binary.GridCacheBinaryStoreBinariesDefaultMappersSelfTest;
import org.apache.ignite.internal.processors.cache.binary.GridCacheBinaryStoreBinariesSimpleNameMappersSelfTest;
import org.apache.ignite.internal.processors.cache.binary.GridCacheBinaryStoreObjectsSelfTest;
import org.apache.ignite.internal.processors.cache.binary.GridCacheClientNodeBinaryObjectMetadataMultinodeTest;
import org.apache.ignite.internal.processors.cache.binary.GridCacheClientNodeBinaryObjectMetadataTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsAtomicNearDisabledOnheapSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsAtomicNearDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsAtomicOnheapSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsPartitionedNearDisabledOnheapSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsPartitionedNearDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsPartitionedOnheapSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.replicated.GridCacheBinaryObjectsReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.binary.local.GridCacheBinaryObjectsAtomicLocalSelfTest;
import org.apache.ignite.internal.processors.cache.binary.local.GridCacheBinaryObjectsLocalOnheapSelfTest;
import org.apache.ignite.internal.processors.cache.binary.local.GridCacheBinaryObjectsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteBinaryMetadataUpdateChangingTopologySelfTest;

/**
 * Test for binary objects stored in cache.
 */
public class IgniteBinaryObjectsTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Binary Objects Test Suite");

        suite.addTestSuite(BinarySimpleNameTestPropertySelfTest.class);

        suite.addTestSuite(BinaryBasicIdMapperSelfTest.class);
        suite.addTestSuite(BinaryBasicNameMapperSelfTest.class);

        suite.addTestSuite(BinaryTreeSelfTest.class);
        suite.addTestSuite(BinaryMarshallerSelfTest.class);
        suite.addTestSuite(BinaryObjectExceptionSelfTest.class);

        suite.addTestSuite(BinarySerialiedFieldComparatorSelfTest.class);
        suite.addTestSuite(BinaryArrayIdentityResolverSelfTest.class);

        suite.addTestSuite(BinaryConfigurationConsistencySelfTest.class);
        suite.addTestSuite(BinaryConfigurationCustomSerializerSelfTest.class);
        suite.addTestSuite(GridBinaryMarshallerCtxDisabledSelfTest.class);
        suite.addTestSuite(BinaryObjectBuilderDefaultMappersSelfTest.class);
        suite.addTestSuite(BinaryObjectBuilderSimpleNameLowerCaseMappersSelfTest.class);
        suite.addTestSuite(BinaryObjectBuilderAdditionalSelfTest.class);
        //suite.addTestSuite(BinaryFieldExtractionSelfTest.class);
        suite.addTestSuite(BinaryFieldsHeapSelfTest.class);
        suite.addTestSuite(BinaryFieldsOffheapSelfTest.class);
        suite.addTestSuite(BinaryFooterOffsetsHeapSelfTest.class);
        suite.addTestSuite(BinaryFooterOffsetsOffheapSelfTest.class);
        suite.addTestSuite(BinaryEnumsSelfTest.class);
        suite.addTestSuite(GridDefaultBinaryMappersBinaryMetaDataSelfTest.class);
        suite.addTestSuite(GridSimpleLowerCaseBinaryMappersBinaryMetaDataSelfTest.class);
        suite.addTestSuite(GridBinaryAffinityKeySelfTest.class);
        suite.addTestSuite(GridBinaryWildcardsSelfTest.class);
        suite.addTestSuite(BinaryObjectToStringSelfTest.class);
        suite.addTestSuite(BinaryObjectTypeCompatibilityTest.class);

        // Tests for objects with non-compact footers.
        suite.addTestSuite(BinaryMarshallerNonCompactSelfTest.class);
        suite.addTestSuite(BinaryObjectBuilderNonCompactDefaultMappersSelfTest.class);
        suite.addTestSuite(BinaryObjectBuilderNonCompactSimpleNameLowerCaseMappersSelfTest.class);
        suite.addTestSuite(BinaryObjectBuilderAdditionalNonCompactSelfTest.class);
        suite.addTestSuite(BinaryFieldsHeapNonCompactSelfTest.class);
        suite.addTestSuite(BinaryFieldsOffheapNonCompactSelfTest.class);
        suite.addTestSuite(BinaryFooterOffsetsHeapNonCompactSelfTest.class);
        suite.addTestSuite(BinaryFooterOffsetsOffheapNonCompactSelfTest.class);

        suite.addTestSuite(GridCacheBinaryObjectsLocalSelfTest.class);
        //suite.addTestSuite(GridCacheBinaryObjectsLocalOnheapSelfTest.class);
        suite.addTestSuite(GridCacheBinaryObjectsAtomicLocalSelfTest.class);
        suite.addTestSuite(GridCacheBinaryObjectsReplicatedSelfTest.class);
        suite.addTestSuite(GridCacheBinaryObjectsPartitionedSelfTest.class);
        suite.addTestSuite(GridCacheBinaryObjectsPartitionedNearDisabledSelfTest.class);
        //suite.addTestSuite(GridCacheBinaryObjectsPartitionedNearDisabledOnheapSelfTest.class);
        //suite.addTestSuite(GridCacheBinaryObjectsPartitionedOnheapSelfTest.class);
        suite.addTestSuite(GridCacheBinaryObjectsAtomicSelfTest.class);
        //suite.addTestSuite(GridCacheBinaryObjectsAtomicOnheapSelfTest.class);
        suite.addTestSuite(GridCacheBinaryObjectsAtomicNearDisabledSelfTest.class);
        //suite.addTestSuite(GridCacheBinaryObjectsAtomicNearDisabledOnheapSelfTest.class);

        suite.addTestSuite(GridCacheBinaryStoreObjectsSelfTest.class);
        suite.addTestSuite(GridCacheBinaryStoreBinariesDefaultMappersSelfTest.class);
        suite.addTestSuite(GridCacheBinaryStoreBinariesSimpleNameMappersSelfTest.class);

        suite.addTestSuite(GridCacheClientNodeBinaryObjectMetadataTest.class);
        suite.addTestSuite(GridCacheBinaryObjectMetadataExchangeMultinodeTest.class);
        suite.addTestSuite(BinaryMetadataUpdatesFlowTest.class);
        suite.addTestSuite(GridCacheClientNodeBinaryObjectMetadataMultinodeTest.class);
        suite.addTestSuite(IgniteBinaryMetadataUpdateChangingTopologySelfTest.class);

        suite.addTestSuite(BinaryTxCacheLocalEntriesSelfTest.class);
        suite.addTestSuite(BinaryAtomicCacheLocalEntriesSelfTest.class);

        // Byte order
        suite.addTestSuite(BinaryHeapStreamByteOrderSelfTest.class);
        suite.addTestSuite(BinaryAbstractOutputStreamTest.class);
        suite.addTestSuite(BinaryOffheapStreamByteOrderSelfTest.class);

        suite.addTestSuite(GridCacheBinaryObjectUserClassloaderSelfTest.class);

        return suite;
    }
}

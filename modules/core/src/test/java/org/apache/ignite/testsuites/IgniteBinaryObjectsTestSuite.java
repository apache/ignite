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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.binary.BinaryArrayIdentityResolverSelfTest;
import org.apache.ignite.internal.binary.BinaryBasicIdMapperSelfTest;
import org.apache.ignite.internal.binary.BinaryBasicNameMapperSelfTest;
import org.apache.ignite.internal.binary.BinaryConfigurationConsistencySelfTest;
import org.apache.ignite.internal.binary.BinaryConfigurationCustomSerializerSelfTest;
import org.apache.ignite.internal.binary.BinaryEnumsSelfTest;
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
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsAtomicNearDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsPartitionedNearDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryObjectsPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.replicated.GridCacheBinaryObjectsReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.binary.local.GridCacheBinaryObjectsAtomicLocalSelfTest;
import org.apache.ignite.internal.processors.cache.binary.local.GridCacheBinaryObjectsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteBinaryMetadataUpdateChangingTopologySelfTest;

/**
 * Test for binary objects stored in cache.
 */
public class IgniteBinaryObjectsTestSuite extends TestSuite {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Binary Objects Test Suite");

        suite.addTest(new JUnit4TestAdapter(BinarySimpleNameTestPropertySelfTest.class));

        suite.addTest(new JUnit4TestAdapter(BinaryBasicIdMapperSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryBasicNameMapperSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(BinaryTreeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryMarshallerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryObjectExceptionSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(BinarySerialiedFieldComparatorSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryArrayIdentityResolverSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(BinaryConfigurationConsistencySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryConfigurationCustomSerializerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridBinaryMarshallerCtxDisabledSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryObjectBuilderDefaultMappersSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryObjectBuilderSimpleNameLowerCaseMappersSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryObjectBuilderAdditionalSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(BinaryFieldExtractionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryFieldsHeapSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryFieldsOffheapSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryFooterOffsetsHeapSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryFooterOffsetsOffheapSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryEnumsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridDefaultBinaryMappersBinaryMetaDataSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSimpleLowerCaseBinaryMappersBinaryMetaDataSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridBinaryAffinityKeySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridBinaryWildcardsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryObjectToStringSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryObjectTypeCompatibilityTest.class));

        // Tests for objects with non-compact footers.
        suite.addTest(new JUnit4TestAdapter(BinaryMarshallerNonCompactSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryObjectBuilderNonCompactDefaultMappersSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryObjectBuilderNonCompactSimpleNameLowerCaseMappersSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryObjectBuilderAdditionalNonCompactSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryFieldsHeapNonCompactSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryFieldsOffheapNonCompactSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryFooterOffsetsHeapNonCompactSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryFooterOffsetsOffheapNonCompactSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheBinaryObjectsLocalSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheBinaryObjectsLocalOnheapSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheBinaryObjectsAtomicLocalSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheBinaryObjectsReplicatedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheBinaryObjectsPartitionedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheBinaryObjectsPartitionedNearDisabledSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheBinaryObjectsPartitionedNearDisabledOnheapSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheBinaryObjectsPartitionedOnheapSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheBinaryObjectsAtomicSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheBinaryObjectsAtomicOnheapSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheBinaryObjectsAtomicNearDisabledSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridCacheBinaryObjectsAtomicNearDisabledOnheapSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheBinaryStoreObjectsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheBinaryStoreBinariesDefaultMappersSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheBinaryStoreBinariesSimpleNameMappersSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheClientNodeBinaryObjectMetadataTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheBinaryObjectMetadataExchangeMultinodeTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryMetadataUpdatesFlowTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheClientNodeBinaryObjectMetadataMultinodeTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteBinaryMetadataUpdateChangingTopologySelfTest.class));

        suite.addTest(new JUnit4TestAdapter(BinaryTxCacheLocalEntriesSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryAtomicCacheLocalEntriesSelfTest.class));

        // Byte order
        suite.addTest(new JUnit4TestAdapter(BinaryHeapStreamByteOrderSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryAbstractOutputStreamTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryOffheapStreamByteOrderSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheBinaryObjectUserClassloaderSelfTest.class));

        return suite;
    }
}

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
import org.apache.ignite.internal.portable.GridPortableAffinityKeySelfTest;
import org.apache.ignite.internal.portable.GridIgniteObjectBuilderAdditionalSelfTest;
import org.apache.ignite.internal.portable.GridIgniteObjectBuilderSelfTest;
import org.apache.ignite.internal.portable.GridIgniteObjectBuilderStringAsCharsAdditionalSelfTest;
import org.apache.ignite.internal.portable.GridIgniteObjectBuilderStringAsCharsSelfTest;
import org.apache.ignite.internal.portable.GridPortableMarshallerCtxDisabledSelfTest;
import org.apache.ignite.internal.portable.GridPortableMarshallerSelfTest;
import org.apache.ignite.internal.portable.GridPortableMetaDataDisabledSelfTest;
import org.apache.ignite.internal.portable.GridPortableMetaDataSelfTest;
import org.apache.ignite.internal.portable.GridPortableWildcardsSelfTest;
import org.apache.ignite.internal.processors.cache.portable.GridCacheClientNodeIgniteObjectMetadataMultinodeTest;
import org.apache.ignite.internal.processors.cache.portable.GridCacheClientNodeIgniteObjectMetadataTest;
import org.apache.ignite.internal.processors.cache.portable.GridCachePortableStoreObjectsSelfTest;
import org.apache.ignite.internal.processors.cache.portable.GridCachePortableStorePortablesSelfTest;
import org.apache.ignite.internal.processors.cache.portable.distributed.dht.GridCacheIgniteObjectsAtomicNearDisabledOffheapTieredSelfTest;
import org.apache.ignite.internal.processors.cache.portable.distributed.dht.GridCacheIgniteObjectsAtomicNearDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.portable.distributed.dht.GridCacheIgniteObjectsAtomicOffheapTieredSelfTest;
import org.apache.ignite.internal.processors.cache.portable.distributed.dht.GridCacheIgniteObjectsAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.portable.distributed.dht.GridCacheIgniteObjectsPartitionedNearDisabledOffheapTieredSelfTest;
import org.apache.ignite.internal.processors.cache.portable.distributed.dht.GridCacheIgniteObjectsPartitionedNearDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.portable.distributed.dht.GridCacheIgniteObjectsPartitionedOffheapTieredSelfTest;
import org.apache.ignite.internal.processors.cache.portable.distributed.dht.GridCacheIgniteObjectsPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.portable.distributed.replicated.GridCacheIgniteObjectsReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.portable.local.GridCacheIgniteObjectsAtomicLocalSelfTest;
import org.apache.ignite.internal.processors.cache.portable.local.GridCacheIgniteObjectsLocalOffheapTieredSelfTest;
import org.apache.ignite.internal.processors.cache.portable.local.GridCacheIgniteObjectsLocalSelfTest;

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
        suite.addTestSuite(GridIgniteObjectBuilderSelfTest.class);
        suite.addTestSuite(GridIgniteObjectBuilderStringAsCharsSelfTest.class);
        suite.addTestSuite(GridPortableMetaDataSelfTest.class);
        suite.addTestSuite(GridPortableMetaDataDisabledSelfTest.class);
        suite.addTestSuite(GridPortableAffinityKeySelfTest.class);
        suite.addTestSuite(GridPortableWildcardsSelfTest.class);
        suite.addTestSuite(GridIgniteObjectBuilderAdditionalSelfTest.class);
        suite.addTestSuite(GridIgniteObjectBuilderStringAsCharsAdditionalSelfTest.class);

        suite.addTestSuite(GridCacheIgniteObjectsLocalSelfTest.class);
        suite.addTestSuite(GridCacheIgniteObjectsAtomicLocalSelfTest.class);
        suite.addTestSuite(GridCacheIgniteObjectsReplicatedSelfTest.class);
        suite.addTestSuite(GridCacheIgniteObjectsPartitionedSelfTest.class);
        suite.addTestSuite(GridCacheIgniteObjectsPartitionedNearDisabledSelfTest.class);
        suite.addTestSuite(GridCacheIgniteObjectsAtomicSelfTest.class);
        suite.addTestSuite(GridCacheIgniteObjectsAtomicNearDisabledSelfTest.class);

        suite.addTestSuite(GridCacheIgniteObjectsLocalOffheapTieredSelfTest.class);
        suite.addTestSuite(GridCacheIgniteObjectsAtomicOffheapTieredSelfTest.class);
        suite.addTestSuite(GridCacheIgniteObjectsAtomicNearDisabledOffheapTieredSelfTest.class);
        suite.addTestSuite(GridCacheIgniteObjectsPartitionedOffheapTieredSelfTest.class);
        suite.addTestSuite(GridCacheIgniteObjectsPartitionedNearDisabledOffheapTieredSelfTest.class);

        suite.addTestSuite(GridCachePortableStoreObjectsSelfTest.class);
        suite.addTestSuite(GridCachePortableStorePortablesSelfTest.class);

        suite.addTestSuite(GridCacheClientNodeIgniteObjectMetadataTest.class);
        suite.addTestSuite(GridCacheClientNodeIgniteObjectMetadataMultinodeTest.class);

        return suite;
    }
}

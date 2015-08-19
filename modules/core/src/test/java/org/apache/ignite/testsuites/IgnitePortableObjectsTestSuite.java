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
import org.apache.ignite.internal.processors.cache.portable.*;
import org.apache.ignite.internal.processors.cache.portable.datastreaming.*;
import org.apache.ignite.internal.processors.cache.portable.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.portable.distributed.replicated.*;
import org.apache.ignite.internal.processors.cache.portable.local.*;

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

        suite.addTestSuite(GridCachePortableObjectsLocalSelfTest.class);
        suite.addTestSuite(GridCachePortableObjectsAtomicLocalSelfTest.class);
        suite.addTestSuite(GridCachePortableObjectsReplicatedSelfTest.class);
        suite.addTestSuite(GridCachePortableObjectsPartitionedSelfTest.class);
        suite.addTestSuite(GridCachePortableObjectsPartitionedNearDisabledSelfTest.class);
        suite.addTestSuite(GridCachePortableObjectsAtomicSelfTest.class);
        suite.addTestSuite(GridCachePortableObjectsAtomicNearDisabledSelfTest.class);

        suite.addTestSuite(GridCachePortableObjectsLocalOffheapTieredSelfTest.class);
        suite.addTestSuite(GridCachePortableObjectsAtomicOffheapTieredSelfTest.class);
        suite.addTestSuite(GridCachePortableObjectsAtomicNearDisabledOffheapTieredSelfTest.class);
        suite.addTestSuite(GridCachePortableObjectsPartitionedOffheapTieredSelfTest.class);
        suite.addTestSuite(GridCachePortableObjectsPartitionedNearDisabledOffheapTieredSelfTest.class);

        suite.addTestSuite(GridCachePortableStoreObjectsSelfTest.class);
        suite.addTestSuite(GridCachePortableStorePortablesSelfTest.class);

        suite.addTestSuite(GridCacheClientNodePortableMetadataTest.class);
        suite.addTestSuite(GridCacheClientNodePortableMetadataMultinodeTest.class);

        return suite;
    }
}


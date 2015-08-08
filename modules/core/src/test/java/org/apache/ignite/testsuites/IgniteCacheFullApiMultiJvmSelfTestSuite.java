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

import junit.framework.*;
import org.apache.ignite.internal.processors.cache.multijvm.*;

/**
 * Multi-JVM test suite.
 */
public class IgniteCacheFullApiMultiJvmSelfTestSuite extends TestSuite {
    /**
     * @return Multi-JVM tests suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Cache Full API Multi Jvm Test Suite");

        // Multi-node.
        suite.addTestSuite(GridCacheReplicatedMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedMultiJvmP2PDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedAtomicMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedAtomicPrimaryWriteOrderMultiJvmFullApiSelfTest.class);

        suite.addTestSuite(GridCachePartitionedMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedCopyOnReadDisabledMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicCopyOnReadDisabledMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedMultiJvmP2PDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicMultiJvmP2PDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderMultiJvmP2PDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicNearEnabledMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicNearEnabledPrimaryWriteOrderMultiJvmFullApiSelfTest.class);

        suite.addTestSuite(GridCachePartitionedNearDisabledMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledMultiJvmP2PDisabledFullApiSelfTest.class);

        suite.addTestSuite(GridCacheNearOnlyMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheNearOnlyMultiJvmP2PDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedNearOnlyMultiJvmFullApiSelfTest.class);

        suite.addTestSuite(GridCacheAtomicClientOnlyMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicClientOnlyMultiJvmP2PDisabledFullApiSelfTest.class);

        suite.addTestSuite(GridCacheAtomicNearOnlyMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicNearOnlyMultiJvmP2PDisabledFullApiSelfTest.class);

        suite.addTestSuite(GridCachePartitionedFairAffinityMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledFairAffinityMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicFairAffinityMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicNearEnabledFairAffinityMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderFairAffinityMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheNearOnlyFairAffinityMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicClientOnlyFairAffinityMultiJvmFullApiSelfTest.class);

        // Multi-node with off-heap values.
        suite.addTestSuite(GridCacheReplicatedOffHeapMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedOffHeapMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicOffHeapMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWrityOrderOffHeapMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledOffHeapMultiJvmFullApiSelfTest.class);

        // Multi-node with off-heap tiered mode.
        suite.addTestSuite(GridCacheReplicatedOffHeapTieredMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedOffHeapTieredMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicOffHeapTieredMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWrityOrderOffHeapTieredMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledOffHeapTieredMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledAtomicOffHeapTieredMultiJvmFullApiSelfTest.class);

        return suite;
    }
}

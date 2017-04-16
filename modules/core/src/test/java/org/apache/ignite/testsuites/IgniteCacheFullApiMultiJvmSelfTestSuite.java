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
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheAtomicClientOnlyMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheAtomicClientOnlyMultiJvmP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheAtomicCopyOnReadDisabledMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheAtomicMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheAtomicMultiJvmP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheAtomicNearEnabledMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheAtomicNearOnlyMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheAtomicNearOnlyMultiJvmP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheAtomicOnheapMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheNearOnlyMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheNearOnlyMultiJvmP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCachePartitionedCopyOnReadDisabledMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCachePartitionedMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCachePartitionedMultiJvmP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCachePartitionedNearDisabledAtomicOnheapMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCachePartitionedNearDisabledMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCachePartitionedNearDisabledMultiJvmP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCachePartitionedNearDisabledOnheapMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCachePartitionedOnheapMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheReplicatedAtomicMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheReplicatedMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheReplicatedMultiJvmP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheReplicatedNearOnlyMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheReplicatedOnheapMultiJvmFullApiSelfTest;

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

        System.setProperty("H2_JDBC_CONNECTIONS", "500");

        // Multi-node.
        suite.addTestSuite(GridCacheReplicatedMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedMultiJvmP2PDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedAtomicMultiJvmFullApiSelfTest.class);

        suite.addTestSuite(GridCachePartitionedMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedCopyOnReadDisabledMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicCopyOnReadDisabledMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedMultiJvmP2PDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicMultiJvmP2PDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicNearEnabledMultiJvmFullApiSelfTest.class);

        suite.addTestSuite(GridCachePartitionedNearDisabledMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledMultiJvmP2PDisabledFullApiSelfTest.class);

        suite.addTestSuite(GridCacheNearOnlyMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheNearOnlyMultiJvmP2PDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedNearOnlyMultiJvmFullApiSelfTest.class);

        suite.addTestSuite(GridCacheAtomicClientOnlyMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicClientOnlyMultiJvmP2PDisabledFullApiSelfTest.class);

        suite.addTestSuite(GridCacheAtomicNearOnlyMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicNearOnlyMultiJvmP2PDisabledFullApiSelfTest.class);

        suite.addTestSuite(GridCacheAtomicOnheapMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledAtomicOnheapMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledOnheapMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedOnheapMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedOnheapMultiJvmFullApiSelfTest.class);

        return suite;
    }
}
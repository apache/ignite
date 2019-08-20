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
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Multi-JVM test suite.
 */
@RunWith(AllTests.class)
public class IgniteCacheFullApiMultiJvmSelfTestSuite {
    /**
     * @return Multi-JVM tests suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Cache Full API Multi Jvm Test Suite");

        System.setProperty("H2_JDBC_CONNECTIONS", "500");

        // Multi-node.
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedMultiJvmFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedMultiJvmP2PDisabledFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedAtomicMultiJvmFullApiSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedMultiJvmFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedCopyOnReadDisabledMultiJvmFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicMultiJvmFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicCopyOnReadDisabledMultiJvmFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedMultiJvmP2PDisabledFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicMultiJvmP2PDisabledFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicNearEnabledMultiJvmFullApiSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedNearDisabledMultiJvmFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedNearDisabledMultiJvmP2PDisabledFullApiSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheNearOnlyMultiJvmFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheNearOnlyMultiJvmP2PDisabledFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedNearOnlyMultiJvmFullApiSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicClientOnlyMultiJvmFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicClientOnlyMultiJvmP2PDisabledFullApiSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicNearOnlyMultiJvmFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicNearOnlyMultiJvmP2PDisabledFullApiSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicOnheapMultiJvmFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedNearDisabledAtomicOnheapMultiJvmFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedNearDisabledOnheapMultiJvmFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedOnheapMultiJvmFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedOnheapMultiJvmFullApiSelfTest.class));

        return suite;
    }
}

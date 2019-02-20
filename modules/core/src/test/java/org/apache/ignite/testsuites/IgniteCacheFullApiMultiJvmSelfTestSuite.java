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
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Multi-JVM test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridCacheReplicatedMultiJvmFullApiSelfTest.class,
    GridCacheReplicatedMultiJvmP2PDisabledFullApiSelfTest.class,
    GridCacheReplicatedAtomicMultiJvmFullApiSelfTest.class,

    GridCachePartitionedMultiJvmFullApiSelfTest.class,
    GridCachePartitionedCopyOnReadDisabledMultiJvmFullApiSelfTest.class,
    GridCacheAtomicMultiJvmFullApiSelfTest.class,
    GridCacheAtomicCopyOnReadDisabledMultiJvmFullApiSelfTest.class,
    GridCachePartitionedMultiJvmP2PDisabledFullApiSelfTest.class,
    GridCacheAtomicMultiJvmP2PDisabledFullApiSelfTest.class,
    GridCacheAtomicNearEnabledMultiJvmFullApiSelfTest.class,

    GridCachePartitionedNearDisabledMultiJvmFullApiSelfTest.class,
    GridCachePartitionedNearDisabledMultiJvmP2PDisabledFullApiSelfTest.class,

    GridCacheNearOnlyMultiJvmFullApiSelfTest.class,
    GridCacheNearOnlyMultiJvmP2PDisabledFullApiSelfTest.class,
    GridCacheReplicatedNearOnlyMultiJvmFullApiSelfTest.class,

    GridCacheAtomicClientOnlyMultiJvmFullApiSelfTest.class,
    GridCacheAtomicClientOnlyMultiJvmP2PDisabledFullApiSelfTest.class,

    GridCacheAtomicNearOnlyMultiJvmFullApiSelfTest.class,
    GridCacheAtomicNearOnlyMultiJvmP2PDisabledFullApiSelfTest.class,

    GridCacheAtomicOnheapMultiJvmFullApiSelfTest.class,
    GridCachePartitionedNearDisabledAtomicOnheapMultiJvmFullApiSelfTest.class,
    GridCachePartitionedNearDisabledOnheapMultiJvmFullApiSelfTest.class,
    GridCachePartitionedOnheapMultiJvmFullApiSelfTest.class,
    GridCacheReplicatedOnheapMultiJvmFullApiSelfTest.class
})
public class IgniteCacheFullApiMultiJvmSelfTestSuite {
    /** */
    @BeforeClass
    public static void init() {
        System.setProperty("H2_JDBC_CONNECTIONS", "500");
    }
}

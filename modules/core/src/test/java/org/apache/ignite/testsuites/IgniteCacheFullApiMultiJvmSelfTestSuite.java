/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

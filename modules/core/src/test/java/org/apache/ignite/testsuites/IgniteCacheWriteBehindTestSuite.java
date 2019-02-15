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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.processors.cache.GridCachePartitionedWritesTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStoreLocalTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStoreMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStorePartitionedMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStorePartitionedTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStoreReplicatedTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStoreSelfTest;
import org.apache.ignite.internal.processors.cache.store.IgnteCacheClientWriteBehindStoreAtomicTest;
import org.apache.ignite.internal.processors.cache.store.IgnteCacheClientWriteBehindStoreNonCoalescingTest;
import org.apache.ignite.internal.processors.cache.store.IgnteCacheClientWriteBehindStoreTxTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite that contains all tests for {@link org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStore}.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheWriteBehindTestSuite {
    /**
     * @return Ignite Bamboo in-memory data grid test suite.
     */
    public static List<Class<?>> suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     */
    public static List<Class<?>> suite(Collection<Class> ignoredTests) {
        List<Class<?>> suite = new ArrayList<>();

        // Write-behind tests.
        GridTestUtils.addTestIfNeeded(suite, GridCacheWriteBehindStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheWriteBehindStoreMultithreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheWriteBehindStoreLocalTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheWriteBehindStoreReplicatedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheWriteBehindStorePartitionedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheWriteBehindStorePartitionedMultiNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedWritesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnteCacheClientWriteBehindStoreAtomicTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnteCacheClientWriteBehindStoreTxTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnteCacheClientWriteBehindStoreNonCoalescingTest.class, ignoredTests);

        return suite;
    }
}

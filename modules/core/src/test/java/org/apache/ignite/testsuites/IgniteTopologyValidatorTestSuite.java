/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.testsuites;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorGridSplitCacheTest;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorNearPartitionedAtomicCacheGroupsTest;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorNearPartitionedAtomicCacheTest;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorNearPartitionedTxCacheGroupsTest;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorNearPartitionedTxCacheTest;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorPartitionedAtomicCacheGroupsTest;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorPartitionedAtomicCacheTest;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorPartitionedTxCacheGroupsTest;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorPartitionedTxCacheTest;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorReplicatedAtomicCacheGroupsTest;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorReplicatedAtomicCacheTest;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorReplicatedTxCacheGroupsTest;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorReplicatedTxCacheTest;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Topology validator test suite.
 */
public class IgniteTopologyValidatorTestSuite {
    /**
     * @param ignoredTests Ignored tests.
     * @return Topology validator tests suite.
     */
    public static List<Class<?>> suite(Collection<Class> ignoredTests) {
        List<Class<?>> suite = new ArrayList<>();

        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorNearPartitionedAtomicCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorNearPartitionedTxCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorPartitionedAtomicCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorPartitionedTxCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorReplicatedAtomicCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorReplicatedTxCacheTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorNearPartitionedAtomicCacheGroupsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorNearPartitionedTxCacheGroupsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorPartitionedAtomicCacheGroupsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorPartitionedTxCacheGroupsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorReplicatedAtomicCacheGroupsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorReplicatedTxCacheGroupsTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorGridSplitCacheTest.class, ignoredTests);

        return suite;
    }
}

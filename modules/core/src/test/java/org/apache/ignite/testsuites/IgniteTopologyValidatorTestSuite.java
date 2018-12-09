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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
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
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Topology validator test suite.
 */
@RunWith(AllTests.class)
public class IgniteTopologyValidatorTestSuite {
    /**
     * @return Topology validator tests suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Topology validator Test Suite");

        suite.addTest(new JUnit4TestAdapter(IgniteTopologyValidatorNearPartitionedAtomicCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteTopologyValidatorNearPartitionedTxCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteTopologyValidatorPartitionedAtomicCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteTopologyValidatorPartitionedTxCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteTopologyValidatorReplicatedAtomicCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteTopologyValidatorReplicatedTxCacheTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteTopologyValidatorNearPartitionedAtomicCacheGroupsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteTopologyValidatorNearPartitionedTxCacheGroupsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteTopologyValidatorPartitionedAtomicCacheGroupsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteTopologyValidatorPartitionedTxCacheGroupsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteTopologyValidatorReplicatedAtomicCacheGroupsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteTopologyValidatorReplicatedTxCacheGroupsTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteTopologyValidatorGridSplitCacheTest.class));

        return suite;
    }
}

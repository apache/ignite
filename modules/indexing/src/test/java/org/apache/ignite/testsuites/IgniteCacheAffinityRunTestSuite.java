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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.IgniteCacheLockPartitionOnAffinityRunAtomicCacheOpTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLockPartitionOnAffinityRunTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLockPartitionOnAffinityRunTxCacheOpTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLockPartitionOnAffinityRunWithCollisionSpiTest;
import org.apache.ignite.internal.processors.database.baseline.IgniteBaselineLockPartitionOnAffinityRunAtomicCacheTest;
import org.apache.ignite.internal.processors.database.baseline.IgniteBaselineLockPartitionOnAffinityRunTxCacheTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Compute and Cache tests for affinityRun/Call. These tests is extracted into separate suite
 * because ones take a lot of time.
 */
@RunWith(AllTests.class)
public class IgniteCacheAffinityRunTestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Compute and Cache Affinity Run Test Suite");

        suite.addTest(new JUnit4TestAdapter(IgniteCacheLockPartitionOnAffinityRunTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheLockPartitionOnAffinityRunWithCollisionSpiTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheLockPartitionOnAffinityRunAtomicCacheOpTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteBaselineLockPartitionOnAffinityRunAtomicCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteBaselineLockPartitionOnAffinityRunTxCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheLockPartitionOnAffinityRunTxCacheOpTest.class));

        return suite;
    }
}

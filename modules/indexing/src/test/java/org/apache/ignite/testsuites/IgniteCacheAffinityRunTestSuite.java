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
import org.apache.ignite.internal.processors.cache.IgniteCacheLockPartitionOnAffinityRunAtomicCacheOpTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLockPartitionOnAffinityRunTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLockPartitionOnAffinityRunTxCacheOpTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLockPartitionOnAffinityRunWithCollisionSpiTest;
import org.apache.ignite.internal.processors.database.baseline.IgniteBaselineLockPartitionOnAffinityRunAtomicCacheTest;
import org.apache.ignite.internal.processors.database.baseline.IgniteBaselineLockPartitionOnAffinityRunTxCacheTest;

/**
 * Compute and Cache tests for affinityRun/Call. These tests is extracted into separate suite
 * because ones take a lot of time.
 */
public class IgniteCacheAffinityRunTestSuite extends TestSuite {
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

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
import org.apache.ignite.cache.ResetLostPartitionTest;
import org.apache.ignite.internal.processors.cache.IgniteClusterActivateDeactivateTestWithPersistenceAndMemoryReuse;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.IgniteRebalanceOnCachesStoppingOrDestroyingTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsTaskCancelingTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsCacheWalDisabledOnRebalancingTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsPartitionPreloadTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsStartWIthEmptyArchive;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsStartWIthEmptyArchiveWALFsync;

/**
 *
 */
public class IgnitePdsTestSuite4 extends TestSuite {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Persistent Store Test Suite 4");

        suite.addTestSuite(IgniteClusterActivateDeactivateTestWithPersistenceAndMemoryReuse.class);

        suite.addTestSuite(IgnitePdsPartitionPreloadTest.class);

        suite.addTestSuite(ResetLostPartitionTest.class);

        suite.addTestSuite(IgniteRebalanceOnCachesStoppingOrDestroyingTest.class);

        suite.addTestSuite(IgnitePdsCacheWalDisabledOnRebalancingTest.class);

        suite.addTestSuite(IgnitePdsTaskCancelingTest.class);

        suite.addTestSuite(IgnitePdsStartWIthEmptyArchive.class);
        suite.addTestSuite(IgnitePdsStartWIthEmptyArchiveWALFsync.class);

        return suite;
    }
}

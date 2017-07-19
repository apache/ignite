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
import org.apache.ignite.internal.processors.cache.CacheJndiTmFactorySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheJtaConfigurationValidationSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheJtaFactoryConfigValidationSelfTest;
import org.apache.ignite.internal.processors.cache.GridJtaTransactionManagerSelfTest;
import org.apache.ignite.internal.processors.cache.GridPartitionedCacheJtaFactorySelfTest;
import org.apache.ignite.internal.processors.cache.GridPartitionedCacheJtaFactoryUseSyncSelfTest;
import org.apache.ignite.internal.processors.cache.GridPartitionedCacheJtaLookupClassNameSelfTest;
import org.apache.ignite.internal.processors.cache.GridReplicatedCacheJtaFactorySelfTest;
import org.apache.ignite.internal.processors.cache.GridReplicatedCacheJtaFactoryUseSyncSelfTest;
import org.apache.ignite.internal.processors.cache.GridReplicatedCacheJtaLookupClassNameSelfTest;
import org.apache.ignite.internal.processors.cache.GridJtaLifecycleAwareSelfTest;
import org.apache.ignite.testframework.IgniteTestSuite;

/**
 * JTA integration tests.
 */
public class IgniteJtaTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new IgniteTestSuite("JTA Integration Test Suite");

        suite.addTestSuite(GridPartitionedCacheJtaFactorySelfTest.class);
        suite.addTestSuite(GridReplicatedCacheJtaFactorySelfTest.class);

        suite.addTestSuite(GridPartitionedCacheJtaLookupClassNameSelfTest.class);
        suite.addTestSuite(GridReplicatedCacheJtaLookupClassNameSelfTest.class);

        suite.addTestSuite(GridPartitionedCacheJtaFactoryUseSyncSelfTest.class);
        suite.addTestSuite(GridReplicatedCacheJtaFactoryUseSyncSelfTest.class);

        suite.addTestSuite(GridJtaLifecycleAwareSelfTest.class);
        suite.addTestSuite(GridCacheJtaConfigurationValidationSelfTest.class);
        suite.addTestSuite(GridCacheJtaFactoryConfigValidationSelfTest.class);

        suite.addTestSuite(GridJtaTransactionManagerSelfTest.class);

        // Factory
        suite.addTestSuite(CacheJndiTmFactorySelfTest.class);

        return suite;
    }
}

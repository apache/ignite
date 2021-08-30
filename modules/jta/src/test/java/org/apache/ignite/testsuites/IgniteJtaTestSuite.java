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

import org.apache.ignite.internal.processors.cache.CacheJndiTmFactorySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheJtaConfigurationValidationSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheJtaFactoryConfigValidationSelfTest;
import org.apache.ignite.internal.processors.cache.GridJtaLifecycleAwareSelfTest;
import org.apache.ignite.internal.processors.cache.GridJtaTransactionManagerSelfTest;
import org.apache.ignite.internal.processors.cache.jta.GridPartitionedCacheJtaFactorySelfTest;
import org.apache.ignite.internal.processors.cache.jta.GridPartitionedCacheJtaFactoryUseSyncSelfTest;
import org.apache.ignite.internal.processors.cache.jta.GridPartitionedCacheJtaLookupClassNameSelfTest;
import org.apache.ignite.internal.processors.cache.jta.GridReplicatedCacheJtaFactorySelfTest;
import org.apache.ignite.internal.processors.cache.jta.GridReplicatedCacheJtaFactoryUseSyncSelfTest;
import org.apache.ignite.internal.processors.cache.jta.GridReplicatedCacheJtaLookupClassNameSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * JTA integration tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridPartitionedCacheJtaFactorySelfTest.class,
    GridReplicatedCacheJtaFactorySelfTest.class,

    GridPartitionedCacheJtaLookupClassNameSelfTest.class,
    GridReplicatedCacheJtaLookupClassNameSelfTest.class,

    GridPartitionedCacheJtaFactoryUseSyncSelfTest.class,
    GridReplicatedCacheJtaFactoryUseSyncSelfTest.class,

    GridJtaLifecycleAwareSelfTest.class,
    GridCacheJtaConfigurationValidationSelfTest.class,
    GridCacheJtaFactoryConfigValidationSelfTest.class,

    GridJtaTransactionManagerSelfTest.class,

    // Factory
    CacheJndiTmFactorySelfTest.class
})
public class IgniteJtaTestSuite {
}

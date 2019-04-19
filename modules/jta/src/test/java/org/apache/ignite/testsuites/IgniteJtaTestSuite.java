/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.internal.processors.cache.GridJtaTransactionManagerSelfTest;
import org.apache.ignite.internal.processors.cache.jta.GridPartitionedCacheJtaFactorySelfTest;
import org.apache.ignite.internal.processors.cache.jta.GridPartitionedCacheJtaFactoryUseSyncSelfTest;
import org.apache.ignite.internal.processors.cache.jta.GridPartitionedCacheJtaLookupClassNameSelfTest;
import org.apache.ignite.internal.processors.cache.jta.GridReplicatedCacheJtaFactorySelfTest;
import org.apache.ignite.internal.processors.cache.jta.GridReplicatedCacheJtaFactoryUseSyncSelfTest;
import org.apache.ignite.internal.processors.cache.jta.GridReplicatedCacheJtaLookupClassNameSelfTest;
import org.apache.ignite.internal.processors.cache.GridJtaLifecycleAwareSelfTest;
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

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testsuites.broken;

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.CacheOperationsWithExpirationTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinNoIndexTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheJoinPartitionedAndReplicatedTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartSelfTest2;
import org.apache.ignite.internal.processors.cache.index.SchemaExchangeSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryAsyncFailoverAtomicSelfTest;
import org.apache.ignite.testframework.IgniteTestSuite;

/**
 *
 */
public class IgniteCacheQuerySelfBrokenTestSuit extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        IgniteTestSuite suite = new IgniteTestSuite("Ignite Cache Queries Broken Test Suite");

        suite.addTestSuite(IgniteCacheJoinPartitionedAndReplicatedTest.class);
        suite.addTestSuite(CacheContinuousQueryAsyncFailoverAtomicSelfTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinNoIndexTest.class);
        suite.addTestSuite(CacheOperationsWithExpirationTest.class);
        suite.addTestSuite(SchemaExchangeSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryNodeRestartSelfTest2.class);

        return suite;
    }
}
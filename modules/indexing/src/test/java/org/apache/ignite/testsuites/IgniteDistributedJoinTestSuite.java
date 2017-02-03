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
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinCollocatedAndNotTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinCustomAffinityMapper;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinNoIndexTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinPartitionedAndReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinQueryConditionsTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryStopOnCancelOrTimeoutDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.query.h2.sql.H2CompareBigQueryDistributedJoinsTest;

/**
 *
 */
public class IgniteDistributedJoinTestSuite extends TestSuite {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Distributed Joins Test Suite.");

        suite.addTestSuite(H2CompareBigQueryDistributedJoinsTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinCollocatedAndNotTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinCustomAffinityMapper.class);
        suite.addTestSuite(IgniteCacheDistributedJoinNoIndexTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinPartitionedAndReplicatedTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinQueryConditionsTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinTest.class);
        suite.addTestSuite(IgniteCacheQueryNodeRestartDistributedJoinSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryStopOnCancelOrTimeoutDistributedJoinSelfTest.class);
        suite.addTestSuite(IgniteSqlDistributedJoinSelfTest.class);

        return suite;
    }
}

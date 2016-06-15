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
import org.apache.ignite.internal.processors.cache.distributed.CacheGetInsideLockChangingTopologyTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCachePutRetryAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCachePutRetryTransactionalSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.IgniteCachePutRetryAtomicPrimaryWriteOrderSelfTest;

/**
 * Test suite.
 */
public class IgniteCacheFailoverTestSuite3 extends TestSuite {
    /**
     * @return Ignite Cache Failover test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Cache Failover Test Suite3");

        suite.addTestSuite(IgniteCachePutRetryAtomicSelfTest.class);
        suite.addTestSuite(IgniteCachePutRetryAtomicPrimaryWriteOrderSelfTest.class);
        suite.addTestSuite(IgniteCachePutRetryTransactionalSelfTest.class);
        suite.addTestSuite(CacheGetInsideLockChangingTopologyTest.class);

        return suite;
    }
}

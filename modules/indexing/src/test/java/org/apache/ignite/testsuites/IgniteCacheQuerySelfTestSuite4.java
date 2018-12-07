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
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryAsyncFailoverAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryAsyncFailoverMvccTxSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryAsyncFailoverTxReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryAsyncFailoverTxSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFailoverAtomicReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFailoverAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFailoverMvccTxReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFailoverMvccTxSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFailoverTxReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFailoverTxSelfTest;

/**
 * Test suite for cache queries.
 */
public class IgniteCacheQuerySelfTestSuite4 extends TestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Cache Queries Test Suite 4");

        // Continuous queries failover tests.
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryFailoverAtomicSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryFailoverAtomicReplicatedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryFailoverTxSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryFailoverTxReplicatedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryFailoverMvccTxSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryFailoverMvccTxReplicatedSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryAsyncFailoverAtomicSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryAsyncFailoverTxReplicatedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryAsyncFailoverTxSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryAsyncFailoverMvccTxSelfTest.class));

        return suite;
    }
}

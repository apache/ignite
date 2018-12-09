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
import org.apache.ignite.internal.processors.cache.GridCachePutAllFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicPutAllFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePutAllRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheAtomicNodeRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheGetRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheAtomicReplicatedNodeRestartSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Cache stability test suite on changing topology.
 */
@RunWith(AllTests.class)
public class IgniteCacheRestartTestSuite2 {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Cache Restart Test Suite2");

        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicNodeRestartTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicReplicatedNodeRestartSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicPutAllFailoverSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCachePutAllRestartTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePutAllFailoverSelfTest.class));

        // TODO IGNITE-4768.
        //suite.addTest(new JUnit4TestAdapter(IgniteBinaryMetadataUpdateNodeRestartTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheGetRestartTest.class));

        return suite;
    }
}

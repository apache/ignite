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
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryVariationsTest;
import org.apache.ignite.testframework.configvariations.ConfigVariationsTestSuiteBuilder;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISCOVERY_HISTORY_SIZE;

/**
 * Test suite for cache queries.
 */
public class IgniteContinuousQueryConfigVariationsSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        System.setProperty(IGNITE_DISCOVERY_HISTORY_SIZE, "100");

        TestSuite suite = new TestSuite("Ignite Continuous Query Config Variations Suite");

        CacheContinuousQueryVariationsTest.singleNode = false;

        suite.addTest(new ConfigVariationsTestSuiteBuilder(
            "5 nodes 1 backup",
            CacheContinuousQueryVariationsTest.class)
            .withBasicCacheParams()
            .gridsCount(5)
            .backups(2)
            .build());

        CacheContinuousQueryVariationsTest.singleNode = true;

        suite.addTest(new ConfigVariationsTestSuiteBuilder(
            "Single node",
            CacheContinuousQueryVariationsTest.class)
            .withBasicCacheParams()
            .gridsCount(1)
            .build());

        return suite;
    }
}

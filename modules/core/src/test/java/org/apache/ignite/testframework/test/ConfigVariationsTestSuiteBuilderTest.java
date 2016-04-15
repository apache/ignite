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

package org.apache.ignite.testframework.test;

import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.configvariations.ConfigVariationsTestSuiteBuilder;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;

/**
 *
 */
public class ConfigVariationsTestSuiteBuilderTest extends TestCase {
    /**
     * @throws Exception If failed.
     */
    public void testDefaults() throws Exception {
        TestSuite dfltSuite = new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class).build();

        assertEquals(4, dfltSuite.countTestCases());

        TestSuite dfltCacheSuite = new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class)
            .withBasicCacheParams().build();

        assertEquals(4 * 12, dfltCacheSuite.countTestCases());

        // With clients.
        dfltSuite = new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class)
            .testedNodesCount(2).withClients().build();

        assertEquals(4 * 2, dfltSuite.countTestCases());

        dfltCacheSuite = new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class)
            .withBasicCacheParams().testedNodesCount(3).withClients().build();

        assertEquals(4 * 12 * 3, dfltCacheSuite.countTestCases());
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    public void testIgniteConfigFilter() throws Exception {
        TestSuite dfltSuite = new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class).build();

        final AtomicInteger cnt = new AtomicInteger();

        TestSuite filteredSuite = new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class)
            .withIgniteConfigFilters(new IgnitePredicate<IgniteConfiguration>() {
                @Override public boolean apply(IgniteConfiguration configuration) {
                    return cnt.getAndIncrement() % 2 == 0;
                }
            })
            .build();

        assertEquals(dfltSuite.countTestCases() / 2, filteredSuite.countTestCases());
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    public void testCacheConfigFilter() throws Exception {
        TestSuite dfltSuite = new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class)
            .withBasicCacheParams()
            .build();

        final AtomicInteger cnt = new AtomicInteger();

        TestSuite filteredSuite = new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class)
            .withBasicCacheParams()
            .withCacheConfigFilters(new IgnitePredicate<CacheConfiguration>() {
                @Override public boolean apply(CacheConfiguration configuration) {
                    return cnt.getAndIncrement() % 2 == 0;
                }
            })
            .build();

        assertEquals(dfltSuite.countTestCases() / 2, filteredSuite.countTestCases());
    }

    /**
     *
     */
    private static class NoopTest extends IgniteConfigVariationsAbstractTest {
        /**
         * @throws Exception If failed.
         */
        public void test1() throws Exception {
            // No-op.
        }
    }
}

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

package org.apache.ignite.testframework.test;

import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.TestSuite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.configvariations.ConfigVariationsTestSuiteBuilder;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class ConfigVariationsTestSuiteBuilderTest {
    /** */
    @Test
    public void testDefaults() {
        TestSuite dfltSuite = new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class).build();

        assertEquals(4, dfltSuite.countTestCases());

        TestSuite dfltCacheSuite = new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class)
            .withBasicCacheParams().build();

        assertEquals(4 * 4 * 2, dfltCacheSuite.countTestCases());

        // With clients.
        dfltSuite = new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class)
            .testedNodesCount(2).withClients().build();

        assertEquals(4 * 2, dfltSuite.countTestCases());

        dfltCacheSuite = new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class)
            .withBasicCacheParams().testedNodesCount(3).withClients().build();

        assertEquals(4 * 4 * 2 * 3, dfltCacheSuite.countTestCases());
    }

    /** */
    @SuppressWarnings("serial")
    @Test
    public void testIgniteConfigFilter() {
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

    /** */
    @SuppressWarnings("serial")
    @Test
    public void testCacheConfigFilter() {
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

    /** */
    public static class NoopTest extends IgniteConfigVariationsAbstractTest {
        /** */
        @Test
        public void test1() {
            // No-op.
        }
    }
}

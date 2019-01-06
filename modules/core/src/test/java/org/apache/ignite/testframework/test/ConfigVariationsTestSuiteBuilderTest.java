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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.configvariations.ConfigVariationsTestSuiteBuilder;
import org.apache.ignite.testframework.configvariations.VariationsTestsConfig;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class ConfigVariationsTestSuiteBuilderTest { // todo test how this runs when invoked from IgniteBasicTestSuite
    /** */
    @Test
    public void testDefaults() {
        List<Class<? extends IgniteConfigVariationsAbstractTest>> classes = new ArrayList<>();
        List<VariationsTestsConfig> cfgs = new ArrayList<>();

        new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class).appendTo(classes, cfgs);

        assertEquals(4, classes.size());
        assertEquals(4, cfgs.size());

        classes = new ArrayList<>();
        cfgs = new ArrayList<>();
        new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class)
            .withBasicCacheParams().appendTo(classes, cfgs);

        assertEquals(4 * 4 * 2, classes.size());
        assertEquals(4 * 4 * 2, cfgs.size());

        // With clients.
        new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class)
            .testedNodesCount(2).withClients().appendTo(classes, cfgs);

        assertEquals(4 * 4 * 2 + 4 * 2, classes.size());
        assertEquals(4 * 4 * 2 + 4 * 2, cfgs.size());

        new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class)
            .withBasicCacheParams().testedNodesCount(3).withClients().appendTo(classes, cfgs);

        assertEquals((4 * 4 * 2 + 4 * 2) + 4 * 4 * 2 * 3, classes.size());
        assertEquals((4 * 4 * 2 + 4 * 2) + 4 * 4 * 2 * 3, classes.size());
    }

    /** */
    @SuppressWarnings("serial")
    @Test
    public void testIgniteConfigFilter() {
        List<Class<? extends IgniteConfigVariationsAbstractTest>> classes = new ArrayList<>();
        List<VariationsTestsConfig> cfgs = new ArrayList<>();
        new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class).appendTo(classes, cfgs);

        final AtomicInteger cnt = new AtomicInteger();

        List<Class<? extends IgniteConfigVariationsAbstractTest>> filteredClasses = new ArrayList<>();
        List<VariationsTestsConfig> filteredCfgs = new ArrayList<>();
        new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class)
            .withIgniteConfigFilters(new IgnitePredicate<IgniteConfiguration>() {
                @Override public boolean apply(IgniteConfiguration configuration) {
                    return cnt.getAndIncrement() % 2 == 0;
                }
            })
            .appendTo(filteredClasses, filteredCfgs);

        assertEquals(classes.size() / 2, filteredClasses.size());
        assertEquals(filteredClasses.size(), filteredCfgs.size());
    }

    /** */
    @SuppressWarnings("serial")
    @Test
    public void testCacheConfigFilter() {
        List<Class<? extends IgniteConfigVariationsAbstractTest>> classes = new ArrayList<>();
        List<VariationsTestsConfig> cfgs = new ArrayList<>();
        new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class)
            .withBasicCacheParams()
            .appendTo(classes, cfgs);

        final AtomicInteger cnt = new AtomicInteger();

        List<Class<? extends IgniteConfigVariationsAbstractTest>> filteredClasses = new ArrayList<>();
        List<VariationsTestsConfig> filteredCfgs = new ArrayList<>();
        new ConfigVariationsTestSuiteBuilder("testSuite", NoopTest.class)
            .withBasicCacheParams()
            .withCacheConfigFilters(new IgnitePredicate<CacheConfiguration>() {
                @Override public boolean apply(CacheConfiguration configuration) {
                    return cnt.getAndIncrement() % 2 == 0;
                }
            })
            .appendTo(filteredClasses, filteredCfgs);

        assertEquals(classes.size() / 2, filteredClasses.size());
        assertEquals(filteredClasses.size(), filteredCfgs.size());
    }

    /** */
    public static class NoopTest extends IgniteConfigVariationsAbstractTest {
        /** */
        @Test
        public void test1() {
            // No-op.
        }
    }

    /** */
    @Ignore
    public static class NoopTestIgnored extends IgniteConfigVariationsAbstractTest { // todo use in test
        /** */
        @Test
        public void test1() {
            // No-op.
        }
    }

    /** */
    public static class NoopTestExtendsIgnored extends NoopTestIgnored { // todo use in test
        /** */
        @Test
        public void test2() {
            // No-op.
        }
    }
}

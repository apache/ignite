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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.configvariations.ConfigVariationsTestSuiteBuilder;
import org.apache.ignite.testframework.configvariations.VariationsTestsConfig;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;

import static org.junit.Assert.assertEquals;

/**
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    ConfigVariationsTestSuiteBuilderTest.BasicTest.class,
    ConfigVariationsTestSuiteBuilderTest.TestSuiteBasic.class,
    ConfigVariationsTestSuiteBuilderTest.TestSuiteWithIgnored.class,
    ConfigVariationsTestSuiteBuilderTest.TestSuiteWithExtendsIgnored.class
})
public class ConfigVariationsTestSuiteBuilderTest {
    /** */
    private static List<Class<? extends IgniteConfigVariationsAbstractTest>> basicBuild(
        Class<? extends IgniteConfigVariationsAbstractTest> cls, List<VariationsTestsConfig> cfgs) {
        List<Class<? extends IgniteConfigVariationsAbstractTest>> classes = new ArrayList<>();

        new ConfigVariationsTestSuiteBuilder("testSuite", cls).appendTo(classes, cfgs);

        return classes;
    }

    /** */
    public static class BasicTest {
        /** */
        @Test
        public void testDefaults() {
            List<VariationsTestsConfig> cfgs = new ArrayList<>();
            List<Class<? extends IgniteConfigVariationsAbstractTest>> classes = basicBuild(NoopTest.class, cfgs);

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
            List<VariationsTestsConfig> cfgs = new ArrayList<>();
            List<Class<? extends IgniteConfigVariationsAbstractTest>> classes = basicBuild(NoopTest.class, cfgs);

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
    }

    /** */
    @RunWith(ConfigVariationsTestSuiteBuilderTest.SuiteBasic.class)
    public static class TestSuiteBasic {
        /** **/
        private static final AtomicBoolean alreadyRun = new AtomicBoolean(false);

        /** */
        @BeforeClass
        public static void init() {
            Assume.assumeFalse("This test already has run.", alreadyRun.getAndSet(true));
        }

        /** */
        @AfterClass
        public static void verify() {
            assertEquals(4, SuiteBasic.cntr.get());
            assertEquals(SuiteBasic.cntr.get(), SuiteBasic.cfgs.size());
        }
    }

    /** */
    @RunWith(ConfigVariationsTestSuiteBuilderTest.SuiteWithIgnored.class)
    public static class TestSuiteWithIgnored {
        /** **/
        private static final AtomicBoolean alreadyRun = new AtomicBoolean(false);

        /** */
        @BeforeClass
        public static void init() {
            Assume.assumeFalse("This test already has run.", alreadyRun.getAndSet(true));
        }

        /** */
        @AfterClass
        public static void verify() {
            assertEquals(8, SuiteWithIgnored.cntr.get());
            assertEquals(SuiteWithIgnored.cntr.get(), SuiteWithIgnored.cfgs.size());
        }
    }

    /** */
    @RunWith(ConfigVariationsTestSuiteBuilderTest.SuiteWithExtendsIgnored.class)
    public static class TestSuiteWithExtendsIgnored {
        /** **/
        private static final AtomicBoolean alreadyRun = new AtomicBoolean(false);

        /** */
        @BeforeClass
        public static void init() {
            Assume.assumeFalse("This test already has run.", alreadyRun.getAndSet(true));
        }

        /** */
        @AfterClass
        public static void verify() {
            assertEquals(4, SuiteWithExtendsIgnored.cntr.get());
            assertEquals(SuiteWithExtendsIgnored.cntr.get(), SuiteWithExtendsIgnored.cfgs.size());
        }
    }

    /** */
    public static class SuiteBasic extends Suite {
        /** */
        private static final List<VariationsTestsConfig> cfgs = new ArrayList<>();

        /** */
        private static final List<Class<? extends IgniteConfigVariationsAbstractTest>> classes
            = basicBuild(NoopTest.class, cfgs);

        /** */
        private static final AtomicInteger cntr = new AtomicInteger(0);

        /** */
        public SuiteBasic(Class<?> cls) throws InitializationError {
            super(cls, classes.toArray(new Class<?>[] {null}));
        }

        /** */
        @Override protected void runChild(Runner runner, RunNotifier ntf) {
            IgniteConfigVariationsAbstractTest.injectTestsConfiguration(cfgs.get(cntr.getAndIncrement()));
            super.runChild(runner, ntf);
        }
    }

    /** */
    public static class SuiteWithIgnored extends Suite {
        /** */
        private static final List<VariationsTestsConfig> cfgs = new ArrayList<>();

        /** */
        private static final List<Class<? extends IgniteConfigVariationsAbstractTest>> classes = Stream
            .concat(basicBuild(NoopTest.class, cfgs).stream(), basicBuild(NoopTestIgnored.class, cfgs).stream())
            .collect(Collectors.toList());

        /** */
        private static final AtomicInteger cntr = new AtomicInteger(0);

        /** */
        public SuiteWithIgnored(Class<?> cls) throws InitializationError {
            super(cls, classes.toArray(new Class<?>[] {null}));
        }

        /** */
        @Override protected void runChild(Runner runner, RunNotifier ntf) {
            IgniteConfigVariationsAbstractTest.injectTestsConfiguration(cfgs.get(cntr.getAndIncrement()));
            super.runChild(runner, ntf);
        }
    }

    /** */
    public static class SuiteWithExtendsIgnored extends Suite {
        /** */
        private static final List<VariationsTestsConfig> cfgs = new ArrayList<>();

        /** */
        private static final List<Class<? extends IgniteConfigVariationsAbstractTest>> classes
            = basicBuild(NoopTestExtendsIgnored.class, cfgs);

        /** */
        private static final AtomicInteger cntr = new AtomicInteger(0);

        /** */
        public SuiteWithExtendsIgnored(Class<?> cls) throws InitializationError {
            super(cls, classes.toArray(new Class<?>[] {null}));
        }

        /** */
        @Override protected void runChild(Runner runner, RunNotifier ntf) {
            IgniteConfigVariationsAbstractTest.injectTestsConfiguration(cfgs.get(cntr.getAndIncrement()));
            super.runChild(runner, ntf);
        }
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
    public static class NoopTestIgnored extends IgniteConfigVariationsAbstractTest {
        /** */
        @Test
        public void test1() {
            // No-op.
        }
    }

    /** */
    public static class NoopTestExtendsIgnored extends NoopTestIgnored {
        // No-op.
    }
}

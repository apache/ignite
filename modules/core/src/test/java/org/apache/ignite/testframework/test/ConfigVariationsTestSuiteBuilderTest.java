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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.configvariations.ConfigVariationsTestSuiteBuilder;
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
    private static List<Class<?>> basicBuild(Class<? extends IgniteConfigVariationsAbstractTest> cls) {
        return new ConfigVariationsTestSuiteBuilder(cls).classes();
    }

    /** */
    public static class BasicTest {
        /** */
        @Test
        public void testDefaults() {
            List<Class<?>> classes = basicBuild(NoopTest.class);

            assertEquals(4, classes.size());

            classes = new ConfigVariationsTestSuiteBuilder(NoopTest.class)
                .withBasicCacheParams().classes();

            assertEquals(4 * 4 * 2, classes.size());

            // With clients.
            classes = new ConfigVariationsTestSuiteBuilder(NoopTest.class)
                .testedNodesCount(2).withClients().classes();

            assertEquals(4 * 2, classes.size());

            classes = new ConfigVariationsTestSuiteBuilder(NoopTest.class)
                .withBasicCacheParams().testedNodesCount(3).withClients().classes();

            assertEquals(4 * 4 * 2 * 3, classes.size());
        }

        /** */
        @SuppressWarnings("serial")
        @Test
        public void testIgniteConfigFilter() {
            List<Class<?>> classes = basicBuild(NoopTest.class);

            final AtomicInteger cnt = new AtomicInteger();

            List<Class<?>> filteredClasses = new ConfigVariationsTestSuiteBuilder(NoopTest.class)
                .withIgniteConfigFilters(new IgnitePredicate<IgniteConfiguration>() {
                    @Override public boolean apply(IgniteConfiguration configuration) {
                        return cnt.getAndIncrement() % 2 == 0;
                    }
                })
                .classes();

            assertEquals(classes.size() / 2, filteredClasses.size());
        }

        /** */
        @SuppressWarnings("serial")
        @Test
        public void testCacheConfigFilter() {
            List<Class<?>> classes = new ConfigVariationsTestSuiteBuilder(NoopTest.class)
                .withBasicCacheParams()
                .classes();

            final AtomicInteger cnt = new AtomicInteger();

            List<Class<?>> filteredClasses = new ConfigVariationsTestSuiteBuilder(NoopTest.class)
                .withBasicCacheParams()
                .withCacheConfigFilters(new IgnitePredicate<CacheConfiguration>() {
                    @Override public boolean apply(CacheConfiguration configuration) {
                        return cnt.getAndIncrement() % 2 == 0;
                    }
                })
                .classes();

            assertEquals(classes.size() / 2, filteredClasses.size());
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
        }
    }

    /** */
    public static class SuiteBasic extends Suite {
        /** */
        private static final List<Class<?>> classes
            = basicBuild(NoopTest.class);

        /** */
        private static final AtomicInteger cntr = new AtomicInteger(0);

        /** */
        public SuiteBasic(Class<?> cls) throws InitializationError {
            super(cls, classes.toArray(new Class<?>[] {null}));
        }

        /** */
        @Override protected void runChild(Runner runner, RunNotifier ntf) {
            cntr.getAndIncrement();

            super.runChild(runner, ntf);
        }
    }

    /** */
    public static class SuiteWithIgnored extends Suite {
        /** */
        private static final List<Class<?>> classes = Stream
            .concat(basicBuild(NoopTest.class).stream(), basicBuild(NoopTestIgnored.class).stream())
            .collect(Collectors.toList());

        /** */
        private static final AtomicInteger cntr = new AtomicInteger(0);

        /** */
        public SuiteWithIgnored(Class<?> cls) throws InitializationError {
            super(cls, classes.toArray(new Class<?>[] {null}));
        }

        /** */
        @Override protected void runChild(Runner runner, RunNotifier ntf) {
            cntr.getAndIncrement();

            super.runChild(runner, ntf);
        }
    }

    /** */
    public static class SuiteWithExtendsIgnored extends Suite {
        /** */
        private static final List<Class<?>> classes
            = basicBuild(NoopTestExtendsIgnored.class);

        /** */
        private static final AtomicInteger cntr = new AtomicInteger(0);

        /** */
        public SuiteWithExtendsIgnored(Class<?> cls) throws InitializationError {
            super(cls, classes.toArray(new Class<?>[] {null}));
        }

        /** */
        @Override protected void runChild(Runner runner, RunNotifier ntf) {
            cntr.getAndIncrement();

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

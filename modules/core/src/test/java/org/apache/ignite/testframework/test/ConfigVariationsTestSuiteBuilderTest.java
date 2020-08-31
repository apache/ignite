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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.configvariations.ConfigVariations;
import org.apache.ignite.testframework.configvariations.ConfigVariationsFactory;
import org.apache.ignite.testframework.configvariations.ConfigVariationsTestSuiteBuilder;
import org.apache.ignite.testframework.configvariations.VariationsTestsConfig;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.apache.ignite.testframework.junits.IgniteCacheConfigVariationsAbstractTest;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISCOVERY_HISTORY_SIZE;
import static org.junit.Assert.assertEquals;

/** */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    ConfigVariationsTestSuiteBuilderTest.BasicTest.class,
    ConfigVariationsTestSuiteBuilderTest.TestSuiteBasic.class,
    ConfigVariationsTestSuiteBuilderTest.TestSuiteWithIgnored.class,
    ConfigVariationsTestSuiteBuilderTest.TestSuiteWithExtendsIgnored.class,
    ConfigVariationsTestSuiteBuilderTest.TestSuiteDummy.class,
    ConfigVariationsTestSuiteBuilderTest.TestSuiteCacheParams.class,
    ConfigVariationsTestSuiteBuilderTest.LegacyLifecycleTestSuite.class
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
            assertEquals(1, SuiteBasic.cntr.get());
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

    /** IMPL NOTE derived from {@code IgniteComputeBasicConfigVariationsFullApiTestSuite}. */
    @RunWith(ConfigVariationsTestSuiteBuilderTest.SuiteDummy.class)
    public static class TestSuiteDummy {
        /** **/
        private static final AtomicBoolean alreadyRun = new AtomicBoolean(false);

        /** */
        @BeforeClass
        public static void init() {
            Assume.assumeFalse("This test already has run.", alreadyRun.getAndSet(true));
        }
    }

    /** */
    public static class SuiteBasic extends Suite {
        /** */
        private static final AtomicInteger cntr = new AtomicInteger(0);

        /** */
        public SuiteBasic(Class<?> cls) throws InitializationError {
            super(cls, basicBuild(NoopTest.class).subList(0, 1).toArray(new Class<?>[] {null}));
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
        private static final AtomicInteger cntr = new AtomicInteger(0);

        /**
         *
         */
        public SuiteWithIgnored(Class<?> cls) throws InitializationError {
            super(cls, Stream
                .concat(basicBuild(NoopTest.class).stream(), basicBuild(NoopTestIgnored.class).stream())
                .collect(Collectors.toList()).toArray(new Class<?>[] {null}));
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
        private static final AtomicInteger cntr = new AtomicInteger(0);

        /** */
        public SuiteWithExtendsIgnored(Class<?> cls) throws InitializationError {
            super(cls, basicBuild(NoopTestExtendsIgnored.class).toArray(new Class<?>[] {null}));
        }

        /** */
        @Override protected void runChild(Runner runner, RunNotifier ntf) {
            cntr.getAndIncrement();

            super.runChild(runner, ntf);
        }
    }

    /** */
    public static class SuiteDummy extends Suite {
        /** */
        public SuiteDummy(Class<?> cls) throws InitializationError {
            super(cls, basicBuild(DummyTest.class).toArray(new Class<?>[] {null}));
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

    /** */
    public static class DummyTest extends IgniteConfigVariationsAbstractTest {
        /**
         * @throws Exception If failed.
         */
        @Test
        public void testDummyExecution() throws Exception {
            runInAllDataModes(new TestRunnable() {
                @SuppressWarnings("deprecation")
                @Override public void run() throws Exception {
                    info("Running dummy test.");

                    beforeTest();

                    afterTest();
                }
            });
        }

        /**
         * Override the base method to return {@code null} value in case the valId is negative.
         */
        @Nullable @Override public Object value(int valId) {
            if (valId < 0)
                return null;

            return super.value(valId);
        }
    }

    /** IMPL NOTE derived from {@code CacheContinuousQueryVariationsTest}. */
    @RunWith(SuiteCacheParams.class)
    public static class TestSuiteCacheParams {
        /** **/
        private static final AtomicBoolean alreadyRun = new AtomicBoolean(false);

        /** */
        @BeforeClass
        public static void init() {
            Assume.assumeFalse("This test already has run.", alreadyRun.getAndSet(true));
        }
    }

    /** */
    public static class SuiteCacheParams extends Suite {
        /** */
        private static List<Class<?>> suiteSingleNode() {
            return new ConfigVariationsTestSuiteBuilder(CacheParamsTest.class)
                .withBasicCacheParams()
                .gridsCount(1)
                .classes();
        }

        /** */
        public SuiteCacheParams(Class<?> cls) throws InitializationError {
            super(cls, suiteSingleNode().subList(0, 2).toArray(new Class<?>[] {null}));
        }

        /** */
        @BeforeClass
        public static void init() {
            System.setProperty(IGNITE_DISCOVERY_HISTORY_SIZE, "100");
        }
    }

    /** */
    public static class CacheParamsTest extends IgniteCacheConfigVariationsAbstractTest {
        /** {@inheritDoc} */
        @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
            IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

            cfg.setClientMode(false);

            return cfg;
        }

        /** */
        @Test(timeout = 10_000)
        public void testRandomOperationJCacheApiKeepBinary() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override protected long getTestTimeout() {
            return TimeUnit.SECONDS.toMillis(20);
        }
    }

    /** Test for legacy lifecycle methods. */
    public static class LegacyLifecycleTest extends IgniteCacheConfigVariationsAbstractTest {
        /** */
        private static final AtomicInteger stageCnt = new AtomicInteger(0);

        /** */
        private static final AtomicInteger testInstCnt = new AtomicInteger(0);

        /** IMPL NOTE new instances may be created rather arbitrarily, eg per every test case. */
        private final int testClsId = testInstCnt.getAndIncrement();

        /** {@inheritDoc} */
        @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
            IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

            cfg.setClientMode(false);

            return cfg;
        }

        /** {@inheritDoc} */
        @Override protected void beforeTestsStarted() throws Exception {
            // IMPL NOTE default config doesn't stop nodes.
            testsCfg = new VariationsTestsConfig(
                new ConfigVariationsFactory(null, new int[] {0}, ConfigVariations.cacheBasicSet(),
                    new int[] {0}), "Dummy config", true, null, 1,
                false);

            processStage("beforeTestsStarted", 0, 1);

            super.beforeTestsStarted();
        }

        /** {@inheritDoc} */
        @Override protected void beforeTest() throws Exception {
            testsCfg = new VariationsTestsConfig(
                new ConfigVariationsFactory(null, new int[] {0}, ConfigVariations.cacheBasicSet(),
                    new int[] {0}), "Dummy config", true, null, 1,
                false);

            processStage("beforeTest", 1, 2);

            super.beforeTest();
        }

        /** */
        @Test
        public void test1() {
            processStage("test1", 2, 3);
            U.warn(null, ">>> inside test 1"); // todo remove
        }

        /** */
        @Test
        public void test2() {
            processStage("test2", 2, 3);
            U.warn(null, ">>> inside test 1"); // todo remove
        }

        /** {@inheritDoc} */
        @Override protected void afterTest() throws Exception {
            processStage("afterTest", 3, 1);

            super.afterTest();
        }

        /** {@inheritDoc} */
        @Override protected void afterTestsStopped() throws Exception {
            processStage("afterTestsStopped", 1, 0);

            super.afterTestsStopped();
        }

        /** */
        private void processStage(String desc, int exp, int update) {
            Assert.assertEquals(desc + " at test class id " + testClsId, exp, stageCnt.get());

            stageCnt.set(update);
        }
    }

    /** */
    @RunWith(DynamicSuite.class)
    public static class LegacyLifecycleTestSuite {
        /** */
        public static List<Class<?>> suite() {
            return new ConfigVariationsTestSuiteBuilder(LegacyLifecycleTest.class)
                .withBasicCacheParams()
                .gridsCount(1)
                .classes()
                .subList(0, 2);
        }

        /** */
        @BeforeClass
        public static void init() {
            System.setProperty(IGNITE_DISCOVERY_HISTORY_SIZE, "100");
        }
    }
}

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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryVariationsTest;
import org.apache.ignite.testframework.configvariations.ConfigVariationsTestSuiteBuilder;
import org.apache.ignite.testframework.configvariations.VariationsTestsConfig;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;
import org.junit.runner.RunWith;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISCOVERY_HISTORY_SIZE;

/**
 * Test suite for cache queries.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IgniteContinuousQueryConfigVariationsSuite.SingleNodeTest.class,
    IgniteContinuousQueryConfigVariationsSuite.MultiNodeTest.class
})
public class IgniteContinuousQueryConfigVariationsSuite {
    /** */
    private static List<Class<? extends IgniteConfigVariationsAbstractTest>> suiteSingleNode(
        List<VariationsTestsConfig> cfgs) {
        List<Class<? extends IgniteConfigVariationsAbstractTest>> classes = new ArrayList<>();

        new ConfigVariationsTestSuiteBuilder(CacheContinuousQueryVariationsTest.class)
            .withBasicCacheParams()
            .gridsCount(1)
            .appendTo(classes, cfgs);

        return classes;
    }

    /** */
    private static List<Class<? extends IgniteConfigVariationsAbstractTest>> suiteMultiNode(
        List<VariationsTestsConfig> cfgs) {
        List<Class<? extends IgniteConfigVariationsAbstractTest>> classes = new ArrayList<>();

        new ConfigVariationsTestSuiteBuilder(CacheContinuousQueryVariationsTest.class)
            .withBasicCacheParams()
            .gridsCount(5)
            .backups(2)
            .appendTo(classes, cfgs);

        return classes;
    }

    /** */
    @RunWith(IgniteContinuousQueryConfigVariationsSuite.SuiteSingleNode.class)
    public static class SingleNodeTest {
    }

    /** */
    @RunWith(IgniteContinuousQueryConfigVariationsSuite.SuiteMultiNode.class)
    public static class MultiNodeTest {
    }

    /** */
    public static class SuiteSingleNode extends Suite {
        /** */
        private static final List<VariationsTestsConfig> cfgs = new ArrayList<>();

        /** */
        private static final List<Class<? extends IgniteConfigVariationsAbstractTest>> classes = suiteSingleNode(cfgs);

        /** */
        private static final AtomicInteger cntr = new AtomicInteger(0);

        /** */
        public SuiteSingleNode(Class<?> cls) throws InitializationError {
            super(cls, classes.toArray(new Class<?>[] {null}));
        }

        /** */
        @Override protected void runChild(Runner runner, RunNotifier ntf) {
            System.setProperty(IGNITE_DISCOVERY_HISTORY_SIZE, "100");

            CacheContinuousQueryVariationsTest.singleNode = true;

            IgniteConfigVariationsAbstractTest.injectTestsConfiguration(cfgs.get(cntr.getAndIncrement()));

            super.runChild(runner, ntf);
        }
    }

    /** */
    public static class SuiteMultiNode extends Suite {
        /** */
        private static final List<VariationsTestsConfig> cfgs = new ArrayList<>();

        /** */
        private static final List<Class<? extends IgniteConfigVariationsAbstractTest>> classes = suiteMultiNode(cfgs);

        /** */
        private static final AtomicInteger cntr = new AtomicInteger(0);

        /** */
        public SuiteMultiNode(Class<?> cls) throws InitializationError {
            super(cls, classes.toArray(new Class<?>[] {null}));
        }

        /** */
        @Override protected void runChild(Runner runner, RunNotifier ntf) {
            System.setProperty(IGNITE_DISCOVERY_HISTORY_SIZE, "100");

            CacheContinuousQueryVariationsTest.singleNode = false;

            IgniteConfigVariationsAbstractTest.injectTestsConfiguration(cfgs.get(cntr.getAndIncrement()));

            super.runChild(runner, ntf);
        }
    }
}

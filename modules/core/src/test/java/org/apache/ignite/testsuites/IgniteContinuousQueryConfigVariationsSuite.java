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

import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryVariationsTest;
import org.apache.ignite.testframework.configvariations.ConfigVariationsTestSuiteBuilder;
import org.apache.ignite.testframework.junits.IgniteCacheConfigVariationsAbstractTest;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.BeforeAll;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.Collections;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISCOVERY_HISTORY_SIZE;

@Suite
@SuiteDisplayName("Ignite Continuous Query Config Variations Suite")
@SelectClasses({
        IgniteContinuousQueryConfigVariationsSuite.SingleNodeTest.class,
        IgniteContinuousQueryConfigVariationsSuite.MultiNodeTest.class
})
public class IgniteContinuousQueryConfigVariationsSuite {
    /** */
    private static List<Class<?>> suiteSingleNode() {
        return new ConfigVariationsTestSuiteBuilder(CacheContinuousQueryVariationsTest.class)
                .withBasicCacheParams()
                .gridsCount(1)
                .classes();
    }

    /** */
    private static List<Class<?>> suiteMultiNode() {
        return new ConfigVariationsTestSuiteBuilder(CacheContinuousQueryVariationsTest.class)
                .withBasicCacheParams()
                .gridsCount(5)
                .backups(2)
                .classes();
    }

    /** */
    public static class SingleNodeTest {
        /** */
        @BeforeAll
        public static void setUp() {
            System.setProperty(IGNITE_DISCOVERY_HISTORY_SIZE, "100");
            CacheContinuousQueryVariationsTest.singleNode = true;
        }

        /** */
        @TestFactory
        public Stream<DynamicContainer> singleNodeTests() {
            // In JUnit 5, you typically need to create dynamic tests or use a different approach
            // Since the original Suite mechanism is more complex, you might need to adapt this
            return suiteSingleNode().stream()
                    .map(testClass -> DynamicContainer.dynamicContainer(
                            testClass.getSimpleName(),
                            Collections.emptyList() // You'll need to implement test generation logic here
                    ));
        }
    }

    /** */
    public static class MultiNodeTest {
        /** */
        @BeforeAll
        public static void setUp() {
            System.setProperty(IGNITE_DISCOVERY_HISTORY_SIZE, "100");
            CacheContinuousQueryVariationsTest.singleNode = false;
        }

        /** */
        @TestFactory
        public Stream<DynamicContainer> multiNodeTests() {
            List<Class<?>> tests = Stream.concat(
                            suiteMultiNode().stream(),
                            new ConfigVariationsTestSuiteBuilder(Sentinel.class)
                                    .withBasicCacheParams()
                                    .classes()
                                    .subList(0, 1).stream()
                    )
                    .collect(Collectors.toList());

            return tests.stream()
                    .map(testClass -> DynamicContainer.dynamicContainer(
                            testClass.getSimpleName(),
                            Collections.emptyList() // You'll need to implement test generation logic here
                    ));
        }

        /**
         * Sole purpose of this class is to trigger
         * {@link IgniteCacheConfigVariationsAbstractTest#unconditionalCleanupAfterTests()}.
         */
        public static class Sentinel extends IgniteCacheConfigVariationsAbstractTest {
            /** */
            @Test
            public void sentinel() {}
        }
    }
}
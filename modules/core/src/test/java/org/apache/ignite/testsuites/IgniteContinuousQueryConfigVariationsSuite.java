/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.testsuites;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryVariationsTest;
import org.apache.ignite.testframework.configvariations.ConfigVariationsTestSuiteBuilder;
import org.apache.ignite.testframework.junits.IgniteCacheConfigVariationsAbstractTest;
import org.junit.Test;
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
    @RunWith(IgniteContinuousQueryConfigVariationsSuite.SuiteSingleNode.class)
    public static class SingleNodeTest {
    }

    /** */
    @RunWith(IgniteContinuousQueryConfigVariationsSuite.SuiteMultiNode.class)
    public static class MultiNodeTest {
    }

    /** {@inheritDoc} */
    public static class SuiteSingleNode extends Suite {
        /** */
        public SuiteSingleNode(Class<?> cls) throws InitializationError {
            super(cls, suiteSingleNode().toArray(new Class<?>[] {null}));
        }

        /** {@inheritDoc} */
        @Override protected void runChild(Runner runner, RunNotifier ntf) {
            System.setProperty(IGNITE_DISCOVERY_HISTORY_SIZE, "100");

            CacheContinuousQueryVariationsTest.singleNode = true;

            super.runChild(runner, ntf);
        }
    }

    /** {@inheritDoc} */
    public static class SuiteMultiNode extends Suite {
        /** */
        public SuiteMultiNode(Class<?> cls) throws InitializationError {
            super(cls, suiteWithSentinel().toArray(new Class<?>[] {null}));
        }

        /** {@inheritDoc} */
        @Override protected void runChild(Runner runner, RunNotifier ntf) {
            System.setProperty(IGNITE_DISCOVERY_HISTORY_SIZE, "100");

            CacheContinuousQueryVariationsTest.singleNode = false;

            super.runChild(runner, ntf);
        }

        /** */
        private static List<Class<?>> suiteWithSentinel() {
            return Stream.concat(suiteMultiNode().stream(),
                new ConfigVariationsTestSuiteBuilder(
                    Sentinel.class).withBasicCacheParams().classes().subList(0, 1).stream())
                .collect(Collectors.toList());
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

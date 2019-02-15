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
package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ALLOW_START_CACHES_IN_PARALLEL;

/**
 * Tests, that cluster could start and activate with all possible values of IGNITE_ALLOW_START_CACHES_IN_PARALLEL.
 */
@RunWith(JUnit4.class)
public class StartCachesInParallelTest extends GridCommonAbstractTest {
    /** IGNITE_ALLOW_START_CACHES_IN_PARALLEL option value before tests. */
    private String allowParallel;

    /** Test failure handler. */
    private TestStopNodeFailureHandler failureHnd;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        cfg.setCacheConfiguration(
            new CacheConfiguration<>()
                .setName(DEFAULT_CACHE_NAME)
                .setIndexedTypes(Integer.class, Integer.class));

        failureHnd = new TestStopNodeFailureHandler();

        cfg.setFailureHandler(failureHnd);
        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        allowParallel = System.getProperty(IGNITE_ALLOW_START_CACHES_IN_PARALLEL);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        if (allowParallel != null)
            System.setProperty(IGNITE_ALLOW_START_CACHES_IN_PARALLEL, allowParallel);
        else
            System.clearProperty(IGNITE_ALLOW_START_CACHES_IN_PARALLEL);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testWithEnabledOption() throws Exception {
        doTest("true");
    }

    /** */
    @Test
    public void testWithDisabledOption() throws Exception {
        doTest("false");
    }

    /** */
    @Test
    public void testWithoutOption() throws Exception {
        doTest(null);
    }

    /**
     * Test routine.
     *
     * @param optionVal IGNITE_ALLOW_START_CACHES_IN_PARALLEL value.
     * @throws Exception If failed.
     */
    private void doTest(String optionVal) throws Exception {
        if (optionVal == null)
            System.clearProperty(IGNITE_ALLOW_START_CACHES_IN_PARALLEL);
        else {
            Boolean.parseBoolean(optionVal);

            System.setProperty(IGNITE_ALLOW_START_CACHES_IN_PARALLEL, optionVal);
        }

        assertEquals("Property wasn't set", optionVal, System.getProperty(IGNITE_ALLOW_START_CACHES_IN_PARALLEL));

        IgniteEx node = startGrid(0);

        node.cluster().active(true);

        assertNull("Node failed with " + failureHnd.lastFailureCtx, failureHnd.lastFailureCtx);

        assertTrue(node.cluster().active());
    }

    /** */
    private static class TestStopNodeFailureHandler extends StopNodeFailureHandler {
        /** Last failure context. */
        private volatile FailureContext lastFailureCtx;

        /** {@inheritDoc} */
        @Override public boolean handle(Ignite ignite, FailureContext failureCtx) {
            lastFailureCtx = failureCtx;

            return super.handle(ignite, failureCtx);
        }
    }
}

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
package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ALLOW_START_CACHES_IN_PARALLEL;

/**
 * Tests, that cluster could start and activate with all possible values of IGNITE_ALLOW_START_CACHES_IN_PARALLEL.
 */
public class StartCachesInParallelTest extends GridCommonAbstractTest {
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
    @WithSystemProperty(key = IGNITE_ALLOW_START_CACHES_IN_PARALLEL, value = "true")
    public void testWithEnabledOption() throws Exception {
        doTest();
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_ALLOW_START_CACHES_IN_PARALLEL, value = "true")
    public void testWithDisabledOption() throws Exception {
        doTest();
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_ALLOW_START_CACHES_IN_PARALLEL, value = "")
    public void testWithoutOption() throws Exception {
        System.clearProperty(IGNITE_ALLOW_START_CACHES_IN_PARALLEL);

        doTest();
    }

    /**
     * Test routine.
     *
     * @throws Exception If failed.
     */
    private void doTest() throws Exception {
        String optionVal = System.getProperty(IGNITE_ALLOW_START_CACHES_IN_PARALLEL);

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

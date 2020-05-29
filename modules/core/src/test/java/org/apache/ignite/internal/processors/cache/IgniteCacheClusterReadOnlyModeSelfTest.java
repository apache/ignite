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

import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.cacheNames;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

public class IgniteCacheClusterReadOnlyModeSelfTest extends GridCommonAbstractTest {
    /** Key. */
    private static final int KEY = 1;

    /** Value. */
    private static final int VAL = 2;

    /** Unknown key. */
    private static final int UNKNOWN_KEY = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(ClusterReadOnlyModeTestUtils.cacheConfigurations());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
        startClientGridsMultiThreaded(3, 2);

        grid(0).cluster().state(ClusterState.ACTIVE);

        for (String cacheName : cacheNames())
            grid(0).cache(cacheName).put(KEY, VAL);

        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        commonChecks();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        commonChecks();

        super.afterTest();
    }

    /** */
    @Test
    public void testGetAndPutIfAbsentAllowedIfKeyIsPresented() {
        performAction(cache -> assertNull(cache.getName(), cache.getAndPutIfAbsent(KEY, VAL)));
    }

    /** */
    @Test
    public void testGetAndPutIfAbsentDeniedIfKeyIsAbsent() {
        performActionReadOnlyExceptionExpected(cache -> cache.getAndPutIfAbsent(UNKNOWN_KEY, VAL));
    }

    private void performActionReadOnlyExceptionExpected(Consumer<IgniteCache> clo) {
        performAction(cache -> {
            Throwable ex = assertThrows(log, ()-> clo.accept(cache), Exception.class, null);

            ClusterReadOnlyModeTestUtils.checkRootCause(ex, cache.getName());
        });
    }

    /** */
    private void performAction(Consumer<IgniteCache> clo) {
        for (Ignite node : G.allGrids()) {
            for (String cacheName : cacheNames())
                clo.accept(node.cache(cacheName));
        }
    }

    /** */
    private void commonChecks() {
        for (Ignite node : G.allGrids()) {
            assertEquals(node.name(), ClusterState.ACTIVE_READ_ONLY, node.cluster().state());

            for (String cacheName : cacheNames()) {
                IgniteCache cache = node.cache(cacheName);

                assertEquals(node.name() + " " + cacheName, 1, cache.size());
                assertEquals(node.name() + " " + cacheName, VAL, cache.get(KEY));
                assertNull(node.name() + " " + cacheName, cache.get(UNKNOWN_KEY));
            }
        }
    }

}

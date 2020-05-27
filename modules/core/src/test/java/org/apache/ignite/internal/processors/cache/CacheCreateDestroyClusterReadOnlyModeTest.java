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
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.cacheConfigurations;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests that {@link Ignite#createCache(CacheConfiguration)}/{@link Ignite#destroyCache(String)} denied if cluster in a
 * {@link ClusterState#ACTIVE_READ_ONLY} state.
 */
public class CacheCreateDestroyClusterReadOnlyModeTest extends CacheCreateDestroyClusterReadOnlyModeAbstractTest {
    /** */
    @Test
    public void testCacheCreate() {
        grid(0).cluster().state(ACTIVE_READ_ONLY);

        G.allGrids().forEach(n -> assertEquals(n.name(), ACTIVE_READ_ONLY, n.cluster().state()));

        for (CacheConfiguration cfg : cacheConfigurations()) {
            for (Ignite node : G.allGrids()) {
                Throwable ex = assertThrows(log, () -> node.createCache(cfg), Exception.class, null);

                ClusterReadOnlyModeTestUtils.checkRootCause(ex, cfg.toString());
            }
        }
    }

    /** */
    @Test
    public void testCacheDestroy() {
        grid(0).createCaches(asList(cacheConfigurations()));

        grid(0).cluster().state(ACTIVE_READ_ONLY);

        G.allGrids().forEach(n -> assertEquals(n.name(), ACTIVE_READ_ONLY, n.cluster().state()));

        for (CacheConfiguration cfg : cacheConfigurations()) {
            for (Ignite node : G.allGrids()) {
                Throwable ex = assertThrows(log, () -> node.destroyCache(cfg.getName()), Exception.class, null);

                ClusterReadOnlyModeTestUtils.checkRootCause(ex, cfg.toString());
            }
        }
    }
}

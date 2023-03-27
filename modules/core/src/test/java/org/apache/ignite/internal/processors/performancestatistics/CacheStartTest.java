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

package org.apache.ignite.internal.processors.performancestatistics;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests cache start operation.
 */
@RunWith(Parameterized.class)
public class CacheStartTest extends AbstractPerformanceStatisticsTest {
    /** Static configured cache name. */
    private static final String STATIC_CACHE_NAME = "static-cache";

    /** Persistence enabled flag. */
    @Parameterized.Parameter
    public boolean persistence;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "persistence={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {{false}, {true}});
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>(STATIC_CACHE_NAME));

        if (persistence) {
            cfg.setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                    )
            );
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        if (persistence)
            cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        if (persistence)
            cleanPersistenceDir();

        cleanPerformanceStatisticsDir();
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheStart() throws Exception {
        IgniteEx srv = startGrids(2);

        if (persistence)
            srv.cluster().state(ClusterState.ACTIVE);

        startClientGrid("client");

        startCollectStatistics();

        stopCollectStatisticsAndCheckCaches(srv);

        cleanPerformanceStatisticsDir();

        startCollectStatistics();

        srv.getOrCreateCache(DEFAULT_CACHE_NAME);

        stopCollectStatisticsAndCheckCaches(srv);

        if (persistence) {
            stopAllGrids();

            srv = startGrids(2);

            cleanPerformanceStatisticsDir();

            startCollectStatistics();

            stopCollectStatisticsAndCheckCaches(srv);
        }
    }

    /** Stops and reads collecting performance statistics. Checks cache start operation. */
    private void stopCollectStatisticsAndCheckCaches(IgniteEx ignite) throws Exception {
        Map<Integer, String> expCaches = new HashMap<>();

        ignite.context().cache().cacheDescriptors().values().forEach(
            desc -> expCaches.put(desc.cacheId(), desc.cacheName()));

        ClusterNode coord = U.oldest(ignite.cluster().nodes(), null);

        Set<Integer> caches = new HashSet<>();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void cacheStart(UUID nodeId, int cacheId, String name) {
                caches.add(cacheId);

                assertEquals(coord.id(), nodeId);
                assertTrue(expCaches.containsKey(cacheId));
                assertEquals(expCaches.get(cacheId), name);
            }
        });

        assertTrue(expCaches.keySet().equals(caches));
    }
}

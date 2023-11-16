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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test create cache in persistent data region with custom name
 */
public class CacheNameTest extends GridCommonAbstractTest {
    /** */
    static final String dataRegionName = "persistence-region";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setName(dataRegionName)
                    .setPersistenceEnabled(true)))
            .setClusterStateOnStart(ClusterState.ACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTestsStopped();
    }

    /** Test cache names */
    @Test
    public void testCreateCacheWithPersistenceAndCustomName() {
        IgniteCluster igniteCluster = grid(0).cluster();

        igniteCluster.state(ClusterState.ACTIVE);

        String name = "/\"";

        CacheConfiguration cacheCfg = new CacheConfiguration()
            .setName(name)
            .setDataRegionName(dataRegionName);

        GridTestUtils.assertThrows(log, () -> grid(0).getOrCreateCache(cacheCfg),
            IgniteCheckedException.class, "Invalid cache name /\"");

        cacheCfg.setName(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, String> cache = grid(0).getOrCreateCache(cacheCfg);

        assertEquals(DEFAULT_CACHE_NAME, cache.getName());

        cache.put(1, "value");

        assertEquals("value", cache.get(1));
    }
}

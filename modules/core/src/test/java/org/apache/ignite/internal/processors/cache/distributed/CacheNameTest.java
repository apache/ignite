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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Collection;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.util.lang.GridFunc.asList;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;

/**
 * Test create cache in persistent data region with custom name
 */
@RunWith(Parameterized.class)
public class CacheNameTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter
    public boolean persistenceEnabled;

    /** */
    @Parameterized.Parameter(1)
    public boolean isGroupName;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "persistenceEnabled={0}, isGroupName={1}")
    public static Collection<?> parameters() {
        return cartesianProduct(
            asList(false, true), asList(false, true)
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(persistenceEnabled)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        super.beforeTestsStarted();
    }

    /** */
    @After
    public void cleanUp() throws Exception {
        grid(0).destroyCaches(grid(0).cacheNames());

        stopAllGrids();
    }

    /** Test cache names. */
    @Test
    public void testCreateCacheWithPersistenceAndCustomName() throws Exception {
        startGrid(0);

        grid(0).cluster().state(ClusterState.ACTIVE);

        F.asMap(
            "/\"", false,
            "app/cache1", false,
            "/", false,
            "app@cache1", true,
            "app>cache1", U.isLinux() || U.isMacOs())
            .forEach(this::checkCreate);
    }

    /**
     * Checking cache names.
     *
     * @param name Name cache or cache group.
     * @param result Expected result of creating the directory {@code true} if name is valid.
     */
    private void checkCreate(String name, boolean result) {
        CacheConfiguration<Integer, String> cfg = new CacheConfiguration<Integer, String>()
            .setName(name);

        if (isGroupName)
            cfg.setGroupName(name);

        if (result || !persistenceEnabled) {
            IgniteCache<Integer, String> cache = grid(0).createCache(cfg);

            assertEquals(name, cache.getName());

            if (isGroupName)
                assertEquals(name, cache.getConfiguration(CacheConfiguration.class).getGroupName());
        }
        else {
            String msg = isGroupName ? "Invalid cache group name " : "Invalid cache name ";

            GridTestUtils.assertThrows(log, () -> grid(0).createCache(cfg),
                IgniteCheckedException.class, msg + name);
        }
    }
}

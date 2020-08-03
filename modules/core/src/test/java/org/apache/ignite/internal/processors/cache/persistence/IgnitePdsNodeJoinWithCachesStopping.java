/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test checks correctness of simultaneous node join and massive caches stopping.
 */
public class IgnitePdsNodeJoinWithCachesStopping extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        return cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(200 * 1024 * 1024)
                .setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     *
     */
    @Test
    public void test() throws Exception {
        final Ignite ig = startGridsMultiThreaded(2);

        for (int i = 0; i < 100; i++)
            ig.createCache(new CacheConfiguration<>("test0" + i).setBackups(0));

        IgniteInternalFuture<Boolean> gridStartFut = GridTestUtils.runAsync(() ->
        {
            try {
                startGrid(2);
            }
            catch (Exception e) {
                return false;
            }

            return true;
        }, "new-server-start-thread");

        for (int k = 0; k < 5; k++) {
            final int l = k;
            GridTestUtils.runAsync(() -> {
                for (int m = l * 20; m < (l + 1) * 20; m++)
                    ig.destroyCache("test0" + m);

            }, "cache-destroy-thread");
        }

        assertTrue(gridStartFut.get());
    }
}

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

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests entries removal after shutdown/deactivation on in-memory/persistence grid.
 */
@RunWith(Parameterized.class)
public class EntriesRemoveOnShutdownTest extends GridCommonAbstractTest {
    /** Persistence. */
    @Parameterized.Parameter
    public boolean pds;

    /** Is any entry removed from the cache. */
    AtomicBoolean removed = new AtomicBoolean();

    /** Ignite instance. */
    IgniteEx ignite;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "pds={0}")
    public static Collection<?> parameters() {
        return F.asList(true, false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(pds)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        removed.set(false);

        ignite = startGrid();

        ignite.cluster().state(ClusterState.ACTIVE);

        ignite.getOrCreateCache(new CacheConfiguration<Integer, String>("cache0"));

        // Create two caches in the same group.
        ignite.getOrCreateCache(new CacheConfiguration<Integer, String>("cache1").setGroupName("grp"));
        ignite.getOrCreateCache(new CacheConfiguration<Integer, String>("cache2").setGroupName("grp"));

        for (String cacheName : ignite.cacheNames()) {
            for (int i = 0; i < 1000; i++)
                ignite.cache(cacheName).put(i, "Test" + i);
        }

        for (String cacheName : ignite.cacheNames()) {
            // Inject row cleaner to know about physical rows removal.
            IgniteCacheOffheapManager offheap = ignite.context().cache().cache(cacheName).context().offheap();
            for (IgniteCacheOffheapManager.CacheDataStore ds : offheap.cacheDataStores())
                ds.setRowCacheCleaner(r -> removed.set(true));
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
        stopAllGrids();
    }

    /** */
    @Test
    public void testShutdown() {
        ignite.close();

        assertFalse(removed.get());
    }

    /** */
    @Test
    public void testDeactivate() {
        ignite.cluster().state(ClusterState.INACTIVE);

        assertFalse(removed.get());
    }

    /** */
    @Test
    public void testCacheGroupDestroy() throws Exception {
        ignite.destroyCache("cache0");

        // For pds entire cache group can be dropped on checkpoint, we don't need to remove entries one by one
        if (pds) {
            forceCheckpoint();
            assertFalse(removed.get());
        }
        else
            assertTrue(removed.get());
    }

    /** */
    @Test
    public void testCacheDestroy() throws Exception {
        ignite.destroyCache("cache1");

        if (pds)
            forceCheckpoint();

        assertTrue(removed.get());
    }
}

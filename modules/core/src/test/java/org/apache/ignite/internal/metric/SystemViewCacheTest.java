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

package org.apache.ignite.internal.metric;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ModifiedExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.systemview.view.CacheGroupIoView;
import org.apache.ignite.spi.systemview.view.CacheGroupView;
import org.apache.ignite.spi.systemview.view.CacheView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHES_VIEW;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHE_GRPS_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.CACHE_GRP_IO_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Tests for {@link SystemView} for caches. */
public class SystemViewCacheTest extends SystemViewAbstractTest {
    /** Tests work of {@link SystemView} for caches. */
    @Test
    public void testCachesView() throws Exception {
        try (IgniteEx g = startGrid()) {
            Set<String> cacheNames = new HashSet<>(Arrays.asList("cache-1", "cache-2"));

            for (String name : cacheNames)
                g.createCache(name);

            SystemView<CacheView> caches = g.context().systemView().view(CACHES_VIEW);

            assertEquals(g.context().cache().cacheDescriptors().size(), F.size(caches.iterator()));

            for (CacheView row : caches)
                cacheNames.remove(row.cacheName());

            assertTrue(cacheNames.toString(), cacheNames.isEmpty());
        }
    }

    /** Tests work of {@link SystemView} for cache groups. */
    @Test
    public void testCacheGroupsView() throws Exception {
        try (IgniteEx g = startGrid()) {
            Set<String> grpNames = new HashSet<>(Arrays.asList("grp-1", "grp-2"));

            for (String grpName : grpNames)
                g.createCache(new CacheConfiguration<>("cache-" + grpName).setGroupName(grpName));

            SystemView<CacheGroupView> grps = g.context().systemView().view(CACHE_GRPS_VIEW);

            assertEquals(g.context().cache().cacheGroupDescriptors().size(), F.size(grps.iterator()));

            for (CacheGroupView row : grps)
                grpNames.remove(row.cacheGroupName());

            assertTrue(grpNames.toString(), grpNames.isEmpty());
        }
    }

    /** Tests work of {@link SystemView} for cache expiry policy info with in-memory configuration. */
    @Test
    public void testCacheViewExpiryPolicyWithInMemory() throws Exception {
        testCacheViewExpiryPolicy(false);
    }

    /** Tests work of {@link SystemView} for cache expiry policy info with persist configuration. */
    @Test
    public void testCacheViewExpiryPolicyWithPersist() throws Exception {
        testCacheViewExpiryPolicy(true);
    }

    /** Tests work of {@link SystemView} for cache groups expiry policy info. */
    private void testCacheViewExpiryPolicy(boolean withPersistence) throws Exception {
        try (IgniteEx g = !withPersistence ? startGrid() : startGrid(getConfiguration().setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            )))) {

            if (withPersistence)
                g.cluster().state(ClusterState.ACTIVE);

            String eternalCacheName = "eternalCache";
            String createdCacheName = "createdCache";
            String eagerTtlCacheName = "eagerTtlCache";
            String withoutGrpCacheName = "withoutGrpCache";
            String dfltCacheName = "defaultCache";

            CacheConfiguration<Long, Long> eternalCache = new CacheConfiguration<Long, Long>(eternalCacheName)
                .setGroupName("group1")
                .setExpiryPolicyFactory(EternalExpiryPolicy.factoryOf());

            CacheConfiguration<Long, Long> createdCache = new CacheConfiguration<Long, Long>(createdCacheName)
                .setGroupName("group2")
                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, 500L)));

            CacheConfiguration<Long, Long> eagerTtlCache = new CacheConfiguration<Long, Long>(eagerTtlCacheName)
                .setGroupName("group2")
                .setEagerTtl(false)
                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, 500L)));

            CacheConfiguration<Long, Long> withoutGrpCache = new CacheConfiguration<Long, Long>(withoutGrpCacheName)
                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, 500L)));

            CacheConfiguration<Long, Long> dfltCache = new CacheConfiguration<Long, Long>(dfltCacheName)
                .setGroupName("group3");

            g.createCache(eternalCache);
            g.createCache(createdCache);
            g.createCache(eagerTtlCache);
            g.createCache(withoutGrpCache);
            g.createCache(dfltCache);

            SystemView<CacheView> caches = g.context().systemView().view(CACHES_VIEW);

            for (CacheView row : caches) {
                switch (row.cacheName()) {
                    case "defaultCache":
                    case "eternalCache":
                        assertEquals("No", row.hasExpiringEntries());

                        g.cache(row.cacheName()).put(0, 0);

                        assertEquals("No", row.hasExpiringEntries());

                        g.cache(row.cacheName())
                            .withExpiryPolicy(new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, 200L)))
                            .put(1, 1);

                        assertEquals("Yes", row.hasExpiringEntries());
                        assertTrue(waitForCondition(() -> "No".equals(row.hasExpiringEntries()), getTestTimeout()));

                        break;

                    case "withoutGrpCache":
                    case "createdCache":
                        assertEquals("No", row.hasExpiringEntries());

                        g.cache(row.cacheName()).put(0, 0);

                        assertEquals("Yes", row.hasExpiringEntries());
                        assertTrue(waitForCondition(() -> "No".equals(row.hasExpiringEntries()), getTestTimeout()));

                        g.cache(row.cacheName())
                            .withExpiryPolicy(new ModifiedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, 200L)))
                            .put(1, 1);

                        assertEquals("Yes", row.hasExpiringEntries());
                        assertTrue(waitForCondition(() -> "No".equals(row.hasExpiringEntries()), getTestTimeout()));

                        if (row.cacheName().equals(createdCacheName)) {
                            g.cache(eagerTtlCacheName).put(2, 2);
                            assertEquals("No", row.hasExpiringEntries());
                        }

                        break;

                    case "eagerTtlCache":
                        assertEquals("Unknown", row.hasExpiringEntries());

                        break;
                }
            }
        }
    }

    /** Tests work of {@link SystemView} for cache group I/O operations. */
    @Test
    public void testCacheGroupIo() throws Exception {
        cleanPersistenceDir();

        try (IgniteEx ignite = startGrid(getConfiguration().setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true))))
        ) {
            ignite.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Object, Object> cache = ignite.createCache("cache");

            cache.put(0, 0);
            cache.get(0);

            SystemView<CacheGroupIoView> view = ignite.context().systemView().view(CACHE_GRP_IO_VIEW);

            CacheGroupIoView row = F.find(view, null,
                (IgnitePredicate<CacheGroupIoView>)r -> "cache".equals(r.cacheGroupName()));

            assertNotNull(row);
            assertTrue(row.logicalReads() > 0);
            assertTrue(row.insertedBytes() > 0);
        }
    }
}

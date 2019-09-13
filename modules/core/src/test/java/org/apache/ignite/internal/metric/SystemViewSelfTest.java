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
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.metric.list.SystemView;
import org.apache.ignite.spi.metric.list.view.CacheGroupView;
import org.apache.ignite.spi.metric.list.view.CacheView;
import org.apache.ignite.spi.metric.list.view.ComputeTaskView;
import org.apache.ignite.spi.metric.list.view.ServiceView;
import org.apache.ignite.internal.processors.service.DummyService;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHES_MON_LIST;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHE_GRPS_MON_LIST;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SVCS_MON_LIST;
import static org.apache.ignite.internal.processors.task.GridTaskProcessor.TASKS_MON_LIST;

/** */
public class SystemViewSelfTest extends GridCommonAbstractTest {
    /** */
    private static CountDownLatch latch;

    /** */
    @Test
    public void testCachesList() throws Exception {
        try (IgniteEx g = startGrid()) {
            Set<String> cacheNames = new HashSet<>(Arrays.asList("cache-1", "cache-2"));

            for (String name : cacheNames)
                g.createCache(name);

            SystemView<CacheView> caches = g.context().metric().list(CACHES_MON_LIST);

            assertEquals(g.context().cache().cacheDescriptors().size(), F.size(caches.iterator()));

            for (CacheView row : caches)
                cacheNames.remove(row.cacheName());

            assertTrue(cacheNames.toString(), cacheNames.isEmpty());
        }
    }

    /** */
    @Test
    public void testCacheGroupsList() throws Exception {
        try(IgniteEx g = startGrid()) {
            Set<String> grpNames = new HashSet<>(Arrays.asList("grp-1", "grp-2"));

            for (String grpName : grpNames)
                g.createCache(new CacheConfiguration<>("cache-" + grpName).setGroupName(grpName));

            SystemView<CacheGroupView> grps = g.context().metric().list(CACHE_GRPS_MON_LIST);

            assertEquals(g.context().cache().cacheGroupDescriptors().size(), F.size(grps.iterator()));

            for (CacheGroupView row : grps)
                grpNames.remove(row.cacheGroupName());

            assertTrue(grpNames.toString(), grpNames.isEmpty());
        }
    }

    /** */
    @Test
    public void testServices() throws Exception {
        try(IgniteEx g = startGrid()) {
            ServiceConfiguration srvcCfg = new ServiceConfiguration();

            srvcCfg.setName("service");
            srvcCfg.setMaxPerNodeCount(1);
            srvcCfg.setService(new DummyService());

            g.services().deploy(srvcCfg);

            SystemView<ServiceView> srvs = g.context().metric().list(SVCS_MON_LIST);

            assertEquals(g.context().service().serviceDescriptors().size(), F.size(srvs.iterator()));

            ServiceView sview = srvs.iterator().next();

            assertEquals(srvcCfg.getName(), sview.name());
            assertEquals(srvcCfg.getMaxPerNodeCount(), sview.maxPerNodeCount());
            assertEquals(DummyService.class, sview.serviceClass());
        }
    }

    /** */
    @Test
    public void testComputeBroadcast() throws Exception {
        latch = new CountDownLatch(1);

        try(IgniteEx g1 = startGrid(0)) {
            SystemView<ComputeTaskView> tasks = g1.context().metric().list(TASKS_MON_LIST);

            for (int i=0; i<5; i++)
                g1.compute().broadcastAsync(() -> {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });

            assertEquals(5, tasks.size());

            ComputeTaskView t = tasks.iterator().next();

            assertFalse(t.internal());
            assertNull(t.affinityCacheName());
            assertEquals(-1, t.affinityPartitionId());
            assertTrue(t.taskClassName().startsWith(getClass().getName()));
            assertTrue(t.taskName().startsWith(getClass().getName()));
            assertEquals(g1.localNode().id(), t.taskNodeId());
            assertEquals("0", t.userVersion());

            latch.countDown();
        }
    }
}

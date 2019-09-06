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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.metric.list.MonitoringList;
import org.apache.ignite.spi.metric.list.view.CacheGroupView;
import org.apache.ignite.spi.metric.list.view.CacheView;
import org.apache.ignite.spi.metric.list.view.ComputeTaskView;
import org.apache.ignite.spi.metric.list.view.ServiceView;
import org.apache.ignite.internal.processors.service.DummyService;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.GridMetricManager.CACHES_MON_LIST;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CACHE_GRPS_MON_LIST;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CACHE_GRPS_MON_LIST_DESC;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SVCS_MON_LIST;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SVCS_MON_LIST_DESC;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.TASKS_MON_LIST;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.TASK_COUNT_DESC;
import static org.apache.ignite.internal.util.lang.GridFunc.alwaysTrue;

/** */
public class MonitoringListSelfTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testCachesList() throws Exception {
        try (IgniteEx g = startGrid()) {
            Set<String> cacheNames = new HashSet<>(Arrays.asList("cache-1", "cache-2"));

            for (String name : cacheNames)
                g.createCache(name);

            MonitoringList<String, CacheView> caches =
                g.context().metric().list(CACHES_MON_LIST, CACHE_GRPS_MON_LIST_DESC, CacheView.class);

            assertEquals("ignite-sys, cache-1, cache-2", 3, F.size(caches.iterator(), alwaysTrue()));

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

            MonitoringList<Integer, CacheGroupView> grps =
                g.context().metric().list(CACHE_GRPS_MON_LIST, CACHE_GRPS_MON_LIST_DESC, CacheGroupView.class);

            assertEquals("ignite-sys, grp-1, grp-2", 3, F.size(grps.iterator(), alwaysTrue()));

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

            MonitoringList<IgniteUuid, ServiceView> srvs =
                g.context().metric().list(SVCS_MON_LIST, SVCS_MON_LIST_DESC, ServiceView.class);

            assertEquals(1, F.size(srvs.iterator(), alwaysTrue()));

            ServiceView sview = srvs.iterator().next();

            assertEquals(srvcCfg.getName(), sview.name());
            assertEquals(srvcCfg.getMaxPerNodeCount(), sview.maxPerNodeCount());
            assertEquals(DummyService.class, sview.serviceClass());
        }
    }

    /** */
    @Test
    public void testComputeBroadcast() throws Exception {
        try(IgniteEx g1 = startGrid(0)) {
            MonitoringList<IgniteUuid, ComputeTaskView> tasks =
                g1.context().metric().list(TASKS_MON_LIST, TASK_COUNT_DESC, ComputeTaskView.class);

            for (int i=0; i<5; i++)
                g1.compute().broadcastAsync(() -> {
                    try {
                        Thread.sleep(3_000L);
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
        }
    }
}

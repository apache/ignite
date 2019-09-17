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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.metric.view.SystemView;
import org.apache.ignite.spi.metric.view.CacheGroupView;
import org.apache.ignite.spi.metric.view.CacheView;
import org.apache.ignite.spi.metric.view.ComputeTaskView;
import org.apache.ignite.spi.metric.view.ServiceView;
import org.apache.ignite.internal.processors.service.DummyService;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHES_VIEW;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHE_GRPS_VIEW;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SVCS_VIEW;
import static org.apache.ignite.internal.processors.task.GridTaskProcessor.TASKS_VIEW;

/** Tests for {@link SystemView}. */
public class SystemViewSelfTest extends GridCommonAbstractTest {
    /** */
    private static CountDownLatch latch;

    /** Tests work of {@link SystemView} for caches. */
    @Test
    public void testCachesView() throws Exception {
        try (IgniteEx g = startGrid()) {
            Set<String> cacheNames = new HashSet<>(Arrays.asList("cache-1", "cache-2"));

            for (String name : cacheNames)
                g.createCache(name);

            SystemView<CacheView> caches = g.context().metric().view(CACHES_VIEW);

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

            SystemView<CacheGroupView> grps = g.context().metric().view(CACHE_GRPS_VIEW);

            assertEquals(g.context().cache().cacheGroupDescriptors().size(), F.size(grps.iterator()));

            for (CacheGroupView row : grps)
                grpNames.remove(row.cacheGroupName());

            assertTrue(grpNames.toString(), grpNames.isEmpty());
        }
    }

    /** Tests work of {@link SystemView} for services. */
    @Test
    public void testServices() throws Exception {
        try (IgniteEx g = startGrid()) {
            {
                ServiceConfiguration srvcCfg = new ServiceConfiguration();

                srvcCfg.setName("service");
                srvcCfg.setMaxPerNodeCount(1);
                srvcCfg.setService(new DummyService());
                srvcCfg.setNodeFilter(new TestNodeFilter());

                g.services().deploy(srvcCfg);

                SystemView<ServiceView> srvs = g.context().metric().view(SVCS_VIEW);

                assertEquals(g.context().service().serviceDescriptors().size(), F.size(srvs.iterator()));

                ServiceView sview = srvs.iterator().next();

                assertEquals(srvcCfg.getName(), sview.name());
                assertNotNull(sview.serviceId());
                assertEquals(srvcCfg.getMaxPerNodeCount(), sview.maxPerNodeCount());
                assertEquals(DummyService.class, sview.serviceClass());
                assertEquals(srvcCfg.getMaxPerNodeCount(), sview.maxPerNodeCount());
                assertNull(sview.cacheName());
                assertNull(sview.affinityKey());
                assertEquals(TestNodeFilter.class, sview.nodeFilter());
                assertFalse(sview.staticallyConfigured());
                assertEquals(g.localNode().id(), sview.originNodeId());
            }

            {
                g.createCache("test-cache");

                ServiceConfiguration srvcCfg = new ServiceConfiguration();

                srvcCfg.setName("service-2");
                srvcCfg.setMaxPerNodeCount(2);
                srvcCfg.setService(new DummyService());
                srvcCfg.setNodeFilter(new TestNodeFilter());
                srvcCfg.setCacheName("test-cache");
                srvcCfg.setAffinityKey(1L);

                g.services().deploy(srvcCfg);

                final ServiceView[] sview = {null};

                g.context().metric().<ServiceView>view(SVCS_VIEW).forEach(sv -> {
                    if (sv.name().equals(srvcCfg.getName()))
                        sview[0] = sv;
                });

                assertEquals(srvcCfg.getName(), sview[0].name());
                assertNotNull(sview[0].serviceId());
                assertEquals(srvcCfg.getMaxPerNodeCount(), sview[0].maxPerNodeCount());
                assertEquals(DummyService.class, sview[0].serviceClass());
                assertEquals(srvcCfg.getMaxPerNodeCount(), sview[0].maxPerNodeCount());
                assertEquals("test-cache", sview[0].cacheName());
                assertEquals("1", sview[0].affinityKey());
                assertEquals(TestNodeFilter.class, sview[0].nodeFilter());
                assertFalse(sview[0].staticallyConfigured());
                assertEquals(g.localNode().id(), sview[0].originNodeId());
            }
        }
    }

    /** Tests work of {@link SystemView} for compute grid {@link IgniteCompute#broadcastAsync(IgniteRunnable)} call. */
    @Test
    public void testComputeBroadcast() throws Exception {
        latch = new CountDownLatch(1);

        try (IgniteEx g1 = startGrid(0)) {
            SystemView<ComputeTaskView> tasks = g1.context().metric().view(TASKS_VIEW);

            for (int i = 0; i < 5; i++)
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

    /** Tests work of {@link SystemView} for compute grid {@link IgniteCompute#runAsync(IgniteRunnable)} call. */
    @Test
    public void testComputeRunnable() throws Exception {
        latch = new CountDownLatch(1);

        try (IgniteEx g1 = startGrid(0)) {
            SystemView<ComputeTaskView> tasks = g1.context().metric().view(TASKS_VIEW);

            g1.compute().runAsync(() -> {
                try {
                    latch.await();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });

            assertEquals(1, tasks.size());

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

    /** Tests work of {@link SystemView} for compute grid {@link IgniteCompute#apply(IgniteClosure, Object)} call. */
    @Test
    public void testComputeApply() throws Exception {
        latch = new CountDownLatch(1);

        try (IgniteEx g1 = startGrid(0)) {
            SystemView<ComputeTaskView> tasks = g1.context().metric().view(TASKS_VIEW);

            GridTestUtils.runAsync(() -> {
                g1.compute().apply(x -> {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    return 0;
                }, 1);
            });

            boolean res = GridTestUtils.waitForCondition(() -> tasks.size() == 1, 5_000);

            assertTrue(res);

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

    /**
     * Tests work of {@link SystemView} for compute grid
     * {@link IgniteCompute#affinityCallAsync(String, Object, IgniteCallable)} call.
     */
    @Test
    public void testComputeAffinityCall() throws Exception {
        latch = new CountDownLatch(1);

        try (IgniteEx g1 = startGrid(0)) {
            SystemView<ComputeTaskView> tasks = g1.context().metric().view(TASKS_VIEW);

            IgniteCache<Integer, Integer> cache = g1.createCache("test-cache");

            cache.put(1, 1);

            g1.compute().affinityCallAsync("test-cache", 1, () -> {
                try {
                    latch.await();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                return 0;
            });

            assertEquals(1, tasks.size());

            ComputeTaskView t = tasks.iterator().next();

            assertFalse(t.internal());
            assertEquals("test-cache", t.affinityCacheName());
            assertEquals(1, t.affinityPartitionId());
            assertTrue(t.taskClassName().startsWith(getClass().getName()));
            assertTrue(t.taskName().startsWith(getClass().getName()));
            assertEquals(g1.localNode().id(), t.taskNodeId());
            assertEquals("0", t.userVersion());

            latch.countDown();
        }
    }


    /** */
    @Test
    public void testComputeTask() throws Exception {
        latch = new CountDownLatch(1);

        try (IgniteEx g1 = startGrid(0)) {
            SystemView<ComputeTaskView> tasks = g1.context().metric().view(TASKS_VIEW);

            IgniteCache<Integer, Integer> cache = g1.createCache("test-cache");

            cache.put(1, 1);

            g1.compute().executeAsync(new ComputeTask<Object, Object>() {
                @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
                    @Nullable Object arg) throws IgniteException {
                    return Collections.singletonMap(new ComputeJob() {
                        @Override public void cancel() {
                            // No-op.
                        }

                        @Override public Object execute() throws IgniteException {
                            return 1;
                        }
                    }, subgrid.get(0));
                }

                @Override public ComputeJobResultPolicy result(ComputeJobResult res,
                    List<ComputeJobResult> rcvd) throws IgniteException {
                    return null;
                }

                @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    return 1;
                }
            }, 1);

            boolean res = GridTestUtils.waitForCondition(() -> tasks.size() == 1, 5_000);

            assertTrue(res);

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

    /** Test node filter. */
    public static class TestNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return true;
        }
    }
}

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
import java.util.UUID;
import javax.cache.event.CacheEntryEventFilter;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.list.MonitoringList;
import org.apache.ignite.internal.processors.metric.list.view.CacheGroupView;
import org.apache.ignite.internal.processors.metric.list.view.CacheView;
import org.apache.ignite.internal.processors.metric.list.view.ContinuousQueryView;
import org.apache.ignite.internal.processors.metric.list.view.ServiceView;
import org.apache.ignite.internal.processors.service.DummyService;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.util.lang.GridFunc.alwaysTrue;

/** */
public class MonitoringListSelfTest extends GridCommonAbstractTest {
    @Test
    /** */
    public void testCachesList() throws Exception {
        try (IgniteEx g = startGrid()) {
            Set<String> cacheNames = new HashSet<>(Arrays.asList("cache-1", "cache-2"));

            for (String name : cacheNames)
                g.createCache(name);

            MonitoringList<String, CacheView> caches = g.context().metric().list("caches");

            assertEquals("ignite-sys, cache-1, cache-2", 3, F.size(caches.iterator(), alwaysTrue()));

            for (CacheView row : caches)
                cacheNames.remove(row.cacheName());

            assertTrue(cacheNames.toString(), cacheNames.isEmpty());
        }
    }

    @Test
    /** */
    public void testCacheGroupsList() throws Exception {
        try(IgniteEx g = startGrid()) {
            Set<String> grpNames = new HashSet<>(Arrays.asList("grp-1", "grp-2"));

            for (String grpName : grpNames)
                g.createCache(new CacheConfiguration<>("cache-" + grpName).setGroupName(grpName));

            MonitoringList<String, CacheGroupView> grps = g.context().metric().list("cacheGroups");

            assertEquals("ignite-sys, grp-1, grp-2", 3, F.size(grps.iterator(), alwaysTrue()));

            for (CacheGroupView row : grps)
                grpNames.remove(row.groupName());

            assertTrue(grpNames.toString(), grpNames.isEmpty());
        }
    }

    @Test
    /** */
    public void testServices() throws Exception {
        try(IgniteEx g = startGrid()) {
            ServiceConfiguration srvcCfg = new ServiceConfiguration();

            srvcCfg.setName("service");
            srvcCfg.setMaxPerNodeCount(1);
            srvcCfg.setService(new DummyService());

            g.services().deploy(srvcCfg);

            MonitoringList<IgniteUuid, ServiceView> srvs = g.context().metric().list("services");

            assertEquals(1, F.size(srvs.iterator(), alwaysTrue()));

            ServiceView sview = srvs.iterator().next();

            assertEquals(srvcCfg.getName(), sview.name());
            assertEquals(srvcCfg.getMaxPerNodeCount(), sview.maxPerNodeCount());
            assertEquals(DummyService.class, sview.service());
        }
    }

    @Test
    /** */
    public void testContinuousQuery() throws Exception {
        try(IgniteEx g0 = startGrid(0); IgniteEx g1 = startGrid(1)) {
            IgniteCache cache = g0.createCache("cache-1");

            QueryCursor qry = cache.query(new ContinuousQuery()
                .setInitialQuery(new ScanQuery<>())
                .setPageSize(100)
                .setTimeInterval(1000)
                .setLocalListener(evts -> {
                    // No-op.
                })
                .setRemoteFilterFactory(() -> {
                    return (CacheEntryEventFilter)evt -> true;
                })
            );

            for (int i=0; i<100; i++)
                cache.put(i, i);

            MonitoringList<UUID, ContinuousQueryView> qrys =
                g0.context().metric().list(metricName("query", "continuous"));

            assertEquals(1, F.size(qrys.iterator(), alwaysTrue()));

            ContinuousQueryView cq = qrys.iterator().next(); //Info on originating node.

            assertEquals("cache-1", cq.cacheName());
            assertEquals(100, cq.bufferSize());
            assertEquals(1000, cq.interval());
            assertEquals(g0.localNode().id().toString(), cq.sessionId());
            //Local listener not null on originating node.
            assertTrue(cq.localListener().startsWith(this.getClass().getName()));
            assertTrue(cq.remoteFilter().startsWith(this.getClass().getName()));
            assertNull(cq.localTransformedListener());
            assertNull(cq.remoteTransformer());

            qrys = g1.context().metric().list(metricName("query", "continuous"));

            assertEquals(1, F.size(qrys.iterator(), alwaysTrue()));

            cq = qrys.iterator().next(); //Info on remote node.

            assertEquals("cache-1", cq.cacheName());
            assertEquals(100, cq.bufferSize());
            assertEquals(1000, cq.interval());
            assertEquals(g0.localNode().id().toString(), cq.sessionId());
            //Local listener is null on remote nodes.
            assertNull(cq.localListener());
            assertTrue(cq.remoteFilter().startsWith(this.getClass().getName()));
            assertNull(cq.localTransformedListener());
            assertNull(cq.remoteTransformer());
        }
    }

    @Test
    /** */
    public void testComputeClosures() throws Exception {
        try(IgniteEx g0 = startGrid(0)) {
            for (int i=0; i<10; i++) {
                g0.compute().run(() -> { });
            }

            MonitoringList<UUID, ContinuousQueryView> computeRunnable =
                g0.context().metric().list(metricName("compute", "runnables"));
        }
    }
}

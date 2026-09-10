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

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.service.DummyService;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.systemview.view.ServiceView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.junit.Test;

import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SVCS_VIEW;

/** Tests for {@link SystemView} for services. */
public class SystemViewServiceTest extends SystemViewAbstractTest {
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

                SystemView<ServiceView> srvs = g.context().systemView().view(SVCS_VIEW);

                assertEquals(g.context().service().serviceDescriptors().size(), F.size(srvs.iterator()));

                ServiceView sview = srvs.iterator().next();

                assertEquals(srvcCfg.getName(), sview.name());
                assertNotNull(sview.serviceId());
                assertEquals(DummyService.class, sview.serviceClass());
                assertEquals(srvcCfg.getMaxPerNodeCount(), sview.maxPerNodeCount());
                assertNull(sview.cacheName());
                assertNull(sview.affinityKey());
                assertEquals(TestNodeFilter.class, sview.nodeFilter());
                assertFalse(sview.staticallyConfigured());
                assertEquals(g.localNode().id(), sview.originNodeId());
                assertEquals(F.asMap(g.localNode().id(), 1), sview.topologySnapshot());
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

                g.context().systemView().<ServiceView>view(SVCS_VIEW).forEach(sv -> {
                    if (sv.name().equals(srvcCfg.getName()))
                        sview[0] = sv;
                });

                assertEquals(srvcCfg.getName(), sview[0].name());
                assertNotNull(sview[0].serviceId());
                assertEquals(DummyService.class, sview[0].serviceClass());
                assertEquals(srvcCfg.getMaxPerNodeCount(), sview[0].maxPerNodeCount());
                assertEquals("test-cache", sview[0].cacheName());
                assertEquals("1", sview[0].affinityKey());
                assertEquals(TestNodeFilter.class, sview[0].nodeFilter());
                assertFalse(sview[0].staticallyConfigured());
                assertEquals(g.localNode().id(), sview[0].originNodeId());
                assertEquals(F.asMap(g.localNode().id(), 2), sview[0].topologySnapshot());
            }
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

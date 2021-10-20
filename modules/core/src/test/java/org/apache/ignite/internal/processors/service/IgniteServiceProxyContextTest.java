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

package org.apache.ignite.internal.processors.service;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.resources.ServiceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.services.ServiceProxyContext;
import org.apache.ignite.services.ServiceProxyContextBuilder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests service proxy execution context.
 */
@RunWith(Parameterized.class)
public class IgniteServiceProxyContextTest extends GridCommonAbstractTest {
    /** Custom context attribute. */
    private static final String ATTR_NAME = "int.attr";

    /** Service name. */
    private static final String SVC_NAME = "test-svc";

    /** Injected service name. */
    private static final String SVC_NAME_INJECTED = "test-svc-injected";

    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** Max number of services per node. */
    private static final int SVC_PER_NODE = 2;

    /** Flag to deploy single service instance per cluster. */
    @Parameterized.Parameter(0)
    public boolean clusterSingleton;

    /** Whether or not Ignite should always contact the same remote service instance. */
    @Parameterized.Parameter(1)
    public boolean sticky;

    /** {@inheritDoc} */
    @Override public void beforeTestsStarted() throws Exception {
        startGrids(NODES_CNT - 1);
        startClientGrid(NODES_CNT - 1);
    }

    /** */
    @Parameterized.Parameters(name = "clusterSingleton={0}, sticky={1}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {
            {false, false},
            {false, true},
            {true, true},
        });
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setServiceConfiguration(
            serviceCfg(SVC_NAME_INJECTED, true),
            serviceCfg(SVC_NAME, clusterSingleton)
        );
    }

    /**
     * @param name Service name.
     * @param clusterSingleton Flag to deploy single service instance per cluster.
     * @return Service configuration.
     */
    private ServiceConfiguration serviceCfg(String name, boolean clusterSingleton) {
        return new ServiceConfiguration().setName(name).setTotalCount(clusterSingleton ? 1 : NODES_CNT * SVC_PER_NODE)
            .setMaxPerNodeCount(SVC_PER_NODE).setService(new TestServiceImpl());
    }

    /**
     * Check context attribute.
     */
    @Test
    public void testContextAttribute() {
        for (int i = 0; i < NODES_CNT; i++) {
            TestService proxy = createProxyWithContext(grid(i), i);

            assertEquals(i, proxy.attribute(false));
            assertEquals(i, proxy.attribute(true));
        }
    }

    /**
     * Check context attribute concurrently.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testContextAttributeMultithreaded() throws Exception {
        Map<TestService, Integer> proxies = new HashMap<>();

        for (int i = 0; i < G.allGrids().size(); i++) {
            proxies.put(createProxyWithContext(grid(i), i * 2), i * 2);
            proxies.put(createProxyWithContext(grid(i), i * 2 + 1), i * 2 + 1);
        }

        CountDownLatch startLatch = new CountDownLatch(1);

        GridCompoundFuture<Long, Long> compFut = new GridCompoundFuture<>();

        for (Map.Entry<TestService, Integer> e : proxies.entrySet()) {
            IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(() -> {
                startLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS);

                for (int i = 0; i < 1_000; i++) {
                    assertEquals(e.getValue(), e.getKey().attribute(false));
                    assertEquals(e.getValue(), e.getKey().attribute(true));
                }

                return true;
            }, 2, "worker");

            compFut.add(fut);
        }

        compFut.markInitialized();

        startLatch.countDown();

        compFut.get(getTestTimeout());
    }

    /**
     * @param node Ignite node.
     * @param attrVal Attribute value.
     * @return Service proxy instance.
     */
    private TestService createProxyWithContext(Ignite node, int attrVal) {
        return node.services().serviceProxy(SVC_NAME, TestService.class, sticky,
            new ServiceProxyContextBuilder(ATTR_NAME, attrVal).build(), 0);
    }

    /** */
    private static interface TestService extends Service {
        /**
         * @param useInjectedSvc Get attribute from the injected service.
         * @return Context attribute value.
         */
        public Object attribute(boolean useInjectedSvc);
    }

    /** */
    private static class TestServiceImpl implements TestService {
        /** Injected service. */
        @ServiceResource(serviceName = SVC_NAME_INJECTED, proxyInterface = TestService.class, forwardRequestAttributes = true)
        private TestService injected;

        /** {@inheritDoc} */
        @Override public Object attribute(boolean useInjectedSvc) {
            assert injected != null;

            return useInjectedSvc ? injected.attribute(false) : ServiceProxyContext.current().attribute(ATTR_NAME);
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }
    }
}

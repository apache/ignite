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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.resources.ServiceContextResource;
import org.apache.ignite.resources.ServiceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceCallContext;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests service caller context.
 */
@RunWith(Parameterized.class)
public class IgniteServiceCallContextTest extends GridCommonAbstractTest {
    /** String attribute name. */
    private static final String STR_ATTR_NAME = "str.attr";

    /** Binary attribute name. */
    private static final String BIN_ATTR_NAME = "bin.attr";

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
        return new ServiceConfiguration()
            .setName(name)
            .setTotalCount(clusterSingleton ? 1 : NODES_CNT * SVC_PER_NODE)
            .setMaxPerNodeCount(SVC_PER_NODE)
            .setService(new TestServiceImpl());
    }

    /**
     * Check proxy creation with an invalid implementation.
     */
    @Test
    public void testInvalidContextImplementation() {
        ServiceCallContext callCtx = new ServiceCallContext() {
            @Override public String attribute(String name) {
                return null;
            }

            @Override public byte[] binaryAttribute(String name) {
                return null;
            }
        };

        GridTestUtils.assertThrowsAnyCause(log, () -> grid(0).services().serviceProxy(SVC_NAME, TestService.class,
            sticky, callCtx), IllegalArgumentException.class, "\"callCtx\" has an invalid type.");
    }

    /**
     * Check context attribute.
     */
    @Test
    public void testContextAttribute() {
        for (int i = 0; i < NODES_CNT; i++) {
            String strVal = String.valueOf(i);
            byte[] binVal = strVal.getBytes();

            TestService proxy = createProxyWithContext(grid(i), strVal, binVal);

            assertEquals(strVal, proxy.attribute(false));
            assertEquals(strVal, proxy.attribute(true));

            assertTrue(Arrays.equals(binVal, proxy.binaryAttribute(false)));
            assertTrue(Arrays.equals(binVal, proxy.binaryAttribute(true)));
        }
    }

    /**
     * Check context attribute concurrently.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testContextAttributeMultithreaded() throws Exception {
        Map<TestService, T2<String, byte[]>> proxies = new HashMap<>();

        for (int i = 0; i < G.allGrids().size(); i++) {
            String strVal1 = String.valueOf(i * 2);
            String strVal2 = String.valueOf(i * 2 + 1);
            byte[] binVal1 = strVal1.getBytes();
            byte[] binVal2 = strVal2.getBytes();

            proxies.put(createProxyWithContext(grid(i), strVal1, binVal1), new T2<>(strVal1, binVal1));
            proxies.put(createProxyWithContext(grid(i), strVal2, binVal2), new T2<>(strVal2, binVal2));
        }

        int threadsPerProxy = 2;
        CyclicBarrier barrier = new CyclicBarrier(proxies.size() * threadsPerProxy);
        GridCompoundFuture<Long, Long> compFut = new GridCompoundFuture<>();

        for (Map.Entry<TestService, T2<String, byte[]>> e : proxies.entrySet()) {
            IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(() -> {
                barrier.await(getTestTimeout(), TimeUnit.MILLISECONDS);

                for (int i = 0; i < 100; i++) {
                    T2<String, byte[]> expVals = e.getValue();
                    TestService proxy = e.getKey();

                    assertEquals(expVals.get1(), proxy.attribute(false));
                    assertEquals(expVals.get1(), proxy.attribute(true));

                    assertTrue(Arrays.equals(expVals.get2(), proxy.binaryAttribute(false)));
                    assertTrue(Arrays.equals(expVals.get2(), proxy.binaryAttribute(true)));
                }

                return true;
            }, threadsPerProxy, "worker");

            compFut.add(fut);
        }

        compFut.markInitialized();

        compFut.get(getTestTimeout());
    }

    /**
     * @param node Ignite node.
     * @param attrVal String attribute value.
     * @param binVal Binary attribute value.
     * @return Service proxy instance.
     */
    private TestService createProxyWithContext(Ignite node, String attrVal, byte[] binVal) {
        ServiceCallContext callCtx = ServiceCallContext.builder()
            .put(STR_ATTR_NAME, attrVal)
            .put(BIN_ATTR_NAME, binVal)
            .build();

        return node.services().serviceProxy(SVC_NAME, TestService.class, sticky, callCtx);
    }

    /** */
    private static interface TestService extends Service {
        /**
         * @param useInjectedSvc Get attribute from the injected service.
         * @return Context attribute value.
         */
        public String attribute(boolean useInjectedSvc);

        /**
         * @param useInjectedSvc Get attribute from the injected service.
         * @return Context attribute value.
         */
        public byte[] binaryAttribute(boolean useInjectedSvc);
    }

    /** */
    private static class TestServiceImpl implements TestService {
        /** Injected service. */
        @ServiceResource(serviceName = SVC_NAME_INJECTED, proxyInterface = TestService.class, forwardCallerContext = true)
        private TestService injected;

        /** Service context. */
        @ServiceContextResource
        private ServiceContext ctx;

        /** {@inheritDoc} */
        @Override public String attribute(boolean useInjectedSvc) {
            assert injected != null;

            ServiceCallContext callCtx = ctx.currentCallContext();

            return useInjectedSvc ? injected.attribute(false) : callCtx.attribute(STR_ATTR_NAME);
        }

        /** {@inheritDoc} */
        @Override public byte[] binaryAttribute(boolean useInjectedSvc) {
            assert injected != null;

            ServiceCallContext callCtx = ctx.currentCallContext();

            return useInjectedSvc ? injected.binaryAttribute(false) : callCtx.binaryAttribute(BIN_ATTR_NAME);
        }
    }
}

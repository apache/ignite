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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.naming.NoPermissionException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.ServiceContextResource;
import org.apache.ignite.resources.ServiceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceCallContext;
import org.apache.ignite.services.ServiceCallInterceptor;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests service call interceptor.
 */
@RunWith(Parameterized.class)
public class IgniteServiceCallInterceptorTest extends GridCommonAbstractTest implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** String attribute name. */
    private static final String STR_ATTR_NAME = "str.attr";

    /** Injected service name. */
    private static final String SVC_NAME_INJECTED = "test-svc-injected";

    /** Intercepted service name. */
    private static final String SVC_NAME_INTERCEPTED = "test-svc-intercepted";

    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** Max number of services per node. */
    private static final int SVC_PER_NODE = 2;

    /** Interceptor call counter. */
    private static final AtomicInteger interceptorCallCntr = new AtomicInteger();

    /** Service method call counter. */
    private static final AtomicInteger svcCallCntr = new AtomicInteger();

    /** Ignite client node. */
    private static Ignite client;

    /** Flag to deploy single service instance per cluster. */
    @Parameterized.Parameter(0)
    public boolean clusterSingleton;

    /** Whether Ignite should always contact the same remote service instance. */
    @Parameterized.Parameter(1)
    public boolean sticky;

    /** {@inheritDoc} */
    @Override public void beforeTestsStarted() throws Exception {
        startGrids(NODES_CNT - 1);
        client = startClientGrid(NODES_CNT - 1);
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
    @Override public void afterTest() {
        grid(0).services().cancelAll();
    }

    /**
     * @param name Service name.
     * @param svc Service.
     * @param singleton Flag to deploy single service instance per cluster.
     * @param interceptors Interceptors.
     * @return Service configuration.
     */
    private ServiceConfiguration serviceCfg(String name, Service svc, boolean singleton, ServiceCallInterceptor... interceptors) {
        return new ServiceConfiguration()
            .setName(name)
            .setTotalCount(singleton ? 1 : NODES_CNT * SVC_PER_NODE)
            .setMaxPerNodeCount(SVC_PER_NODE)
            .setService(svc)
            .setInterceptors(interceptors);
    }

    /**
     * Validates configuration comparison during service deployment.
     */
    @Test
    public void testRedeploy() {
        ServiceCallInterceptor interceptor = (mtd, args, ctx, next) -> "1";

        ServiceConfiguration cfg = serviceCfg(SVC_NAME_INTERCEPTED, new TestServiceImpl(), clusterSingleton, interceptor);
        client.services().deploy(cfg);

        TestService proxy = client.services().serviceProxy(SVC_NAME_INTERCEPTED, TestService.class, false);
        assertEquals("1", proxy.method(0));

        // Redeploy with the same configuration.
        cfg.setInterceptors(interceptor);
        client.services().deploy(cfg);

        // Redeploy with different configuration.
        cfg.setInterceptors((mtd, args, ctx, next) -> "2");

        GridTestUtils.assertThrowsAnyCause(log, () -> {
            client.services().deploy(cfg);

            return null;
        }, ServiceDeploymentException.class, null);

        // Redeploy the service with a valid configuration.
        client.services().cancel(SVC_NAME_INTERCEPTED);

        client.services().deploy(cfg);
        assertEquals("2", proxy.method(0));

        // Undeploy service.
        client.services().cancel(SVC_NAME_INTERCEPTED);

        // Deploy with an interceptor that will not be able to deserialize.
        cfg.setInterceptors(new CriticalErrorInterceptor());

        GridTestUtils.assertThrowsAnyCause(log, () -> {
            client.services().deploy(cfg);

            return null;
        }, ServiceDeploymentException.class, null);

        for (int i = 0; i < NODES_CNT - 1; i++)
            assertNull("idx=" + i, grid(i).services().service(SVC_NAME_INTERCEPTED));
    }

    /**
     * Checks if an interceptor exception is being passed to the user.
     */
    @Test
    public void testException() {
        interceptorCallCntr.set(0);
        svcCallCntr.set(0);

        Exception expE = new NoPermissionException("Request is forbidden.");
        String ctxVal = "42";

        ServiceCallInterceptor security = (mtd, args, ctx, next) -> {
            ServiceCallContext callCtx = ctx.currentCallContext();

            if (callCtx == null || !ctxVal.equals(callCtx.attribute(STR_ATTR_NAME)))
                throw expE;

            return next.call();
        };

        ServiceCallInterceptor second = (mtd, args, ctx, next) -> {
            interceptorCallCntr.incrementAndGet();

            return next.call();
        };

        client.services().deployAll(Collections.singletonList(
            serviceCfg(SVC_NAME_INTERCEPTED, new TestServiceImpl(), clusterSingleton, security, second)
        ));

        TestService noCtxProxy = client.services().serviceProxy(SVC_NAME_INTERCEPTED, TestService.class, sticky);
        GridTestUtils.assertThrowsAnyCause(null, () -> noCtxProxy.method(0), expE.getClass(), expE.getMessage());
        assertEquals(0, interceptorCallCntr.get());
        assertEquals(0, svcCallCntr.get());

        ServiceCallContext callCtx = ServiceCallContext.builder().put(STR_ATTR_NAME, "42").build();

        String res = client.services().serviceProxy(SVC_NAME_INTERCEPTED, TestService.class, sticky, callCtx).method(0);
        assertEquals("cls=" + TestServiceImpl.class.getSimpleName() + ", ctxVal=" + ctxVal + ", arg=0", res);
        assertEquals(1, interceptorCallCntr.get());
        assertEquals(1, svcCallCntr.get());
    }

    /**
     * Ensures that the interceptor can completely override the call and prevent the service method invocation.
     */
    @Test
    public void testBasicInterception() {
        svcCallCntr.set(0);

        String expMsg = "intercepted";

        client.services().deployAll(Collections.singletonList(
            serviceCfg(SVC_NAME_INTERCEPTED, new TestServiceImpl(), clusterSingleton, (mtd, args, ctx, next) -> expMsg)
        ));

        TestService svcProxy = client.services().serviceProxy(SVC_NAME_INTERCEPTED, TestService.class, sticky);

        assertEquals(expMsg, svcProxy.method(1));
        assertEquals(0, svcCallCntr.get());
    }

    /**
     * Checks the call order and parameters of the interceptor in multithreaded mode.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOrder() throws Exception {
        int interceptorsCnt = 8;
        int threadCnt = 16;

        ServiceCallInterceptor[] interceptors = new ServiceCallInterceptor[interceptorsCnt];

        for (int i = 0; i < interceptorsCnt; i++)
            interceptors[interceptorsCnt - i - 1] = new SvcInterceptor(null, i);

        client.services().deployAll(Arrays.asList(
            serviceCfg(SVC_NAME_INTERCEPTED, new TestServiceImpl(), clusterSingleton, interceptors),
            serviceCfg(SVC_NAME_INJECTED, new TestServiceInjected(), !clusterSingleton, interceptors)
        ));

        ServiceCallContext callCtx1 = ServiceCallContext.builder().put(STR_ATTR_NAME, "ctxVal1").build();
        ServiceCallContext callCtx2 = ServiceCallContext.builder().put(STR_ATTR_NAME, "ctxVal2").build();

        AtomicInteger cntr = new AtomicInteger();
        CyclicBarrier barrier = new CyclicBarrier(threadCnt);

        GridTestUtils.runMultiThreaded(() -> {
            int arg = cntr.incrementAndGet();
            IgniteServices services = grid(arg % NODES_CNT).services();

            TestService ctx1Proxy = services.serviceProxy(SVC_NAME_INTERCEPTED, TestService.class, sticky, callCtx1);
            TestService ctx2Proxy = services.serviceProxy(SVC_NAME_INTERCEPTED, TestService.class, sticky, callCtx2);

            String exoCtx1 = fmtExpResult(TestServiceImpl.class, callCtx1, interceptors.length, "method", null, arg);
            String exoCtx2 = fmtExpResult(TestServiceImpl.class, callCtx2, interceptors.length, "method", null, arg);
            String exoCtx1inj = fmtExpResult(TestServiceInjected.class, callCtx1, interceptors.length, "method", "injected", arg);
            String exoCtx2inj = fmtExpResult(TestServiceInjected.class, callCtx2, interceptors.length, "method", "injected", arg);

            barrier.await(getTestTimeout(), TimeUnit.MILLISECONDS);

            for (int i = 0; i < 100; i++) {
                assertEquals(exoCtx1inj, ctx1Proxy.injected(arg));
                assertEquals(exoCtx2inj, ctx2Proxy.injected(arg));
                assertEquals(exoCtx1, ctx1Proxy.method(arg));
                assertEquals(exoCtx2, ctx2Proxy.method(arg));
            }

            return null;
        }, threadCnt, "worker");
    }

    /**
     * @param cls Service implementation class name.
     * @param ctx Service call context.
     * @param incpsCnt Count of interceptors.
     * @param mtd1 First method that was called.
     * @param mtd2 Second method that was called (if one service calls another).
     * @param arg Method argument.
     * @return Expected result of the method call.
     */
    private String fmtExpResult(Class<?> cls, ServiceCallContext ctx, int incpsCnt, String mtd1, String mtd2, int arg) {
        SB buf = new SB("cls=" + cls.getSimpleName() + ", ctxVal=" + ctx.attribute(STR_ATTR_NAME) + ", arg=" + arg);

        for (String method : new String[] {mtd1, mtd2}) {
            if (method == null)
                break;

            for (int i = 0; i < incpsCnt; i++)
                buf.a("; mtd=").a(method).a(", ctxVal=").a(ctx.attribute(STR_ATTR_NAME)).a(", arg=").a(arg).a(", id=").a(i);
        }

        return buf.toString();
    }

    /** */
    private interface TestService extends Service {
        /** */
        String method(int arg);

        /** */
        String injected(int arg);
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
        @Override public String method(int arg) {
            svcCallCntr.incrementAndGet();

            ServiceCallContext callCtx = ctx.currentCallContext();

            String attrVal = callCtx == null ? null : ctx.currentCallContext().attribute(STR_ATTR_NAME);

            return "cls=" + getClass().getSimpleName() + ", ctxVal=" + attrVal + ", arg=" + arg;
        }

        /** {@inheritDoc} */
        @Override public String injected(int arg) {
            assert injected != null;

            return injected.method(arg);
        }
    }

    /** */
    private static class TestServiceInjected extends TestServiceImpl {
        /** */
        private static final long serialVersionUID = 0L;
    }

    /** Service call interceptor. */
    static class SvcInterceptor implements ServiceCallInterceptor {
        /** */
        private static final long serialVersionUID = 0L;

        /** Ignite. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Logger. */
        @LoggerResource
        private IgniteLogger log;

        /** Expected method name. */
        private final String expMtdName;

        /** Interceptor ID. */
        private final int id;

        /**
         * @param expMtdName Expected method name.
         * @param id Interceptor ID.
         */
        public SvcInterceptor(String expMtdName, int id) {
            this.expMtdName = expMtdName;
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public Object invoke(String mtd, Object[] args, ServiceContext ctx, Callable<Object> next) throws Exception {
            assert ignite != null;
            assert log != null;

            if (expMtdName != null)
                assertEquals(expMtdName, mtd);

            Object arg = args[0];

            ServiceCallContext callCtx = ctx.currentCallContext();

            assert callCtx != null;

            Object res = next.call();

            callCtx = ctx.currentCallContext();

            assert callCtx != null : res;

            String ctxVal = callCtx == null ? null : ctx.currentCallContext().attribute(STR_ATTR_NAME);

            assert ctxVal != null;

            return res + "; mtd=" + mtd + ", ctxVal=" + ctxVal + ", arg=" + arg + ", id=" + id;
        }
    }

    /** */
    private static class CriticalErrorInterceptor implements ServiceCallInterceptor, Externalizable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** */
        public CriticalErrorInterceptor() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Object invoke(String mtd, Object[] args, ServiceContext ctx,
            Callable<Object> next) throws Exception {

            return next.call();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            throw new AssertionError("Expected critical error.");
        }
    }
}

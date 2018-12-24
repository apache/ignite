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

package org.apache.ignite.marshaller;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.IgniteScheduler;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.GridKernalTestUtils;
import org.apache.ignite.internal.IgniteComputeImpl;
import org.apache.ignite.internal.IgniteEventsImpl;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteMessagingImpl;
import org.apache.ignite.internal.IgniteSchedulerImpl;
import org.apache.ignite.internal.IgniteServicesImpl;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.cluster.ClusterNodeLocalMapImpl;
import org.apache.ignite.internal.executor.GridExecutorService;
import org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy;
import org.apache.ignite.internal.processors.service.DummyService;
import org.apache.ignite.internal.util.GridByteArrayList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.p2p.GridP2PTestJob;
import org.apache.ignite.p2p.GridP2PTestTask;
import org.apache.ignite.testframework.GridTestClassLoader;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.events.EventType.EVTS_CACHE;

/**
 * Common test for marshallers.
 */
@RunWith(JUnit4.class)
public abstract class GridMarshallerAbstractTest extends GridCommonAbstractTest implements Serializable {
    /** */
    private static final String CACHE_NAME = "namedCache";

    /** */
    private static Marshaller marsh;

    /** */
    private static String igniteInstanceName;

    /** Closure job. */
    protected IgniteInClosure<String> c1 = new IgniteInClosure<String>() {
        @Override public void apply(String s) {
            // No-op.
        }
    };

    /** Closure job. */
    protected IgniteClosure<String, String> c2 = new IgniteClosure<String, String>() {
        @Override public String apply(String s) {
            return s;
        }
    };

    /** Argument producer. */
    protected IgniteOutClosure<String> c3 = new IgniteOutClosure<String>() {
        @Nullable @Override public String apply() {
            return null;
        }
    };

    /** Reducer. */
    protected IgniteReducer<String, Object> c4 = new IgniteReducer<String, Object>() {
        @Override public boolean collect(String e) {
            return true;
        }

        @Nullable @Override public Object reduce() {
            return null;
        }
    };

    /** */
    protected GridMarshallerAbstractTest() {
        super(/*start grid*/true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration namedCache = new CacheConfiguration(DEFAULT_CACHE_NAME);

        namedCache.setName(CACHE_NAME);
        namedCache.setAtomicityMode(TRANSACTIONAL);

        cfg.setMarshaller(marshaller());
        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME), namedCache);

        return cfg;
    }

    /**
     * @return Marshaller.
     */
    protected abstract Marshaller marshaller();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        marsh = grid().configuration().getMarshaller();
        igniteInstanceName = grid().configuration().getIgniteInstanceName();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefaultCache() throws Exception {
        IgniteCache<String, String> cache = grid().cache(DEFAULT_CACHE_NAME);

        cache.put("key", "val");

        GridMarshallerTestBean inBean = newTestBean(cache);

        byte[] buf = marshal(inBean);

        GridMarshallerTestBean outBean = unmarshal(buf);

        assert inBean.getObjectField() != null;
        assert outBean.getObjectField() != null;

        assert inBean.getObjectField().getClass().equals(GatewayProtectedCacheProxy.class);
        assert outBean.getObjectField().getClass().equals(GatewayProtectedCacheProxy.class);

        assert inBean != outBean;

        IgniteCache<String, String> cache0 = (IgniteCache<String, String>)outBean.getObjectField();

        assertEquals(DEFAULT_CACHE_NAME, cache0.getName());
        assertEquals("val", cache0.get("key"));

        outBean.checkNullResources();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNamedCache() throws Exception {
        IgniteCache<String, String> cache = grid().cache(CACHE_NAME);

        cache.put("key", "val");

        GridMarshallerTestBean inBean = newTestBean(cache);

        byte[] buf = marshal(inBean);

        GridMarshallerTestBean outBean = unmarshal(buf);

        assert inBean.getObjectField() != null;
        assert outBean.getObjectField() != null;

        assert inBean.getObjectField().getClass().equals(GatewayProtectedCacheProxy.class);
        assert outBean.getObjectField().getClass().equals(GatewayProtectedCacheProxy.class);

        assert inBean != outBean;

        IgniteCache<String, String> cache0 = (IgniteCache<String, String>)outBean.getObjectField();

        assertEquals(CACHE_NAME, cache0.getName());
        assertEquals("val", cache0.get("key"));

        outBean.checkNullResources();
    }

    /**
     * Tests marshalling.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testMarshalling() throws Exception {
        GridMarshallerTestBean inBean = newTestBean(new Object());

        byte[] buf = marshal(inBean);

        GridMarshallerTestBean outBean = unmarshal(buf);

        assert inBean.getObjectField() != null;
        assert outBean.getObjectField() != null;

        assert inBean.getObjectField().getClass().equals(Object.class);
        assert outBean.getObjectField().getClass().equals(Object.class);

        assert inBean.getObjectField() != outBean.getObjectField();

        assert inBean != outBean;

        assert inBean.equals(outBean);

        outBean.checkNullResources();
    }

    /**
     * Tests marshal anonymous class instance.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testMarshallingAnonymousClassInstance() throws Exception {
        final Ignite g = grid();

        GridMarshallerTestBean inBean = newTestBean(new IgniteClosure() {
            /** */
            private Iterable<ClusterNode> nodes = g.cluster().nodes();

            /** {@inheritDoc} */
            @Override public Object apply(Object o) {
                return nodes;
            }
        });

        byte[] buf = marshal(inBean);

        GridMarshallerTestBean outBean = unmarshal(buf);

        assert inBean.getObjectField() != null;
        assert outBean.getObjectField() != null;

        assert IgniteClosure.class.isAssignableFrom(inBean.getObjectField().getClass());
        assert IgniteClosure.class.isAssignableFrom(outBean.getObjectField().getClass());

        assert inBean.getObjectField() != outBean.getObjectField();

        assert inBean != outBean;

        assert inBean.equals(outBean);

        outBean.checkNullResources();
    }

    /**
     * Tests marshal local class instance.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testMarshallingLocalClassInstance() throws Exception {
        /**
         * Local class.
         */
        class LocalRunnable implements Runnable, Serializable {
            /** {@inheritDoc} */
            @Override public void run() {
                // No-op.
            }
        }

        GridMarshallerTestBean inBean = newTestBean(new LocalRunnable());

        byte[] buf = marshal(inBean);

        GridMarshallerTestBean outBean = unmarshal(buf);

        assert inBean.getObjectField() != null;
        assert outBean.getObjectField() != null;

        assert Runnable.class.isAssignableFrom(inBean.getObjectField().getClass());
        assert Runnable.class.isAssignableFrom(outBean.getObjectField().getClass());

        assert inBean.getObjectField() != outBean.getObjectField();

        assert inBean != outBean;

        assert inBean.equals(outBean);

        outBean.checkNullResources();
    }

    /**
     * Tests marshal nested class instance.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testMarshallingNestedClassInstance() throws Exception {
        GridMarshallerTestBean inBean = newTestBean(new NestedClass());

        byte[] buf = marshal(inBean);

        GridMarshallerTestBean outBean = unmarshal(buf);

        assert inBean.getObjectField() != null;
        assert outBean.getObjectField() != null;

        assert inBean.getObjectField().getClass().equals(NestedClass.class);
        assert outBean.getObjectField().getClass().equals(NestedClass.class);

        assert inBean.getObjectField() != outBean.getObjectField();

        assert inBean != outBean;

        assert inBean.equals(outBean);

        outBean.checkNullResources();
    }

    /**
     * Tests marshal static nested class instance.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testMarshallingStaticNestedClassInstance() throws Exception {
        GridMarshallerTestBean inBean = newTestBean(new StaticNestedClass());

        byte[] buf = marshal(inBean);

        GridMarshallerTestBean outBean = unmarshal(buf);

        assert inBean.getObjectField() != null;
        assert outBean.getObjectField() != null;

        assert inBean.getObjectField().getClass().equals(StaticNestedClass.class);
        assert outBean.getObjectField().getClass().equals(StaticNestedClass.class);

        assert inBean.getObjectField() != outBean.getObjectField();

        assert inBean != outBean;

        assert inBean.equals(outBean);

        outBean.checkNullResources();
    }

    /**
     * Tests marshal {@code null}.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testMarshallingNullObject() throws Exception {
        GridMarshallerTestBean inBean = newTestBean(null);

        byte[] buf = marshal(inBean);

        GridMarshallerTestBean outBean = unmarshal(buf);

        assert inBean.getObjectField() == null;
        assert outBean.getObjectField() == null;

        assert inBean != outBean;

        assert inBean.equals(outBean);

        outBean.checkNullResources();
    }

    /**
     * Tests marshal arrays of primitives.
     *
     * @throws IgniteCheckedException If marshalling failed.
     */
    @SuppressWarnings({"ZeroLengthArrayAllocation"})
    @Test
    public void testMarshallingArrayOfPrimitives() throws IgniteCheckedException {
        char[] inChars = "vasya".toCharArray();

        char[] outChars = unmarshal(marshal(inChars));

        assertTrue(Arrays.equals(inChars, outChars));

        boolean[][] inBools = new boolean[][]{
            {true}, {}, {true, false, true}
        };

        boolean[][] outBools = unmarshal(marshal(inBools));

        assertEquals(inBools.length, outBools.length);

        for (int i = 0; i < inBools.length; i++)
            assertTrue(Arrays.equals(inBools[i], outBools[i]));

        int[] inInts = new int[] {1,2,3,4,5,6,7};

        int[] outInts = unmarshal(marshal(inInts));

        assertEquals(inInts.length, outInts.length);

        assertTrue(Arrays.equals(inInts, outInts));
    }

    /**
     * Tests marshal classes
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testExternalClassesMarshalling() throws Exception {
        ClassLoader tstClsLdr = new GridTestClassLoader(
            Collections.singletonMap("org/apache/ignite/p2p/p2p.properties", "resource=loaded"),
            getClass().getClassLoader(),
            GridP2PTestTask.class.getName(), GridP2PTestJob.class.getName()
            );

        ComputeTask<?, ?> inTask = (ComputeTask<?, ?>)tstClsLdr.loadClass(GridP2PTestTask.class.getName()).
            newInstance();

        byte[] buf = marsh.marshal(inTask);

        ComputeTask<?, ?> outTask = marsh.unmarshal(buf, tstClsLdr);

        assert inTask != outTask;
        assert inTask.getClass().equals(outTask.getClass());
    }

    /**
     * Tests marshal {@link IgniteKernal} instance.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testGridKernalMarshalling() throws Exception {
        GridMarshallerTestBean inBean = newTestBean(grid());

        byte[] buf = marshal(inBean);

        GridMarshallerTestBean outBean = unmarshal(buf);

        assert inBean.getObjectField() != null;
        assert outBean.getObjectField() != null;

        assert inBean.getObjectField().getClass().equals(IgniteKernal.class);
        assert outBean.getObjectField().getClass().equals(IgniteKernal.class);

        assert inBean != outBean;

        assert inBean.equals(outBean);

        outBean.checkNullResources();
    }

    /**
     * Tests marshal {@link org.apache.ignite.cluster.ClusterGroup} instance.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testSubgridMarshalling() throws Exception {
        final Ignite ignite = grid();

        GridMarshallerTestBean inBean = newTestBean(ignite.cluster().forPredicate(new IgnitePredicate<ClusterNode>() {
            @Override public boolean apply(ClusterNode n) {
                return n.id().equals(ignite.cluster().localNode().id());
            }
        }));

        byte[] buf = marshal(inBean);

        GridMarshallerTestBean outBean = unmarshal(buf);

        assert inBean.getObjectField() != null;
        assert outBean.getObjectField() != null;

        assert inBean.getObjectField().getClass().equals(ClusterGroupAdapter.class);
        assert outBean.getObjectField().getClass().equals(ClusterGroupAdapter.class);

        assert inBean != outBean;
        assert inBean.equals(outBean);

        outBean.checkNullResources();
    }

    /**
     * Tests marshal {@link org.apache.ignite.IgniteLogger} instance.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testLoggerMarshalling() throws Exception {
        GridMarshallerTestBean inBean = newTestBean(grid().log());

        byte[] buf = marshal(inBean);

        GridMarshallerTestBean outBean = unmarshal(buf);

        assert inBean.getObjectField() != null;
        assert outBean.getObjectField() != null;

        assert IgniteLogger.class.isAssignableFrom(inBean.getObjectField().getClass());
        assert IgniteLogger.class.isAssignableFrom(outBean.getObjectField().getClass());

        assert inBean != outBean;
        assert inBean.equals(outBean);

        outBean.checkNullResources();
    }

    /**
     * Tests marshal {@link ClusterNodeLocalMapImpl} instance.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testNodeLocalMarshalling() throws Exception {
        ConcurrentMap<String, String> loc = grid().cluster().nodeLocalMap();

        String key = "test-key";
        String val = "test-val";

        loc.put(key, val);

        GridMarshallerTestBean inBean = newTestBean(loc);

        byte[] buf = marshal(inBean);

        GridMarshallerTestBean outBean = unmarshal(buf);

        assert inBean.getObjectField() != null;
        assert outBean.getObjectField() != null;

        assert inBean.getObjectField().getClass().equals(ClusterNodeLocalMapImpl.class);
        assert outBean.getObjectField().getClass().equals(ClusterNodeLocalMapImpl.class);

        assert inBean != outBean;
        assert inBean.equals(outBean);

        outBean.checkNullResources();

        loc = (ConcurrentMap<String, String>)outBean.getObjectField();

        assert loc.size() == 1;
        assert val.equals(loc.get(key));
    }

    /**
     * Tests marshal {@link GridExecutorService} instance.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testExecutorServiceMarshalling() throws Exception {
        ExecutorService inSrvc = grid().executorService();

        GridMarshallerTestBean inBean = newTestBean(inSrvc);

        byte[] buf = marshal(inBean);

        GridMarshallerTestBean outBean = unmarshal(buf);

        assert inBean.getObjectField() != null;
        assert outBean.getObjectField() != null;

        assert inBean.getObjectField().getClass().equals(GridExecutorService.class);
        assert outBean.getObjectField().getClass().equals(GridExecutorService.class);

        assert inBean != outBean;
        assert inBean.equals(outBean);

        outBean.checkNullResources();

        ExecutorService outSrvc = (ExecutorService)outBean.getObjectField();

        assert inSrvc.isShutdown() == outSrvc.isShutdown();
        assert inSrvc.isTerminated() == outSrvc.isTerminated();
    }

    /**
     * Tests marshal {@link GridKernalContext} instance.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testKernalContext() throws Exception {
        GridMarshallerTestBean inBean = newTestBean(GridKernalTestUtils.context(grid()));

        byte[] buf = marshal(inBean);

        GridMarshallerTestBean outBean = unmarshal(buf);

        assert inBean.getObjectField() != null;
        assert outBean.getObjectField() != null;

        assert inBean.getObjectField().getClass().equals(GridKernalContextImpl.class);
        assert outBean.getObjectField().getClass().equals(GridKernalContextImpl.class);

        assert inBean != outBean;
        assert inBean.equals(outBean);

        outBean.checkNullResources();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScheduler() throws Exception {
        IgniteScheduler scheduler = grid().scheduler();

        IgniteFuture<?> fut = scheduler.runLocal(new Runnable() {
            @Override public void run() {
                // No-op.
            }
        });

        fut.get();

        GridMarshallerTestBean inBean = newTestBean(scheduler);

        byte[] buf = marshal(inBean);

        GridMarshallerTestBean outBean = unmarshal(buf);

        assert inBean.getObjectField() != null;
        assert outBean.getObjectField() != null;

        assert inBean.getObjectField().getClass().equals(IgniteSchedulerImpl.class);
        assert outBean.getObjectField().getClass().equals(IgniteSchedulerImpl.class);

        assert inBean != outBean;
        assert inBean.equals(outBean);

        outBean.checkNullResources();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCompute() throws Exception {
        IgniteConfiguration cfg = optimize(getConfiguration("g1"));

        try (Ignite g1 = G.start(cfg)) {
            IgniteCompute compute = compute(grid().cluster().forNode(g1.cluster().localNode()));

            compute.run(new IgniteRunnable() {
                @Override public void run() {
                    // No-op.
                }
            });

            GridMarshallerTestBean inBean = newTestBean(compute);

            byte[] buf = marshal(inBean);

            GridMarshallerTestBean outBean = unmarshal(buf);

            assert inBean.getObjectField() != null;
            assert outBean.getObjectField() != null;

            assert inBean.getObjectField().getClass().equals(IgniteComputeImpl.class);
            assert outBean.getObjectField().getClass().equals(IgniteComputeImpl.class);

            assert inBean != outBean;
            assert inBean.equals(outBean);

            ClusterGroup inPrj = compute.clusterGroup();
            ClusterGroup outPrj = ((IgniteCompute)outBean.getObjectField()).clusterGroup();

            assert inPrj.getClass().equals(outPrj.getClass());
            assert F.eqNotOrdered(inPrj.nodes(), outPrj.nodes());

            outBean.checkNullResources();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEvents() throws Exception {
        IgniteConfiguration cfg = optimize(getConfiguration("g1"));

        try (Ignite g1 = G.start(cfg)) {
            IgniteEvents evts = events(grid().cluster().forNode(g1.cluster().localNode()));

            evts.localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event gridEvt) {
                    return true;
                }
            }, EVTS_CACHE);

            grid().cache(DEFAULT_CACHE_NAME).put(1, 1);

            GridMarshallerTestBean inBean = newTestBean(evts);

            byte[] buf = marshal(inBean);

            GridMarshallerTestBean outBean = unmarshal(buf);

            assert inBean.getObjectField() != null;
            assert outBean.getObjectField() != null;

            assert inBean.getObjectField().getClass().equals(IgniteEventsImpl.class);
            assert outBean.getObjectField().getClass().equals(IgniteEventsImpl.class);

            assert inBean != outBean;
            assert inBean.equals(outBean);

            ClusterGroup inPrj = evts.clusterGroup();
            ClusterGroup outPrj = ((IgniteEvents)outBean.getObjectField()).clusterGroup();

            assert inPrj.getClass().equals(outPrj.getClass());
            assert F.eqNotOrdered(inPrj.nodes(), outPrj.nodes());

            outBean.checkNullResources();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMessaging() throws Exception {
        IgniteConfiguration cfg = optimize(getConfiguration("g1"));

        try (Ignite g1 = G.start(cfg)) {
            IgniteMessaging messaging = message(grid().cluster().forNode(g1.cluster().localNode()));

            messaging.send(null, "test");

            GridMarshallerTestBean inBean = newTestBean(messaging);

            byte[] buf = marshal(inBean);

            GridMarshallerTestBean outBean = unmarshal(buf);

            assert inBean.getObjectField() != null;
            assert outBean.getObjectField() != null;

            assert inBean.getObjectField().getClass().equals(IgniteMessagingImpl.class);
            assert outBean.getObjectField().getClass().equals(IgniteMessagingImpl.class);

            assert inBean != outBean;
            assert inBean.equals(outBean);

            ClusterGroup inPrj = messaging.clusterGroup();
            ClusterGroup outPrj = ((IgniteMessaging)outBean.getObjectField()).clusterGroup();

            assert inPrj.getClass().equals(outPrj.getClass());
            assert F.eqNotOrdered(inPrj.nodes(), outPrj.nodes());

            outBean.checkNullResources();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testServices() throws Exception {
        IgniteConfiguration cfg = optimize(getConfiguration("g1"));

        try (Ignite g1 = G.start(cfg)) {
            IgniteServices services = grid().services(grid().cluster().forNode(g1.cluster().localNode()));

            services.deployNodeSingleton("test", new DummyService());

            GridMarshallerTestBean inBean = newTestBean(services);

            byte[] buf = marshal(inBean);

            GridMarshallerTestBean outBean = unmarshal(buf);

            assert inBean.getObjectField() != null;
            assert outBean.getObjectField() != null;

            assert inBean.getObjectField().getClass().equals(IgniteServicesImpl.class);
            assert outBean.getObjectField().getClass().equals(IgniteServicesImpl.class);

            assert inBean != outBean;
            assert inBean.equals(outBean);

            ClusterGroup inPrj = services.clusterGroup();
            ClusterGroup outPrj = ((IgniteServices)outBean.getObjectField()).clusterGroup();

            assert inPrj.getClass().equals(outPrj.getClass());
            assert F.eqNotOrdered(inPrj.nodes(), outPrj.nodes());

            outBean.checkNullResources();
        }
    }

    /**
     * @param obj Object field to use.
     * @return New test bean.
     */
    public static GridMarshallerTestBean newTestBean(@Nullable Object obj) {
        GridByteArrayList buf = new GridByteArrayList(1);

        buf.add((byte)321);

        StringBuilder str = new StringBuilder(33 * 1024);

        // 31KB as jboss is failing at 32KB due to a bug.
        for (int i = 0; i < 33 * 1024; i++)
            str.append('A');

        return new GridMarshallerTestBean(obj, str.toString(), 123, buf, Integer.class, String.class);
    }

    /**
     * @param bean Object to marshal.
     * @return Byte buffer.
     * @throws IgniteCheckedException Thrown if any exception occurs while marshalling.
     */
    protected static byte[] marshal(Object bean) throws IgniteCheckedException {
        return marsh.marshal(bean);
    }

    /**
     * @param buf Byte buffer to unmarshal.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException Thrown if any exception occurs while unmarshalling.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    protected static <T> T unmarshal(final byte[] buf) throws IgniteCheckedException {
        RunnableFuture<T> f = new FutureTask<>(new Callable<T>() {
            @Override public T call() throws IgniteCheckedException {
                return marsh.<T>unmarshal(buf, Thread.currentThread().getContextClassLoader());
            }
        });

        // Any deserialization has to be executed under a thread, that contains the Ignite instance name.
        new IgniteThread(igniteInstanceName, "unmarshal-thread", f).start();

        try {
            return f.get();
        }
        catch (Exception e) {
            if (e.getCause() instanceof IgniteCheckedException)
                throw (IgniteCheckedException)e.getCause();

            fail(e.getCause().getMessage());
        }

        return null;
    }

    /**
     * Nested class.
     */
    @SuppressWarnings({"InnerClassMayBeStatic"})
    private class NestedClass implements Serializable {
        // No-op.
    }

    /**
     * Static nested class.
     */
    private static class StaticNestedClass implements Serializable {
        // No-op.
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReadArray() throws Exception {
        byte[] arr = new byte[10];

        for (int i = 0; i < arr.length; i++)
            arr[i] = (byte)i;

        arr[5] = -1;

        ReadArrayTestClass obj = unmarshal(marshal(new ReadArrayTestClass(arr, false)));

        assertTrue(Arrays.equals(arr, obj.arr));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReadFully() throws Exception {
        byte[] arr = new byte[10];

        for (int i = 0; i < arr.length; i++)
            arr[i] = (byte)i;

        arr[5] = -1;

        ReadArrayTestClass obj = unmarshal(marshal(new ReadArrayTestClass(arr, true)));

        assertTrue(Arrays.equals(arr, obj.arr));
    }

    /**
     *
     */
    private static class ReadArrayTestClass implements Externalizable {
        /** */
        private byte[] arr;

        /** */
        private boolean fully;

        /**
         *
         */
        public ReadArrayTestClass() {
            // No-op.
        }

        /**
         * @param arr Array.
         * @param fully Read fully flag.
         */
        private ReadArrayTestClass(byte[] arr, boolean fully) {
            this.arr = arr;
            this.fully = fully;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeBoolean(fully);
            out.writeInt(arr.length);
            out.write(arr);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            fully = in.readBoolean();

            arr = new byte[in.readInt()];

            if (fully)
                in.readFully(arr);
            else
                in.read(arr);
        }
    }
}
